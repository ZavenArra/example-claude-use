-- =============================================================================
-- 00_schema.sql — organization_aggregates: schema, extensions, refresh plumbing
-- =============================================================================
-- Deploy first. Idempotent: safe to re-run.
--
-- WARNING: every column and type referenced by this package is INFERRED from
-- 2-current-query-shape.MD and 3-organization-children-view.sql. There was no
-- schema access when it was written. Diff README.md's inferred-columns table
-- against production before running any of this.
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS organization_aggregates;

COMMENT ON SCHEMA organization_aggregates IS
  'Pre-joined, pre-aggregated org/tree/cluster views serving the map tile server. '
  'Refreshed every 6h by the organization_aggregates_refresh Airflow DAG.';

-- PostGIS is assumed already present (the source tables use geometry columns).
CREATE EXTENSION IF NOT EXISTS postgis;

-- btree_gist lets one GiST index cover (organization_id, zoom_level, centroid),
-- which is exactly the organization_clusters access pattern. If you would rather
-- not add it, see the fallback indexes at the bottom of 30_organization_clusters.sql.
CREATE EXTENSION IF NOT EXISTS btree_gist;


-- -----------------------------------------------------------------------------
-- refresh_log — observability for the DAG
-- -----------------------------------------------------------------------------
-- NOTE ON FAILURES: a refresh that raises aborts its transaction, which rolls
-- back the log row too. So this table records *completed* refreshes. Failures are
-- written by the DAG's record_failure task, which runs on its own connection
-- after the failing task has rolled back.
CREATE TABLE IF NOT EXISTS organization_aggregates.refresh_log (
  id                bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  view_name         text        NOT NULL,
  started_at        timestamptz NOT NULL DEFAULT clock_timestamp(),
  finished_at       timestamptz,
  duration          interval GENERATED ALWAYS AS (finished_at - started_at) STORED,
  used_concurrently boolean,
  row_count         bigint,
  status            text        NOT NULL DEFAULT 'running'
                      CHECK (status IN ('running', 'ok', 'error')),
  error_message     text,
  dag_run_id        text
);

CREATE INDEX IF NOT EXISTS refresh_log_view_started_idx
  ON organization_aggregates.refresh_log (view_name, started_at DESC);


-- -----------------------------------------------------------------------------
-- refresh_view() — the refresh entry point the DAG calls
-- -----------------------------------------------------------------------------
-- Solves the CONCURRENTLY bootstrap problem: REFRESH MATERIALIZED VIEW
-- CONCURRENTLY is illegal against a view that has never been populated, and
-- requires a unique index. All three views here are created WITH NO DATA, so the
-- very first refresh MUST be non-concurrent. This checks pg_class.relispopulated
-- and pg_index and picks the legal form automatically.
--
-- Returns the resulting row count.
CREATE OR REPLACE FUNCTION organization_aggregates.refresh_view(
  p_view              regclass,
  p_concurrently      boolean DEFAULT true,
  p_statement_timeout text    DEFAULT '2h',
  p_lock_timeout      text    DEFAULT '5min',
  p_dag_run_id        text    DEFAULT NULL
) RETURNS bigint
LANGUAGE plpgsql
AS $fn$
DECLARE
  v_log_id        bigint;
  v_populated     boolean;
  v_has_unique    boolean;
  v_concurrently  boolean;
  v_rows          bigint;
BEGIN
  SELECT c.relispopulated INTO v_populated
    FROM pg_class c
   WHERE c.oid = p_view;

  IF v_populated IS NULL THEN
    RAISE EXCEPTION 'organization_aggregates.refresh_view: relation % not found', p_view;
  END IF;

  SELECT EXISTS (
    SELECT 1 FROM pg_index i
     WHERE i.indrelid = p_view AND i.indisunique AND i.indisvalid
  ) INTO v_has_unique;

  v_concurrently := p_concurrently AND v_populated AND v_has_unique;

  IF p_concurrently AND NOT v_concurrently THEN
    RAISE NOTICE 'refresh_view: falling back to a blocking refresh of % (populated=%, unique_index=%)',
      p_view, v_populated, v_has_unique;
  END IF;

  INSERT INTO organization_aggregates.refresh_log (view_name, used_concurrently, dag_run_id)
  VALUES (p_view::text, v_concurrently, p_dag_run_id)
  RETURNING id INTO v_log_id;

  -- Transaction-local: reverted automatically when this call's transaction ends.
  PERFORM set_config('statement_timeout', p_statement_timeout, true);
  PERFORM set_config('lock_timeout',      p_lock_timeout,      true);

  IF v_concurrently THEN
    EXECUTE format('REFRESH MATERIALIZED VIEW CONCURRENTLY %s', p_view::text);
  ELSE
    EXECUTE format('REFRESH MATERIALIZED VIEW %s', p_view::text);
  END IF;

  EXECUTE format('SELECT count(*) FROM %s', p_view::text) INTO v_rows;

  UPDATE organization_aggregates.refresh_log
     SET finished_at = clock_timestamp(),
         status      = 'ok',
         row_count   = v_rows
   WHERE id = v_log_id;

  RETURN v_rows;
END;
$fn$;

COMMENT ON FUNCTION organization_aggregates.refresh_view(regclass, boolean, text, text, text) IS
  'Refresh a matview, choosing CONCURRENTLY only when legal (view populated and a '
  'unique index exists). Logs completed refreshes to organization_aggregates.refresh_log.';
