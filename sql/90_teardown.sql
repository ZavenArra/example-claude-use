-- =============================================================================
-- 90_teardown.sql — full rollback, reverse deploy order
-- =============================================================================
-- Drops everything 00..30 created. Indexes go with their materialized views, so
-- they are not listed separately.
--
-- Nothing here is destructive to SOURCE data: entity, entity_relationship,
-- planter, trees, active_tree_region and region are only ever read.
--
-- Order matters -- MV 3 depends on MVs 1 and 2, MV 2 on MV 1 -- so drop in
-- reverse. IF EXISTS everywhere makes this safe to run against a partial deploy.
-- =============================================================================

DROP MATERIALIZED VIEW IF EXISTS organization_aggregates.organization_clusters;
DROP MATERIALIZED VIEW IF EXISTS organization_aggregates.organization_trees;
DROP MATERIALIZED VIEW IF EXISTS organization_aggregates.organization_hierarchy;

DROP FUNCTION IF EXISTS organization_aggregates.refresh_view(regclass, boolean, text, text, text);

-- refresh_log holds the refresh history. Keep it if you are redeploying and want
-- the timings; uncomment to remove it completely.
-- DROP TABLE IF EXISTS organization_aggregates.refresh_log;

-- Only succeeds once the objects above are gone (and refresh_log is dropped).
-- Deliberately NOT `CASCADE`: if something unexpected still lives in the schema,
-- this should fail loudly rather than delete it.
DROP SCHEMA IF EXISTS organization_aggregates RESTRICT;

-- The extensions are intentionally left in place. postgis is certainly used by
-- the source tables. btree_gist may be used elsewhere; drop it only if you
-- confirmed 00_schema.sql introduced it:
--   DROP EXTENSION IF EXISTS btree_gist;
