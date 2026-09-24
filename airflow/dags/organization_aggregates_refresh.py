"""Refresh the organization_aggregates materialized views every 6 hours.

Airflow 2.x, classic (non-decorator) operators.

The three views form a strict data dependency chain, so the tasks are linear and
NOT parallel:

    organization_hierarchy   (reads source tables only)
      -> organization_trees  (reads organization_hierarchy at depth 0)
        -> organization_clusters (reads organization_trees AND organization_hierarchy)

Refreshing them out of order does not error -- it silently produces a view built
from the previous run's data. Do not reorder these tasks.

Each refresh is followed by an ANALYZE. A freshly refreshed matview has no
statistics of its own, and the live query's plan (see
sql/50_query_clusters_with_zoom_target.sql) depends on the planner choosing the
GiST index; without ANALYZE it can pick a sequential scan for hours.

DEPLOY: copy into $AIRFLOW_HOME/dags/. The SQL in sql/00..30 must already be
deployed -- this DAG only refreshes, it never creates anything.
"""

from __future__ import annotations

import pendulum
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.utils.trigger_rule import TriggerRule

# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #

# MUST BE SET before this DAG will run: create an Airflow Postgres connection
# with this id (Admin -> Connections, or `airflow connections add`). The role it
# uses needs OWNER on the three materialized views -- REFRESH MATERIALIZED VIEW
# requires ownership, not just SELECT/INSERT -- plus INSERT on
# organization_aggregates.refresh_log.
POSTGRES_CONN_ID = "greenstand_postgres"

# Per-view statement timeout handed to refresh_view(). The hierarchy closure and
# the cluster aggregation are the long ones; size these from the timings that
# sql/99_sizing_checks.sql produces against production.
STATEMENT_TIMEOUT = "2h"
LOCK_TIMEOUT = "5min"

# CONCURRENTLY keeps the views readable by the tile server during a refresh, at
# the cost of roughly double the work and temporary disk. refresh_view() falls
# back to a blocking refresh automatically on the very first run (the views are
# created WITH NO DATA and CONCURRENTLY is illegal against an unpopulated view).
REFRESH_CONCURRENTLY = True

# In dependency order. Do not reorder.
VIEWS = [
    "organization_hierarchy",
    "organization_trees",
    "organization_clusters",
]

SCHEMA = "organization_aggregates"

# NOTE ON THE OPERATOR: SQLExecuteQueryOperator is the current, provider-agnostic
# operator. PostgresOperator was deprecated in apache-airflow-providers-postgres
# 5.x and REMOVED in 6.0. If you are pinned below 5.x, the swap is one line:
#     from airflow.providers.postgres.operators.postgres import PostgresOperator
# and replace SQLExecuteQueryOperator(conn_id=...) with
# PostgresOperator(postgres_conn_id=...); every other argument is the same.

default_args = {
    "owner": "greenstand-map",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": pendulum.duration(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": pendulum.duration(minutes=30),
}

with DAG(
    dag_id="organization_aggregates_refresh",
    description="Refresh organization_aggregates materialized views (map tile server)",
    default_args=default_args,
    # 00:00, 06:00, 12:00, 18:00 in the DAG's timezone.
    schedule_interval="0 */6 * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    # A 6-hourly full refresh has nothing to backfill: only the latest state
    # matters, and each run fully replaces the previous output.
    catchup=False,
    # A refresh can outlive its 6h window. Overlapping runs would fight over the
    # same views and, with CONCURRENTLY, double the temporary disk.
    max_active_runs=1,
    dagrun_timeout=pendulum.duration(hours=5, minutes=30),
    tags=["organization_aggregates", "materialized-view", "map"],
) as dag:

    start = EmptyOperator(task_id="start")

    previous = start
    for view in VIEWS:
        refresh = SQLExecuteQueryOperator(
            task_id=f"refresh_{view}",
            conn_id=POSTGRES_CONN_ID,
            # refresh_view() logs to organization_aggregates.refresh_log, picks
            # CONCURRENTLY only when legal, and returns the resulting row count.
            sql=(
                f"SELECT {SCHEMA}.refresh_view("
                f"  '{SCHEMA}.{view}'::regclass,"
                f"  {str(REFRESH_CONCURRENTLY).lower()},"
                f"  '{STATEMENT_TIMEOUT}',"
                f"  '{LOCK_TIMEOUT}',"
                "  '{{ run_id }}'"
                ");"
            ),
            # The row count lands in XCom, so a run's output sizes are visible in
            # the UI without querying refresh_log.
            do_xcom_push=True,
        )

        # Fresh matview == no statistics. Without this the planner can choose a
        # sequential scan over the GiST index on the live query.
        analyze = SQLExecuteQueryOperator(
            task_id=f"analyze_{view}",
            conn_id=POSTGRES_CONN_ID,
            sql=f"ANALYZE {SCHEMA}.{view};",
            # ANALYZE cannot run inside a transaction block in some setups and is
            # cheap to redo, so let it manage its own.
            autocommit=True,
        )

        previous >> refresh >> analyze
        previous = analyze

    done = EmptyOperator(task_id="done")
    previous >> done

    # ----------------------------------------------------------------------- #
    # Failure bookkeeping
    # ----------------------------------------------------------------------- #
    # A refresh that raises aborts its own transaction, which rolls back the
    # 'running' row refresh_view() inserted -- so a failed refresh leaves NO
    # trace in refresh_log. This task runs on a separate connection after that
    # rollback and writes the error row.
    #
    # ONE_FAILED fires as soon as any upstream task fails, so it is wired to
    # every refresh/analyze task, not just the last one.
    record_failure = SQLExecuteQueryOperator(
        task_id="record_failure",
        conn_id=POSTGRES_CONN_ID,
        trigger_rule=TriggerRule.ONE_FAILED,
        sql=(
            f"INSERT INTO {SCHEMA}.refresh_log "
            "  (view_name, finished_at, status, error_message, dag_run_id) "
            f"VALUES ('{SCHEMA} (dag run)', clock_timestamp(), 'error', "
            "        'One or more refresh tasks failed; see Airflow task logs for "
            "dag_run_id ' || '{{ run_id }}', '{{ run_id }}');"
        ),
        autocommit=True,
    )

    for task in dag.tasks:
        if task.task_id.startswith(("refresh_", "analyze_")):
            task >> record_failure
