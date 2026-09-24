# `organization_aggregates`

Pre-joined, pre-aggregated Postgres views that replace the live map-tile query in
`2-current-query-shape.MD`. Refreshed every 6 hours by Airflow. The tile server's
query becomes a filtered read of one view plus one self-join — no recursive CTE,
no on-demand aggregation.

> ## ⚠️ Nothing here has been run
>
> These files were written **without any access to the database**. Every table and
> column name is **inferred** from `2-current-query-shape.MD` and
> `3-organization-children-view.sql`. No statement in this package has ever been
> executed, and no query plan has been observed. Diff the [inferred schema](#inferred-source-schema)
> against production and run [`sql/99_sizing_checks.sql`](sql/99_sizing_checks.sql)
> **before** deploying anything.

---

## What replaces what

| Current query does | Now |
|---|---|
| `organization_children` recursive CTE, per request | `organization_hierarchy` (MV, 6-hourly) |
| `org_tree_id` CTE resolving planter/planting org, per request | `organization_trees` (MV, 6-hourly) |
| `case1 tile` GROUP BY over `active_tree_region`, per request | `organization_clusters` (MV, 6-hourly) |
| `contained` GROUP BY at the deeper zoom, per request | the same `organization_clusters` rows |
| `DISTINCT ON (region.id) ... ORDER BY total DESC` | `LEFT JOIN LATERAL ... LIMIT 1` |

`organization_clusters` is keyed on **any ancestor** org, which collapses a
requirement: *filter by org id* and *filter by top-level parent id* are the same
predicate on the same column. Pass a child id for that org's rollup, a root id for
the whole-tree rollup.

## Deploy order

```bash
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f sql/99_sizing_checks.sql   # READ-ONLY, run FIRST
# stop and read the numbers before continuing

psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f sql/00_schema.sql
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f sql/10_organization_hierarchy.sql
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f sql/20_organization_trees.sql
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f sql/30_organization_clusters.sql
```

The order is a hard dependency, not a convention: MV 2 reads MV 1, MV 3 reads
MV 1 and MV 2.

Every view is created `WITH NO DATA`, so all four files run in milliseconds —
indexes build against empty relations and nothing is populated yet. The **first
refresh** does all the work. Trigger it by hand, or let the DAG do it:

```sql
SELECT organization_aggregates.refresh_view('organization_aggregates.organization_hierarchy'::regclass);
ANALYZE organization_aggregates.organization_hierarchy;
SELECT organization_aggregates.refresh_view('organization_aggregates.organization_trees'::regclass);
ANALYZE organization_aggregates.organization_trees;
SELECT organization_aggregates.refresh_view('organization_aggregates.organization_clusters'::regclass);
ANALYZE organization_aggregates.organization_clusters;
```

`refresh_view()` detects that the views are unpopulated and uses a blocking
refresh for that first run; `CONCURRENTLY` is illegal against a view that has
never held data. Later runs are concurrent automatically.

`sql/50_query_clusters_with_zoom_target.sql` is **not deployed** — it is the
statement the tile server sends.

## Inferred source schema

Read only. Nothing in this package writes to any of these.

| Table | Columns assumed | Notes |
|---|---|---|
| `entity` | `id`, `name`, `map_name` | orgs; `map_name` is the public filter key |
| `entity_relationship` | `child_id`, `parent_id`, `type`, `role` | `type`/`role` carried through, never filtered on |
| `planter` | `id`, `organization_id` | |
| `trees` | `id`, `planter_id`, `planting_organization_id`, `estimated_geometric_location` | |
| `active_tree_region` | `id`, `tree_id`, `region_id`, `zoom_level`, `centroid`, `type_id` | |
| `region` | `id`, `geom` | `id` assumed to be the primary key |

**Types are inferred too.** `entity.id` is assumed to be a single scalar column
comparable to `entity_relationship.parent_id`, `trees.planting_organization_id` and
`planter.organization_id` — the `ARRAY[e.id]` cycle-guard in MV 1 requires it to be
a type with an array equality operator (any scalar; uuid, int and bigint all work).
`zoom_level` is assumed integer, since `c.zoom_level + 2` is arithmetic.

Anything in this table that turns out to be wrong will surface as a plain
`column ... does not exist` at deploy time, not as bad data.

## Deviations and behaviour decisions

Each of these changes something relative to the current query, or is a judgement
call worth a second opinion. Confirm them before deploying.

### 1. The closure shape, and the `depth = 0` footgun
`organization_hierarchy` is a transitive **ancestor closure**: one row per
`(ancestor_id, organization_id)` pair, including the depth-0 self-row. An org's own
attributes (`name`, `map_name`, `parent_id`, `root_id`) therefore **repeat once per
ancestor**.

Any query that wants one row per org must add `depth = 0`:

```sql
-- correct: one row
SELECT organization_id FROM organization_aggregates.organization_hierarchy
 WHERE map_name = 'leadfoundation' AND depth = 0;

-- wrong: one row per ancestor of that org
SELECT organization_id FROM organization_aggregates.organization_hierarchy
 WHERE map_name = 'leadfoundation';
```

`depth = 0` is equivalent to `ancestor_id = organization_id` and is backed by a
partial index.

### 2. The original `LIMIT 20` is gone
The `org_tree_id` CTE contained `SELECT id AS entity_id FROM organization_children
LIMIT 20`. With no `ORDER BY`, that silently dropped trees for any org whose
hierarchy exceeded 20 members, non-deterministically. It is treated as a bug and
not reproduced. **This means tile counts for large hierarchies will go up.** If
current numbers are baked into anyone's expectations, that is the reason.

### 3. Trees with no resolvable org are dropped
`organization_trees` inner-joins the hierarchy, so a tree with no
`planting_organization_id` **and** no planter (or a planter with a null
`organization_id`), or whose org id is absent from `entity`, produces no row. Such
trees cannot be reached by any org filter, so tile output is unchanged — but the
view is not a complete list of trees. Check 5 in `99_sizing_checks.sql` counts them.

### 4. `region.geom` is not stored in `organization_clusters`
The zoom-target join needs the low-zoom region's polygon for `ST_Contains`.
Duplicating polygons across every (org × zoom × region) row is the largest storage
risk in this design, so the live query does an indexed PK lookup on `region`
instead.

If that probe measures as the bottleneck, the store-it-anyway variant is: add
`r.geom AS region_geom` to MV 3 (join `region r ON r.id = atr.region_id`, and add
`r.geom` to the `GROUP BY`), add a GiST index on it, and drop the
`LEFT JOIN region r` from the live query. Measure the size increase from check 8
first — for a deep hierarchy it can dominate the whole view.

### 5. SRID 0 is preserved deliberately
`st_point(...)` returns SRID 0, and the current query is equally unprojected, so
`estimated_geometric_location` and `latlon` are byte-identical to today's output.
Adding `ST_SetSRID(..., 4326)` would change what the tile server receives. The
spatial predicates use `centroid`, which keeps its original SRID, so the missing
SRID affects only the output payload.

`count_text` likewise inherits integer division verbatim: 1500 renders as `1K`.

### 6. `LEFT JOIN LATERAL ... LIMIT 1` instead of `DISTINCT ON`
Same semantics per cluster. The original sorted a full cross of low-zoom regions ×
high-zoom clusters and threw away all but the first row per region; the lateral
stops at the first match. It also adds `ORDER BY z.count DESC, z.id` — the
original's tie-break was arbitrary, so **today's output for count-tied subregions
is not reproducible run to run**, and this version's is.

### 7. The zoom-target envelope filter is kept
The current query constrains the zoom-target set to the request envelope, which
means a subregion whose centroid sits inside the parent region but outside the
envelope is missed. Keeping the filter preserves existing output exactly; dropping
it is arguably more correct. It is one line in
`sql/50_query_clusters_with_zoom_target.sql`, marked `FIDELITY:`.

### 8. No hardcoded zoom levels
The source hardcoded `zoom_level = 2` and `= 4`. The views group by whatever zoom
levels exist and the live query takes zoom as `$2` with the target at
`$2 + 2`, so an unexpected zoom range cannot silently truncate output. Check 7 in
`99_sizing_checks.sql` lists what is actually present.

### 9. Multi-parent orgs get one deterministic root
If `entity_relationship` is a DAG rather than a tree, an org can have several
root ancestors. `root_id` picks the deepest, tie-broken by id, so it is stable
across refreshes; `parent_id` similarly keeps the lowest immediate parent. The
**full** set is still in the closure (all roots appear as `ancestor_id` rows with
`is_root_ancestor = true`), so rollups are unaffected — only the scalar
`root_id`/`parent_id` convenience columns are narrowed. Check 2 says whether this
case exists at all.

### 10. Cycles are survived, not rejected
The recursive walk carries a `path` array and refuses to revisit a node. A cycle in
`entity_relationship` yields a truncated ancestor set for the orgs involved rather
than an infinite loop. Check 3 (`cycle_edges_skipped`) reports whether any exist.

## Airflow

`airflow/dags/organization_aggregates_refresh.py` — Airflow 2.x, classic operators.

- **Set `POSTGRES_CONN_ID`** (module-level constant, currently
  `greenstand_postgres`). The connection's role needs **OWNER** on the three
  materialized views — `REFRESH MATERIALIZED VIEW` requires ownership, not
  `SELECT` — plus `INSERT` on `organization_aggregates.refresh_log`.
- Schedule `0 */6 * * *`, `catchup=False`, `max_active_runs=1`.
- Tasks are strictly linear: `refresh → analyze` per view, in dependency order.
  The `ANALYZE` steps matter: a freshly refreshed matview has no statistics, and
  without them the planner can pick a sequential scan over the GiST index.
- `record_failure` (`trigger_rule=ONE_FAILED`) writes the error row. It exists
  because a failed refresh **rolls back its own log row** — see the note in
  `sql/00_schema.sql` — so failures must be recorded on a separate connection.
- Uses `SQLExecuteQueryOperator` from `apache-airflow-providers-common-sql`.
  `PostgresOperator` was removed in postgres provider 6.0; the one-line swap for
  older pins is commented in the DAG.

## How to verify

Ordered cheapest-first. Steps 1–2 need no deploy.

1. **Sizing.** `sql/99_sizing_checks.sql`, read-only. Check 6
   (`projected_cluster_rows` vs `distinct_cells`) is the number that decides whether
   the every-ancestor grain is affordable. Check 6 does the same work as the MV 3
   refresh, so its runtime is your refresh-time estimate — use it to size
   `STATEMENT_TIMEOUT` in the DAG.
2. **Schema diff.** Compare the [inferred table](#inferred-source-schema) against
   `information_schema.columns` for the six source tables.
3. **Deploy + first refresh**, then check 9 for actual sizes, and:
   ```sql
   SELECT view_name, duration, row_count, used_concurrently, status
   FROM organization_aggregates.refresh_log ORDER BY started_at DESC LIMIT 10;
   ```
4. **Output equivalence.** Run the old query from `2-current-query-shape.MD` and the
   new one from `sql/50_query_clusters_with_zoom_target.sql` with matching
   parameters (`map_name = 'leadfoundation'`, zoom 2, envelope
   `54.84375, 47.989921667414194, 180, 85.05112874735957`) and diff the rows.
   Expect differences only where deviations 2, 6 and 9 apply. Deviation 2 (the
   dropped `LIMIT 20`) will show up as **higher counts**, which is the fix, not a
   regression.
5. **Plan check.** `EXPLAIN (ANALYZE, BUFFERS)` on the new query. The expected plan
   is written at the bottom of `sql/50_query_clusters_with_zoom_target.sql`. A
   `Recursive Union`, `CTE Scan`, or `HashAggregate` over `active_tree_region` means
   a view is unpopulated or an index is missing.

## Rollback

```bash
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f sql/90_teardown.sql
```

Reverse-order drops, all `IF EXISTS`, so it is safe against a partial deploy. It
reads no source data and deletes none. `refresh_log` and the `postgis` /
`btree_gist` extensions are kept by default — uncomment the relevant lines to
remove them. The final `DROP SCHEMA` is `RESTRICT`, not `CASCADE`, on purpose: if
anything unexpected is living in the schema it should fail loudly.

Point the tile server back at the old query and the views become inert — nothing
else reads them.

## Files

| File | |
|---|---|
| `sql/00_schema.sql` | schema, extensions, `refresh_log`, `refresh_view()` |
| `sql/10_organization_hierarchy.sql` | MV 1 — ancestor closure + indexes |
| `sql/20_organization_trees.sql` | MV 2 — tree → org + indexes |
| `sql/30_organization_clusters.sql` | MV 3 — cluster aggregates + indexes |
| `sql/50_query_clusters_with_zoom_target.sql` | the live query (`$1..$6`), not deployed |
| `sql/90_teardown.sql` | rollback |
| `sql/99_sizing_checks.sql` | read-only pre-deploy measurements |
| `airflow/dags/organization_aggregates_refresh.py` | 6-hourly refresh DAG |
| `COST-AND-IMPACT.md` | measured cost of producing this |
