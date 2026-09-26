# Implementation Plan — `organization_aggregates` (standalone)

**For:** a fresh Claude Code session with no prior context.
**Working directory:** `/Users/zaven/software-development/Greenstand/map/upcycling`
**Read first:** `2-current-query-shape.MD` (the query being replaced) and
`3-organization-children-view.sql` (prior art). `1-case-1-upcycle.MD` is the original request.
`plan-1.md` is the earlier narrative plan including the review of the prior-art view — this file
supersedes it for implementation purposes.

---

## 0. Goal

Replace an expensive live query that feeds a map tile server. The current query does a recursive
org-hierarchy CTE, a tree→org resolution CTE, and two on-demand cluster aggregations per request.
Move all of that into three materialized views in a new `organization_aggregates` schema, refreshed
every 6 hours by Airflow, so the live query is a filtered read plus one self-join.

## 1. Decisions already made — do not re-litigate

| Decision | Value |
|---|---|
| Schema name | `organization_aggregates` |
| DB access | **None.** Every column/type is inferred from the two source files. Flag all inferences. |
| `organization_clusters` grain | **Every ancestor in the hierarchy** (a child org's trees roll up to the child and to every org above it) |
| Airflow | **2.x, classic (non-decorator) operators** |
| `organization_trees` columns | **Keys + geometry only** |
| Refresh cadence | Every 6 hours, `0 */6 * * *` |

## 2. Assumed source schema (INFERRED — flag in README)

Derived from `2-current-query-shape.MD`. Nothing was verified against a live database.

| Table | Columns assumed |
|---|---|
| `entity` | `id`, `name`, `map_name` |
| `entity_relationship` | `child_id`, `parent_id`, `type`, `role` |
| `planter` | `id`, `organization_id` |
| `trees` | `id`, `planter_id`, `planting_organization_id`, `estimated_geometric_location` |
| `active_tree_region` | `id`, `tree_id`, `region_id`, `zoom_level`, `centroid`, `type_id` |
| `region` | `id`, `geom` |

## 3. Current state of the working tree

- `sql/00_schema.sql` — **already written.** Schema, `postgis` + `btree_gist` extensions,
  `refresh_log` table, and the `organization_aggregates.refresh_view()` plpgsql helper. Read it first;
  everything else calls that helper. Do not rewrite it unless you find a bug.
- Everything below still needs writing.

---

## 4. Files to produce

```
sql/00_schema.sql                          DONE
sql/10_organization_hierarchy.sql          view + indexes
sql/20_organization_trees.sql              view + indexes
sql/30_organization_clusters.sql           view + indexes
sql/50_query_clusters_with_zoom_target.sql the live query
sql/90_teardown.sql                        reverse-order drops
sql/99_sizing_checks.sql                   run against prod BEFORE deploying
airflow/dags/organization_aggregates_refresh.py
README.md                                  deploy order, inferred-column table, deviations, verification
COST-AND-IMPACT.md                         see §8
```

Conventions: indexes live inline in each view's file so each is independently deployable. Every view is
created `WITH NO DATA` — DDL deploy is then instant, indexes build on an empty view, and the DAG's first
refresh populates it (`refresh_view()` already handles the non-concurrent first build).

---

## 5. `sql/10_organization_hierarchy.sql`

An **ancestor closure**: one row per `(ancestor_id, organization_id)` pair, including depth-0 self-rows.
`organization_id` is the org the row is about — named to match `organization_trees.organization_id` so
the join is self-evident. Its attribute columns are unprefixed (`name`, `map_name`, `parent_id`), which
also makes the `map_name` / `parent_id` filter columns from the requirement exist literally.

The closure form is what makes the every-ancestor grain possible, and it collapses a requirement: since
every root is an ancestor of itself and of all descendants, **"filter by org id" and "filter by top-level
parent" become the same predicate** on `organization_clusters`.

```sql
CREATE MATERIALIZED VIEW organization_aggregates.organization_hierarchy AS
WITH RECURSIVE
edge AS (
  -- Dedupe, and require the parent to actually exist in entity: a dangling
  -- parent_id would otherwise produce ancestor rows with no attributes.
  SELECT DISTINCT er.child_id, er.parent_id
  FROM entity_relationship er
  JOIN entity pe ON pe.id = er.parent_id
  WHERE er.child_id IS NOT NULL
),
ancestry AS (
  -- Anchor: every org is its own ancestor at depth 0.
  SELECT e.id AS organization_id, e.id AS ancestor_id, 0 AS depth, ARRAY[e.id] AS path
  FROM entity e
  UNION ALL
  -- Walk upward. The path guard is the cycle protection: plain UNION does not
  -- stop a cycle here, because each loop produces a new depth and so a new row.
  SELECT a.organization_id, edge.parent_id, a.depth + 1, a.path || edge.parent_id
  FROM ancestry a
  JOIN edge ON edge.child_id = a.ancestor_id
  WHERE NOT (edge.parent_id = ANY (a.path))
),
pair AS (
  -- A multi-parent DAG can reach the same ancestor by two paths; keep the
  -- shortest. This is also what makes the UNIQUE index below possible.
  SELECT organization_id, ancestor_id, min(depth) AS depth
  FROM ancestry
  GROUP BY organization_id, ancestor_id
),
root_org AS (
  SELECT e.id FROM entity e
  WHERE NOT EXISTS (SELECT 1 FROM edge WHERE edge.child_id = e.id)
),
root AS (
  -- Deepest parentless ancestor = top-level parent. Deterministic tie-break for
  -- multi-parent orgs, which can legitimately have more than one root.
  SELECT DISTINCT ON (p.organization_id)
         p.organization_id, p.ancestor_id AS root_id
  FROM pair p
  JOIN root_org ro ON ro.id = p.ancestor_id
  ORDER BY p.organization_id, p.depth DESC, p.ancestor_id
),
immediate AS (
  SELECT DISTINCT ON (er.child_id)
         er.child_id, er.parent_id, er.type, er.role
  FROM entity_relationship er
  JOIN entity pe ON pe.id = er.parent_id
  ORDER BY er.child_id, er.parent_id
)
SELECT
  p.ancestor_id,
  ae.map_name                  AS ancestor_map_name,
  ae.name                      AS ancestor_name,
  p.organization_id,
  oe.map_name                  AS map_name,
  oe.name                      AS name,
  p.depth,
  im.parent_id                 AS parent_id,
  r.root_id,
  re.map_name                  AS root_map_name,
  (aro.id IS NOT NULL)         AS is_root_ancestor,
  im.type                      AS rel_type,
  im.role                      AS rel_role
FROM pair p
JOIN entity oe        ON oe.id  = p.organization_id
JOIN entity ae        ON ae.id  = p.ancestor_id
LEFT JOIN immediate im ON im.child_id = p.organization_id
LEFT JOIN root r       ON r.organization_id = p.organization_id
LEFT JOIN entity re    ON re.id = r.root_id
LEFT JOIN root_org aro ON aro.id = p.ancestor_id
WITH NO DATA;
```

Indexes:

```sql
CREATE UNIQUE INDEX organization_hierarchy_uk
  ON organization_aggregates.organization_hierarchy (ancestor_id, organization_id);
CREATE INDEX organization_hierarchy_org_depth_idx
  ON organization_aggregates.organization_hierarchy (organization_id, depth);
CREATE INDEX organization_hierarchy_self_idx
  ON organization_aggregates.organization_hierarchy (organization_id) WHERE depth = 0;
CREATE INDEX organization_hierarchy_map_name_idx
  ON organization_aggregates.organization_hierarchy (map_name) WHERE map_name IS NOT NULL;
CREATE INDEX organization_hierarchy_parent_idx
  ON organization_aggregates.organization_hierarchy (parent_id) WHERE parent_id IS NOT NULL;
CREATE INDEX organization_hierarchy_root_idx
  ON organization_aggregates.organization_hierarchy (root_id);
```

**Document this footgun in the README:** because this is a closure, each org appears once per ancestor
and its attribute columns repeat across those rows. Any query filtering on `map_name`, `name` or
`parent_id` that wants one row per org must add `depth = 0` (equivalently `ancestor_id = organization_id`).

## 6. `sql/20_organization_trees.sql`

One row per tree. Resolves the owning org with `planting_organization_id` taking priority over the
planter's org, exactly as the source CTE does. Pulls `map_name` / root from the hierarchy's depth-0 rows,
so MV (1) must be refreshed first.

```sql
CREATE MATERIALIZED VIEW organization_aggregates.organization_trees AS
SELECT
  t.id                                                        AS tree_id,
  t.planter_id,
  t.planting_organization_id                                  AS tree_planting_organization_id,
  p.organization_id                                           AS planter_organization_id,
  COALESCE(t.planting_organization_id, p.organization_id)     AS organization_id,
  CASE WHEN t.planting_organization_id IS NOT NULL
       THEN 'tree' ELSE 'planter' END                         AS organization_source,
  h.map_name,
  h.root_id                                                   AS top_level_parent_id,
  h.root_map_name,
  t.estimated_geometric_location
FROM trees t
LEFT JOIN planter p ON p.id = t.planter_id            -- a tree may have no planter
JOIN organization_aggregates.organization_hierarchy h
       ON h.organization_id = COALESCE(t.planting_organization_id, p.organization_id)
      AND h.depth = 0                                  -- exactly one attribute row per org
WITH NO DATA;
```

The inner join deliberately **drops trees with no resolvable organization** — they are unreachable by any
org filter. Call this out in the README as a behaviour decision to confirm.

`lat` / `lon` are omitted as derivable via `ST_X` / `ST_Y` (keys + geometry only).

Indexes: `UNIQUE (tree_id)`; btree on `organization_id`, `top_level_parent_id`, `map_name`;
GiST on `estimated_geometric_location`.

## 7. `sql/30_organization_clusters.sql`

Grain `(organization_id, zoom_level, region_id)` where `organization_id` is **any ancestor**. Exposes
every column the current query's inner tile SELECT exposes.

```sql
CREATE MATERIALIZED VIEW organization_aggregates.organization_clusters AS
SELECT
  'cluster'::text                       AS type,
  h.ancestor_id                         AS organization_id,
  h.is_root_ancestor                    AS is_root_org,
  atr.zoom_level,
  atr.region_id                         AS id,
  atr.type_id                           AS region_type,
  atr.centroid,                                        -- raw: envelope filter + containment match
  st_point(LEAST(st_x(atr.centroid), 170), st_y(atr.centroid))                AS estimated_geometric_location,
  st_asgeojson(st_point(LEAST(st_x(atr.centroid), 170), st_y(atr.centroid)))  AS latlon,
  count(atr.id)                         AS count,
  CASE WHEN count(atr.id) > 1000
       THEN (count(atr.id) / 1000) || 'K'
       ELSE count(atr.id) || ''
  END                                   AS count_text
FROM active_tree_region atr
JOIN organization_aggregates.organization_trees ot     ON ot.tree_id = atr.tree_id
JOIN organization_aggregates.organization_hierarchy h  ON h.organization_id = ot.organization_id
GROUP BY h.ancestor_id, h.is_root_ancestor, atr.zoom_level, atr.region_id, atr.type_id, atr.centroid
WITH NO DATA;
```

Two deliberate decisions to preserve and document:

- **No hardcoded zoom list** — it groups by whatever `zoom_level` values exist, so an unexpected zoom
  range cannot silently truncate output.
- **`region.geom` is not stored.** The zoom-target join needs the low-zoom region's polygon for
  `ST_Contains`; duplicating polygons across every org × zoom level is the largest storage risk in this
  design. The live query does an indexed PK lookup on `region` instead. Note the store-it-anyway variant
  in the README.
- `st_point()` returns SRID 0, matching the current query's behaviour exactly. Do not "fix" this —
  changing it changes output. Note it in the README as a fidelity choice.

Indexes:

```sql
CREATE UNIQUE INDEX organization_clusters_uk
  ON organization_aggregates.organization_clusters (organization_id, zoom_level, id);
CREATE INDEX organization_clusters_lookup_gix
  ON organization_aggregates.organization_clusters
  USING gist (organization_id, zoom_level, centroid);   -- needs btree_gist
```

Fallback if `btree_gist` is unwanted — include it commented out with an explanation:

```sql
-- CREATE INDEX ... (organization_id, zoom_level);
-- CREATE INDEX ... USING gist (centroid);
```

## 8. `sql/50_query_clusters_with_zoom_target.sql`

Parameterized `$1..$6`. `$1` is an organization id — pass a root id to get the top-level-parent rollup;
same column, no separate query.

```sql
SELECT
  'cluster'                     AS type,
  'case1 with zoom target tile' AS log,
  c.id,
  c.estimated_geometric_location,
  c.latlon,
  c.region_type,
  c.count,
  c.count_text,
  zoom_target.centroid          AS zoom_to
FROM organization_aggregates.organization_clusters c
LEFT JOIN region r ON r.id = c.id
LEFT JOIN LATERAL (
  SELECT st_asgeojson(z.centroid) AS centroid
  FROM organization_aggregates.organization_clusters z
  WHERE z.organization_id = c.organization_id
    AND z.zoom_level      = c.zoom_level + 2
    AND z.centroid && r.geom
    AND ST_Contains(r.geom, z.centroid)
    -- Fidelity: the current query also constrains the zoom-target set to the
    -- request envelope. Keeping it preserves existing output exactly. Dropping
    -- it is arguably more correct (a subregion centroid can sit inside the
    -- parent region but outside the envelope, and is currently missed).
    AND z.centroid && ST_MakeEnvelope($3, $4, $5, $6, 4326)
  ORDER BY z.count DESC, z.id      -- highest count wins; z.id makes ties deterministic
  LIMIT 1
) zoom_target ON true
WHERE c.organization_id = $1
  AND c.zoom_level      = $2
  AND c.centroid && ST_MakeEnvelope($3, $4, $5, $6, 4326);
```

`LEFT JOIN LATERAL ... LIMIT 1` replaces the original's `DISTINCT ON (region.id) ... ORDER BY total DESC`.
Same semantics, plus a deterministic tie-break, and it stops after the first match per cluster instead of
sorting the whole joined set. Document this as a deviation.

## 9. `airflow/dags/organization_aggregates_refresh.py`

Airflow 2.x, classic operators, `SQLExecuteQueryOperator` from
`airflow.providers.common.sql.operators.sql`. Note in a comment that `PostgresOperator` was removed in
postgres provider 6.0 and give the one-line swap for older pins.

- `dag_id='organization_aggregates_refresh'`, `schedule_interval='0 */6 * * *'`, `catchup=False`,
  `max_active_runs=1`, `start_date` a fixed past date, retries with exponential backoff.
- Connection id in a module-level constant (`POSTGRES_CONN_ID`) with a comment that it must be set.
- One task per view calling `SELECT organization_aggregates.refresh_view('<schema.view>'::regclass, true,
  '2h', '5min', '{{ run_id }}')`, each followed by an `ANALYZE` task.
- Strictly linear: `hierarchy → analyze → trees → analyze → clusters → analyze`. The order is a real data
  dependency, not a preference — MV 2 reads MV 1, MV 3 reads MV 1 and MV 2.
- A `record_failure` task with `trigger_rule=TriggerRule.ONE_FAILED` that inserts an `error` row into
  `refresh_log`. This exists because a failed refresh rolls back its own log row (explained in
  `00_schema.sql`); the failure row must be written on a separate connection after the rollback.

## 10. `sql/99_sizing_checks.sql`

Read-only queries to run against production **before** deploying, since the every-ancestor grain
multiplies rows by average hierarchy depth and nobody has measured it yet:

- org count, edge count, max/avg depth, count of multi-parent orgs, count of cycles detected
- projected `organization_hierarchy` row count (the closure size)
- projected `organization_clusters` row count: distinct `(ancestor, zoom_level, region_id)`
- trees with no resolvable org (how many rows §6 drops)
- `pg_size_pretty` projections

## 11. `README.md`

Must contain: deploy order; the full inferred-column table from §2 with a prominent warning that nothing
was verified against a live DB; every deviation and behaviour decision flagged above (closure shape and
the `depth = 0` footgun, dropped unresolvable-org trees, `region.geom` not stored, SRID 0 preserved,
LATERAL vs DISTINCT ON, envelope filter on the zoom target); Airflow setup (connection id, provider
version); how to verify; and how to roll back (`90_teardown.sql`).

## 12. Verification available without a database

State plainly in the final report that no query was executed. Do:

- `python3 -c "import ast; ast.parse(open('airflow/dags/organization_aggregates_refresh.py').read())"`
- If `pg_format` or `psql` happens to be installed, a syntax-only parse; otherwise say so rather than
  implying validation happened.
- Check every column referenced exists in the §2 inferred table, and that view dependency order matches
  the DAG task order.

Do **not** claim the SQL runs. It has never been executed.

## 13. `COST-AND-IMPACT.md`

Per `~/.claude/CLAUDE.md` → *Scoping cost and impact to one task*: report the **implementation segment**,
measured by difference, not the session total.

- Baseline for this work is already saved at `.baseline/pre-implementation.json`.
- On completion, re-read `~/.claude/cost-report.sh --json` (`costUSD`) and the session transcript's
  cumulative `output_tokens`, and report `after - before`.
- **Deduplicate transcript entries by `.message.id` before summing** — the raw transcript repeats some
  assistant messages, so a naive sum overstates output tokens by roughly 2x.
- Feed the **token delta** to `https://api.ecologits.ai/v1beta/estimations` with
  `{"provider":"anthropic","model_name":"claude-opus-4-8","output_token_count":<delta>,
  "electricity_mix_zone":"USA"}` and report midpoints of gwp / wcf / energy, matching
  `~/.claude/ecologits.config.sh`.
- Add a short qualitative note on expected query-side compute savings.
