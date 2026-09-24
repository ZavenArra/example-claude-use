# Plan 1 — `organization_aggregates` schema

**Status:** reported, awaiting approval to implement
**Date:** 2026-09-24
**Source prompt:** `1-case-1-upcycle.MD`

## Decisions confirmed with the user

| Question | Answer |
|---|---|
| Live schema access to verify tables/columns | **No access** — infer from `2-current-query-shape.MD` and `3-organization-children-view.sql`, flag every inference |
| `organization_clusters` grain | **Every ancestor in the hierarchy** |
| Airflow target | **Airflow 2.x, classic (non-decorator) operators** |
| `organization_trees` column set | **Keys + geometry only** |

Environmental criteria: `~/.claude/ecologits.config.sh` — metrics `gwp` / `wcf` / `energy`, zone `USA`, model `auto`.

---

## Review of `3-organization-children-view.sql`

Six issues, all fixed in the new version:

1. **Anchor predicate is ambiguous.** `WHERE parent_id IS NULL` is unqualified. If `entity` has its own
   `parent_id` column this silently binds to the wrong table. Must be `entity_relationship.parent_id IS NULL`.
2. **`parent_map_name` doesn't do what the comment claims.** The header says "top level parent map name",
   but the recursive term sets `c.map_name` — the *immediate* parent's map_name, which at depth 3+ is a
   mid-tier org, not the root.
3. **`top_level_parent_id` is claimed in the comment but absent from the SELECT list.** MV (2) and (3) both
   depend on it, so the root must be carried explicitly through the recursion.
4. **No cycle protection.** `UNION` dedupes identical rows, but a cycle A→B→A produces rows with
   ever-increasing `depth`, which are never identical — so it loops until it exhausts memory.
   Fix: carry a `path bigint[]` and exclude `parent_id = ANY(path)`.
5. **Multi-parent orgs are unhandled.** If `entity_relationship` allows two parents for one child, "the"
   top-level parent isn't unique and rows silently duplicate. Fix: one row per (ancestor, organization) at
   `min(depth)` — which also makes a `UNIQUE` index possible, required for `REFRESH CONCURRENTLY`.
6. **`ORDER BY name` is dead weight in a matview.** Costs a sort on every refresh, buys nothing. Drop it;
   use `CLUSTER` on the access index if physical ordering matters.

Also: orgs whose parent chain is broken (parent row exists but the parent isn't in `entity`) never appear
at all under a roots-only anchor. Depth-0 self-rows fix the related problem of an org not being its own
ancestor.

---

## MV (1) `organization_hierarchy`

### Shape change, and why

The every-ancestor grain chosen for MV (3) requires an ancestor→descendant closure, so
`organization_hierarchy` is built **as the closure** — one row per `(ancestor_id, organization_id)`
including depth-0 self-rows — rather than one row per org. This avoids adding a fourth matview.

It still satisfies the stated filters: the view exposes `map_name` and `parent_id` directly, and
`ancestor_id` gives subtree filtering (the whole subtree, not just direct children).

It also collapses a requirement: because every root is an ancestor of itself and of all its descendants,
**"filter by top-level parent" and "filter by organization id" become the same predicate** on MV (3) —
one indexed `organization_id` column, no separate root column and no second index.

### Why the closure stores both `ancestor_id` and `organization_id`

The descendant column is named **`organization_id`** (not `descendant_id`): it holds the id of the org the
row is *about*, and it is the column that `organization_trees.organization_id` joins to, so matching names
across the two views makes the join self-evident. Its sibling attribute columns drop the prefix to match —
`name`, `map_name`, `parent_id` — which also means the view literally exposes the `map_name` and
`parent_id` filter columns named in the source requirement.

The two id columns play different roles in the same row, so neither can be dropped:

- `organization_id` is the **join key**. MV (2) resolves each tree to exactly one owning org; MV (3) joins
  `organization_trees.organization_id = organization_hierarchy.organization_id` to find the trees.
- `ancestor_id` is the **group-by key** — the org the count rolls up *into*.

```sql
FROM organization_aggregates.organization_trees t
JOIN organization_aggregates.organization_hierarchy h
  ON h.organization_id = t.organization_id
GROUP BY h.ancestor_id, ...
```

With only `ancestor_id` you can't tell which subtree orgs feed a rollup; with only `organization_id` you
can't tell which rollups a given org participates in. The depth-0 self-rows are what make an org's own
directly-owned trees count toward itself, so no `UNION` or special case is needed.

Note that `organization_clusters` itself stores **only** `organization_id`, and there it means
`h.ancestor_id` — the rollup target. The descendant side is consumed by the aggregation and does not
survive into MV (3).

**Footgun to document:** because this is a closure, each org appears once per ancestor, so its attribute
columns repeat across those rows. Any query that filters on `map_name`, `name` or `parent_id` and wants
one row per org must add `depth = 0` (or `ancestor_id = organization_id`). Filtering on `parent_id` alone
returns the direct children of that parent once per ancestor of each child. The README will state this
next to the view definition.

### Columns

`ancestor_id`, `ancestor_map_name`, `ancestor_name`, `organization_id`, `map_name`, `name`, `depth`,
`parent_id` (immediate), `root_id`, `root_map_name`, `is_root_ancestor`, `rel_type`, `rel_role`.

---

## MV (2) `organization_trees`

One row per tree. Keys + geometry only:

`tree_id`, `planter_id`, `tree_planting_organization_id`, `planter_organization_id`,
`organization_id` (= `COALESCE(trees.planting_organization_id, planter.organization_id)` — tree wins, as
specified), `organization_source` (`'tree'`/`'planter'`), `map_name`, `top_level_parent_id`,
`root_map_name`, `estimated_geometric_location`.

- `LEFT JOIN planter` — a tree may have no planter.
- `lat` / `lon` omitted as derivable via `ST_X` / `ST_Y`. Trivial to add if wanted.
- Trees with no resolvable org are excluded (unreachable by any org filter). **Flagged assumption** —
  revisit if an all-trees case needs them.
- Depends on MV (1) for `top_level_parent_id` → refresh ordering in the DAG.
- Indexes: `UNIQUE(tree_id)`; btree on `organization_id`, `top_level_parent_id`, `map_name`;
  GiST on geometry.

---

## MV (3) `organization_clusters`

Grain: `(organization_id, zoom_level, region_id)` where `organization_id` is **any ancestor** — so a child
org's trees count toward the child and toward every org above it, matching the recursive-CTE semantics of
the current query.

Exposes everything the current shape does: `type`, `id` (= `region_id`), `estimated_geometric_location`
(the `LEAST(st_x, 170)` longitude clamp, preserved), `latlon` GeoJSON, `region_type` (= `type_id`),
`count`, `count_text` (the `/1000 || 'K'` formatting), plus raw `centroid` for envelope filtering and the
containment match, and `is_root_org` for partial indexing.

Two deliberate decisions:

- **No hardcoded zoom list.** Groups by whatever `zoom_level` values exist in `active_tree_region`, so it
  won't break on an unexpected zoom range.
- **`region.geom` is not stored.** The zoom-target join needs the low-zoom region's polygon for
  `ST_Contains`. Duplicating polygons across every org × zoom level is the largest storage risk in this
  design, so the live query does a PK lookup on `region` for the handful of clusters inside the envelope
  instead — a small indexed dimension join, not one of the expensive joins being eliminated. The
  store-it-anyway variant will be documented.

Indexes: `UNIQUE(organization_id, zoom_level, region_id)` (required for `CONCURRENTLY`);
`gist(organization_id, zoom_level, centroid)` via `btree_gist`, with a documented fallback of separate
GiST + btree if the extension isn't wanted.

**Sizing risk:** the every-ancestor grain multiplies rows by average tree depth. `99_sizing_checks.sql`
will ship so the real number can be measured against prod before deploying rather than guessed.

---

## Query (1)

`organization_clusters` filtered on `organization_id` + `zoom_level` + `centroid && ST_MakeEnvelope(...)`,
self-joined to itself at `zoom_level + 2`, `DISTINCT ON (region.id) ... ORDER BY region.id, total DESC` to
take the highest-count subregion whose centroid falls inside the parent region — same pattern and same
tie-break as the current shape. Parameterized with `$1..$n`.

---

## Airflow DAG

Airflow 2.x, classic (non-decorator) operators, `SQLExecuteQueryOperator` from `common.sql` —
`PostgresOperator` was removed in postgres provider 6.0; the one-line swap for older pins will be noted.

- `dag_id='organization_aggregates_refresh'`, `schedule_interval='0 */6 * * *'`, `catchup=False`,
  `max_active_runs=1`.
- Linear chain matching the dependency order: `hierarchy → trees → clusters`, each followed by `ANALYZE`.
- A `organization_aggregates.refresh_view()` plpgsql helper handles the `CONCURRENTLY` bootstrap problem —
  the first refresh of an unpopulated matview *cannot* be concurrent, so it checks
  `pg_class.relispopulated` and picks the right form. It also sets `lock_timeout` / `statement_timeout`
  and writes to a `refresh_log` table for observability.

---

## Files to produce

```
sql/00_schema.sql                          schema, extensions, refresh_log, refresh_view() helper
sql/10_organization_hierarchy.sql          view + indexes
sql/20_organization_trees.sql              view + indexes
sql/30_organization_clusters.sql           view + indexes
sql/50_query_clusters_with_zoom_target.sql
sql/90_teardown.sql
sql/99_sizing_checks.sql
airflow/dags/organization_aggregates_refresh.py
README.md                                  deploy order, full assumption list, deviations, verification
COST-AND-IMPACT.md                         session cost + EcoLogits gwp/wcf/energy
```

Indexes live inline with each view rather than in a separate file, so each is independently deployable.

---

## Cost & impact

Scoped to **the implementation only**, not the planning, and measured by difference:

1. Baseline captured immediately before implementation into `.baseline/pre-implementation.json`
   (`costUSD` from `~/.claude/cost-report.sh --json`, plus cumulative `output_tokens` from the session
   transcript).
2. After the files are written, re-read both and report `after - before`.
3. The **token delta** — not the session total — is what goes to the EcoLogits estimations endpoint for
   gwp / wcf / energy at `zone=USA`, opus family, matching the statusline config.

Whole-session totals are reported alongside for context only. `~/.claude/CLAUDE.md` has been updated with
this rule under *Scoping cost and impact to one task*, so it is the default for future work.
Plus a note on expected query-side compute savings.

---

## Standing caveat

Every column name and type is inferred from the two SQL files; there is no schema access. `README.md` will
carry the complete list of inferred columns for diffing against prod. **Nothing here should be applied to a
live database unverified.**
