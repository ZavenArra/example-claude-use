-- =============================================================================
-- 99_sizing_checks.sql — READ-ONLY. Run against production BEFORE deploying.
-- =============================================================================
-- Every query here is a SELECT against existing tables. Nothing is created and
-- nothing in organization_aggregates needs to exist yet.
--
-- WHY THIS EXISTS: organization_clusters uses the every-ancestor grain, which
-- multiplies rows by (average hierarchy depth + 1). Nobody has measured the
-- hierarchy. If check 6 projects a row count you are not willing to store and
-- refresh every 6 hours, change the grain before deploying, not after.
--
-- Run each block separately and record the numbers in the README.
-- =============================================================================

-- ── 1. Hierarchy size and shape ──────────────────────────────────────────────
SELECT
  (SELECT count(*) FROM entity)                                   AS org_count,
  (SELECT count(*) FROM entity_relationship)                      AS edge_rows,
  (SELECT count(*) FROM (
     SELECT DISTINCT child_id, parent_id FROM entity_relationship
      WHERE child_id IS NOT NULL AND parent_id IS NOT NULL) d)     AS distinct_edges,
  (SELECT count(*) FROM entity_relationship
    WHERE parent_id IS NOT NULL
      AND NOT EXISTS (SELECT 1 FROM entity e WHERE e.id = entity_relationship.parent_id))
                                                                   AS dangling_parent_rows,
  (SELECT count(*) FROM entity e
    WHERE NOT EXISTS (SELECT 1 FROM entity_relationship er
                       WHERE er.child_id = e.id AND er.parent_id IS NOT NULL))
                                                                   AS root_orgs;

-- ── 2. Multi-parent orgs ─────────────────────────────────────────────────────
-- Non-zero means the hierarchy is a DAG, not a tree: the closure's `pair` CTE
-- (min(depth)) and its `root` tie-break are load-bearing, and one org can roll
-- up under more than one root.
SELECT count(*) AS multi_parent_orgs, coalesce(max(n), 0) AS max_parents_per_org
FROM (
  SELECT child_id, count(DISTINCT parent_id) AS n
  FROM entity_relationship
  WHERE child_id IS NOT NULL AND parent_id IS NOT NULL
  GROUP BY child_id
  HAVING count(DISTINCT parent_id) > 1
) x;

-- ── 3. Depth distribution, and cycle detection ───────────────────────────────
-- This is the same walk 10_organization_hierarchy.sql does, including the path
-- guard. `cycle_edges_skipped` counts upward steps the guard refused: > 0 means
-- there is at least one cycle in entity_relationship, which would make a naive
-- recursive CTE loop forever.
WITH RECURSIVE
edge AS (
  SELECT DISTINCT er.child_id, er.parent_id
  FROM entity_relationship er
  JOIN entity pe ON pe.id = er.parent_id
  WHERE er.child_id IS NOT NULL
),
ancestry AS (
  SELECT e.id AS organization_id, e.id AS ancestor_id, 0 AS depth, ARRAY[e.id] AS path
  FROM entity e
  UNION ALL
  SELECT a.organization_id, edge.parent_id, a.depth + 1, a.path || edge.parent_id
  FROM ancestry a
  JOIN edge ON edge.child_id = a.ancestor_id
  WHERE NOT (edge.parent_id = ANY (a.path))
),
per_org AS (
  SELECT organization_id,
         max(depth)                  AS max_depth_per_org,
         count(DISTINCT ancestor_id) AS ancestors_per_org
  FROM ancestry
  GROUP BY organization_id
)
-- One row per org, so these averages are unweighted. avg_ancestors_per_org is
-- the closure fan-out factor: MV 1 holds roughly org_count x this.
SELECT
  max(max_depth_per_org)                AS max_depth,
  round(avg(max_depth_per_org), 2)      AS avg_depth_per_org,
  round(avg(ancestors_per_org), 2)      AS avg_ancestors_per_org,
  max(ancestors_per_org)                AS max_ancestors_per_org
FROM per_org;

-- Cycle check, run on its own (cheap, and unambiguous):
WITH RECURSIVE
edge AS (
  SELECT DISTINCT er.child_id, er.parent_id
  FROM entity_relationship er
  JOIN entity pe ON pe.id = er.parent_id
  WHERE er.child_id IS NOT NULL
),
ancestry AS (
  SELECT e.id AS organization_id, e.id AS ancestor_id, ARRAY[e.id] AS path, false AS looped
  FROM entity e
  UNION ALL
  SELECT a.organization_id, edge.parent_id, a.path || edge.parent_id,
         edge.parent_id = ANY (a.path)
  FROM ancestry a
  JOIN edge ON edge.child_id = a.ancestor_id
  WHERE NOT a.looped
)
SELECT count(*) AS cycle_edges_skipped FROM ancestry WHERE looped;

-- ── 4. Projected organization_hierarchy row count ────────────────────────────
-- This is the closure size = exactly what MV 1 will hold.
WITH RECURSIVE
edge AS (
  SELECT DISTINCT er.child_id, er.parent_id
  FROM entity_relationship er
  JOIN entity pe ON pe.id = er.parent_id
  WHERE er.child_id IS NOT NULL
),
ancestry AS (
  SELECT e.id AS organization_id, e.id AS ancestor_id, 0 AS depth, ARRAY[e.id] AS path
  FROM entity e
  UNION ALL
  SELECT a.organization_id, edge.parent_id, a.depth + 1, a.path || edge.parent_id
  FROM ancestry a
  JOIN edge ON edge.child_id = a.ancestor_id
  WHERE NOT (edge.parent_id = ANY (a.path))
)
SELECT count(*) AS projected_hierarchy_rows
FROM (SELECT DISTINCT organization_id, ancestor_id FROM ancestry) p;

-- ── 5. Projected organization_trees row count, and what gets dropped ─────────
SELECT
  count(*)                                                          AS tree_rows,
  count(*) FILTER (WHERE t.planting_organization_id IS NOT NULL)     AS resolved_via_tree,
  count(*) FILTER (WHERE t.planting_organization_id IS NULL
                     AND p.organization_id IS NOT NULL)              AS resolved_via_planter,
  count(*) FILTER (WHERE coalesce(t.planting_organization_id, p.organization_id) IS NULL)
                                                                     AS dropped_no_org,
  count(*) FILTER (WHERE coalesce(t.planting_organization_id, p.organization_id) IS NOT NULL
                     AND NOT EXISTS (SELECT 1 FROM entity e
                                      WHERE e.id = coalesce(t.planting_organization_id, p.organization_id)))
                                                                     AS dropped_org_not_in_entity
FROM trees t
LEFT JOIN planter p ON p.id = t.planter_id;

-- ── 6. Projected organization_clusters row count — THE NUMBER THAT MATTERS ───
-- Distinct (ancestor, zoom_level, region_id). Compare against
-- `distinct_cells` below: the ratio is the closure fan-out you are paying for.
WITH RECURSIVE
edge AS (
  SELECT DISTINCT er.child_id, er.parent_id
  FROM entity_relationship er
  JOIN entity pe ON pe.id = er.parent_id
  WHERE er.child_id IS NOT NULL
),
ancestry AS (
  SELECT e.id AS organization_id, e.id AS ancestor_id, ARRAY[e.id] AS path
  FROM entity e
  UNION ALL
  SELECT a.organization_id, edge.parent_id, a.path || edge.parent_id
  FROM ancestry a
  JOIN edge ON edge.child_id = a.ancestor_id
  WHERE NOT (edge.parent_id = ANY (a.path))
),
pair AS (SELECT DISTINCT organization_id, ancestor_id FROM ancestry),
tree_org AS (
  SELECT t.id AS tree_id, coalesce(t.planting_organization_id, p.organization_id) AS organization_id
  FROM trees t
  LEFT JOIN planter p ON p.id = t.planter_id
  WHERE coalesce(t.planting_organization_id, p.organization_id) IS NOT NULL
)
SELECT
  count(*)                                                     AS projected_cluster_rows,
  (SELECT count(*) FROM (
     SELECT DISTINCT atr.zoom_level, atr.region_id
     FROM active_tree_region atr
     JOIN tree_org tg ON tg.tree_id = atr.tree_id) d)           AS distinct_cells
FROM (
  SELECT DISTINCT pr.ancestor_id, atr.zoom_level, atr.region_id
  FROM active_tree_region atr
  JOIN tree_org tg ON tg.tree_id = atr.tree_id
  JOIN pair pr     ON pr.organization_id = tg.organization_id
) c;
-- If check 6 is slow or memory-hungry, that is itself a finding: it is the same
-- work the MV refresh does. Time it, and size the DAG's statement_timeout from it.

-- ── 7. Zoom levels actually present ──────────────────────────────────────────
-- The source query hardcoded 2 and 4. Anything else here is a zoom the current
-- query cannot serve and the MV will.
SELECT zoom_level, count(*) AS rows, count(DISTINCT region_id) AS regions
FROM active_tree_region
GROUP BY zoom_level ORDER BY zoom_level;

-- ── 8. Current source-table footprint, for a size projection ─────────────────
-- Multiply active_tree_region's per-row cost by (projected_cluster_rows /
-- its own row count) for a rough organization_clusters size. The MV stores one
-- geometry + a text geojson per row, so treat it as an under-estimate.
SELECT
  c.relname                                            AS table_name,
  to_char(c.reltuples::numeric, 'FM999,999,999,999')   AS est_rows,
  pg_size_pretty(pg_total_relation_size(c.oid))        AS total_size,
  pg_size_pretty(pg_relation_size(c.oid))              AS heap_size
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE c.relname IN ('entity','entity_relationship','planter','trees','active_tree_region','region')
  AND c.relkind = 'r'
ORDER BY pg_total_relation_size(c.oid) DESC;

-- ── 9. After the first refresh: actual sizes ─────────────────────────────────
SELECT
  c.oid::regclass                                 AS view_name,
  to_char(c.reltuples::numeric, 'FM999,999,999,999') AS est_rows,
  pg_size_pretty(pg_total_relation_size(c.oid))   AS total_size,
  pg_size_pretty(pg_indexes_size(c.oid))          AS index_size
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = 'organization_aggregates' AND c.relkind = 'm'
ORDER BY pg_total_relation_size(c.oid) DESC;
