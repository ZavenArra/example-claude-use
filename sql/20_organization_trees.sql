-- =============================================================================
-- 20_organization_trees.sql — MV 2 of 3: tree → owning organization
-- =============================================================================
-- Deploy after 10_organization_hierarchy.sql, and REFRESH it after that view
-- has data: this view reads organization_hierarchy at depth 0.
--
-- SHAPE: one row per tree that has a resolvable organization.
--
-- Replaces the `org_tree_id` CTE in 2-current-query-shape.MD. That CTE UNIONed
-- "trees whose planter belongs to an org in the subtree" with "trees whose
-- planting_organization_id is the requested org". Here the two paths collapse
-- into one resolved organization_id per tree, with planting_organization_id
-- taking priority, and the subtree expansion moves to the closure join in
-- 30_organization_clusters.sql.
--
-- NOTE: the original CTE also had a `LIMIT 20` on the org list. That was a
-- correctness bug (it silently truncated large hierarchies), not a feature, and
-- is deliberately not reproduced. See README.
--
-- INFERRED COLUMNS (never verified against a live database):
--   trees(id, planter_id, planting_organization_id, estimated_geometric_location)
--   planter(id, organization_id)
-- =============================================================================

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

-- BEHAVIOUR DECISION (confirm before deploying): the join to organization_hierarchy
-- is INNER, so trees with no resolvable organization -- no planting_organization_id
-- and either no planter or a planter with a NULL organization_id -- are DROPPED.
-- Such trees are unreachable by any org filter, so they cannot appear in tile
-- output either way. Run 99_sizing_checks.sql to see how many rows this is.

COMMENT ON MATERIALIZED VIEW organization_aggregates.organization_trees IS
  'One row per tree with a resolvable owning org. planting_organization_id wins '
  'over the planter''s organization_id. Trees with neither are excluded.';


-- -----------------------------------------------------------------------------
-- Indexes
-- -----------------------------------------------------------------------------
-- Required for REFRESH MATERIALIZED VIEW CONCURRENTLY.
CREATE UNIQUE INDEX organization_trees_uk
  ON organization_aggregates.organization_trees (tree_id);

-- The join driver for 30_organization_clusters.sql.
CREATE INDEX organization_trees_org_idx
  ON organization_aggregates.organization_trees (organization_id);

CREATE INDEX organization_trees_top_level_parent_idx
  ON organization_aggregates.organization_trees (top_level_parent_id);

CREATE INDEX organization_trees_map_name_idx
  ON organization_aggregates.organization_trees (map_name) WHERE map_name IS NOT NULL;

-- Point-in-envelope / nearest-tree lookups against the raw tree geometry.
-- lat/lon are intentionally not stored: derive with ST_X() / ST_Y().
CREATE INDEX organization_trees_location_gix
  ON organization_aggregates.organization_trees
  USING gist (estimated_geometric_location);
