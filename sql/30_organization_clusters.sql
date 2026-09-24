-- =============================================================================
-- 30_organization_clusters.sql — MV 3 of 3: pre-aggregated tile clusters
-- =============================================================================
-- Deploy after 20_organization_trees.sql, and REFRESH it after both earlier
-- views have data: it reads organization_trees AND organization_hierarchy.
--
-- SHAPE: one row per (organization_id, zoom_level, region_id), where
-- organization_id is ANY ANCESTOR of the org that owns the trees. A child org's
-- trees therefore roll up into the child's own rows and into a row for every org
-- above it, up to the root. That is what makes the live query a single indexed
-- read with no recursion: "give me the rollup for org X" and "give me the rollup
-- for top-level parent X" are the same predicate on the same column.
--
-- SIZING WARNING: row count is (distinct region/zoom cells) x (average hierarchy
-- depth + 1), not (distinct cells). Run 99_sizing_checks.sql against production
-- BEFORE deploying this file.
--
-- Replaces both on-demand aggregations in 2-current-query-shape.MD: the `case1
-- tile` inner SELECT and the `contained` subquery feeding the zoom target. Both
-- computed the same GROUP BY at different zoom levels; there is only one here.
--
-- INFERRED COLUMNS (never verified against a live database):
--   active_tree_region(id, tree_id, region_id, zoom_level, centroid, type_id)
-- =============================================================================

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

-- FIDELITY NOTES — three things here are deliberate. Do not "fix" them.
--
-- 1. NO HARDCODED ZOOM LIST. The source query hardcoded zoom_level = 2 (tile) and
--    zoom_level = 4 (zoom target). This groups by whatever zoom_level values
--    exist, so an unexpected zoom range cannot silently truncate tile output.
--
-- 2. region.geom IS NOT STORED. The zoom-target join needs the low-zoom region's
--    polygon for ST_Contains; duplicating polygons across every org x zoom level
--    is the largest storage risk in this design. 50_query_clusters_with_zoom_target.sql
--    does an indexed PK lookup on `region` instead. The store-it-anyway variant is
--    described in the README if the PK lookup turns out to be the bottleneck.
--
-- 3. st_point() RETURNS SRID 0. The source query's st_point(...) is equally
--    unprojected, so estimated_geometric_location and latlon are byte-identical
--    to today's output. Adding ST_SetSRID(..., 4326) would change what the tile
--    server receives. `centroid` keeps its original SRID and is what the spatial
--    predicates use, so the missing SRID affects only the output payload.
--
-- Also inherited verbatim: count_text uses integer division, so 1500 renders as
-- '1K'. That is the current behaviour.

COMMENT ON MATERIALIZED VIEW organization_aggregates.organization_clusters IS
  'Tree counts per (ancestor org, zoom_level, region). organization_id is any '
  'ancestor, so one predicate serves both org and top-level-parent rollups.';


-- -----------------------------------------------------------------------------
-- Indexes
-- -----------------------------------------------------------------------------
-- Required for REFRESH MATERIALIZED VIEW CONCURRENTLY, and it is the grain.
CREATE UNIQUE INDEX organization_clusters_uk
  ON organization_aggregates.organization_clusters (organization_id, zoom_level, id);

-- The live query's entire WHERE clause in one index: equality on organization_id
-- and zoom_level, then the centroid && envelope test. Needs btree_gist (created
-- in 00_schema.sql) to put the two scalar columns into a GiST index.
CREATE INDEX organization_clusters_lookup_gix
  ON organization_aggregates.organization_clusters
  USING gist (organization_id, zoom_level, centroid);

-- FALLBACK if btree_gist is unwanted: drop the index above and the extension from
-- 00_schema.sql, then use these two instead. Postgres will bitmap-AND them, which
-- costs an extra heap-bitmap step per request but needs no extra extension.
-- CREATE INDEX organization_clusters_org_zoom_idx
--   ON organization_aggregates.organization_clusters (organization_id, zoom_level);
-- CREATE INDEX organization_clusters_centroid_gix
--   ON organization_aggregates.organization_clusters USING gist (centroid);
