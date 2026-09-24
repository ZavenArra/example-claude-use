-- =============================================================================
-- 50_query_clusters_with_zoom_target.sql — the live tile-server query
-- =============================================================================
-- This is the replacement for 2-current-query-shape.MD. It is NOT deployed as an
-- object; it is the statement the tile server should send. No recursion, no
-- on-demand aggregation: a filtered read of organization_clusters plus one
-- self-join for the zoom target.
--
-- PARAMETERS
--   $1  organization id (uuid/int -- whatever entity.id is)
--         Pass a child org id for that org's own rollup.
--         Pass a ROOT org id for the top-level-parent rollup.
--         Same column, same query -- the closure in MV 1 makes these identical.
--   $2  zoom_level (int)     the tile's zoom level
--   $3  xmin (float8)  \
--   $4  ymin (float8)   |  request envelope, EPSG:4326, as in ST_MakeEnvelope
--   $5  xmax (float8)   |
--   $6  ymax (float8)  /
--
-- Resolving a map_name to $1 is one extra indexed lookup:
--   SELECT organization_id FROM organization_aggregates.organization_hierarchy
--    WHERE map_name = $1 AND depth = 0;          -- depth = 0: one row per org
-- =============================================================================

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
-- region.geom is looked up per cluster rather than stored in the MV; this is an
-- indexed PK probe. See fidelity note 2 in 30_organization_clusters.sql.
LEFT JOIN region r ON r.id = c.id
LEFT JOIN LATERAL (
  -- The most-populated subregion two zoom levels in, whose centroid falls inside
  -- this cluster's region polygon.
  SELECT st_asgeojson(z.centroid) AS centroid
  FROM organization_aggregates.organization_clusters z
  WHERE z.organization_id = c.organization_id
    AND z.zoom_level      = c.zoom_level + 2
    AND z.centroid && r.geom
    AND ST_Contains(r.geom, z.centroid)
    -- FIDELITY: the source query also constrained the zoom-target set to the
    -- request envelope. Keeping it preserves existing output exactly. Dropping it
    -- is arguably more correct -- a subregion centroid can sit inside the parent
    -- region but outside the envelope, and is currently missed -- but that is a
    -- behaviour change, so it stays in. Delete this line to change it.
    AND z.centroid && ST_MakeEnvelope($3, $4, $5, $6, 4326)
  ORDER BY z.count DESC, z.id      -- highest count wins; z.id makes ties deterministic
  LIMIT 1
) zoom_target ON true
WHERE c.organization_id = $1
  AND c.zoom_level      = $2
  AND c.centroid && ST_MakeEnvelope($3, $4, $5, $6, 4326);

-- DEVIATION (document, then verify): the source query built the zoom target with
--   DISTINCT ON (region.id) ... ORDER BY region.id, total DESC
-- over a full cross of low-zoom regions x high-zoom clusters, sorting the entire
-- joined set and discarding all but the first row per region. LEFT JOIN LATERAL
-- ... LIMIT 1 has the same semantics per cluster but stops at the first match,
-- and adds a deterministic tie-break the original lacked (with equal counts the
-- original returned an arbitrary subregion, so today's output for tied regions is
-- not reproducible run to run).
--
-- The +2 zoom offset was hardcoded as 2 -> 4 in the source. It is expressed
-- relative to c.zoom_level here so the query works at any requested zoom.


-- -----------------------------------------------------------------------------
-- Expected plan (for verification with EXPLAIN (ANALYZE, BUFFERS))
-- -----------------------------------------------------------------------------
--   Nested Loop Left Join
--     -> Index Scan using organization_clusters_lookup_gix on ... c
--          Index Cond: (organization_id = $1 AND zoom_level = $2 AND centroid && ...)
--     -> Index Scan using region_pkey on region r   (Index Cond: id = c.id)
--     -> Limit  ->  Index Scan using organization_clusters_lookup_gix on ... z
-- No CTE Scan, no Recursive Union, no HashAggregate over active_tree_region.
-- Seeing any of those three means a view is stale/unpopulated or an index is missing.
