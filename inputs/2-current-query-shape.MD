 /* sql case1 */

        WITH RECURSIVE organization_children AS (
           SELECT entity.id, entity_relationship.parent_id, 1 as depth, entity_relationship.type, entity_relationship.role
           FROM entity
           LEFT JOIN entity_relationship ON entity_relationship.child_id = entity.id
           WHERE entity.id IN (SELECT id FROM entity WHERE map_name = 'leadfoundation')
          UNION
           SELECT next_child.id, entity_relationship.parent_id, depth + 1, entity_relationship.type, entity_relationship.role
           FROM entity next_child
           JOIN entity_relationship ON entity_relationship.child_id = next_child.id
           JOIN organization_children c ON entity_relationship.parent_id = c.id
          )
            ,org_tree_id AS (
              SELECT id FROM (
              SELECT trees.id as id from trees
                WHERE
                  planter_id IN (
                    SELECT id FROM planter
                    JOIN (
                      SELECT id AS entity_id FROM organization_children LIMIT 20
                    ) org ON planter.organization_id = org.entity_id
                  )
              UNION
                select id from trees where planting_organization_id = (
                select id from entity where map_name = 'leadfoundation'
                )
              ) ids

            )

  SELECT
    'cluster' AS type,
    'case1 with zoom target tile' AS log,
    cluster_1.id,
    cluster_1.estimated_geometric_location,
    cluster_1.latlon,
    cluster_1.region_type,
    cluster_1.count,
    cluster_1.count_text,
    zoom_target.centroid zoom_to
  FROM (

      /* sql case1 tile */

      SELECT
      'cluster' AS type,
      'case1 tile' AS log,
      NULL AS zoom_to,
      region_id id,
      st_point( LEAST( st_x(centroid), 170 ), st_y(centroid)) estimated_geometric_location,
      St_asgeojson(st_point(LEAST(st_x(centroid), 170), st_y(centroid))) latlon,
      type_id as region_type,
      count(tree_region.id) count,
      CASE WHEN count(tree_region.id) > 1000
      THEN  (count(tree_region.id) / 1000) || 'K'
      ELSE count(tree_region.id) || ''
      END AS count_text
      FROM active_tree_region tree_region
      INNER JOIN org_tree_id ON org_tree_id.id = tree_region.tree_id

      WHERE zoom_level = 2

       AND centroid && ST_MakeEnvelope(54.84375,47.989921667414194,180,85.05112874735957, 4326)

      GROUP BY region_id, centroid, type_id
  ) cluster_1
  LEFT JOIN
  (SELECT
    DISTINCT ON
    (region.id) region.id region_id,
    contained.region_id most_populated_subregion_id,
    contained.total, -- sorted here to give the most populated subregion
    contained.zoom_level,
    ST_ASGeoJson(contained.centroid) centroid
  FROM
    (
    SELECT
      region_id,
      zoom_level
    FROM active_tree_region tree_region
    INNER JOIN org_tree_id ON org_tree_id.id = tree_region.tree_id

    WHERE
      zoom_level = 2

       AND centroid && ST_MakeEnvelope(54.84375,47.989921667414194,180,85.05112874735957, 4326)

    GROUP BY
      region_id,
      zoom_level ) populated_region
  JOIN region ON
    region.id = populated_region.region_id
  JOIN (
    SELECT
      region_id,
      zoom_level,
      count(tree_region.id) AS total,
      centroid
    FROM active_tree_region tree_region
    INNER JOIN org_tree_id ON org_tree_id.id = tree_region.tree_id

    WHERE
      zoom_level = 4

       AND centroid && ST_MakeEnvelope(54.84375,47.989921667414194,180,85.05112874735957, 4326)

    GROUP BY
      region_id,
      zoom_level,
      centroid ) contained ON
    ST_CONTAINS(region.geom,
    contained.centroid)
  WHERE
    TRUE
  ORDER BY
    region.id,
    total DESC
  ) zoom_target
  ON cluster_1.id = zoom_target.region_id
```
