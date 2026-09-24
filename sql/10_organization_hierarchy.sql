-- =============================================================================
-- 10_organization_hierarchy.sql — MV 1 of 3: the org ancestor closure
-- =============================================================================
-- Deploy after 00_schema.sql. Nothing else depends on 00 except the schema
-- itself and refresh_view(); this file only needs the schema to exist.
--
-- SHAPE: one row per (ancestor_id, organization_id) pair, INCLUDING the depth-0
-- self-row where ancestor_id = organization_id. This is a transitive closure,
-- not a parent list.
--
-- Replaces the `organization_children` recursive CTE in 2-current-query-shape.MD,
-- and subsumes 3-organization-children-view.sql (which walked downward from the
-- roots and carried only the immediate parent's map_name).
--
-- FOOTGUN: because this is a closure, each org appears once per ancestor and its
-- own attribute columns (name, map_name, parent_id, root_id) REPEAT across those
-- rows. Any query that wants one row per org must add `depth = 0`. See README.
--
-- INFERRED COLUMNS (never verified against a live database):
--   entity(id, name, map_name)
--   entity_relationship(child_id, parent_id, type, role)
-- =============================================================================

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
  -- One immediate parent per org, for the `parent_id` filter column. Orgs with
  -- several parents keep the lowest parent_id; the full set is still reachable
  -- through the closure itself (depth = 1).
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

COMMENT ON MATERIALIZED VIEW organization_aggregates.organization_hierarchy IS
  'Ancestor closure over entity_relationship: one row per (ancestor_id, organization_id), '
  'including depth-0 self-rows. Filter on depth = 0 for one row per org.';


-- -----------------------------------------------------------------------------
-- Indexes
-- -----------------------------------------------------------------------------
-- Required for REFRESH MATERIALIZED VIEW CONCURRENTLY. Also the closure's grain.
CREATE UNIQUE INDEX organization_hierarchy_uk
  ON organization_aggregates.organization_hierarchy (ancestor_id, organization_id);

-- "Who is above org X" — the direction organization_clusters joins in.
CREATE INDEX organization_hierarchy_org_depth_idx
  ON organization_aggregates.organization_hierarchy (organization_id, depth);

-- One-row-per-org attribute lookups (used by 20_organization_trees.sql).
CREATE INDEX organization_hierarchy_self_idx
  ON organization_aggregates.organization_hierarchy (organization_id) WHERE depth = 0;

-- The requirement's filter columns.
CREATE INDEX organization_hierarchy_map_name_idx
  ON organization_aggregates.organization_hierarchy (map_name) WHERE map_name IS NOT NULL;

CREATE INDEX organization_hierarchy_parent_idx
  ON organization_aggregates.organization_hierarchy (parent_id) WHERE parent_id IS NOT NULL;

CREATE INDEX organization_hierarchy_root_idx
  ON organization_aggregates.organization_hierarchy (root_id);
