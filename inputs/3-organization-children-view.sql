--  This gives a list of all orgs with their own map name and their top level parent map name, as well as top level parent id


WITH RECURSIVE
  organization_children AS (
    SELECT
      entity.id,
      entity.name,
      entity.map_name parent_map_name,
      entity.map_name,
      entity_relationship.parent_id parent_id,
      1 as depth,
      entity_relationship.type,
      entity_relationship.role
    FROM
      entity
      LEFT JOIN entity_relationship ON entity_relationship.child_id = entity.id -- joins the children in, which are also in entity table
    WHERE
      parent_id IS NULL
    -- WHERE
    --   entity.id IN (
    --     SELECT
    --       id
    --     FROM
    --       entity
    --     WHERE
    --       map_name = 'freetown'
    --   )
    UNION
    SELECT
      next_child.id,
      next_child.name,
      c.map_name parent_map_name, -- the parent map name
      next_child.map_name,
      entity_relationship.parent_id parent_id,
      depth + 1,
      entity_relationship.type,
      entity_relationship.role
    FROM
      entity next_child
      JOIN entity_relationship ON entity_relationship.child_id = next_child.id
      JOIN organization_children c ON entity_relationship.parent_id = c.id
  )
  SELECT * from organization_children order by name asc;

