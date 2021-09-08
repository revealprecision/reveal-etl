SET schema 'reveal';

DROP MATERIALIZED VIEW IF EXISTS jurisdictions_materialized_view CASCADE;

CREATE MATERIALIZED VIEW IF NOT EXISTS jurisdictions_materialized_view
AS
WITH RECURSIVE tree AS
(
    --base case
    SELECT
        jurisdictions.id AS jurisdiction_id,
        jurisdictions.parent_id AS jurisdiction_parent_id,
        jurisdictions.name AS jurisdiction_name,
        jurisdictions.geometry AS jurisdiction_geometry,
        0 AS jurisdiction_depth,
        ARRAY[]::varchar[] AS jurisdiction_path,
        ARRAY[]::varchar[] AS jurisdiction_names
    FROM jurisdictions
    -- setting this to id includes current object
    -- setting to parent_id includes only children
    -- setting to '' gets the whole tree
    WHERE jurisdictions.parent_id = ''
    UNION ALL
--recursive part
SELECT
    jurisdictions.id AS jurisdictions_id,
    jurisdictions.parent_id AS jurisdictions_parent_id,
    jurisdictions.name AS jurisdictions_name,
    jurisdictions.geometry AS jurisdictions_geometry,
    tree.jurisdiction_depth + 1,
    tree.jurisdiction_path || ARRAY[jurisdictions.parent_id],
    tree.jurisdiction_names || ARRAY[jurisdictions_parent.name]
FROM jurisdictions, tree
    LEFT JOIN jurisdictions AS jurisdictions_parent ON tree.jurisdiction_id = jurisdictions_parent.id
WHERE tree.jurisdiction_id = jurisdictions.parent_id
)
SELECT
    tree.jurisdiction_id AS jurisdiction_id,
    tree.jurisdiction_parent_id AS jurisdiction_parent_id,
    tree.jurisdiction_name AS jurisdiction_name,
    tree.jurisdiction_geometry AS jurisdiction_geometry,
    tree.jurisdiction_depth AS jurisdiction_depth,
    tree.jurisdiction_path AS jurisdiction_path,
    tree.jurisdiction_names AS jurisdiction_name_path,
    CASE
        WHEN array_length(tree.jurisdiction_path, 1) IS NULL
        THEN tree.jurisdiction_id
        ELSE tree.jurisdiction_path[1]
    END AS jurisdiction_root_parent_id,
    CASE
        WHEN array_length(tree.jurisdiction_names, 1) IS NULL
        THEN tree.jurisdiction_name
        ELSE tree.jurisdiction_names[1]
    END AS jurisdiction_root_parent_name
from tree;

CREATE INDEX IF NOT EXISTS jurisdictions_materialized_view_parent_idx ON jurisdictions_materialized_view (jurisdiction_parent_id);
CREATE INDEX IF NOT EXISTS jurisdictions_materialized_view_depth_idx ON jurisdictions_materialized_view (jurisdiction_depth);
CREATE INDEX IF NOT EXISTS jurisdictions_materialized_view_root_parent_id_idx ON jurisdictions_materialized_view (jurisdiction_root_parent_id);
CREATE INDEX IF NOT EXISTS jurisdictions_materialized_view_geom_gix ON jurisdictions_materialized_view USING GIST (jurisdiction_geometry);
CREATE UNIQUE INDEX IF NOT EXISTS jurisdictions_materialized_view_idx ON jurisdictions_materialized_view (jurisdiction_id);