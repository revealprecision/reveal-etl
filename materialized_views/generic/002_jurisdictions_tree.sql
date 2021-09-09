SET schema 'reveal';

DROP MATERIALIZED VIEW IF EXISTS jurisdictions_tree CASCADE;

CREATE MATERIALIZED VIEW IF NOT EXISTS jurisdictions_tree
AS
WITH RECURSIVE tree AS (
    SELECT
        jurisdictions.id AS jurisdiction_id,
        jurisdictions.parent_id AS jurisdiction_parent_id,
        jurisdictions.name AS jurisdiction_name,
        jurisdictions.geometry AS jurisdiction_geometry,
        0 AS jurisdiction_depth,
        ARRAY[]::character varying[] AS jurisdiction_path,
        ARRAY[]::character varying[] AS jurisdiction_names
    FROM reveal.jurisdictions
    WHERE ((jurisdictions.parent_id IS NULL) OR ((jurisdictions.parent_id)::text = ''::text))

    UNION ALL

    SELECT
        jurisdictions.id AS jurisdictions_id,
        jurisdictions.parent_id AS jurisdictions_parent_id,
        jurisdictions.name AS jurisdictions_name,
        jurisdictions.geometry AS jurisdictions_geometry,
        (tree_1.jurisdiction_depth + 1),
        (tree_1.jurisdiction_path || ARRAY[jurisdictions.parent_id]),
        (tree_1.jurisdiction_names || ARRAY[jurisdictions_parent.name])
    FROM reveal.jurisdictions,
        (tree tree_1
    LEFT JOIN reveal.jurisdictions jurisdictions_parent ON (((tree_1.jurisdiction_id)::text = (jurisdictions_parent.id)::text)))
    WHERE ((tree_1.jurisdiction_id)::text = (jurisdictions.parent_id)::text)
), parents AS (
    SELECT
        DISTINCT jurisdictions.parent_id AS jurisdiction_id
    FROM reveal.jurisdictions
    WHERE ((jurisdictions.parent_id IS NOT NULL) AND ((jurisdictions.parent_id)::text <> ''::text))
)

SELECT
    tree.jurisdiction_id,
    tree.jurisdiction_parent_id,
    tree.jurisdiction_name,
    tree.jurisdiction_geometry,
    tree.jurisdiction_depth,
    tree.jurisdiction_path,
    tree.jurisdiction_names AS jurisdiction_name_path,
    CASE
        WHEN ((tree.jurisdiction_id)::text IN ( SELECT parents.jurisdiction_id
        FROM parents)) THEN false
        ELSE true
    END AS is_leaf_node,
    CASE
        WHEN (array_length(tree.jurisdiction_path, 1) IS NULL) THEN tree.jurisdiction_id
        ELSE tree.jurisdiction_path[1]
    END AS jurisdiction_root_parent_id,
    CASE
        WHEN (array_length(tree.jurisdiction_names, 1) IS NULL) THEN tree.jurisdiction_name
        ELSE tree.jurisdiction_names[1]
    END AS jurisdiction_root_parent_name
from tree;

CREATE INDEX IF NOT EXISTS jurisdictions_tree_parent_idx ON jurisdictions_tree (jurisdiction_parent_id);
CREATE INDEX IF NOT EXISTS jurisdictions_tree_depth_idx ON jurisdictions_tree (jurisdiction_depth);
CREATE INDEX IF NOT EXISTS jurisdictions_tree_root_parent_id_idx ON jurisdictions_tree (jurisdiction_root_parent_id);
CREATE INDEX IF NOT EXISTS jurisdictions_tree_geom_gix ON jurisdictions_tree USING GIST (jurisdiction_geometry);
CREATE INDEX IF NOT EXISTS jurisdictions_is_leaf_node_idx ON jurisdictions_tree (is_leaf_node);
CREATE UNIQUE INDEX IF NOT EXISTS jurisdictions_tree_idx ON jurisdictions_tree (jurisdiction_id);