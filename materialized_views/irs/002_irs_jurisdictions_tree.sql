SET schema 'reveal';

DROP MATERIALIZED VIEW IF EXISTS irs_jurisdictions_tree CASCADE;

CREATE MATERIALIZED VIEW IF NOT EXISTS irs_jurisdictions_tree
AS
SELECT jurisdictions_tree.jurisdiction_id,
    jurisdictions_tree.jurisdiction_parent_id,
    jurisdictions_tree.jurisdiction_name,
    jurisdictions_tree.jurisdiction_geometry,
    jurisdictions_tree.jurisdiction_depth,
    jurisdictions_tree.jurisdiction_path,
    jurisdictions_tree.jurisdiction_name_path,
    jurisdictions_tree.is_leaf_node,
    jurisdictions_tree.jurisdiction_root_parent_id,
    jurisdictions_tree.jurisdiction_root_parent_name,
    jurisdictions_tree.jurisdiction_id AS opensrp_jurisdiction_id,
        CASE
            WHEN (jurisdictions_tree.is_leaf_node <> true) THEN (md5(concat(jurisdictions_tree.jurisdiction_id, '_remainder')))::character varying
            ELSE jurisdictions_tree.jurisdiction_id
        END AS zambia_jurisdiction_id,
    false AS is_virtual_jurisdiction
FROM reveal.jurisdictions_tree;

-- UNION ALL

-- SELECT md5(concat(jurisdictions_tree.jurisdiction_id, '_remainder')) AS jurisdiction_id,
--     jurisdictions_tree.jurisdiction_id AS jurisdiction_parent_id,
--     ((jurisdictions_tree.jurisdiction_name)::text || ' (other)'::text) AS jurisdiction_name,
--     jurisdictions_tree.jurisdiction_geometry,
--     (jurisdictions_tree.jurisdiction_depth + 1) AS jurisdiction_depth,
--     array_append(jurisdictions_tree.jurisdiction_path, jurisdictions_tree.jurisdiction_id) AS jurisdiction_path,
--     array_append(jurisdictions_tree.jurisdiction_name_path, jurisdictions_tree.jurisdiction_name) AS jurisdiction_name_path,
--     jurisdictions_tree.is_leaf_node,
--     jurisdictions_tree.jurisdiction_root_parent_id,
--     jurisdictions_tree.jurisdiction_root_parent_name,
--     jurisdictions_tree.jurisdiction_id AS opensrp_jurisdiction_id,
--     md5(concat(jurisdictions_tree.jurisdiction_id, '_remainder')) AS zambia_jurisdiction_id,
--     true AS is_virtual_jurisdiction
-- FROM reveal.jurisdictions_tree
-- WHERE (jurisdictions_tree.is_leaf_node = false);

CREATE UNIQUE INDEX IF NOT EXISTS irs_jurisdictions_tree_id_idx ON irs_jurisdictions_tree (jurisdiction_id);
CREATE INDEX IF NOT EXISTS irs_jurisdictions_tree_opensrp_jurisdiction_id_idx ON irs_jurisdictions_tree (opensrp_jurisdiction_id);
CREATE INDEX IF NOT EXISTS irs_jurisdictions_tree_parent_id_idx ON irs_jurisdictions_tree (jurisdiction_parent_id);
CREATE INDEX IF NOT EXISTS irs_jurisdictions_tree_jurisdiction_depth_idx ON irs_jurisdictions_tree (jurisdiction_depth);
CREATE INDEX IF NOT EXISTS irs_jurisdictions_tree_jurisdiction_root_parent_id_idx ON irs_jurisdictions_tree (jurisdiction_root_parent_id);
CREATE INDEX IF NOT EXISTS irs_jurisdictions_tree_jurisdiction_geometry_gix ON irs_jurisdictions_tree USING GIST (jurisdiction_geometry);