SET schema 'reveal';

DROP MATERIALIZED VIEW IF EXISTS irs_structure_jurisdictions CASCADE;

CREATE MATERIALIZED VIEW IF NOT EXISTS irs_structure_jurisdictions
AS
SELECT
    structure_geo_hierarchy.id,
    structure_geo_hierarchy.jurisdiction_id,
    structure_geo_hierarchy.jurisdiction_depth,
    structure_geo_hierarchy.geo_jurisdiction_id,
    structure_geo_hierarchy.geo_jurisdiction_depth,
    structure_geo_hierarchy.geo_strict_within,
    COALESCE(virtual_jurisdictions.jurisdiction_id, structure_geo_hierarchy.geo_jurisdiction_id, structure_geo_hierarchy.jurisdiction_id) AS zambia_jurisdiction_id,
    COALESCE(virtual_jurisdictions.jurisdiction_depth, structure_geo_hierarchy.geo_jurisdiction_depth, structure_geo_hierarchy.jurisdiction_depth) AS zambia_jurisdiction_depth
FROM (reveal.structure_geo_hierarchy
LEFT JOIN (
    SELECT
        irs_jurisdictions_tree.jurisdiction_id,
        irs_jurisdictions_tree.jurisdiction_parent_id,
        irs_jurisdictions_tree.jurisdiction_name,
        irs_jurisdictions_tree.jurisdiction_geometry,
        irs_jurisdictions_tree.jurisdiction_depth,
        irs_jurisdictions_tree.jurisdiction_path,
        irs_jurisdictions_tree.jurisdiction_name_path,
        irs_jurisdictions_tree.is_leaf_node,
        irs_jurisdictions_tree.jurisdiction_root_parent_id,
        irs_jurisdictions_tree.jurisdiction_root_parent_name,
        irs_jurisdictions_tree.opensrp_jurisdiction_id,
        irs_jurisdictions_tree.zambia_jurisdiction_id,
        irs_jurisdictions_tree.is_virtual_jurisdiction
    FROM reveal.irs_jurisdictions_tree irs_jurisdictions_tree
    WHERE (irs_jurisdictions_tree.is_virtual_jurisdiction = true)
) virtual_jurisdictions ON (((virtual_jurisdictions.opensrp_jurisdiction_id)::text = (structure_geo_hierarchy.geo_jurisdiction_id)::text)));

CREATE UNIQUE INDEX IF NOT EXISTS irs_structure_jurisdictions_id_idx ON irs_structure_jurisdictions (id);
CREATE INDEX IF NOT EXISTS irs_structure_jurisdictions_jurisdiction_id_idx ON irs_structure_jurisdictions (jurisdiction_id);
CREATE INDEX IF NOT EXISTS irs_structure_jurisdictions_geo_jurisdiction_id_idx ON irs_structure_jurisdictions (geo_jurisdiction_id);
CREATE INDEX IF NOT EXISTS irs_structure_jurisdictions_zambia_jurisdiction_id_idx ON irs_structure_jurisdictions (zambia_jurisdiction_id);