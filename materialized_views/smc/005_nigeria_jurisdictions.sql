SET schema 'reveal';

DROP MATERIALIZED VIEW IF EXISTS nigeria_jurisdictions CASCADE;

CREATE MATERIALIZED VIEW IF NOT EXISTS nigeria_jurisdictions
AS
SELECT
    jurisdictions_materialized_view.jurisdiction_id,
    jurisdictions_materialized_view.jurisdiction_parent_id,
    jurisdictions_materialized_view.jurisdiction_name,
    jurisdictions_materialized_view.jurisdiction_geometry,
    jurisdictions_materialized_view.jurisdiction_depth,
    jurisdictions_materialized_view.jurisdiction_path,
    jurisdictions_materialized_view.jurisdiction_name_path,
    jurisdictions_materialized_view.jurisdiction_root_parent_id,
    jurisdictions_materialized_view.jurisdiction_root_parent_name,
    jurisdictions_materialized_view.jurisdiction_id AS opensrp_jurisdiction_id,
    CASE
        WHEN (jurisdictions_materialized_view.jurisdiction_depth < 4) THEN (md5(concat(jurisdictions_materialized_view.jurisdiction_id,'_remainder')))::character varying
        ELSE jurisdictions_materialized_view.jurisdiction_id
    END AS nigeria_jurisdiction_id,
    false AS is_virtual_jurisdiction
FROM jurisdictions_materialized_view;

CREATE UNIQUE INDEX IF NOT EXISTS nigeria_jurisdictions_id_idx ON nigeria_jurisdictions (jurisdiction_id);
CREATE INDEX IF NOT EXISTS nigeria_jurisdictions_opensrp_jurisdiction_id_idx ON nigeria_jurisdictions (opensrp_jurisdiction_id);
CREATE INDEX IF NOT EXISTS nigeria_jurisdictions_parent_id_idx ON nigeria_jurisdictions (jurisdiction_parent_id);
CREATE INDEX IF NOT EXISTS nigeria_jurisdictions_jurisdiction_depth_idx ON nigeria_jurisdictions (jurisdiction_depth);
CREATE INDEX IF NOT EXISTS nigeria_jurisdictions_jurisdiction_root_parent_id_idx ON nigeria_jurisdictions (jurisdiction_root_parent_id);
CREATE INDEX IF NOT EXISTS nigeria_jurisdictions_jurisdiction_geometry_gix ON nigeria_jurisdictions USING GIST (jurisdiction_geometry);