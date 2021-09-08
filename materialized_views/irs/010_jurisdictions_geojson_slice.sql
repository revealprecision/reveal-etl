SET schema 'reveal';

DROP VIEW IF EXISTS jurisdictions_geojson_slice;

DROP MATERIALIZED VIEW IF EXISTS jurisdictions_geojson_slice CASCADE;

CREATE MATERIALIZED VIEW IF NOT EXISTS jurisdictions_geojson_slice
AS
SELECT
    "row".jurisdiction_id,
    jsonb_build_object('type', 'Feature', 'id', "row".jurisdiction_id, 'geometry', public.st_asgeojson("row".jurisdiction_geometry)::jsonb, 'properties', to_jsonb("row".*) - 'jurisdiction_id'::text - 'jurisdiction_geometry'::text) AS geojson
FROM (
    SELECT
        plans_materialized_view.jurisdiction_id,
        plans_materialized_view.jurisdiction_name,
        plans_materialized_view.jurisdiction_parent_id,
        plans_materialized_view.jurisdiction_geometry
    FROM
        plans_materialized_view
) "row";
