SET schema 'reveal';

DROP MATERIALIZED VIEW IF EXISTS irs_lite_structure_report CASCADE;

CREATE MATERIALIZED VIEW IF NOT EXISTS irs_lite_structure_report
AS
SELECT
    results.id::text,
    results.structure_id,
    results.jurisdiction_id,
    results.task_id,
    results.plan_id,
    results.business_status,
    jsonb_build_object('type', 'Feature', 'id', results.structure_id, 'geometry', public.st_asgeojson(results.geometry)::jsonb, 'properties', to_jsonb(results.*) - 'jurisdiction_id'::text - 'geometry'::text) AS geojson
FROM (
    SELECT
        id::text,
        structure_id,
        jurisdiction_id,
        task_id,
        plan_id,
        business_status,
        structure_name,
        geometry
    FROM
        irs_lite_structures
) results;