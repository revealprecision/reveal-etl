SET schema 'reveal';

DROP VIEW IF EXISTS irs_structures_report;

DROP MATERIALIZED VIEW IF EXISTS irs_structures_report CASCADE;

CREATE MATERIALIZED VIEW IF NOT EXISTS irs_structures_report
AS
SELECT
    "row".id::character varying AS id,
    "row".structure_id,
    "row".structure_jurisdiction_id AS jurisdiction_id,
    "row".task_id,
    "row".plan_id,
    jsonb_build_object('type', 'Feature', 'id', "row".structure_id, 'geometry', public.st_asgeojson("row".structure_geometry)::jsonb, 'properties', to_jsonb("row".*) - 'structure_id'::text - 'structure_geometry'::text) AS geojson
FROM (
    SELECT
        irs_structures_report.id,
        irs_structures_report.structure_id,
        irs_structures_report.structure_jurisdiction_id,
        locations.code AS structure_code,
        locations.name AS structure_name,
        locations.type AS structure_type,
        locations.geometry AS structure_geometry,
        irs_structures_report.task_id,
        irs_structures_report.plan_id,
        irs_structures_report.event_date,
        irs_structures_report.business_status,
        irs_structures_report.rooms_eligible,
        irs_structures_report.rooms_sprayed,
        irs_structures_report.eligibility,
        irs_structures_report.structure_sprayed,
        irs_structures_report.business_status
    FROM irs_structures_report_no_geojson irs_structures_report
    LEFT JOIN locations ON irs_structures_report.structure_id::text = locations.id::text) "row"

    (id, structure_id, structure_jurisdiction_id, structure_code, structure_name, structure_type, structure_geometry, task_id, plan_id, event_date, business_status, rooms_eligible, rooms_sprayed, eligibility, structure_sprayed, business_status_1);
