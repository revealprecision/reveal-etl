SET schema 'reveal';

DROP MATERIALIZED VIEW IF EXISTS irs_structures_report_no_geojson CASCADE;

CREATE MATERIALIZED VIEW IF NOT EXISTS irs_structures_report_no_geojson
AS
SELECT
    irs_structures.id,
    irs_structures.structure_id,
    irs_structures.structure_jurisdiction_id,
    irs_structures.task_id,
    irs_structures.plan_id,
    irs_structures.event_date,
    irs_structures.business_status,
    irs_structures.rooms_eligible,
    irs_structures.rooms_sprayed,
    irs_structures.eligibility,
    irs_structures.structure_sprayed
FROM reveal.irs_structures;

CREATE INDEX IF NOT EXISTS irs_structures_report_no_geojson_idx ON irs_structures_report_no_geojson (id);
CREATE INDEX IF NOT EXISTS irs_structures_report_no_geojson_structure_idx ON irs_structures_report_no_geojson (structure_id);
CREATE INDEX IF NOT EXISTS irs_structures_report_no_geojson_task_idx ON irs_structures_report_no_geojson (task_id);
CREATE INDEX IF NOT EXISTS irs_structures_report_no_geojson_plan_idx ON irs_structures_report_no_geojson (plan_id);