SET schema 'reveal';

DROP MATERIALIZED VIEW IF EXISTS nigeria_smc_structures_geojson CASCADE;

CREATE MATERIALIZED VIEW IF NOT EXISTS nigeria_smc_structures_geojson
AS
SELECT
    "row".structure_id,
    "row".jurisdiction_id AS jurisdiction_id,
    "row".task_id,
    "row".plan_id,
    jsonb_build_object('type', 'Feature', 'id', "row".structure_id, 'geometry', (public.st_asgeojson("row".structure_geometry))::jsonb, 'properties', ((to_jsonb("row".*) - 'structure_id'::text) - 'structure_geometry'::text)) AS geojson
FROM (
    SELECT
        smc_structures.structure_id,
        smc_structures.jurisdiction_id,
        smc_structures.structure_code,
        smc_structures.structure_name,
        smc_structures.structure_type,
        smc_structures.structure_geometry,
        smc_structures.task_id,
        smc_structures.plan_id,
        smc_structures.event_date,
        smc_structures.business_status,
        smc_structures.found_structures,
        smc_structures.structures_recieved_spaq,
        smc_structures.eligible_children,
        smc_structures.treated_children,
        smc_structures.referred_children,
        smc_structures.business_status
    FROM reveal.smc_structures
) "row";