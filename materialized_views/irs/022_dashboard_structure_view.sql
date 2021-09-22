SET schema 'reveal';

DROP MATERIALIZED VIEW IF EXISTS dashboard_structure_view CASCADE;

CREATE MATERIALIZED VIEW IF NOT EXISTS dashboard_structure_view
AS
SELECT
    structures.plan_id AS plan_id,
    plans.title AS plan_title,
    jurisdictions.jurisdiction_name_path[1] AS country,
    jurisdictions.jurisdiction_name_path[2] AS province,
    jurisdictions.jurisdiction_name_path[3] AS district,
    jurisdictions.jurisdiction_name_path[4] AS catchment,
    jurisdictions.jurisdiction_name AS jurisdiction,
    structures.village_name AS village_name,
    structures.geo_jurisdiction_id AS jurisdiction_id,
    structures.structure_id AS structure_id,
    structures.lat_lon AS lat_lon,
    structures.business_status AS business_status,
    structures.eligibility AS eligibility,
    structures.structure_sprayed AS structure_sprayed,
    structures.rooms_eligible AS room_eligible,
    structures.rooms_sprayed AS rooms_sprayed,
    structures.sprayed_totalpop AS sprayed_totalpop,
    structures.sprayed_totalmale AS sprayed_totalmale,
    structures.sprayed_totalfemale AS sprayed_totalfemale,
    structures.sprayed_males AS sprayed_males,
    structures.sprayed_females AS sprayed_females,
    structures.sprayed_pregwomen AS sprayed_pregwomen,
    structures.sprayed_childrenU5 AS sprayed_childrenU5,
    structures.notsprayed_totalpop AS notsprayed_totalpop,
    structures.mix_serial_numbers AS mix_serial_numbers,
    structures.event_date
FROM reveal.irs_structures structures
LEFT JOIN reveal.jurisdictions_tree jurisdictions ON (structures.geo_jurisdiction_id = jurisdictions.jurisdiction_id)
LEFT JOIN reveal.plans plans ON structures.plan_id = plans.identifier
WHERE
    structures.business_status NOT IN ('Not Visited','No Tasks')
ORDER BY event_date DESC;