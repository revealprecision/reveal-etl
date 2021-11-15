SET schema 'reveal';

DROP MATERIALIZED VIEW IF EXISTS smc_structure_data;

CREATE MATERIALIZED VIEW IF NOT EXISTS smc_structure_data
AS
SELECT
    jurisdiction.jurisdiction_name_path[1] AS country,
    jurisdiction.jurisdiction_name_path[2] AS province,
    jurisdiction.jurisdiction_name_path[3] AS district,
    jurisdiction.jurisdiction_name_path[4] AS catchment,
    jurisdiction.jurisdiction_name AS jurisdiction,
    structures.jurisdiction_id AS jurisdiction_id,
    structures.structure_id AS structure_id,
    public.st_astext(public.st_centroid(locations.geometry)) AS lat_lon,
    structures.task_id AS registration_task_id,
    reg_event.provider_id AS registered_by,
    reg_event.id AS registration_event_id,
    reg_event.date_created AS registration_event_date,
    reg_event.form_data ->> 'start_time' AS family_registration_start_time,
    reg_event.form_data ->> 'end_time' AS family_registration_end_time,
    reg_event.form_data ->> 'compoundPart' AS part_of_compound,
    reg_event.form_data ->> 'withinCompound' AS withinCompound,
    reg_event.form_data ->> 'compoundStructure' AS compoundStructure,
    reg_event.form_data ->> 'numCompoundStructures' AS numCompoundStructures,
    family.firstname AS family_name,
    headofhouse.firstname AS head_of_household_name,
    headofhouse_event.form_data ->> 'start_time' AS headofhouse_registration_start_time,
    headofhouse_event.form_data ->> 'end_time' AS headofhouse_registration_end_time,
    structures.business_status,
    not_eligible_event.form_data ->> 'eligible' AS not_eligible_reason,
    structures.plan_id AS plan_id,
    structures.eligible_children AS eligible_children,
    recon_event.form_data ->> 'start_time' AS drug_reconciliation_start_time,
    recon_event.form_data ->> 'end_time' AS drug_reconciliation_end_time,
    recon_event.form_data ->> 'totalAdministeredSpaq' AS drug_reconciliation_totalAdministeredSpaq,
    recon_event.form_data ->> 'additionalDosesAdministered' AS drug_reconciliation_additionalDosesAdministered,
    recon_event.form_data ->> 'totalNumberOfAdditionalDoses' AS drug_reconciliation_totalNumberOfAdditionalDoses,
    recon_event.form_data ->> 'blisterPacketsNumber' AS blisterPacketsNumber,
    recon_event.form_data ->> 'childrenTreated' AS childrenTreated
FROM reveal.smc_structures structures
JOIN reveal.locations locations ON structures.structure_id = locations.id
JOIN reveal.jurisdictions_materialized_view jurisdiction ON (structures.jurisdiction_id = jurisdiction.jurisdiction_id)
LEFT JOIN reveal.clients family ON ((structures.structure_id)::text = (family.residence)::text AND lastname = 'Family')
LEFT JOIN reveal.clients headofhouse ON ((family.relationships -> 'family_head' ->> 0)::text = (headofhouse.baseentityid)::text)
LEFT JOIN reveal.events reg_event ON reg_event.base_entity_id = family.baseentityid AND reg_event.event_type = 'Family_Registration'
LEFT JOIN reveal.events headofhouse_event ON headofhouse_event.event_type = 'Family_Member_Registration' AND headofhouse_event.base_entity_id = headofhouse.baseentityid
LEFT JOIN LATERAL (
    SELECT
        form_data
    FROM reveal.events
    WHERE structures.structure_id::text = events.structure_id::text
    AND events.event_type = 'Family_Registration_Ineligible'
    ORDER BY event_date DESC
    LIMIT 1
) not_eligible_event ON TRUE
LEFT JOIN LATERAL (
    SELECT
        form_data
    FROM reveal.events
    WHERE structures.structure_id::text = events.structure_id::text
    AND structures.plan_id::text = events.plan_id::text
    AND events.event_type = 'mda_drug_reconciliation'
    ORDER BY event_date DESC
    LIMIT 1
) recon_event ON TRUE
WHERE
    structures.business_status NOT IN ('Not Visited','No Tasks');