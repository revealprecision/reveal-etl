SET schema 'reveal';

DROP MATERIALIZED VIEW IF EXISTS smc_structures CASCADE;

CREATE MATERIALIZED VIEW IF NOT EXISTS smc_structures
AS
SELECT
    locations.id AS structure_id,
    smc_plan_jurisdictions.plan_id AS plan_id,
    smc_plan_jurisdictions.jurisdiction_id AS jurisdiction_id,
    locations.jurisdiction_id AS geo_jurisdiction_id,
    4 AS geo_jurisdiction_depth,
    locations.code AS structure_code,
    locations.name AS structure_name,
    locations.type AS structure_type,
    locations.geometry AS structure_geometry,
    locations.status AS structure_status,
    COALESCE(tasks_query.identifier, events_query_1.task_id, events_query_2.task_id) AS task_id,
    COALESCE(tasks_query.status, events_query_1.task_status, events_query_2.task_status) AS task_status,
    COALESCE(events_query_1.event_id, events_query_2.event_id) AS event_id,
    COALESCE(events_query_1.event_date, events_query_2.event_date) AS event_date,
    CASE WHEN (events_query_2.business_status = 'Family Registered' AND events_query_3.eligible_children = 0) THEN 'Ineligible'
        WHEN (events_query_2.business_status = 'Not Visited' AND events_query_1.found_structures = 1) THEN 'Ineligible'
        WHEN (tasks_query.business_status <> 'Not Visited' AND events_query_1.event_id IS NOT NULL AND events_query_2.business_status IS NULL) THEN 'Ineligible'
        WHEN (tasks_query.business_status <> 'Not Visited') THEN tasks_query.business_status
        WHEN (tasks_query.business_status is NULL) THEN 'No Tasks'
        ELSE COALESCE(events_query_2.business_status,'Not Visited') END AS business_status,
    COALESCE(events_query_1.found_structures, 0) AS found_structures,
    CASE WHEN (events_query_4.treated_children > 0) THEN 1 ELSE 0 END AS structures_recieved_spaq,
    events_query_3.eligible_children AS eligible_children,
    events_query_4.treated_children AS treated_children,
    events_query_6.treated_redose_children AS treated_redose_children,
    events_query_5.referred_children AS referred_children
FROM (
    (
        locations locations
        JOIN smc_plan_jurisdictions smc_plan_jurisdictions ON locations.jurisdiction_id = smc_plan_jurisdictions.jurisdiction_id
    )
LEFT JOIN LATERAL (
    SELECT
        events.id AS event_id,
        events.task_id,
        events.event_date,
        tasks.plan_identifier AS plan_id,
        tasks.status AS task_status,
        events.form_data ->> 'business_status' AS business_status,
        CASE WHEN ((events.event_type)::text = ANY (ARRAY['Family_Registration'::text, 'Register_Structure'::text])) THEN 1 ELSE 0 END AS found_structures
    FROM
    (
        events events
        LEFT JOIN tasks tasks ON (((tasks.identifier)::text = (events.task_id)::text))
    )
    WHERE (((locations.id)::text = events.structure_id)
    AND ((events.event_type)::text = ANY ((ARRAY['Family_Registration'::character varying, 'Register_Structure'::character varying])::text[])))
    ORDER BY event_date DESC
    LIMIT 1) events_query_1 ON (true)
LEFT JOIN LATERAL (
    SELECT
        events.id AS event_id,
        events.task_id,
        events.event_date,
        tasks.plan_identifier AS plan_id,
        tasks.status AS task_status,
        events.form_data ->> 'business_status' AS business_status
    FROM
        events events
    LEFT JOIN tasks tasks ON (((tasks.identifier)::text = (events.task_id)::text))
    WHERE ((locations.id)::text = events.structure_id)
    AND events.event_type NOT IN ('Update_Family_Member_Registration')
    AND (smc_plan_jurisdictions.plan_id)::text = (tasks.plan_identifier)::text
    ORDER BY
        CASE
            WHEN (events.event_type = 'mda_dispense' AND events.form_data ->> 'business_status' = 'Not Dispensed') THEN 100

            WHEN (events.event_type = 'mda_drug_reconciliation' AND events.form_data ->> 'business_status' = 'Complete') THEN 80

            WHEN (events.event_type = 'mda_adherence' AND events.form_data ->> 'business_status' = 'SPAQ Complete') THEN 70

            WHEN (events.event_type = 'mda_dispense' AND events.form_data ->> 'business_status' = 'Ineligible') THEN 51
            WHEN (events.event_type = 'mda_dispense' AND events.form_data ->> 'business_status' = 'SMC Complete') THEN 50

            WHEN (events.event_type = 'Family_Member_Registration' AND events.form_data ->> 'business_status' = 'Family Registered') THEN 4
            WHEN (events.event_type = 'Family_Member_Registration' AND events.form_data ->> 'business_status' IS NULL) THEN 3
            WHEN (events.event_type = 'Family_Member_Registration' AND events.form_data ->> 'business_status' = 'Not Visited') THEN 2
            WHEN (events.event_type = 'Family_Registration' AND events.form_data ->> 'business_status' = 'Not Visited') THEN 1
            ELSE 0 END DESC
    LIMIT 1
) events_query_2 ON (true)
LEFT JOIN LATERAL (
    SELECT
        COALESCE(COUNT(DISTINCT(events.base_entity_id)),0) AS eligible_children
    FROM
        events events
    LEFT JOIN tasks tasks ON (((tasks.identifier)::text = (events.task_id)::text))
    LEFT JOIN LATERAL (
        SELECT
            business_status AS task_status
        FROM
            tasks
        WHERE
            ((tasks.task_for)::text = events.base_entity_id)
        AND
            code = 'MDA Dispense'
    ) tasks_query_1 ON (true)
    WHERE
        (
            (events.event_type = 'mda_dispense' AND events.form_data ->> 'business_status' <> 'Ineligible')
        OR
            (
                events.event_type = 'Family_Member_Registration' AND events.form_data ->> 'business_status' = 'Family Registered'
            AND
                (
                    tasks_query_1.task_status IS NOT NULL
                AND
                    tasks_query_1.task_status <> ('Ineligible')
                )
            )
        )
    AND
        ((locations.id)::text = events.structure_id)
) events_query_3 ON (true)
LEFT JOIN LATERAL (
    SELECT
        COALESCE(COUNT(DISTINCT(events.base_entity_id)),0) AS treated_children
    FROM events
    WHERE events.event_type in ('mda_dispense')
    AND events.form_data ->> 'business_status' = 'SMC Complete'
    AND ((locations.id)::text = events.structure_id)
    AND (smc_plan_jurisdictions.plan_id)::text = (events.plan_id)::text
) events_query_4 ON (true)
LEFT JOIN LATERAL (
    SELECT
        COUNT(DISTINCT(sub_refer.base_entity_id)) AS referred_children
    FROM (
        SELECT
            base_entity_id,
            COALESCE(events.form_data ->> 'childHfReferred','0')::int AS refer_adhere,
            COALESCE(events.form_data ->> 'referred','0')::int AS refer_dispense
        FROM events
        WHERE events.event_type in ('mda_dispense','mda_adherence')
        AND ((locations.id)::text = events.structure_id)
        AND (smc_plan_jurisdictions.plan_id)::text = (events.plan_id)::text
    ) sub_refer
    WHERE (refer_adhere > 0 OR refer_dispense > 0)
) events_query_5 ON (true)
LEFT JOIN LATERAL (
    SELECT
        COALESCE(COUNT(form_data ->> 'number_of_additional_doses'),0) AS treated_redose_children
    FROM events
    WHERE event_type = 'mda_adherence'
    AND ((locations.id)::text = events.structure_id)
    AND events.form_data ->> 'number_of_additional_doses' > '0'
    AND (smc_plan_jurisdictions.plan_id)::text = (events.plan_id)::text
) events_query_6 ON (true)
LEFT JOIN LATERAL (
    SELECT
        tasks.identifier,
        tasks.server_version,
        tasks.plan_identifier,
        tasks.status,
        tasks.business_status
    FROM tasks tasks
    WHERE ((tasks.task_for)::text = (locations.id)::text)
    AND (smc_plan_jurisdictions.plan_id)::text = (tasks.plan_identifier)::text
    ORDER BY tasks.server_version DESC
    LIMIT 1
) tasks_query ON (true));

CREATE INDEX IF NOT EXISTS smc_structures_business_status_idx ON smc_structures (business_status);
CREATE INDEX IF NOT EXISTS smc_structures_event_date_idx ON smc_structures (event_date);
CREATE INDEX IF NOT EXISTS smc_structures_task_id_idx ON smc_structures (task_id);
CREATE INDEX IF NOT EXISTS smc_structures_plan_id_idx ON smc_structures (plan_id);
CREATE INDEX IF NOT EXISTS smc_structures_plan_jurisdiction_id_idx ON smc_structures (plan_id, jurisdiction_id);
CREATE INDEX IF NOT EXISTS smc_structures_structure_jurisdiction_idx ON smc_structures (jurisdiction_id);
CREATE INDEX IF NOT EXISTS smc_structures_geom_gix ON smc_structures USING GIST (structure_geometry);