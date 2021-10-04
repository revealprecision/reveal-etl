SET schema 'reveal';

 DROP MATERIALIZED VIEW IF EXISTS smc_family_data;

CREATE MATERIALIZED VIEW IF NOT EXISTS smc_family_data
AS
SELECT
    jurisdictions.jurisdiction_name_path[1] AS country,
    jurisdictions.jurisdiction_name_path[2] AS province,
    jurisdictions.jurisdiction_name_path[3] AS district,
    jurisdictions.jurisdiction_name_path[4] AS catchment,
    jurisdictions.jurisdiction_name AS jurisdiction,
    locations.jurisdiction_id AS jurisdiction_id,
    locations.id AS structure_id,
    public.st_astext(public.st_centroid(locations.geometry)) AS structure_lat_lon,
    family.baseentityid AS family_id,
    family.firstname AS family_name,
    family.identifiers ->> 'opensrp_id' AS family_code,
    registration_task.task_id AS registration_task_id,
    registration_task.plan_id AS registration_plan_id,
    registration_task.business_status AS registration_task_status,
    registration_task.event_id AS registration_event_id,
    registration_task.user_id AS registration_registered_by,
    registration_task.event_date AS registration_event_date,
    registration_task.form_data ->> 'start_time' AS registration_start_time,
    registration_task.form_data ->> 'end_time' AS registration_end_time,
    registration_task.form_data ->> 'compoundPart' AS part_of_compound,
    COALESCE(registration_task.form_data ->> 'compoundStructure', 'n/a') AS compoundStructure,
    COALESCE(registration_task.form_data ->> 'numCompoundStructures', '0') AS numCompoundStructures,
    headofhouse.firstname AS head_of_household_name,
    headofhouse_event.form_data ->> 'start_time' AS headofhouse_registration_start_time,
    headofhouse_event.form_data ->> 'end_time' AS headofhouse_registration_end_time,
    elegible_children.eligible_children
FROM
    reveal.clients family
JOIN reveal.locations ON family.attributes ->> 'residence' = locations.id
JOIN reveal.jurisdictions_materialized_view jurisdictions ON (locations.jurisdiction_id = jurisdictions.jurisdiction_id)
LEFT JOIN LATERAL (
    SELECT
        tasks.identifier AS task_id,
        tasks.plan_identifier AS plan_id,
        tasks.business_status,
        events.id AS event_id,
        events.provider_id AS user_id,
        events.event_date::date AS event_date,
        events.form_data AS form_data
    FROM
        reveal.tasks
    JOIN reveal.events ON tasks.identifier = events.task_id AND events.event_type = 'Family_Registration'
    WHERE locations.id = tasks.task_for
    AND tasks.code = 'RACD Register Family'
    AND tasks.business_status = 'Complete'
    ORDER BY execution_start_date
    LIMIT 1
) registration_task ON TRUE
LEFT JOIN reveal.clients headofhouse ON (family.relationships -> 'family_head' ->> 0)::text = headofhouse.baseentityid::text
LEFT JOIN reveal.events headofhouse_event ON headofhouse_event.event_type = 'Family_Member_Registration' AND headofhouse_event.base_entity_id = headofhouse.baseentityid
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
        events.base_entity_id IN (
            SELECT
                baseentityid
            FROM clients
            WHERE relationships -> 'family' ->> 0 = family.baseentityid
            AND baseentityid <> headofhouse.baseentityid
            AND attributes ->> 'dateRemoved' IS NULL
        )
    AND
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
) elegible_children ON TRUE
WHERE
    family.lastname  = 'Family';