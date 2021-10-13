SET schema 'reveal';

DROP MATERIALIZED VIEW IF EXISTS irs_lite_structures CASCADE;

CREATE MATERIALIZED VIEW IF NOT EXISTS irs_lite_structures
AS
SELECT DISTINCT ON (locations.id, events_query.task_id)
    public.uuid_generate_v5(
        '6ba7b810-9dad-11d1-80b4-00c04fd430c8',
        concat(locations.id, events_query.task_id)) AS id,
    locations.id AS structure_id,
    locations.jurisdiction_id AS jurisdiction_id,
    locations.code AS structure_code,
    locations.name AS structure_name,
    locations.type AS structure_type,
    locations.status AS structure_status,
    locations.geometry AS geometry,
    events_query.event_id AS event_id,
    events_query.task_id AS task_id,
    events_query.task_status AS task_status,
    events_query.plan_id AS plan_id,
    events_query.event_date AS event_date,
    COALESCE(events_query.business_status, 'Not Visited') AS business_status,
    COALESCE(structure_setting_count.totStruct, '0') AS totStruct,
    COALESCE(structure_setting_target.target_flag, '0') AS target_flag
FROM locations
JOIN plan_jurisdiction ON locations.jurisdiction_id = plan_jurisdiction.jurisdiction_id
JOIN irs_lite_plans ON plan_jurisdiction.plan_id = irs_lite_plans.plan_id
LEFT JOIN LATERAL (
    SELECT
        subq.event_id AS event_id,
        subq.task_id AS task_id,
        subq.task_status AS task_status,
        subq.event_date AS event_date,
        subq.plan_id AS plan_id,
        subq.business_status AS business_status
    FROM (
        SELECT
            DISTINCT ON (tasks.identifier)
            events.id AS event_id,
            tasks.identifier AS task_id,
            tasks.status AS task_status,
            events.event_date AS event_date,
            tasks.plan_identifier AS plan_id,
            COALESCE (events.form_data -> 'business_status' ->> 0, 'Not Visited')::text AS business_status
        FROM tasks
        LEFT JOIN events
            ON tasks.identifier = events.task_id
        AND events.entity_type = 'Structure'
        AND events.event_type = 'irs_lite_verification'
        WHERE locations.id = tasks.task_for
        AND tasks.status != 'Cancelled'
        AND tasks.plan_identifier = irs_lite_plans.plan_id
        ORDER BY tasks.identifier, events.form_data->'end' DESC
    ) AS subq
) AS events_query ON true
LEFT JOIN LATERAL (
    SELECT
        key as structure_id,
        COALESCE(data ->> 0, '0')::INTEGER as totStruct
    FROM opensrp_settings
    WHERE identifier = 'jurisdiction_metadata-structures'
    AND locations.id = opensrp_settings.key
    LIMIT 1
) AS structure_setting_count ON true
LEFT JOIN LATERAL (
    SELECT
        key as structure_id,
        COALESCE(data ->> 0, '0')::INTEGER as target_flag
    FROM opensrp_settings
    WHERE identifier = 'jurisdiction_metadata-target'
    AND locations.id = opensrp_settings.key
    LIMIT 1
) AS structure_setting_target ON true
WHERE locations.status != 'Inactive'
AND locations.geographic_level = 4;

CREATE INDEX IF NOT EXISTS irs_lite_structures_business_status_idx ON irs_lite_structures (business_status);
CREATE INDEX IF NOT EXISTS irs_lite_structures_event_date_idx ON irs_lite_structures (event_date);
CREATE INDEX IF NOT EXISTS irs_lite_structures_task_id_idx ON irs_lite_structures (task_id);
CREATE INDEX IF NOT EXISTS irs_lite_structures_plan_id_idx ON irs_lite_structures (plan_id);
CREATE INDEX IF NOT EXISTS irs_lite_structures_plan_jurisdiction_id_idx ON irs_lite_structures (plan_id, jurisdiction_id);
CREATE INDEX IF NOT EXISTS irs_lite_structures_structure_jurisdiction_idx ON irs_lite_structures (jurisdiction_id);
CREATE INDEX IF NOT EXISTS irs_lite_structures_geom_gix ON irs_lite_structures USING GIST (geometry);
CREATE UNIQUE INDEX IF NOT EXISTS irs_lite_structures_structure_task_idx ON irs_lite_structures (structure_id, task_id);
CREATE UNIQUE INDEX IF NOT EXISTS irs_lite_structures_idx ON irs_lite_structures (id);