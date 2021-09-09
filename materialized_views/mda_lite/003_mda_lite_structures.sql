SET schema 'reveal';

DROP MATERIALIZED VIEW IF EXISTS mda_lite_structures CASCADE;

CREATE MATERIALIZED VIEW IF NOT EXISTS mda_lite_structures
AS
SELECT DISTINCT ON (locations.id, events_query.task_id)
    public.uuid_generate_v5(
            '6ba7b810-9dad-11d1-80b4-00c04fd430c8',
            concat(locations.id, events_query.task_id)) AS id,

    locations.id AS structure_id,
    locations.jurisdiction_id AS structure_jurisdiction_id,
    locations.code AS structure_code,
    events_query.event_id AS event_id,
    events_query.plan_id AS plan_id,
    events_query.task_id AS task_id
FROM locations AS locations
         LEFT JOIN LATERAL (
    SELECT
        subq.event_id AS event_id,
        subq.task_id AS task_id,
        subq.plan_id AS plan_id
    FROM (
             SELECT
                 DISTINCT ON (tasks.identifier)
                 events.id AS event_id,
                 tasks.identifier AS task_id,
                 tasks.plan_identifier AS plan_id
             FROM tasks as tasks
                      LEFT JOIN events as events
                                ON tasks.identifier = events.task_id
                                    AND events.entity_type = 'Structure'
                                    AND events.event_type IN ('cdd_supervisor_daily_summary', 'tablet_accountability', 'cell_coordinator_daily_summary')
             WHERE locations.id = tasks.task_for
               AND tasks.status != 'Cancelled'
             ORDER BY tasks.identifier, events.form_data->'end' DESC
         ) AS subq
    ) AS events_query ON true
WHERE locations.status != 'Inactive';

CREATE INDEX IF NOT EXISTS mda_lite_structures_task_id_idx ON mda_lite_structures (task_id);
CREATE INDEX IF NOT EXISTS mda_lite_structures_plan_id_idx ON mda_lite_structures (plan_id);
CREATE INDEX IF NOT EXISTS mda_lite_structures_plan_jurisdiction_id_idx ON mda_lite_structures (plan_id, structure_jurisdiction_id);
CREATE INDEX IF NOT EXISTS mda_lite_structures_structure_jurisdiction_idx ON mda_lite_structures (structure_jurisdiction_id);
CREATE UNIQUE INDEX IF NOT EXISTS mda_lite_structures_structure_task_idx ON mda_lite_structures (structure_id, task_id);
CREATE UNIQUE INDEX IF NOT EXISTS mda_lite_structures_idx ON mda_lite_structures (id);

CREATE OR REPLACE VIEW mda_lite_wards_geojson
AS
SELECT
    public.uuid_generate_v5(
            '6ba7b810-9dad-11d1-80b4-00c04fd430c8',
            concat(ward_data.id, ward_data.jurisdiction_id, plans.plan_id)) AS id,
    ward_data.jurisdiction_id,
    plans.plan_id,
    ward_data.business_status,
    jsonb_build_object('type', 'Feature', 'id', ward_data.id, 'geometry', public.st_asgeojson(ward_data.geometry)::jsonb, 'properties', to_jsonb(ward_data.*) - 'jurisdiction_id'::text - 'jurisdiction_geometry'::text) AS geojson
FROM mda_lite_plans as plans
         LEFT JOIN LATERAL(
    SELECT
        locations.id,
        locations.created_at,
        locations.uid,
        locations.jurisdiction_id,
        locations.code,
        locations.type,
        locations.name,
        locations.status,
        locations.geometry,
        locations.geographic_level,
        locations.effective_start_date,
        locations.effective_end_date,
        locations.version,
        locations.server_version,
        COALESCE(loc_events.total_males, 0) as total_males,
        COALESCE(loc_events.total_females, 0) as total_females,
        COALESCE(loc_events.business_status, 'Not Visited') as business_status
    FROM locations as locations
             LEFT JOIN LATERAL(
        SELECT
            sum(COALESCE((events.form_data -> 'treated_male_1_to_4'::text) ->> 0, '0'::text)::integer + COALESCE((events.form_data -> 'treated_male_5_to_14'::text) ->> 0, '0'::text)::integer + COALESCE((events.form_data -> 'treated_male_above_15'::text) ->> 0, '0'::text)::integer) AS total_males,
            sum(COALESCE((events.form_data -> 'treated_female_1_to_4'::text) ->> 0, '0'::text)::integer + COALESCE((events.form_data -> 'treated_female_5_to_14'::text) ->> 0, '0'::text)::integer + COALESCE((events.form_data -> 'treated_female_above_15'::text) ->> 0, '0'::text)::integer) AS total_females,
            CASE
                WHEN count(*) = 0 THEN 'Not Visited'
                WHEN (count(*) - (count(*) filter (WHERE (events.form_data ->> 'task_complete') = 'Yes'))) = 0 THEN 'Complete'
                ELSE 'In Progress'
                END AS business_status
        FROM events
        WHERE events.event_type::text = ANY (ARRAY['tablet_accountability'::character varying, 'cdd_supervisor_daily_summary'::character varying, 'cell_coordinator_daily_summary'::character varying]::text[])
          AND events.entity_type = 'Structure'
          AND events.base_entity_id = locations.id
          AND events.plan_id = plans.plan_id
        GROUP BY events.plan_id, events.base_entity_id
        ) as loc_events ON true
    WHERE locations.id IS NOT NULL
      AND locations.jurisdiction_id IS NOT NULL
    ) as ward_data ON true;

