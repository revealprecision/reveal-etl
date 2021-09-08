SET schema 'reveal';

DROP MATERIALIZED VIEW IF EXISTS irs_spray_event CASCADE;

CREATE MATERIALIZED VIEW IF NOT EXISTS irs_spray_event
AS
SELECT
    event.id,
    date(event.event_date) AS event_date,
    event.task_id,
    (event.form_data ->> 'end_time'::text) AS end_time,
    (event.form_data ->> 'start_time'::text) AS start_time,
    (event.form_data ->> 'sprayop_code'::text) AS sop,
    event.provider_id AS data_collector,
    (event.form_data ->> 'notsprayed_reason'::text) AS notsprayed_reason,
    event.location_id,
    event.base_entity_id,
    datediff('second'::character varying, ((event.form_data ->> 'start'::text))::timestamp without time zone, ((event.form_data ->> 'end'::text))::timestamp without time zone) AS field_duration,
    CASE
        WHEN ((event.form_data ->> 'eligibility'::text) = 'eligible'::text) THEN 1
        ELSE 1
    END AS found,
    CASE
        WHEN ((event.form_data ->> 'structure_sprayed'::text) = 'yes'::text) THEN 1
        ELSE 0
    END AS sprayed,
    CASE
        WHEN (((event.form_data ->> 'structure_sprayed'::text) <> 'yes'::text) AND ((event.form_data ->> 'notsprayed_reason'::text) = 'refused'::text)) THEN 1
        ELSE 0
    END AS refused,
    CASE
        WHEN (((event.form_data ->> 'structure_sprayed'::text) <> 'yes'::text) AND ((event.form_data ->> 'notsprayed_reason'::text) <> 'refused'::text)) THEN 1
        ELSE 0
    END AS other_reason,
    materialized_jur.jurisdiction_parent_id,
    materialized_jur.jurisdiction_name,
    materialized_jur.jurisdiction_depth,
    materialized_jur.jurisdiction_path,
    materialized_jur.jurisdiction_name_path,
    tasks_query.plan_identifier AS plan_id,
    tasks_query.code,
    jurisdiction_query.id AS district_id,
    jurisdiction_query.name AS district_name
FROM (((events event
LEFT JOIN jurisdictions_tree materialized_jur ON (((event.location_id)::text = (materialized_jur.jurisdiction_id)::text)))
LEFT JOIN tasks tasks_query ON (((event.task_id)::text = (tasks_query.identifier)::text)))
LEFT JOIN jurisdictions jurisdiction_query ON (((jurisdiction_query.id)::text = (materialized_jur.jurisdiction_path[3])::text)))
WHERE (((event.event_type)::text = 'Spray'::text) AND ((event.form_data ->> 'eligibility'::text) <> 'notEligible'::text) AND (event.provider_id IS NOT NULL) AND ((event.form_data ->> 'sprayop_code'::text) IS NOT NULL) AND (tasks_query.plan_identifier IS NOT NULL) AND ((tasks_query.plan_identifier)::text <> ''::text) AND (jurisdiction_query.id IS NOT NULL) AND (jurisdiction_query.name IS NOT NULL));


CREATE INDEX IF NOT EXISTS irs_spray_event_plan_id_idx ON irs_spray_event (plan_id);
CREATE INDEX IF NOT EXISTS irs_spray_event_district_id_idx ON irs_spray_event (district_id);
CREATE INDEX IF NOT EXISTS irs_spray_event_data_collector_idx ON irs_spray_event (data_collector);
CREATE INDEX IF NOT EXISTS irs_spray_event_sop_idx ON irs_spray_event (sop);
CREATE UNIQUE INDEX IF NOT EXISTS irs_spray_event_idx ON irs_spray_event (id);