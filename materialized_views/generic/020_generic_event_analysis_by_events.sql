SET schema 'reveal';

DROP MATERIALIZED VIEW IF EXISTS generic_event_analysis_by_events CASCADE;

CREATE MATERIALIZED VIEW IF NOT EXISTS generic_event_analysis_by_events
AS
SELECT
    events.id AS event_id,
    events.plan_id AS plan_id,
    events.structure_id as structure_id,
    events.location_id as jurisdiction_id,
    events.event_type AS event_type,
    events.provider_id AS user_id,
    form_data ->> 'start_time' AS start_time,
    form_data ->> 'end_time' AS end_time,
    (form_data ->> 'end_time')::timestamp - (form_data ->> 'start_time')::timestamp AS capture_time,
    events.details ->> 'appVersionName' AS appVersion
FROM
    reveal.events events
WHERE
    event_type NOT IN ('reset_task');