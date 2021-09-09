SET schema 'reveal';

DROP MATERIALIZED VIEW IF EXISTS daily_summary_event CASCADE;

CREATE MATERIALIZED VIEW IF NOT EXISTS daily_summary_event
AS
SELECT
  all_events.id,
  all_events.base_entity_id,
  all_events.location_id,
  all_events.event_type,
  all_events.provider_id,
  all_events.date_created,
  all_events.event_date,
  all_events.entity_type,
  all_events.task_id,
  all_events.team_id,
  all_events.server_version,
  all_events.plan_id,
  all_events.created_at,
  (all_events.form_data ->> 'collection_date')::date AS collection_date,
  ((all_events.form_data ->> 'bottles_full'::text))::integer AS bottles_full,
  ((all_events.form_data ->> 'bottles_accounted'::text))::integer AS bottles_accounted,
  ((all_events.form_data ->> 'bottles_empty'::text))::integer AS bottles_empty,
  ((all_events.form_data ->> 'bottles_lostdamaged'::text))::integer AS bottles_lostdamaged,
  ((all_events.form_data ->> 'bottles_start'::text))::integer AS bottles_start,
  ((all_events.form_data ->> 'found'::text))::integer AS daily_found,
  ((all_events.form_data ->> 'sprayed'::text))::integer AS daily_sprayed,
  COALESCE(all_events.form_data ->> 'sprayop_code'::text, 'unknown') AS sop
FROM (
  events all_events
JOIN (
    SELECT
      max(events.date_created) AS latest_date_created,
      events.location_id,
      events.plan_id,
      events.event_date,
      COALESCE(events.form_data ->> 'sprayop_code'::text,'unknown') AS sop,
      events.provider_id
    FROM events
    WHERE ((events.event_type)::text = 'daily_summary'::text)
    GROUP BY events.location_id, events.plan_id, events.event_date, (events.form_data ->> 'sprayop_code'::text), events.provider_id
  ) latest_event
  ON ((((latest_event.location_id)::text = (all_events.location_id)::text)
  AND ((latest_event.plan_id)::text = (all_events.plan_id)::text)
  AND (latest_event.event_date = all_events.event_date)
  AND (latest_event.sop = (all_events.form_data ->> 'sprayop_code'::text))
  AND ((latest_event.provider_id)::text = (latest_event.provider_id)::text) AND (latest_event.latest_date_created = all_events.date_created)))
)
WHERE ((all_events.event_type)::text = 'daily_summary'::text);

CREATE INDEX IF NOT EXISTS daily_summary_event_event_date_idx ON daily_summary_event (event_date);
CREATE INDEX IF NOT EXISTS daily_summary_event_location_id_idx ON daily_summary_event (location_id);
CREATE INDEX IF NOT EXISTS daily_summary_event_provider_id_idx ON daily_summary_event (provider_id);
CREATE INDEX IF NOT EXISTS daily_summary_event_plan_id_idx ON daily_summary_event (plan_id);
CREATE INDEX IF NOT EXISTS daily_summary_event_event_date_location_id_provider_id_plan_id_idx ON daily_summary_event (event_date, location_id, provider_id, plan_id);
CREATE INDEX IF NOT EXISTS daily_summary_event_event_date_location_id_plan_id_idx ON daily_summary_event (event_date, location_id, plan_id);
CREATE UNIQUE INDEX IF NOT EXISTS daily_summary_event_idx ON daily_summary_event (id);