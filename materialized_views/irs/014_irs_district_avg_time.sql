SET schema 'reveal';

DROP MATERIALIZED VIEW IF EXISTS irs_district_avg_time CASCADE;

CREATE MATERIALIZED VIEW IF NOT EXISTS irs_district_avg_time
AS
SELECT
  public.uuid_generate_v5('6ba7b810-9dad-11d1-80b4-00c04fd430c8'::uuid, concat(subq.district_id, subq.plan_id)) AS id,
  subq.plan_id,
  subq.district_id,
  to_char(avg((subq.start_time)::interval), 'HH24:MI'::text) AS start_time,
  to_char(avg((subq.end_time)::interval), 'HH24:MI'::text) AS end_time,
  to_char(avg((subq.end_time - subq.start_time)), 'HH24:MI'::text) AS field_duration,
  count(DISTINCT subq.event_date) AS days_worked
FROM (
    SELECT
      irs_spray_event.plan_id,
      irs_spray_event.district_id,
      irs_spray_event.event_date,
      min((irs_spray_event.start_time)::time without time zone) AS start_time,
      max((irs_spray_event.end_time)::time without time zone) AS end_time
    FROM irs_spray_event
    GROUP BY irs_spray_event.event_date, irs_spray_event.plan_id, irs_spray_event.district_id
) subq
GROUP BY subq.plan_id, subq.district_id;

CREATE INDEX IF NOT EXISTS irs_district_avg_time_plan_id_idx ON irs_district_avg_time (plan_id);
CREATE INDEX IF NOT EXISTS irs_district_avg_time_district_id_idx ON irs_district_avg_time (district_id);
CREATE UNIQUE INDEX IF NOT EXISTS irs_district_avg_time_idx ON irs_district_avg_time (id);
