SET schema 'reveal';

DROP MATERIALIZED VIEW IF EXISTS irs_sop_date_performance CASCADE;

CREATE MATERIALIZED VIEW IF NOT EXISTS irs_sop_date_performance
AS
SELECT
    subq.id,
    subq.plan_id,
    subq.district_id,
    subq.data_collector,
    subq.location_id,
    subq.sop,
    subq.event_date,
    subq.found,
    subq.sprayed,
    subq.refused,
    subq.other_reason,
    subq.not_sprayed,
    subq.start_time,
    subq.end_time,
    subq.field_duration,
    COALESCE(daily_summary.bottles_full, 0) AS bottles_full,
    COALESCE(daily_summary.bottles_accounted, 0) AS bottles_accounted,
    COALESCE(daily_summary.bottles_empty, 0) AS bottles_empty,
    COALESCE(daily_summary.bottles_lostdamaged, 0) AS bottles_lostdamaged,
    COALESCE(daily_summary.bottles_start, 0) AS bottles_start,
    COALESCE(daily_summary.daily_found, 0) AS daily_found,
    COALESCE(daily_summary.daily_sprayed, 0) AS daily_sprayed,
    COALESCE(daily_summary.event_date, (subq.event_date)::timestamp without time zone) AS daily_event_date,
    COALESCE((subq.sprayed - daily_summary.daily_sprayed), subq.sprayed) AS sprayed_diff,
    COALESCE((subq.found - daily_summary.daily_found), subq.found) AS found_diff,
        CASE
            WHEN (((subq.found - daily_summary.daily_found) = 0) AND ((subq.sprayed - daily_summary.daily_sprayed) = 0)) THEN true
            ELSE false
        END AS data_quality_check
FROM (( SELECT public.uuid_generate_v5('6ba7b810-9dad-11d1-80b4-00c04fd430c8'::uuid, concat(irs_spray_event.district_id, irs_spray_event.plan_id, irs_spray_event.data_collector, irs_spray_event.sop, irs_spray_event.location_id, irs_spray_event.event_date)) AS id,
             irs_spray_event.plan_id,
             irs_spray_event.district_id,
             irs_spray_event.data_collector,
             irs_spray_event.location_id,
             irs_spray_event.sop,
             irs_spray_event.event_date,
             sum(irs_spray_event.found) AS found,
             sum(irs_spray_event.sprayed) AS sprayed,
             sum(irs_spray_event.refused) AS refused,
             sum(irs_spray_event.other_reason) AS other_reason,
             (sum(irs_spray_event.refused) + sum(irs_spray_event.other_reason)) AS not_sprayed,
             to_char(min((irs_spray_event.start_time)::timestamp without time zone), 'HH24:MI'::text) AS start_time,
             to_char(max((irs_spray_event.end_time)::timestamp without time zone), 'HH24:MI'::text) AS end_time,
             to_char((max((irs_spray_event.end_time)::timestamp without time zone) - min((irs_spray_event.start_time)::timestamp without time zone)), 'HH24:MI'::text) AS field_duration
            FROM irs_spray_event
           GROUP BY irs_spray_event.event_date, irs_spray_event.sop, irs_spray_event.data_collector, irs_spray_event.district_id, irs_spray_event.plan_id, irs_spray_event.location_id) subq
      LEFT JOIN daily_summary_event daily_summary ON ((((daily_summary.location_id)::text = (subq.location_id)::text) AND (daily_summary.event_date = subq.event_date) AND ((daily_summary.plan_id)::text = (subq.plan_id)::text) AND ((daily_summary.provider_id)::text = (subq.data_collector)::text) AND (daily_summary.sop = subq.sop))));
