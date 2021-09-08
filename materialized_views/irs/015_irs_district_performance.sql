SET schema 'reveal';

DROP MATERIALIZED VIEW IF EXISTS irs_district_performance CASCADE;

CREATE MATERIALIZED VIEW IF NOT EXISTS irs_district_performance
AS
SELECT
    subq.id,
    subq.plan_id,
    subq.district_id,
    subq.district_name,
    subq.found,
    subq.sprayed,
    subq.refused,
    subq.other_reason,
    subq.bottles_full,
    subq.bottles_accounted,
    subq.bottles_empty,
    subq.bottles_lostdamaged,
    subq.bottles_start,
    subq.daily_found,
    subq.daily_sprayed,
    subq.sprayed_diff,
    subq.found_diff,
    avg_time.start_time,
    avg_time.end_time,
    avg_time.field_duration,
    COALESCE(((subq.found)::numeric / (NULLIF(avg_time.days_worked, 0))::numeric), (subq.found)::numeric) AS avg_found,
    COALESCE(((subq.sprayed)::numeric / (NULLIF(avg_time.days_worked, 0))::numeric), (subq.sprayed)::numeric) AS avg_sprayed,
    COALESCE(((subq.refused)::numeric / (NULLIF(avg_time.days_worked, 0))::numeric), (subq.refused)::numeric) AS avg_refused,
    CASE
        WHEN ((subq.sprayed > 0) AND (avg_time.days_worked = 0)) THEN (1)::bigint
        ELSE avg_time.days_worked
    END AS days_worked,
    CASE
        WHEN (subq.daily_sprayed = 0) THEN (0)::numeric
        ELSE ((subq.bottles_empty)::numeric / (subq.daily_sprayed)::numeric)
    END AS usage_rate,
    CASE
        WHEN ((subq.found_diff = 0) AND (subq.sprayed_diff = 0)) THEN true
        ELSE false
    END AS data_quality_check
FROM (
        (
            WITH aggregates AS (
                    SELECT
                        public.uuid_generate_v5('6ba7b810-9dad-11d1-80b4-00c04fd430c8'::uuid, concat(spray_event.district_id, spray_event.plan_id)) AS id,
                        spray_event.plan_id,
                        spray_event.district_id,
                        spray_event.district_name,
                        sum(spray_event.found) AS found,
                        sum(spray_event.sprayed) AS sprayed,
                        sum(spray_event.refused) AS refused,
                        sum(spray_event.other_reason) AS other_reason,
                        COALESCE(sum(daily_summary.bottles_full), (0)::bigint) AS bottles_full,
                        COALESCE(sum(daily_summary.bottles_accounted), (0)::bigint) AS bottles_accounted,
                        COALESCE(sum(daily_summary.bottles_empty), (0)::bigint) AS bottles_empty,
                        COALESCE(sum(daily_summary.bottles_lostdamaged), (0)::bigint) AS bottles_lostdamaged,
                        COALESCE(sum(daily_summary.bottles_start), (0)::bigint) AS bottles_start,
                        COALESCE(sum(daily_summary.daily_found), (0)::bigint) AS daily_found,
                        COALESCE(sum(daily_summary.daily_sprayed), (0)::bigint) AS daily_sprayed
                    FROM (irs_spray_event spray_event
                    JOIN daily_summary_event daily_summary ON ((((daily_summary.location_id)::text = (spray_event.location_id)::text) AND ((daily_summary.plan_id)::text = (daily_summary.plan_id)::text) AND ((daily_summary.collection_date)::date = (spray_event.event_date)::date) AND ((daily_summary.provider_id)::text = (spray_event.data_collector)::text) AND (daily_summary.sop = spray_event.sop))))
                    GROUP BY spray_event.plan_id, spray_event.district_id, spray_event.district_name
            )
            SELECT
                aggregates.id,
                aggregates.plan_id,
                aggregates.district_id,
                aggregates.district_name,
                aggregates.found,
                aggregates.sprayed,
                aggregates.refused,
                aggregates.other_reason,
                aggregates.bottles_full,
                aggregates.bottles_accounted,
                aggregates.bottles_empty,
                aggregates.bottles_lostdamaged,
                aggregates.bottles_start,
                aggregates.daily_found,
                aggregates.daily_sprayed,
                COALESCE((aggregates.sprayed - aggregates.daily_sprayed), aggregates.sprayed) AS sprayed_diff,
                COALESCE((aggregates.found - aggregates.daily_found), aggregates.found) AS found_diff
            FROM aggregates
        ) subq
        LEFT JOIN irs_district_avg_time avg_time ON ((((avg_time.plan_id)::text = (subq.plan_id)::text) AND ((avg_time.district_id)::text = (subq.district_id)::text))));

CREATE INDEX IF NOT EXISTS irs_district_performance_plan_id_idx ON irs_district_performance (plan_id);
CREATE INDEX IF NOT EXISTS irs_district_performance_district_id_idx ON irs_district_performance (district_id);
CREATE UNIQUE INDEX IF NOT EXISTS irs_district_performance_idx ON irs_district_performance (id);