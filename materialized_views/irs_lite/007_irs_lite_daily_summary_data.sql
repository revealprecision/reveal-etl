SET schema 'reveal';

DROP MATERIALIZED VIEW IF EXISTS irs_lite_daily_summary_data CASCADE;

CREATE MATERIALIZED VIEW IF NOT EXISTS irs_lite_daily_summary_data
AS
SELECT
    jurisdiction.jurisdiction_name_path[1] AS country,
    jurisdiction.jurisdiction_name_path[2] AS province,
    jurisdiction.jurisdiction_name_path[3] AS district,
    jurisdiction.jurisdiction_name_path[4] AS catchment,
    jurisdiction.jurisdiction_name AS jurisdiction,
    events_list.jurisdiction_id,
    events_list.plan_id,
    events_list.collection_date,
    events_data.event_date,
    events_data.provider_id AS user_id,
    events_data.form_data ->> 'zone' AS zone,
    events_data.form_data ->> 'spray_areas' AS spray_areas,
    events_data.form_data ->> 'found' AS found,
    events_data.form_data ->> 'sprayed' AS sprayed,
    events_data.form_data ->> 'bottles_full' AS bottles_full,
    events_data.form_data ->> 'bottles_empty' AS bottles_empty,
    events_data.form_data ->> 'bottles_start' AS bottles_start,
    events_data.form_data ->> 'bottles_accounted' AS bottles_accounted,
    events_data.form_data ->> 'bottles_lostdamaged' AS bottles_lostdamaged,
    events_data.form_data ->> 'district_manager' AS district_manager,
    events_data.form_data ->> 'supervisor' AS supervisor,
    events_data.form_data ->> 'sprayop_code' AS sprayoperator_code
FROM (
    SELECT
        events.location_id AS jurisdiction_id,
        events.plan_id,
        events.form_data ->> 'collection_date' AS collection_date
    FROM reveal.events
    WHERE event_type = 'daily_summary'
    GROUP BY events.location_id, events.plan_id, events.form_data ->> 'collection_date'
) events_list
JOIN reveal.irs_lite_plans plans ON events_list.plan_id = plans.plan_id
JOIN reveal.plans_materialized_view jurisdiction ON events_list.plan_id = jurisdiction.plan_id AND events_list.jurisdiction_id = jurisdiction.jurisdiction_id
JOIN LATERAL (
    SELECT
        events.event_date,
        events.provider_id AS provider_id,
        events.form_data
    FROM events
    WHERE events.location_id = events_list.jurisdiction_id
    AND events.plan_id = events_list.plan_id
    AND event_type = 'daily_summary'
    AND events_list.collection_date = events_list.collection_date
    ORDER BY events.event_date DESC
    LIMIT 1
) events_data ON TRUE
ORDER BY events_list.collection_date DESC
;