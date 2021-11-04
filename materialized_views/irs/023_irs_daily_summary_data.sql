SET schema 'reveal';

DROP MATERIALIZED VIEW IF EXISTS irs_daily_summary_data CASCADE;

CREATE MATERIALIZED VIEW IF NOT EXISTS irs_daily_summary_data
AS
SELECT
    jurisdiction.jurisdiction_name_path[1] AS country,
    jurisdiction.jurisdiction_name_path[2] AS province,
    jurisdiction.jurisdiction_name_path[3] AS district,
    jurisdiction.jurisdiction_name_path[4] AS catchment,
    jurisdiction.jurisdiction_name AS jurisdiction,
    jurisdiction.jurisdiction_id AS jurisdiction_id,
    events.id AS event_id,
    events.plan_id,
    events.provider_id AS user_id,
    events.event_date,
    events.form_data ->> 'zone' AS zone,
    events.form_data ->> 'spray_areas' AS spray_areas,
    events.form_data ->> 'collection_date' AS collection_date,
    events.form_data ->> 'found' AS found,
    events.form_data ->> 'sprayed' AS sprayed,
    events.form_data ->> 'bottles_full' AS bottles_full,
    events.form_data ->> 'bottles_empty' AS bottles_empty,
    events.form_data ->> 'bottles_start' AS bottles_start,
    events.form_data ->> 'bottles_accounted' AS bottles_accounted,
    events.form_data ->> 'bottles_lostdamaged' AS bottles_lostdamaged,
    events.form_data ->> 'district_manager' AS district_manager,
    events.form_data ->> 'supervisor' AS supervisor,
    events.form_data ->> 'sprayop_code' AS sprayoperator_code,
    events.form_data ->> 'bottles_accounted_approve' AS bottles_accounted_approve,
    events.form_data ->> 'bottles_reasontext' AS bottles_reasontext
FROM reveal.events
JOIN reveal.irs_plans plans ON events.plan_id = plans.plan_id
JOIN reveal.plans_materialized_view jurisdiction ON events.plan_id = jurisdiction.plan_id AND events.location_id = jurisdiction.jurisdiction_id
WHERE event_type = 'daily_summary';
