SET schema 'reveal';

DROP MATERIALIZED VIEW IF EXISTS irs_lite_verification_data CASCADE;

CREATE MATERIALIZED VIEW IF NOT EXISTS irs_lite_verification_data
AS
SELECT
    jurisdiction.jurisdiction_name_path[1] AS country,
    jurisdiction.jurisdiction_name_path[2] AS province,
    jurisdiction.jurisdiction_name_path[3] AS district,
    jurisdiction.jurisdiction_name_path[4] AS catchment,
    jurisdiction.jurisdiction_name AS jurisdiction,
    jurisdiction.jurisdiction_id AS jurisdiction_id,
    events.structure_id,
    events.plan_id,
    events.provider_id AS user_id,
    events.event_date,
    events.form_data ->> 'date' AS collection_date,
    events.form_data ->> 'zone' AS zone,
    events.form_data ->> 'Villagename' AS Villagename,
    events.form_data ->> 'headman' AS headman,
    events.form_data ->> 'visited' AS visited,
    events.form_data ->> 'dateComm' AS date_community_meeting,
    events.form_data ->> 'mobilized' AS mobilized,
    events.form_data ->> 'sprayDate' AS spray_date,
    events.form_data ->> 'sprayDays' AS spray_days,
    events.form_data ->> 'sprayTeams' AS spray_teams,
    events.form_data ->> 'supervisor' AS supervisor,
    events.form_data ->> 'anotherHeadMan' AS another_head_man,
    events.form_data ->> 'business_status' AS business_status
FROM reveal.events
JOIN reveal.irs_lite_plans plans ON events.plan_id = plans.plan_id
JOIN reveal.plans_materialized_view jurisdiction ON events.plan_id = jurisdiction.plan_id AND events.location_id = jurisdiction.jurisdiction_id
WHERE event_type = 'irs_lite_verification';