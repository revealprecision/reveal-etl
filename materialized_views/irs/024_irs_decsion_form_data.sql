SET schema 'reveal';

DROP MATERIALIZED VIEW IF EXISTS irs_decision_form_data CASCADE;

CREATE MATERIALIZED VIEW IF NOT EXISTS irs_decision_form_data
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
    events.form_data ->> 'village' AS village,
    events.form_data ->> 'spray_areas' AS spray_areas,
    events.form_data ->> 'supervisor' AS supervisor,
    events.form_data ->> 'health_facility' AS health_facility,
    events.form_data ->> 'reasonnotsprayed' AS reasonnotsprayed,
    events.form_data ->> 'return' AS return,
    events.form_data ->> 'reason_noteffective' AS reason_noteffective,
    events.form_data ->> 'reason_notreturning' AS reason_notreturning,
    events.form_data ->> 'spray_effectiveness' AS spray_effectiveness,
    events.form_data ->> 'structures_ground' AS structures_ground,
    events.form_data ->> 'structures_sprayed' AS structures_sprayed,
    events.form_data ->> 'structures_tospray' AS structures_tospray,
    events.form_data ->> 'structures_remaining' AS structures_remaining,
    events.form_data ->> 'structures_toreach90' AS structures_toreach90
FROM reveal.events
JOIN reveal.irs_plans plans ON events.plan_id = plans.plan_id
JOIN reveal.plans_materialized_view jurisdiction ON events.plan_id = jurisdiction.plan_id AND events.location_id = jurisdiction.jurisdiction_id
WHERE event_type = 'irs_sa_decision';


