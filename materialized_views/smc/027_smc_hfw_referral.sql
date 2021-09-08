SET schema 'reveal';

DROP MATERIALIZED VIEW IF EXISTS smc_hfw_referral;

CREATE MATERIALIZED VIEW IF NOT EXISTS smc_hfw_referral
AS
SELECT
    events.id AS event_id,
    jurisdiction.jurisdiction_name_path[1] AS country,
    jurisdiction.jurisdiction_name_path[2] AS province,
    jurisdiction.jurisdiction_name_path[3] AS district,
    jurisdiction.jurisdiction_name_path[4] AS catchment,
    jurisdiction.jurisdiction_name AS jurisdiction,
    jurisdiction.jurisdiction_id AS jurisdiction_id,
    events.provider_id AS username,
    events.date_created AS event_date,
    events.plan_id AS plan_id,
    plans.title AS plan_title,
    events.form_data ->> 'start_time' AS start_time,
    events.form_data ->> 'end_time' AS end_time,
    events.form_data ->> 'childFirstName' AS childFirstName,
    events.form_data ->> 'surnameOfChild' AS surnameOfChild,
    events.form_data ->> 'referralReason' AS referralReason,
    events.form_data ->> 'dateOfEvaluation' AS dateOfEvaluation,
    events.form_data ->> 'health_facility_visited' AS health_facility_visited,
    events.form_data ->> 'checklist' AS checklist,
    events.form_data ->> 'checklist_diagnosis' AS checklist_diagnosis,
    events.form_data ->> 'checklist_child_treated' AS checklist_child_treated,
    events.form_data ->> 'checklist_child_admitted' AS checklist_child_admitted,
    events.form_data ->> 'checklist_treatment_name_dose' AS checklist_treatment_name_dose,
    events.form_data ->> 'isChildHaveFever' AS isChildHaveFever,
    events.form_data ->> 'isChildWasTestedForMalaria' AS isChildWasTestedForMalaria,
    events.form_data ->> 'isRdtResult' AS isRdtResult,
    events.form_data ->> 'isChildAdmittedToHF' AS isChildAdmittedToHF,
    events.form_data ->> 'isChildWithConfirmedMalaria' AS isChildWithConfirmedMalaria,
    events.form_data ->> 'isChildNegativeRDT' AS isChildNegativeRDT,
    events.form_data ->> 'isChildSideEffects' AS isChildSideEffects,
    events.form_data ->> 'isChildEvaluatedForAdverseDrugReaction' AS isChildEvaluatedForAdverseDrugReaction,
    events.form_data ->> 'isNationalPVFormCompleted' AS isNationalPVFormCompleted,
    events.form_data ->> 'isChildAdmittedToHFOrHFForSAE' AS isChildAdmittedToHFOrHFForSAE,
    events.form_data ->> 'checklist_outcome' AS checklist_outcome,
    events.form_data ->> 'spray_area' AS spray_area
FROM reveal.events events
LEFT JOIN reveal.jurisdictions_materialized_view jurisdiction ON (events.location_id = jurisdiction.jurisdiction_id)
LEFT JOIN reveal.plans plans ON events.plan_id = plans.identifier
WHERE
    event_type = 'hfw_referral';
