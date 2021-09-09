SET schema 'reveal';

DROP MATERIALIZED VIEW IF EXISTS smc_ccd_supervisor_checklist;

CREATE MATERIALIZED VIEW IF NOT EXISTS smc_ccd_supervisor_checklist
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
    events.form_data ->> 'settlement' AS settlement,
    events.form_data ->> 'dayOfCycle' AS dayOfCycle,
    events.form_data ->> 'preparedRequiredMaterials' AS preparedRequiredMaterials,
    events.form_data ->> 'wearingUniform' AS wearingUniform,
    events.form_data ->> 'giveCaregiverInfo' AS giveCaregiverInfo,
    events.form_data ->> 'detAge' AS detAge,
    events.form_data ->> 'exclIneligibl' AS exclIneligibl,
    events.form_data ->> 'askCaregiverQuestionsForEligibility' AS askCaregiverQuestionsForEligibility,
    events.form_data ->> 'excludeWithFeverOrSick' AS excludeWithFeverOrSick,
    events.form_data ->> 'refersSickOrWithSideEffects' AS refersSickOrWithSideEffects,
    events.form_data ->> 'hygiene' AS hygiene,
    events.form_data ->> 'dotObs' AS dotObs,
    events.form_data ->> 'dotDose' AS dotDose,
    events.form_data ->> 'observeIfVomits' AS observeIfVomits,
    events.form_data ->> 'redosesIfVomit' AS redosesIfVomit,
    events.form_data ->> 'completesTallySheet' AS completesTallySheet,
    events.form_data ->> 'completesChildRecordCard' AS completesChildRecordCard,
    events.form_data ->> 'adherence' AS adherence,
    events.form_data ->> 'explainChildRecordAndSafeKeeping' AS explainChildRecordAndSafeKeeping,
    events.form_data ->> 'explainWhenToTakeToFacility' AS explainWhenToTakeToFacility,
    events.form_data ->> 'usesJobAid' AS usesJobAid,
    events.form_data ->> 'feedback' AS feedback,
    events.form_data ->> 'mentoringProvided' AS mentoringProvided,
    events.form_data ->> 'noted' AS noted
FROM reveal.events events
LEFT JOIN reveal.locations locations ON (events.structure_id = locations.uid)
LEFT JOIN reveal.jurisdictions_materialized_view jurisdiction ON (locations.jurisdiction_id = jurisdiction.jurisdiction_id)
LEFT JOIN reveal.plans plans ON events.plan_id = plans.identifier
WHERE
    event_type = 'ccd_supervisor_checklist';