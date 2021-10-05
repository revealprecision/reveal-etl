SET schema 'reveal';

 DROP MATERIALIZED VIEW IF EXISTS smc_individual_data;

CREATE MATERIALIZED VIEW IF NOT EXISTS smc_individual_data
AS
SELECT
    -- jurisdiction
    jurisdiction.jurisdiction_name_path[1] AS country,
    jurisdiction.jurisdiction_name_path[2] AS province,
    jurisdiction.jurisdiction_name_path[3] AS district,
    jurisdiction.jurisdiction_name_path[4] AS catchment,
    jurisdiction.jurisdiction_name AS jurisdiction,
    jurisdiction.jurisdiction_id AS jurisdiction_id,

    -- registration info
    reg_event.id AS registration_event_id,
    reg_event.plan_id AS registration_plan_id,
    reg_event.provider_id AS registration_registered_by,
    reg_event.form_data ->> 'start_time' AS registration_start_time,
    reg_event.form_data ->> 'end_time' AS registration_end_time,

    -- individual info
    individual.baseentityid AS person_id,
    individual.datecreated AS date_created,
    individual.firstname AS first_name,
    individual.lastname AS last_name,
    family.firstname AS family_name,
    individual.relationships -> 'family' ->> 0 AS family_id,
    individual.identifiers ->> 'opensrp_id' AS household_id,
    individual.gender AS gender,
    individual.birthdate AS birth_date,
    CASE WHEN individual.birthdateapprox = 't' THEN 'True'
       ELSE 'False' END AS birth_date_approx,
    COALESCE(individual.attributes ->> 'age_entered', reg_event.form_data ->> 'job_aid') AS age_entered,
    reg_event.form_data ->> 'job_aid' AS unkown_selection,
    CASE
        WHEN (individual.birthdateapprox = 'f' AND (individual.birthdate + INTERVAL '3 month') > NOW()) THEN '<3 mnth'
        WHEN (individual.birthdateapprox = 'f' AND (individual.birthdate + INTERVAL '1 year') > NOW()) THEN '3to12 mnth'
        WHEN (individual.birthdateapprox = 'f' AND (individual.birthdate + INTERVAL '5 year') > NOW()) THEN '12to59 mnth'
        WHEN (individual.birthdateapprox = 'f' AND (individual.birthdate + INTERVAL '5 year') < NOW()) THEN '>60 mnth'

        WHEN (individual.birthdateapprox = 't' AND COALESCE(individual.attributes ->> 'age_entered', 0::text)::numeric < 1) THEN '3to12 mnth'
        WHEN (individual.birthdateapprox = 't' AND COALESCE(individual.attributes ->> 'age_entered', 0::text)::numeric = 1 AND reg_event.form_data ->> 'job_aid' = 'threeToTwelve') THEN '3to12 mnth'
        WHEN (individual.birthdateapprox = 't' AND COALESCE(individual.attributes ->> 'age_entered', 0::text)::numeric >= 1 AND COALESCE(individual.attributes ->> 'age_entered', 0::text)::numeric < 6) THEN '12to59 mnth'
        WHEN (individual.birthdateapprox = 't' AND COALESCE(individual.attributes ->> 'age_entered', 0::text)::numeric > 5) THEN '>60 mnth'
        ELSE 'unknown'
    END AS age_category,
    individual.attributes ->> 'residence' AS structure_id,
    CASE WHEN family.relationships -> 'family_head' ->> 0 = individual.baseentityid THEN 'parent'
        ELSE 'child' END AS individual_type,
    reg_event.form_data ->> 'child_stay_perm' AS child_stay_perm,

    -- dispense info
    dis_task.plan_identifier AS dispense_plan_id,
    dis_task.identifier AS dispense_task_id,
    dis_task.business_status AS dispense_task_status,
    COALESCE(dispense_event_id, 'None') AS dispense_event_id,
    COALESCE(dispense_start_time, 'None')::text AS dispense_start_time,
    COALESCE(dispense_end_time, 'None')::text AS dispense_end_time,
    COALESCE(currently_present, 0::text) AS currently_present,
    COALESCE(child_present_days, 0::text) AS child_present_days,
    COALESCE(dispense_child_sick, 0::text) AS dispense_child_sick,
    COALESCE(dispense_allergic_severe, 0::text) AS dispense_allergic_severe,
    COALESCE(dispense_administeredSpaq, ''::text) AS dispense_administeredSpaq,
    COALESCE(dispense_child_record, 0::text) AS dispense_child_record,
    COALESCE(dispense_referred, 0::text) AS dispense_referred,
    COALESCE(dispense_referral_reason, ''::text) AS dispense_referral_reason,
    COALESCE(dispense_referral_reason_other, ''::text) AS dispense_referral_reason_other,

    -- adherence info
    COALESCE(adh_task.identifier, 'None') AS redose_task_id,
    COALESCE(adh_task.business_status, 'None') AS redose_task_status,
    COALESCE(adh_event.redose_event_id, 'None') AS redose_event_id,
    COALESCE(adh_event.redose_start_time, 'None')::text AS redose_start_time,
    COALESCE(adh_event.redose_end_time, 'None')::text AS redose_end_time,
    adh_event.redose_additional_doses AS redose_additional_doses,
    adh_event.redose_referred AS redose_referred,
    adh_event.redose_referral_reason AS redose_referral_reason,
    adh_event.redose_referral_reason_other AS redose_referral_reason_other
FROM reveal.clients individual
JOIN reveal.clients family ON individual.relationships -> 'family' ->> 0 = family.baseentityid
JOIN reveal.locations locations ON (individual.attributes ->> 'residence' = locations.uid)
JOIN reveal.jurisdictions_materialized_view jurisdiction ON (locations.jurisdiction_id = jurisdiction.jurisdiction_id)
JOIN reveal.tasks dis_task ON dis_task.task_for = individual.baseentityid AND dis_task.code = 'MDA Dispense'
LEFT JOIN reveal.events reg_event ON individual.baseentityid = reg_event.base_entity_id AND reg_event.event_type = 'Family_Member_Registration'
LEFT JOIN LATERAL (
                SELECT
                    id AS dispense_event_id,
                    plan_id AS dispense_plan_id,
                    form_data ->> 'start_time' AS dispense_start_time,
                    form_data ->> 'end_time' AS dispense_end_time,
                    form_data ->> 'currently_present' AS currently_present,
                    COALESCE(form_data ->> 'child_present_days', 0::text) AS child_present_days,
                    COALESCE(form_data ->> 'child_sick', 0::text) AS dispense_child_sick,
                    form_data ->> 'allergic_severe' AS dispense_allergic_severe,
                    form_data ->> 'administeredSpaq' AS dispense_administeredSpaq,
                    form_data ->> 'child_record' AS dispense_child_record,
                    form_data ->> 'referred' AS dispense_referred,
                    form_data ->> 'referralReasons' AS dispense_referral_reason,
                    form_data ->> 'otherReferralReason' AS dispense_referral_reason_other
                FROM reveal.events
                WHERE base_entity_id = individual.baseentityid
                AND plan_id = dis_task.plan_identifier
                AND event_type = 'mda_dispense'
                ORDER BY form_data ->> 'end_time' DESC
                LIMIT 1
) dis_event ON TRUE
LEFT JOIN LATERAL (
    SELECT
        identifier,
        business_status
    FROM reveal.tasks
    WHERE task_for = individual.baseentityid
    AND plan_identifier = dis_task.plan_identifier
    AND code = 'MDA Adherence'
    LIMIT 1
) adh_task ON TRUE
LEFT JOIN LATERAL (
                SELECT
                    id AS redose_event_id,
                    task_id AS redose_task_id,
                    form_data ->> 'start_time' AS redose_start_time,
                    form_data ->> 'end_time' AS redose_end_time,
                    form_data ->> 'number_of_additional_doses' AS redose_additional_doses,
                    form_data ->> 'childHfReferred' AS redose_referred,
                    -- BUG RVL-1636 -- dis_event.redose_hfreferred as this field is empty fieldCode
                    form_data ->> 'referralReason' AS redose_referral_reason,
                    form_data ->> 'otherReason' AS redose_referral_reason_other
                FROM reveal.events
                WHERE base_entity_id = individual.baseentityid
                AND plan_id = dis_task.plan_identifier
                AND event_type = 'mda_adherence'
                ORDER BY form_data ->> 'end_time' DESC
                LIMIT 1
) adh_event ON TRUE
WHERE
    individual.lastname <> 'Family'
AND
    family.relationships -> 'family_head' ->> 0 <> individual.baseentityid
AND
    individual.birthdate <> 'infinity'
ORDER BY structure_id;
