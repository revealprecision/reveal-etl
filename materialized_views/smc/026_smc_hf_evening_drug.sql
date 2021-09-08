SET schema 'reveal';

DROP MATERIALIZED VIEW IF EXISTS smc_hf_evening_drug;

CREATE MATERIALIZED VIEW IF NOT EXISTS smc_hf_evening_drug
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
    events.form_data ->> 'dayOfCycle' AS dayOfCycle,
    events.form_data ->> 'nameOfHWIssuing' AS nameOfHWIssuing,
    events.form_data ->> 'nameOfCDDreceiving' AS nameOfCDDreceiving,
    events.form_data ->> 'spaqBlistersIssuedFirst' AS spaqBlistersIssuedFirst,
    events.form_data ->> 'spaqBlistersIssuedSecond' AS spaqBlistersIssuedSecond,
    events.form_data ->> 'spaqBlistersIssuedThird' AS spaqBlistersIssuedThird,
    events.form_data ->> 'spaqBlistersIssuedFourth' AS spaqBlistersIssuedFourth,
    events.form_data ->> 'spaqBlistersIssuedFifth' AS spaqBlistersIssuedFifth,
    events.form_data ->> 'spaqBlistersIssuedSixth' AS spaqBlistersIssuedSixth
FROM reveal.events events
LEFT JOIN reveal.jurisdictions_materialized_view jurisdiction ON (events.location_id = jurisdiction.jurisdiction_id)
LEFT JOIN reveal.plans plans ON events.plan_id = plans.identifier
WHERE
    event_type = 'hw_drug_evening';