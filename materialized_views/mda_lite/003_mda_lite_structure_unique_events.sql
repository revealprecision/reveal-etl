SET schema 'reveal';

DROP MATERIALIZED VIEW IF EXISTS mda_lite_structure_unique_events CASCADE;

CREATE MATERIALIZED VIEW IF NOT EXISTS mda_lite_structure_unique_events
AS
SELECT
    plan_id,
    structure_id,
    location_id,
    form_data ->> 'health_worker_supervisor' as supervisor,
    form_data ->> 'cdd_name' as cdd,
    form_data ->> 'drug_distributed' as drugs,
    event_type,
    active_event,
    active_event_date,
    form_data
FROM
(
    SELECT
        plan_id,
        base_entity_id AS structure_id,
        location_id,
        form_data ->> 'date' AS collection_date,
        form_data ->> 'health_worker_supervisor' AS supervisor,
        form_data ->> 'cdd_name' AS cdd_name,
        form_data ->> 'ntd_treated' AS ntd_treated,
        form_data ->> 'drugs' AS drugs
    FROM events
    WHERE event_type = 'cdd_supervisor_daily_summary'
    GROUP BY plan_id,base_entity_id, location_id, form_data ->> 'date', form_data ->> 'health_worker_supervisor', form_data ->> 'cdd_name', form_data ->> 'ntd_treated', form_data ->> 'drugs'
) distinct_events
LEFT JOIN LATERAL (
    SELECT
        id AS active_event,
        event_date AS active_event_date,
        event_type,
        form_data
    FROM
        events
    WHERE event_type = 'cdd_supervisor_daily_summary'
    AND events.plan_id = distinct_events.plan_id
    AND events.base_entity_id = distinct_events.structure_id
    AND form_data ->> 'date' = distinct_events.collection_date
    AND form_data ->> 'health_worker_supervisor' = distinct_events.supervisor
    AND form_data ->> 'cdd_name' = distinct_events.cdd_name
    AND form_data ->> 'ntd_treated' = distinct_events.ntd_treated
    AND form_data ->> 'drugs' = distinct_events.drugs
    ORDER BY event_date DESC
    LIMIT 1
) AS active_event ON TRUE
UNION
SELECT
    plan_id,
    structure_id,
    location_id,
    form_data ->> 'health_worker_supervisor' as supervisor,
    form_data ->> 'cdd_name' as cdd,
    form_data ->> 'drug_distributed' as drugs,
    event_type,
    active_event,
    active_event_date,
    form_data
FROM (
    SELECT
        plan_id,
        base_entity_id AS structure_id,
        location_id,
        form_data ->> 'location' as ward,
        form_data ->> 'health_worker_supervisor' as supervisor,
        form_data ->> 'cdd_name' as cdd,
        form_data ->> 'drug_distributed' as drugs
    FROM events
    WHERE event_type = 'tablet_accountability'
    GROUP BY plan_id, base_entity_id, location_id, form_data ->> 'location', form_data ->> 'health_worker_supervisor', form_data ->> 'cdd_name', form_data ->> 'drug_distributed'
) distinct_events
LEFT JOIN LATERAL (
    SELECT
        id AS active_event,
        event_date AS active_event_date,
        event_type,
        form_data
    FROM
        events
    WHERE event_type = 'tablet_accountability'
    AND events.plan_id = distinct_events.plan_id
    AND events.base_entity_id = distinct_events.structure_id
    AND form_data ->> 'location' = distinct_events.ward
    AND form_data ->> 'health_worker_supervisor' = distinct_events.supervisor
    AND form_data ->> 'cdd_name' = distinct_events.cdd
    AND form_data ->> 'drug_distributed' = distinct_events.drugs
    ORDER BY event_date DESC
    LIMIT 1
) AS active_event ON TRUE;

CREATE INDEX IF NOT EXISTS mda_lite_structure_unique_events_plan_idx ON mda_lite_structure_unique_events (plan_id);
CREATE INDEX IF NOT EXISTS mda_lite_structure_unique_events_structure_idx ON mda_lite_structure_unique_events (structure_id);
CREATE INDEX IF NOT EXISTS mda_lite_structure_unique_events_event_type ON mda_lite_structure_unique_events (event_type);
