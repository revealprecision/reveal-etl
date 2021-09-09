SET schema 'reveal';

DROP MATERIALIZED VIEW IF EXISTS generic_sync_analysis;

CREATE MATERIALIZED VIEW IF NOT EXISTS generic_sync_analysis
AS
SELECT
    events.plan_id AS plan_id,
    plans.title AS plan_title,
    events.provider_id AS user_id,
    MAX(events.created_at) AS last_sync_time_gmt,
    COUNT(events.created_at) AS event_count
FROM
    reveal.events events
JOIN reveal.plans plans ON events.plan_id = plans.identifier
WHERE event_type NOT IN ('reset_task')
GROUP BY events.plan_id, plans.title, events.provider_id
ORDER BY last_sync_time_gmt DESC;