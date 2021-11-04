SET schema 'reveal';

DROP MATERIALIZED VIEW IF EXISTS generic_event_analysis_by_user;

CREATE MATERIALIZED VIEW IF NOT EXISTS generic_event_analysis_by_user
AS
SELECT
    *
FROM (
    SELECT
        events.plan_id AS plan_id,
        events.user_id AS user_id,
        COUNT(events.capture_time) AS count_event_type,
        MAX(events.capture_time) AS max_capture_time,
        MIN(events.capture_time) AS min_capture_time,
        AVG(events.capture_time) AS avg_capture_time
    FROM
        reveal.generic_event_analysis_by_events events
    GROUP BY events.plan_id,events.user_id
) stats_query
LEFT JOIN LATERAL (
    SELECT
        events.details ->> 'appVersionName' AS appVersion
    FROM
        events
    WHERE
        events.provider_id = stats_query.user_id
    GROUP BY events.provider_id, events.details ->> 'appVersionName'
    ORDER BY events.provider_id, CASE events.details ->> 'appVersionName' WHEN '5.3.26' THEN 0 ELSE 1 END DESC LIMIT 1
) version_query ON TRUE;