SET schema 'reveal';
DROP MATERIALIZED VIEW IF EXISTS mda_lite_plans CASCADE ;
CREATE MATERIALIZED VIEW IF NOT EXISTS mda_lite_plans
AS
SELECT
    plans.identifier AS plan_id,
    plans.title AS plan_title,
    plans.name AS plan_name,
    plans.status AS plan_status,
    plans.date AS plan_date,
    plans.effective_period_start AS plan_effective_period_start,
    plans.effective_period_end AS plan_effective_period_end,
    plans.intervention_type AS plan_intervention_type,
    plans.version AS plan_version
FROM plans
WHERE plans.intervention_type IN ('MDA-Lite') AND plans.status NOT IN ('draft', 'retired')
ORDER BY plans.date DESC;

CREATE UNIQUE INDEX IF NOT EXISTS mda_lite_plans_idx ON mda_lite_plans(plan_id);