SET schema 'reveal';

DROP MATERIALIZED VIEW IF EXISTS mda_plans CASCADE;

CREATE MATERIALIZED VIEW IF NOT EXISTS mda_plans
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
    plans.version AS plan_version,
    (
        SELECT array_agg(DISTINCT subq.jurisdiction_root_parent_id) AS root_parent_ids
        FROM
            (
                SELECT
                    plan_jurisdiction.jurisdiction_id,
                    jurisdictions_materialized_view.jurisdiction_name AS jurisdiction_name,
                    jurisdictions_materialized_view.jurisdiction_root_parent_id AS jurisdiction_root_parent_id,
                    COALESCE(jurisdictions_materialized_view.jurisdiction_path, '{}') AS jurisdiction_path
                FROM plan_jurisdiction
                LEFT JOIN jurisdictions_materialized_view
                    ON plan_jurisdiction.jurisdiction_id = jurisdictions_materialized_view.jurisdiction_id
                WHERE plans.identifier = plan_jurisdiction.plan_id
            ) AS subq
    ) AS jurisdiction_root_parent_ids
FROM plans plans
WHERE (plans.intervention_type = 'Dynamic-MDA' ) AND plans.status IN ('active', 'complete')
ORDER BY plans.date DESC) WITH DATA;

CREATE UNIQUE INDEX IF NOT EXISTS mda_plans_index ON mda_plans (plan_id);