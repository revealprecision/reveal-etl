SET schema 'reveal';

DROP MATERIALIZED VIEW IF EXISTS smc_plans CASCADE;

CREATE MATERIALIZED VIEW IF NOT EXISTS smc_plans
AS
SELECT plans.identifier AS plan_id,
  plans.title AS plan_title,
  plans.name AS plan_name,
  plans.status AS plan_status,
  plans.date AS plan_date,
  plans.effective_period_start AS plan_effective_period_start,
  plans.effective_period_end AS plan_effective_period_end,
  plans.intervention_type AS plan_intervention_type,
  plans.version AS plan_version,
  ( SELECT array_agg(DISTINCT subq.jurisdiction_root_parent_id) AS root_parent_ids
        FROM ( SELECT plan_jurisdiction.jurisdiction_id,
                  jurisdictions_materialized_view.jurisdiction_name,
                  jurisdictions_materialized_view.jurisdiction_root_parent_id,
                  COALESCE(jurisdictions_materialized_view.jurisdiction_path, '{}'::character varying[]) AS jurisdiction_path
                FROM (reveal.plan_jurisdiction
                  LEFT JOIN reveal.jurisdictions_materialized_view ON (((plan_jurisdiction.jurisdiction_id)::text = (jurisdictions_materialized_view.jurisdiction_id)::text)))
                WHERE ((plans.identifier)::text = (plan_jurisdiction.plan_id)::text)) subq) AS jurisdiction_root_parent_ids
FROM reveal.plans plans
WHERE ((plans.status)::text = ANY ((ARRAY['active'::character varying, 'complete'::character varying])::text[]))
AND plans.intervention_type = ('Dynamic-MDA')
ORDER BY plans.date DESC;

CREATE UNIQUE INDEX IF NOT EXISTS smc_plans_index ON smc_plans (plan_id);
