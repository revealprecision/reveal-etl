SET schema 'reveal';

DROP MATERIALIZED VIEW IF EXISTS plans_materialized_view CASCADE;

CREATE MATERIALIZED VIEW IF NOT EXISTS plans_materialized_view
AS
SELECT
    DISTINCT ON (plans.identifier, plan_jurisdiction.jurisdiction_id) public.uuid_generate_v5('6ba7b810-9dad-11d1-80b4-00c04fd430c8'::uuid, concat(plans.identifier, plan_jurisdiction.jurisdiction_id)) AS id,
    plans.identifier AS plan_id,
    plans.title AS plan_title,
    plans.name AS plan_name,
    plans.status AS plan_status,
    tree.jurisdiction_id,
    plans.fi_status AS plan_fi_status,
    plans.fi_reason AS plan_fi_reason,
    plans.date AS plan_date,
    plans.effective_period_start AS plan_effective_period_start,
    plans.effective_period_end AS plan_effective_period_end,
    plans.intervention_type AS plan_intervention_type,
    plans.version AS plan_version,
    tree.jurisdiction_parent_id,
    tree.jurisdiction_name,
    tree.jurisdiction_geometry,
    tree.jurisdiction_depth,
    tree.jurisdiction_path,
    tree.jurisdiction_name_path,
    tree.jurisdiction_root_parent_id,
    tree.jurisdiction_root_parent_name
FROM ((plans
LEFT JOIN plan_jurisdiction ON (((plans.identifier)::text = (plan_jurisdiction.plan_id)::text)))
LEFT JOIN LATERAL (
    SELECT
        jurisdictions_materialized_view.jurisdiction_id,
        jurisdictions_materialized_view.jurisdiction_parent_id,
        jurisdictions_materialized_view.jurisdiction_name,
        jurisdictions_materialized_view.jurisdiction_geometry,
        jurisdictions_materialized_view.jurisdiction_depth,
        jurisdictions_materialized_view.jurisdiction_path,
        jurisdictions_materialized_view.jurisdiction_name_path,
        jurisdictions_materialized_view.jurisdiction_root_parent_id,
        jurisdictions_materialized_view.jurisdiction_root_parent_name
    FROM jurisdictions_materialized_view
    WHERE ((jurisdictions_materialized_view.jurisdiction_id)::text = (plan_jurisdiction.jurisdiction_id)::text)) tree ON (true))
ORDER BY plans.identifier, plan_jurisdiction.jurisdiction_id, plans.date DESC;

CREATE INDEX IF NOT EXISTS plans_materialized_view_plan_idx ON plans_materialized_view (plan_id);
CREATE INDEX IF NOT EXISTS plans_materialized_view_jurisdiction_idx ON plans_materialized_view (jurisdiction_id);
CREATE INDEX IF NOT EXISTS plans_materialized_view_intervention_typex ON plans_materialized_view (plan_intervention_type);
CREATE UNIQUE INDEX IF NOT EXISTS plans_materialized_view_idx ON plans_materialized_view (id);