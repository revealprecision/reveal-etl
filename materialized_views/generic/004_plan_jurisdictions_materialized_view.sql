SET schema 'reveal';

DROP MATERIALIZED VIEW IF EXISTS plan_jurisdictions_materialized_view CASCADE;

CREATE MATERIALIZED VIEW IF NOT EXISTS plan_jurisdictions_materialized_view
AS
SELECT DISTINCT ON (plan_id, jurisdiction_id)
    plan_jurisdiction.plan_id,
    plan_hierarchy.jurisdiction_id AS jurisdiction_id,
    plan_hierarchy.jurisdiction_path AS jurisdiction_path,
    plan_hierarchy.is_focus_area AS is_focus_area
FROM plan_jurisdiction AS plan_jurisdiction
LEFT JOIN lateral (
    SELECT DISTINCT(jurisdictions.jurisdiction_id),
    jurisdictions.jurisdiction_path,
    CASE WHEN plan_jurisdiction.jurisdiction_id = jurisdictions.jurisdiction_id THEN true ELSE false END AS is_focus_area
    FROM jurisdictions_materialized_view AS jurisdictions
    WHERE jurisdictions.jurisdiction_id IN (
        SELECT unnest(jurisdiction_id || jurisdiction_path)
        FROM jurisdictions_materialized_view
        WHERE jurisdiction_id = plan_jurisdiction.jurisdiction_id
    )
) AS plan_hierarchy ON true;

CREATE INDEX IF NOT EXISTS plan_jurisdictions_materialized_view_path_idx_gin ON plan_jurisdictions_materialized_view using GIN (jurisdiction_path);
CREATE INDEX IF NOT EXISTS plan_jurisdictions_materialized_view_plan_idx ON plan_jurisdictions_materialized_view (plan_id);
CREATE INDEX IF NOT EXISTS plan_jurisdictions_materialized_view_plan_3mstk_idx ON plan_jurisdictions_materialized_view (is_focus_area, jurisdiction_path, plan_id);
CREATE INDEX IF NOT EXISTS plan_jurisdictions_materialized_view_jurisdiction_idx ON plan_jurisdictions_materialized_view (jurisdiction_id);
CREATE INDEX IF NOT EXISTS plan_jurisdictions_materialized_view_is_focus_area_idx ON plan_jurisdictions_materialized_view (is_focus_area);
CREATE UNIQUE INDEX IF NOT EXISTS plan_jurisdictions_materialized_view_idx ON plan_jurisdictions_materialized_view (plan_id, jurisdiction_id);