SET schema 'reveal';

DROP MATERIALIZED VIEW IF EXISTS plan_jurisdictions CASCADE;

CREATE MATERIALIZED VIEW IF NOT EXISTS plan_jurisdictions
AS
SELECT DISTINCT ON (plan_id, jurisdiction_id)
    plan_jurisdiction.plan_id,
    plan_hierarchy.jurisdiction_id,
    plan_hierarchy.jurisdiction_parent_id,
    plan_hierarchy.jurisdiction_name,
    plan_hierarchy.jurisdiction_path,
    plan_hierarchy.jurisdiction_depth,
    plan_hierarchy.is_focus_area
FROM plan_jurisdiction AS plan_jurisdiction
LEFT JOIN lateral (
    SELECT DISTINCT(jurisdictions.jurisdiction_id),
    jurisdictions.jurisdiction_name,
    jurisdictions.jurisdiction_path,
    jurisdictions.jurisdiction_depth,
    jurisdictions.jurisdiction_parent_id,
    CASE WHEN plan_jurisdiction.jurisdiction_id = jurisdictions.jurisdiction_id THEN true ELSE false END AS is_focus_area
    FROM jurisdictions_materialized_view AS jurisdictions
    WHERE jurisdictions.jurisdiction_id IN (
        SELECT unnest(jurisdiction_id || jurisdiction_path)
        FROM jurisdictions_materialized_view
        WHERE jurisdiction_id = plan_jurisdiction.jurisdiction_id
    )
) AS plan_hierarchy ON true;

CREATE INDEX IF NOT EXISTS plan_jurisdictions_path_idx_gin ON plan_jurisdictions using GIN (jurisdiction_path);
CREATE INDEX IF NOT EXISTS plan_jurisdictions_plan_idx ON plan_jurisdictions (plan_id);
CREATE INDEX IF NOT EXISTS plan_jurisdictions_plan_isfocus_idx ON plan_jurisdictions (is_focus_area, jurisdiction_path, plan_id);
CREATE INDEX IF NOT EXISTS plan_jurisdictions_jurisdiction_idx ON plan_jurisdictions (jurisdiction_id);
CREATE INDEX IF NOT EXISTS plan_jurisdictions_is_focus_area_idx ON plan_jurisdictions (is_focus_area);
CREATE UNIQUE INDEX IF NOT EXISTS plan_jurisdictions_idx ON plan_jurisdictions (plan_id, jurisdiction_id);