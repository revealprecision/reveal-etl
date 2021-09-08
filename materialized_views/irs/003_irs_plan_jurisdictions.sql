SET schema 'reveal';

DROP MATERIALIZED VIEW IF EXISTS irs_plan_jurisdictions CASCADE;

CREATE MATERIALIZED VIEW IF NOT EXISTS irs_plan_jurisdictions
AS
SELECT DISTINCT ON (all_plan_jurisdictions.plan_id, all_plan_jurisdictions.jurisdiction_id) all_plan_jurisdictions.plan_id,
    all_plan_jurisdictions.jurisdiction_id,
    COALESCE(virtual_jurisdictions.jurisdiction_id, all_plan_jurisdictions.jurisdiction_id) AS zambia_jurisdiction_id,
    COALESCE(virtual_jurisdictions.jurisdiction_depth, jurisdictions.geographic_level) AS zambia_jurisdiction_depth
FROM
    (SELECT plan_jurisdiction.plan_id,
            UNNEST(ARRAY_APPEND(jurisdictions_ex.jurisdiction_path, jurisdictions_ex.jurisdiction_id)) AS jurisdiction_id
     FROM plan_jurisdiction
     LEFT JOIN jurisdictions_materialized_view
        AS jurisdictions_ex
        ON jurisdictions_ex.jurisdiction_id = plan_jurisdiction.jurisdiction_id) AS all_plan_jurisdictions
LEFT JOIN jurisdictions ON jurisdictions.id = all_plan_jurisdictions.jurisdiction_id
LEFT JOIN
    (
        SELECT
            *
        FROM
            irs_jurisdictions_tree
        WHERE is_virtual_jurisdiction = TRUE
    ) AS virtual_jurisdictions ON virtual_jurisdictions.opensrp_jurisdiction_id = all_plan_jurisdictions.jurisdiction_id
ORDER BY all_plan_jurisdictions.plan_id ASC, all_plan_jurisdictions.jurisdiction_id ASC;


CREATE UNIQUE INDEX IF NOT EXISTS irs_plan_jurisdictions_plan_id_jurisdiction_id_idx ON irs_plan_jurisdictions (plan_id, jurisdiction_id);
CREATE INDEX IF NOT EXISTS irs_plan_jurisdictions_plan_id_idx ON irs_plan_jurisdictions (plan_id);
CREATE INDEX IF NOT EXISTS irs_plan_jurisdictions_jurisdiction_id_idx ON irs_plan_jurisdictions (jurisdiction_id);
CREATE INDEX IF NOT EXISTS irs_plan_jurisdictions_zambia_jurisdiction_id_idx ON irs_plan_jurisdictions (zambia_jurisdiction_id);