SET schema 'reveal';

DROP MATERIALIZED VIEW IF EXISTS smc_plan_jurisdictions CASCADE;

CREATE MATERIALIZED VIEW IF NOT EXISTS smc_plan_jurisdictions
AS
SELECT
  DISTINCT ON (all_plan_jurisdictions.plan_id, all_plan_jurisdictions.jurisdiction_id) all_plan_jurisdictions.plan_id,
  all_plan_jurisdictions.jurisdiction_id,
  jurisdictions.geographic_level AS jurisdiction_depth
FROM
  (
    (
      SELECT
        plan_jurisdiction.plan_id,
        unnest(array_append(jurisdictions_ex.jurisdiction_path, jurisdictions_ex.jurisdiction_id)) AS jurisdiction_id
      FROM (
        reveal.plan_jurisdiction plan_jurisdiction
LEFT JOIN reveal.jurisdictions_materialized_view jurisdictions_ex ON (((jurisdictions_ex.jurisdiction_id)::text = (plan_jurisdiction.jurisdiction_id)::text)))) all_plan_jurisdictions
 LEFT JOIN reveal.jurisdictions ON (((jurisdictions.id)::text = (all_plan_jurisdictions.jurisdiction_id)::text)))
  ORDER BY all_plan_jurisdictions.plan_id, all_plan_jurisdictions.jurisdiction_id;

CREATE UNIQUE INDEX IF NOT EXISTS smc_plan_jurisdictions_plan_id_jurisdiction_id_idx ON smc_plan_jurisdictions (plan_id, jurisdiction_id);

CREATE INDEX IF NOT EXISTS smc_plan_jurisdictions_plan_id_idx ON smc_plan_jurisdictions (plan_id);

CREATE INDEX IF NOT EXISTS smc_plan_jurisdictions_jurisdiction_id_idx ON smc_plan_jurisdictions (jurisdiction_id);