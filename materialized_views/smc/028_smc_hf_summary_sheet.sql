SET schema 'reveal';

DROP MATERIALIZED VIEW IF EXISTS smc_hf_summary_sheet CASCADE;

CREATE MATERIALIZED VIEW IF NOT EXISTS smc_hf_summary_sheet
AS
SELECT
    plan_id,
    jurisdiction_id,
    jurisdiction_name_path[1] AS country,
    jurisdiction_name_path[2] AS province,
    jurisdiction_name_path[3] AS district,
    jurisdiction_name,
    eligible_children,
    treated_children,
    treated_redose_children,
    referred_children,
    referral_form_count,
    fever_rdt_positive_child
FROM
    reveal.smc_jurisdictions
WHERE
    jurisdiction_depth = 3
AND
    eligible_children > 0;