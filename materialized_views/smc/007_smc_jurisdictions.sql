SET schema 'reveal';

DROP MATERIALIZED VIEW IF EXISTS smc_jurisdictions CASCADE;

CREATE MATERIALIZED VIEW IF NOT EXISTS smc_jurisdictions
AS
SELECT
    main_query.id,
    main_query.plan_id,
    main_query.jurisdiction_id,
    main_query.jurisdiction_parent_id,
    main_query.jurisdiction_name,
    main_query.jurisdiction_geometry,
    main_query.jurisdiction_depth,
    main_query.jurisdiction_path,
    main_query.jurisdiction_name_path,
    main_query.is_virtual_jurisdiction,
    main_query.operational_areas_visited,
    main_query.distribution_effectivness,
    main_query.total_structures,
    main_query.total_found_structures,
    main_query.found_coverage,
    main_query.total_structures_recieved_spaq,
    main_query.distribution_coverage,
    main_query.treatment_coverage,
    main_query.referral_treatment_rate,
    main_query.eligible_children,
    main_query.treated_children,
    main_query.treated_redose_children,
    main_query.referred_children,
    main_query.distribution_coverage_total,
    main_query.referral_form_count,
    main_query.fever_rdt_positive_child
FROM
    (
        SELECT DISTINCT ON (jurisdictions_query.jurisdiction_id, plans.identifier) public.uuid_generate_v5('6ba7b810-9dad-11d1-80b4-00c04fd430c8'::uuid, concat(jurisdictions_query.jurisdiction_id, plans.identifier)) AS id,
            plans.identifier AS plan_id,
            jurisdictions_query.jurisdiction_id,
            jurisdictions_query.jurisdiction_parent_id,
            jurisdictions_query.jurisdiction_name,
            jurisdictions_query.jurisdiction_geometry,
            jurisdictions_query.jurisdiction_depth,
            jurisdictions_query.jurisdiction_path,
            jurisdictions_query.jurisdiction_name_path,
            CASE
                WHEN (jurisdictions_query.jurisdiction_depth < 4) THEN 1
                ELSE 0
            END AS is_virtual_jurisdiction,
            jurisdictions_query.operational_areas_visited,
            jurisdictions_query.distribution_effectivness,
            jurisdictions_query.totstruct AS total_structures,
            jurisdictions_query.foundstruct AS total_found_structures,
            jurisdictions_query.foundcoverage AS found_coverage,
            jurisdictions_query.structures_recieved_spaq AS total_structures_recieved_spaq,
            jurisdictions_query.distribution_coverage,
            jurisdictions_query.treatment_coverage,
            jurisdictions_query.referral_treatment_rate,
            jurisdictions_query.eligible_children,
            jurisdictions_query.treated_children,
            jurisdictions_query.treated_redose_children,
            jurisdictions_query.referred_children,
            jurisdictions_query.distribution_coverage_total,
            jurisdictions_query.referral_form_count,
            jurisdictions_query.fever_rdt_positive_child
        FROM
            (
                reveal.plans
                LEFT JOIN LATERAL
                    (
                        SELECT
                            nigeria_jurisdictions.jurisdiction_id,
                            nigeria_jurisdictions.jurisdiction_parent_id,
                            nigeria_jurisdictions.jurisdiction_name,
                            nigeria_jurisdictions.jurisdiction_geometry,
                            nigeria_jurisdictions.jurisdiction_depth,
                            nigeria_jurisdictions.jurisdiction_path,
                            nigeria_jurisdictions.jurisdiction_name_path,
                            smc_focus_area_query.totstruct,
                            smc_focus_area_query.foundstruct,
                            smc_focus_area_query.totareas,
                            smc_focus_area_query.visitedareas,
                            smc_focus_area_query.structures_recieved_spaq,
                            smc_focus_area_query.eligible_children,
                            smc_focus_area_query.treated_children,
                            smc_focus_area_query.treated_redose_children,
                            smc_focus_area_query.referred_children,
                            smc_focus_area_query.referral_form_count,
                            smc_focus_area_query.fever_rdt_positive_child,
                            CASE
                                WHEN (smc_focus_area_query.totstruct = (0)::numeric) THEN (0)::numeric
                                ELSE ((smc_focus_area_query.foundstruct)::numeric(10,4) / (smc_focus_area_query.totstruct)::numeric(10,4))
                            END AS foundcoverage,
                            CASE
                                WHEN (smc_focus_area_query.totareas = 0) THEN '0/0'::text
                                ELSE concat((smc_focus_area_query.visitedareas)::character varying, '/', (smc_focus_area_query.totareas)::character varying)
                            END AS operational_areas_visited,
                            CASE
                                WHEN (smc_focus_area_query.totareas = 0) THEN (0)::numeric
                                ELSE ((smc_focus_area_query.visitedareas)::numeric(10,4) / (smc_focus_area_query.totareas)::numeric(10,4))
                            END AS distribution_effectivness,
                            CASE
                                WHEN (smc_focus_area_query.foundstruct = (0)::numeric) THEN (0)::numeric
                                ELSE ((smc_focus_area_query.structures_recieved_spaq)::numeric(10,4) / (smc_focus_area_query.foundstruct)::numeric(10,4))
                            END AS distribution_coverage,
                            CASE
                                WHEN (smc_focus_area_query.totstruct = (0)::numeric) THEN (0)::numeric
                                ELSE ((smc_focus_area_query.structures_recieved_spaq)::numeric(10,4) / (smc_focus_area_query.totstruct)::numeric(10,4))
                            END AS distribution_coverage_total,
                            CASE
                                WHEN (smc_focus_area_query.eligible_children = (0)::numeric) THEN (0)::numeric
                                ELSE ((smc_focus_area_query.treated_children)::numeric(10,4) / (smc_focus_area_query.eligible_children)::numeric(10,4))
                            END AS treatment_coverage,
                            CASE
                                WHEN (smc_focus_area_query.treated_children = (0)::numeric) THEN (0)::numeric
                                ELSE ((smc_focus_area_query.referred_children)::numeric(10,4) / (smc_focus_area_query.treated_children)::numeric(10,4))
                            END AS referral_treatment_rate
                        FROM
                            (
                                reveal.nigeria_jurisdictions nigeria_jurisdictions
                                LEFT JOIN LATERAL
                                    (
                                        SELECT
                                            COALESCE(count(smc_focus_area.jurisdiction_id) FILTER (WHERE smc_focus_area.totstruct > (0)),(0)::bigint) AS totareas,
                                            COALESCE(count(smc_focus_area.jurisdiction_id) FILTER (WHERE (COALESCE(smc_focus_area.foundcov, (0)::numeric) > (0)::numeric)), (0)::bigint) AS visitedareas,
                                            COALESCE(sum(smc_focus_area.totstruct), (0)::numeric) AS totstruct,
                                            COALESCE(sum(smc_focus_area.foundstruct), (0)::numeric) AS foundstruct,
                                            COALESCE(sum(smc_focus_area.structures_recieved_spaq), (0)::numeric) AS structures_recieved_spaq,
                                            COALESCE(sum(smc_focus_area.eligible_children), (0)::numeric) AS eligible_children,
                                            COALESCE(sum(smc_focus_area.treated_children), (0)::numeric) AS treated_children,
                                            COALESCE(sum(smc_focus_area.treated_redose_children), (0)::numeric) AS treated_redose_children,
                                            COALESCE(sum(smc_focus_area.referred_children), (0)::numeric) AS referred_children,
                                            COALESCE(sum(smc_focus_area.referral_form_count), (0)::numeric) AS referral_form_count,
                                            COALESCE(sum(smc_focus_area.fever_rdt_positive_child), (0)::numeric) AS fever_rdt_positive_child
                                        FROM reveal.smc_focus_area smc_focus_area
                                        WHERE
                                            (
                                                ((smc_focus_area.plan_id)::text = (plans.identifier)::text)
                                            AND
                                                (
                                                        (smc_focus_area.jurisdiction_path @> ARRAY[nigeria_jurisdictions.jurisdiction_id])
                                                    OR
                                                        ((smc_focus_area.jurisdiction_id)::text = (nigeria_jurisdictions.jurisdiction_id)::text)
                                                )
                                            )
                                    ) smc_focus_area_query ON (true)
                            )
                    ) jurisdictions_query ON (true)
                )
                WHERE
                (
                        ((plans.status)::text <> ALL ((ARRAY['draft'::character varying, 'retired'::character varying])::text[]))
                    AND
                        (jurisdictions_query.totstruct > (0)::numeric)
                )
    ) main_query;

CREATE INDEX IF NOT EXISTS smc_jurisdictions_path_idx_gin on smc_jurisdictions using GIN(jurisdiction_path);

CREATE INDEX IF NOT EXISTS smc_jurisdictions_plan_idx ON smc_jurisdictions (plan_id);

CREATE INDEX IF NOT EXISTS smc_jurisdictions_jurisdiction_idx ON smc_jurisdictions (jurisdiction_id);

CREATE INDEX IF NOT EXISTS smc_jurisdictions_jurisdiction_parent_idx ON smc_jurisdictions (jurisdiction_parent_id);

CREATE UNIQUE INDEX IF NOT EXISTS smc_jurisdictions_idx ON smc_jurisdictions (id);