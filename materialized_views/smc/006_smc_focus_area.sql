SET schema 'reveal';

DROP MATERIALIZED VIEW IF EXISTS smc_focus_area CASCADE;

CREATE MATERIALIZED VIEW IF NOT EXISTS smc_focus_area
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
    main_query.totstruct,
    main_query.foundstruct,
    main_query.foundcov,
    main_query.structures_recieved_spaq,
    main_query.eligible_children,
    main_query.treated_children,
    main_query.treated_redose_children,
    main_query.referred_children,
    main_query.referral_form_count,
    main_query.fever_rdt_positive_child
FROM (
    SELECT
        public.uuid_generate_v5('6ba7b810-9dad-11d1-80b4-00c04fd430c8'::uuid,concat(plans.identifier,nigeria_jurisdictions.jurisdiction_id)) AS id,
        plans.identifier AS plan_id,
        nigeria_jurisdictions.jurisdiction_id,
        nigeria_jurisdictions.jurisdiction_parent_id,
        nigeria_jurisdictions.jurisdiction_name,
        nigeria_jurisdictions.jurisdiction_geometry,
        nigeria_jurisdictions.jurisdiction_depth,
        nigeria_jurisdictions.jurisdiction_path,
        nigeria_jurisdictions.jurisdiction_name_path,
        smc_structures_query.totstruct,
        smc_structures_query.foundstruct,
        coverage_query.foundcov,
        smc_structures_query.structures_recieved_spaq,
        smc_structures_query.eligible_children,
        smc_structures_query.treated_children,
        smc_structures_query.treated_redose_children,
        smc_structures_query.referred_children,
        referal_forms.referral_form_count,
        fever_query_sum.fever_rdt_positive_child
    FROM (((((((
        reveal.plans
        LEFT JOIN reveal.smc_plan_jurisdictions smc_plan_jurisdictions ON (((plans.identifier)::text = (smc_plan_jurisdictions.plan_id)::text)))
        LEFT JOIN reveal.nigeria_jurisdictions nigeria_jurisdictions ON (((smc_plan_jurisdictions.jurisdiction_id)::text = (nigeria_jurisdictions.jurisdiction_id)::text))) 
        LEFT JOIN LATERAL (
            SELECT
                COALESCE(count(smc_structures.structure_id),(0)::bigint) AS totstruct,
                COALESCE(sum(smc_structures.found_structures),(0)::bigint) AS foundstruct,
                COALESCE(sum(smc_structures.structures_recieved_spaq),(0)::bigint) AS structures_recieved_spaq,
                COALESCE(sum(smc_structures.eligible_children),(0)::bigint) AS eligible_children,
                COALESCE(sum(smc_structures.treated_children),(0)::bigint) AS treated_children,
                COALESCE(sum(smc_structures.treated_redose_children),(0)::bigint) AS treated_redose_children,
                COALESCE(sum(smc_structures.referred_children),(0)::bigint) AS referred_children
            FROM reveal.smc_structures smc_structures
            WHERE (
                ((smc_structures.jurisdiction_id)::text = (nigeria_jurisdictions.jurisdiction_id)::text)
                AND (smc_structures.business_status <> ALL (ARRAY['Not Eligible'::text,'No Tasks'::text]))
                AND (((plans.identifier)::text = (smc_structures.plan_id)::text) OR (smc_structures.plan_id IS NULL))
            )
            LIMIT 1
        ) smc_structures_query ON (true))
        LEFT JOIN LATERAL (
            SELECT
                CASE WHEN (smc_structures_query.totstruct = 0) THEN (0)::numeric ELSE ((smc_structures_query.foundstruct)::numeric(7,2) / (smc_structures_query.totstruct)::numeric(7,2)) END AS foundcov
        ) coverage_query ON (true))
        LEFT JOIN LATERAL (
            SELECT
                CASE WHEN (smc_structures_query.foundstruct = 0) THEN (0)::numeric ELSE ((smc_structures_query.structures_recieved_spaq)::numeric(7,2) / (smc_structures_query.foundstruct)::numeric(7,2)) END AS distributioncov
        ) dist_coverage_query ON (true))
        LEFT JOIN LATERAL (
            SELECT
                CASE WHEN (smc_structures_query.eligible_children = 0) THEN (0)::numeric ELSE ((smc_structures_query.treated_children)::numeric(7,2) / (smc_structures_query.eligible_children)::numeric(7,2)) END AS treatmentcov
        ) treatment_coverage_query ON (true))
        LEFT JOIN LATERAL (
            SELECT
                CASE WHEN (smc_structures_query.treated_children = 0) THEN (0)::numeric ELSE ((smc_structures_query.referred_children)::numeric(7,2) / (smc_structures_query.treated_children)::numeric(7,2)) END AS referredcov
        ) referred_coverage_query ON (true))
    LEFT JOIN LATERAL (
        SELECT
            COALESCE(COUNT(refer_events.id),0) AS referral_form_count
        FROM
            events refer_events
        LEFT JOIN events referral_events ON (refer_events.event_type = 'mda_adherence' and refer_events.form_data ->> 'childHfReferred' <> '0') AND (refer_events.form_data ->> 'referralQRCode' = referral_events.form_data ->> 'referralQRCode')
        WHERE referral_events.event_type = 'hfw_referral'
        AND refer_events.location_id = nigeria_jurisdictions.jurisdiction_id
        AND refer_events.plan_id = plans.identifier
    ) referal_forms ON true
    LEFT JOIN LATERAL (
        SELECT
            COALESCE(SUM(fever_rdt_positive_child),0) AS fever_rdt_positive_child
        FROM (
            SELECT
                CASE WHEN (referral_events.form_data ->> 'referralReason' = 'Fever' AND referral_events.form_data ->> 'isChildNegativeRDT' = 'no') THEN 1 ELSE 0 END AS fever_rdt_positive_child
            FROM
                events referral_events
            WHERE referral_events.event_type = 'hfw_referral'
            AND referral_events.location_id = nigeria_jurisdictions.jurisdiction_id
            AND referral_events.plan_id = plans.identifier
        ) fever_sum
    ) fever_query_sum ON true
    WHERE ((plans.status)::text <> ALL ((ARRAY['draft'::character varying,'retired'::character varying])::text[]))
) main_query
ORDER BY main_query.jurisdiction_name;

CREATE INDEX IF NOT EXISTS smc_focus_area_path_idx_gin on smc_focus_area using GIN(jurisdiction_path);
CREATE INDEX IF NOT EXISTS smc_focus_area_plan_idx ON smc_focus_area (plan_id);
CREATE INDEX IF NOT EXISTS smc_focus_area_jurisdiction_idx ON smc_focus_area (jurisdiction_id);
CREATE INDEX IF NOT EXISTS smc_focus_area_jurisdiction_parent_idx ON smc_focus_area (jurisdiction_parent_id);
CREATE UNIQUE INDEX IF NOT EXISTS smc_focus_area_idx ON smc_focus_area (id);