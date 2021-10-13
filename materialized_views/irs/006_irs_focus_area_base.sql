SET schema 'reveal';

DROP MATERIALIZED VIEW IF EXISTS irs_focus_area_base CASCADE;

CREATE MATERIALIZED VIEW IF NOT EXISTS irs_focus_area_base
AS
SELECT
    main_query.id,
    main_query.plan_id,
    main_query.jurisdiction_id,
    main_query.jurisdiction_parent_id,
    main_query.jurisdiction_name,
    main_query.jurisdiction_depth,
    main_query.jurisdiction_path,
    main_query.jurisdiction_name_path,
    main_query.is_virtual_jurisdiction,
    main_query.is_leaf_node,
    main_query.health_center_jurisdiction_id,
    main_query.health_center_jurisdiction_name,
    main_query.totstruct,
    main_query.targstruct,
    main_query.rooms_eligible,
    main_query.rooms_sprayed,
    main_query.sprayed_rooms_eligible,
    main_query.sprayed_rooms_sprayed,
    main_query.foundstruct,
    main_query.notsprayed,
    main_query.noteligible,
    main_query.notasks,
    main_query.sprayedstruct,
    main_query.duplicates,
    main_query.sprayed_totalpop,
    main_query.sprayed_totalmale,
    main_query.sprayed_totalfemale,
    main_query.sprayed_males,
    main_query.sprayed_females,
    main_query.sprayed_pregwomen,
    main_query.sprayed_childrenU5,
    main_query.notsprayed_totalpop,
    main_query.notsprayed_males,
    main_query.notsprayed_females,
    main_query.notsprayed_pregwomen,
    main_query.notsprayed_childrenU5,
    main_query.sprayed_duplicates,
    main_query.notsprayed_reasons,
    main_query.notsprayed_reasons_counts,
    main_query.latest_spray_event_id,
    main_query.latest_spray_event_date,
    main_query.spraycov,
    main_query.rooms_on_ground,
    main_query.spraytarg,
    main_query.spraysuccess,
    main_query.spray_effectiveness,
    main_query.structures_remaining_to_90_se,
    main_query.tla_days_to_90_se,
    main_query.roomcov,
    main_query.reviewed_with_decision,
    main_query.latest_sa_event_id,
    main_query.latest_sa_event_date
FROM (
    SELECT
        public.uuid_generate_v5('6ba7b810-9dad-11d1-80b4-00c04fd430c8'::uuid, concat(plans.identifier, irs_jurisdictions_tree.jurisdiction_id)) AS id,
        plans.identifier AS plan_id,
        irs_jurisdictions_tree.jurisdiction_id,
        COALESCE(irs_jurisdictions_tree.jurisdiction_parent_id, ''::character varying) AS jurisdiction_parent_id,
        irs_jurisdictions_tree.jurisdiction_name,
        irs_jurisdictions_tree.jurisdiction_depth,
        irs_jurisdictions_tree.jurisdiction_path,
        irs_jurisdictions_tree.jurisdiction_name_path,
        irs_jurisdictions_tree.is_virtual_jurisdiction,
        irs_jurisdictions_tree.is_leaf_node,
        health_center_jurisdictions.id AS health_center_jurisdiction_id,
        health_center_jurisdictions.name AS health_center_jurisdiction_name,
        irs_structures.totstruct,
        CASE
            WHEN COALESCE(operational_target.target,0::numeric) > 0 THEN irs_structures.totstruct
            ELSE 0::numeric
        END AS targstruct,
        irs_structures.rooms_eligible,
        irs_structures.rooms_sprayed,
        irs_structures.sprayed_rooms_eligible,
        irs_structures.sprayed_rooms_sprayed,
        irs_structures.foundstruct,
        irs_structures.notsprayed,
        inactive_irs_structures.noteligible,
        inactive_irs_structures.notasks,
        irs_structures.sprayedstruct,
        irs_structures.duplicates,
        irs_structures.sprayed_duplicates,
        irs_structures.sprayed_totalpop,
        irs_structures.sprayed_totalmale,
        irs_structures.sprayed_totalfemale,
        irs_structures.sprayed_males,
        irs_structures.sprayed_females,
        irs_structures.sprayed_pregwomen,
        irs_structures.sprayed_childrenU5,
        irs_structures.notsprayed_totalpop,
        irs_structures.notsprayed_males,
        irs_structures.notsprayed_females,
        irs_structures.notsprayed_pregwomen,
        irs_structures.notsprayed_childrenU5,
        COALESCE(irs_structures.notsprayed_reasons, '{}'::text[]) AS notsprayed_reasons,
        COALESCE(reveal.count_elements(irs_structures.notsprayed_reasons), '{}'::json) AS notsprayed_reasons_counts,
        irs_structures.latest_spray_event_id,
        irs_structures.latest_spray_event_date,
        coverage_query.spraycov,
        CASE
            WHEN (irs_structures.targstruct = 0) THEN (0)::numeric
            ELSE ((irs_structures.foundstruct)::numeric / (irs_structures.targstruct)::numeric)
        END AS spraytarg,
        CASE
            WHEN (irs_structures.foundstruct = 0) THEN (0)::numeric
            ELSE ((irs_structures.sprayedstruct)::numeric / (irs_structures.foundstruct)::numeric)
        END AS spraysuccess,
        CASE
            WHEN (irs_structures.totstruct = 0) THEN NULL::numeric
            ELSE ((irs_structures.sprayedstruct)::numeric / (irs_structures.totstruct)::numeric)
        END AS spray_effectiveness,
        GREATEST((0)::numeric, ceil((((irs_structures.totstruct)::numeric * 0.9) - (irs_structures.sprayedstruct)::numeric))) AS structures_remaining_to_90_se,
        (GREATEST((0)::numeric, (((irs_structures.totstruct)::numeric * 0.9) - (irs_structures.sprayedstruct)::numeric)) / 15.0) AS tla_days_to_90_se,
            CASE
                WHEN (irs_structures.sprayed_rooms_eligible = 0) THEN (0)::numeric
                ELSE ((irs_structures.sprayed_rooms_sprayed)::numeric / (irs_structures.sprayed_rooms_eligible)::numeric)
            END AS roomcov,
            CASE
                WHEN (irs_structures.foundstruct = 0) THEN (0)::numeric
             ELSE ((irs_structures.sprayed_rooms_eligible)::numeric / (irs_structures.foundstruct)::numeric)
            END AS rooms_on_ground,
            CASE
                WHEN (coverage_query.spraycov >= 0.9) THEN '0'::text
                WHEN (irs_sa_events.latest_event_id IS NOT NULL) THEN 'Reviewed with decision'::text
                WHEN (coverage_query.spraycov = (0)::numeric) THEN 'n/a'::text
                ELSE 'Not done'::text
            END AS reviewed_with_decision,
        irs_sa_events.latest_event_id AS latest_sa_event_id,
        irs_sa_events.latest_event_date AS latest_sa_event_date
    FROM (
        (
            (
                (
                    (
                        (
                            (reveal.plans
                            JOIN reveal.irs_plan_jurisdictions irs_plan_jurisdictions ON (((plans.identifier)::text = (irs_plan_jurisdictions.plan_id)::text)))
                            JOIN reveal.irs_jurisdictions_tree irs_jurisdictions_tree ON (((irs_plan_jurisdictions.zambia_jurisdiction_id)::text = (irs_jurisdictions_tree.jurisdiction_id)::text)))
                            JOIN reveal.jurisdictions health_center_jurisdictions ON (((health_center_jurisdictions.id)::text = (irs_jurisdictions_tree.jurisdiction_path[4])::text)))
                            LEFT JOIN LATERAL (
                                SELECT
                                    COALESCE(count(irs_structures.structure_id), (0)::bigint) AS totstruct,
                                    COALESCE(count(irs_structures.structure_id) FILTER (WHERE (irs_structures.structure_status::text = ANY ((ARRAY['Active'::character varying, 'Pending Review'::character varying])::text[]))), (0)::bigint) AS targstruct,
                                    COALESCE(count(irs_structures.structure_id) FILTER (WHERE (irs_structures.business_status <> 'Not Visited'::text)), (0)::bigint) AS foundstruct,
                                    COALESCE(count(irs_structures.structure_id) FILTER (WHERE (irs_structures.business_status = 'Not Sprayed'::text)), (0)::bigint) AS notsprayed,
                                    COALESCE(count(irs_structures.structure_id) FILTER (WHERE (irs_structures.business_status = ANY (ARRAY['Partially Sprayed'::text, 'Complete'::text]))), (0)::bigint) AS sprayedstruct,
                                    COALESCE(sum(irs_structures.rooms_eligible), (0)::bigint) AS rooms_eligible,
                                    COALESCE(sum(irs_structures.rooms_eligible) FILTER (WHERE (irs_structures.business_status = ANY (ARRAY['Partially Sprayed'::text, 'Complete'::text]))), (0)::bigint) AS sprayed_rooms_eligible,
                                    COALESCE(sum(irs_structures.rooms_sprayed), (0)::bigint) AS rooms_sprayed,
                                    COALESCE(sum(irs_structures.rooms_sprayed) FILTER (WHERE (irs_structures.business_status = ANY (ARRAY['Partially Sprayed'::text, 'Complete'::text]))), (0)::bigint) AS sprayed_rooms_sprayed,
                                    COALESCE(count(irs_structures.structure_id) FILTER (WHERE (irs_structures.duplicate = true)), (0)::bigint) AS duplicates,
                                    COALESCE(count(irs_structures.structure_id) FILTER (WHERE (irs_structures.sprayed_duplicate = true)), (0)::bigint) AS sprayed_duplicates,
                                    COALESCE(sum(irs_structures.sprayed_totalpop), (0)::bigint) AS sprayed_totalpop,
                                    COALESCE(sum(irs_structures.sprayed_totalmale), (0)::bigint) AS sprayed_totalmale,
                                    COALESCE(sum(irs_structures.sprayed_totalfemale), (0)::bigint) AS sprayed_totalfemale,
                                    COALESCE(sum(irs_structures.sprayed_males), (0)::bigint) AS sprayed_males,
                                    COALESCE(sum(irs_structures.sprayed_females), (0)::bigint) AS sprayed_females,
                                    COALESCE(sum(irs_structures.sprayed_pregwomen), (0)::bigint) AS sprayed_pregwomen,
                                    COALESCE(sum(irs_structures.sprayed_childrenU5), (0)::bigint) AS sprayed_childrenU5,
                                    COALESCE(sum(irs_structures.notsprayed_totalpop), (0)::bigint) AS notsprayed_totalpop,
                                    COALESCE(sum(irs_structures.notsprayed_males), (0)::bigint) AS notsprayed_males,
                                    COALESCE(sum(irs_structures.notsprayed_females), (0)::bigint) AS notsprayed_females,
                                    COALESCE(sum(irs_structures.notsprayed_pregwomen), (0)::bigint) AS notsprayed_pregwomen,
                                    COALESCE(sum(irs_structures.notsprayed_childrenU5), (0)::bigint) AS notsprayed_childrenU5,
                                    reveal.array_concat_agg(irs_structures.notsprayed_reasons) FILTER (WHERE (irs_structures.notsprayed_reasons <> '{}'::text[])) AS notsprayed_reasons,
                                    (max(ARRAY[(to_json(irs_structures.event_date) #>> '{}'::text[]), (irs_structures.event_id)::text]) FILTER (WHERE (irs_structures.event_id IS NOT NULL)))[2] AS latest_spray_event_id,
                                    max(irs_structures.event_date) AS latest_spray_event_date
                                FROM reveal.irs_structures irs_structures
                                WHERE (((irs_structures.structure_jurisdiction_id)::text = (irs_jurisdictions_tree.jurisdiction_id)::text)
                                AND (irs_structures.business_status <> ALL (ARRAY['Not Eligible'::text, 'No Tasks'::text]))
                                AND (((plans.identifier)::text = (irs_structures.plan_id)::text) OR (irs_structures.plan_id IS NULL)))
                            ) irs_structures ON (true))
                            LEFT JOIN LATERAL (
                                SELECT
                                    CASE
                                        WHEN (irs_structures.totstruct = 0) THEN (0)::numeric
                                        ELSE ((irs_structures.sprayedstruct)::numeric / (irs_structures.totstruct)::numeric)
                                    END AS spraycov
                            ) coverage_query ON (true))
                            LEFT JOIN LATERAL (
                                SELECT
                                    opensrp_settings.key as jurisdiction_id,
                                    (opensrp_settings.data ->> 0)::numeric AS target
                                FROM opensrp_settings
                                WHERE identifier = plans.identifier
                                AND opensrp_settings.key = irs_plan_jurisdictions.zambia_jurisdiction_id
                            ) AS operational_target ON TRUE
                            LEFT JOIN LATERAL (
                                SELECT
                                    COALESCE(count(irs_structures.structure_id) FILTER (WHERE (irs_structures.business_status = 'Not Eligible'::text)), (0)::bigint) AS noteligible,
                                    COALESCE(count(irs_structures.structure_id) FILTER (WHERE (irs_structures.business_status = 'No Tasks'::text)), (0)::bigint) AS notasks
                                FROM reveal.irs_structures irs_structures
                                WHERE (
                                    ((irs_structures.structure_jurisdiction_id)::text = (irs_jurisdictions_tree.jurisdiction_id)::text)
                                    AND (irs_structures.business_status = ANY (ARRAY['Not Eligible'::text, 'No Tasks'::text]))
                                    AND (
                                        ((plans.identifier)::text = (irs_structures.plan_id)::text)
                                        OR (irs_structures.plan_id IS NULL)
                                    )
                                )
                            ) inactive_irs_structures ON (true))
                            LEFT JOIN LATERAL (
                                SELECT
                                    events.id AS latest_event_id,
                                    events.event_date AS latest_event_date,
                                    form_data ->> 'spray_areas',
                                    jurisdictions.id
                                FROM reveal.events
                                LEFT JOIN jurisdictions ON events.form_data ->> 'spray_areas' = jurisdictions.name
                                WHERE ((events.event_type)::text = 'irs_sa_decision'::text)
                                AND (jurisdictions.id)::text = (irs_jurisdictions_tree.jurisdiction_id)::text
                                ORDER BY (events.form_data ->> 'end'::text) DESC, events.server_version DESC, events.id DESC
                                LIMIT 1
                            ) irs_sa_events ON (true))
                            WHERE (((plans.intervention_type)::text = ANY ((ARRAY['IRS'::character varying, 'Dynamic-IRS'::character varying])::text[]))
                            AND ((plans.status)::text <> ALL ((ARRAY['draft'::character varying, 'retired'::character varying])::text[])))
                        ) main_query
   ORDER BY
         CASE
             WHEN main_query.is_virtual_jurisdiction THEN 1
             ELSE 0
         END, main_query.jurisdiction_name;
