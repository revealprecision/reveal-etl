SET schema 'reveal';

DROP MATERIALIZED VIEW IF EXISTS irs_jurisdictions CASCADE;

CREATE MATERIALIZED VIEW IF NOT EXISTS irs_jurisdictions
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
    main_query.totstruct,
    main_query.rooms_eligible,
    main_query.rooms_sprayed,
    main_query.sprayed_rooms_eligible,
    main_query.sprayed_rooms_sprayed,
    main_query.foundstruct,
    main_query.notsprayed,
    main_query.noteligible,
    main_query.sprayedstruct,
    main_query.totareas,
    main_query.targareas,
    main_query.visitedareas,
    main_query.targstruct,
    main_query.perctvisareaseffect,
    main_query.spraycovtarg,
    main_query.foundcoverage,
    main_query.spraysuccess,
    main_query.roomcov,
    main_query.avg_spray_effectiveness,
    main_query.rooms_on_ground,
    main_query.structures_remaining_to_90_se,
    main_query.tla_days_to_90_se,
    main_query.num_health_centers_below_90_se,
    main_query.num_spray_areas_below_90_se,
    main_query.latest_spray_event_id,
    main_query.latest_spray_event_date,
    main_query.latest_sa_event_id,
    main_query.latest_sa_event_date
FROM (
    SELECT
        DISTINCT ON (jurisdictions_query.jurisdiction_id, plans.identifier) public.uuid_generate_v5('6ba7b810-9dad-11d1-80b4-00c04fd430c8'::uuid, concat(jurisdictions_query.jurisdiction_id, plans.identifier)) AS id,
            plans.identifier AS plan_id,
            jurisdictions_query.jurisdiction_id,
            COALESCE(jurisdictions_query.jurisdiction_parent_id, ''::character varying) AS jurisdiction_parent_id,
            jurisdictions_query.jurisdiction_name,
            jurisdictions_query.jurisdiction_depth,
            jurisdictions_query.jurisdiction_path,
            jurisdictions_query.jurisdiction_name_path,
            jurisdictions_query.is_virtual_jurisdiction,
            jurisdictions_query.is_leaf_node,
            jurisdictions_query.totstruct,
            jurisdictions_query.rooms_eligible,
            jurisdictions_query.rooms_sprayed,
            jurisdictions_query.sprayed_rooms_eligible,
            jurisdictions_query.sprayed_rooms_sprayed,
            jurisdictions_query.foundstruct,
            jurisdictions_query.notsprayed,
            jurisdictions_query.noteligible,
            jurisdictions_query.sprayedstruct,
                CASE
                    WHEN jurisdictions_query.is_virtual_jurisdiction THEN (0)::bigint
                    ELSE jurisdictions_query.totareas
                END AS totareas,
                CASE
                    WHEN jurisdictions_query.is_virtual_jurisdiction THEN (0)::bigint
                    ELSE jurisdictions_query.targareas
                END AS targareas,
                CASE
                    WHEN jurisdictions_query.is_virtual_jurisdiction THEN (0)::bigint
                    ELSE jurisdictions_query.visitedareas
                END AS visitedareas,
            jurisdictions_query.targstruct,
            jurisdictions_query.perctvisareaseffect,
            jurisdictions_query.spraycovtarg,
            jurisdictions_query.foundcoverage,
            jurisdictions_query.spraysuccess,
            jurisdictions_query.rooms_on_ground,
            jurisdictions_query.roomcov,
            jurisdictions_query.avg_spray_effectiveness,
            jurisdictions_query.structures_remaining_to_90_se,
            jurisdictions_query.tla_days_to_90_se,
            jurisdictions_query.num_health_centers_below_90_se,
            jurisdictions_query.num_spray_areas_below_90_se,
            jurisdictions_query.latest_spray_event_id,
            jurisdictions_query.latest_spray_event_date,
            jurisdictions_query.latest_sa_event_id,
            jurisdictions_query.latest_sa_event_date
        FROM (reveal.plans
        LEFT JOIN LATERAL (
            SELECT
                irs_jurisdictions_tree.jurisdiction_id,
                COALESCE(irs_jurisdictions_tree.jurisdiction_parent_id, ''::character varying) AS jurisdiction_parent_id,
                irs_jurisdictions_tree.jurisdiction_name,
                irs_jurisdictions_tree.jurisdiction_geometry,
                irs_jurisdictions_tree.jurisdiction_depth,
                irs_jurisdictions_tree.jurisdiction_path,
                irs_jurisdictions_tree.jurisdiction_name_path,
                irs_jurisdictions_tree.is_virtual_jurisdiction,
                irs_jurisdictions_tree.is_leaf_node,
                irs_focus_area_base_query.totstruct,
                irs_focus_area_base_query.rooms_eligible,
                irs_focus_area_base_query.rooms_sprayed,
                irs_focus_area_base_query.sprayed_rooms_eligible,
                irs_focus_area_base_query.sprayed_rooms_sprayed,
                irs_focus_area_base_query.foundstruct,
                irs_focus_area_base_query.notsprayed,
                irs_focus_area_base_query.noteligible,
                irs_focus_area_base_query.sprayedstruct,
                irs_focus_area_base_query.totareas,
                irs_focus_area_base_query.targareas,
                irs_focus_area_base_query.targstruct,
                irs_focus_area_base_query.visitedareas,
                    CASE
                        WHEN (irs_focus_area_base_query.spraycovabovemin = 0) THEN (0)::numeric
                        ELSE ((irs_focus_area_base_query.spraycovhigh)::numeric / (irs_focus_area_base_query.spraycovabovemin)::numeric)
                    END AS perctvisareaseffect,
                    CASE
                        WHEN (irs_focus_area_base_query.targstruct = (0)::numeric) THEN (0)::numeric
                        ELSE (irs_focus_area_base_query.sprayedstruct / irs_focus_area_base_query.targstruct)
                    END AS spraycovtarg,
                    CASE
                        WHEN (irs_focus_area_base_query.targstruct = (0)::numeric) THEN (0)::numeric
                        ELSE (irs_focus_area_base_query.foundstruct / irs_focus_area_base_query.targstruct)
                    END AS foundcoverage,
                    CASE
                        WHEN (irs_focus_area_base_query.foundstruct = (0)::numeric) THEN (0)::numeric
                        ELSE (irs_focus_area_base_query.sprayedstruct / irs_focus_area_base_query.foundstruct)
                    END AS spraysuccess,
                    CASE
                        WHEN (irs_focus_area_base_query.sprayed_rooms_eligible = (0)::numeric) THEN (0)::numeric
                        ELSE (irs_focus_area_base_query.sprayed_rooms_sprayed / irs_focus_area_base_query.sprayed_rooms_eligible)
                    END AS roomcov,
                    CASE
                      WHEN (irs_focus_area_base_query.foundstruct = (0)::numeric) THEN (0)::numeric
                      ELSE ( irs_focus_area_base_query.sprayed_rooms_eligible/ irs_focus_area_base_query.foundstruct)
                    END AS rooms_on_ground,
                irs_focus_area_base_query.avg_spray_effectiveness,
                irs_focus_area_base_query.structures_remaining_to_90_se,
                irs_focus_area_base_query.tla_days_to_90_se,
                irs_focus_area_base_query.num_health_centers_below_90_se,
                irs_focus_area_base_query.num_spray_areas_below_90_se,
                irs_focus_area_base_query.latest_spray_event_id,
                irs_focus_area_base_query.latest_spray_event_date,
                irs_focus_area_base_query.latest_sa_event_id,
                irs_focus_area_base_query.latest_sa_event_date
            FROM (reveal.irs_jurisdictions_tree
            LEFT JOIN LATERAL (
                SELECT
                    COALESCE(count(irs_focus_area_base.jurisdiction_id), (0)::bigint) AS totareas,
                    COALESCE(count(irs_focus_area_base.jurisdiction_id) FILTER (WHERE (irs_focus_area_base.targstruct > 0)), (0)::bigint) AS targareas,
                    COALESCE(sum(irs_focus_area_base.targstruct), (0)::numeric) AS targstruct,
                    COALESCE(count(irs_focus_area_base.jurisdiction_id) FILTER (WHERE (irs_focus_area_base.foundstruct > 0)), (0)::bigint) AS visitedareas,
                    COALESCE(count(irs_focus_area_base.jurisdiction_id) FILTER (WHERE (irs_focus_area_base.spraycov > 0.85)), (0)::bigint) AS spraycovhigh,
                    COALESCE(count(irs_focus_area_base.jurisdiction_id) FILTER (WHERE ((irs_focus_area_base.spraycov > 0.20) AND (irs_focus_area_base.spraycov < 0.85))), (0)::bigint) AS spraycovlow,
                    COALESCE(count(irs_focus_area_base.jurisdiction_id) FILTER (WHERE (irs_focus_area_base.spraycov > 0.20)), (0)::bigint) AS spraycovabovemin,
                    COALESCE(sum(irs_focus_area_base.totstruct), (0)::numeric) AS totstruct,
                    COALESCE(sum(irs_focus_area_base.rooms_eligible), (0)::numeric) AS rooms_eligible,
                    COALESCE(sum(irs_focus_area_base.rooms_sprayed), (0)::numeric) AS rooms_sprayed,
                    COALESCE(sum(irs_focus_area_base.sprayed_rooms_eligible), (0)::numeric) AS sprayed_rooms_eligible,
                    COALESCE(sum(irs_focus_area_base.sprayed_rooms_sprayed), (0)::numeric) AS sprayed_rooms_sprayed,
                    COALESCE(sum(irs_focus_area_base.foundstruct), (0)::numeric) AS foundstruct,
                    COALESCE(sum(irs_focus_area_base.notsprayed), (0)::numeric) AS notsprayed,
                    COALESCE(sum(irs_focus_area_base.noteligible), (0)::numeric) AS noteligible,
                    COALESCE(sum(irs_focus_area_base.sprayedstruct), (0)::numeric) AS sprayedstruct,
                    count(DISTINCT
                        CASE
                            WHEN ((NOT irs_focus_area_base.is_virtual_jurisdiction) AND (COALESCE(irs_focus_area_base.structures_remaining_to_90_se, (0)::numeric) > (0)::numeric)) THEN irs_focus_area_base.health_center_jurisdiction_id
                            ELSE NULL::character varying
                        END) AS num_health_centers_below_90_se,
                    sum(
                        CASE
                            WHEN ((NOT irs_focus_area_base.is_virtual_jurisdiction) AND (COALESCE(irs_focus_area_base.structures_remaining_to_90_se, (0)::numeric) > (0)::numeric)) THEN 1
                            ELSE 0
                        END) AS num_spray_areas_below_90_se,
                    avg(irs_focus_area_base.spray_effectiveness) AS avg_spray_effectiveness,
                    sum(COALESCE(irs_focus_area_base.structures_remaining_to_90_se, (0)::numeric)) AS structures_remaining_to_90_se,
                    sum(COALESCE(irs_focus_area_base.tla_days_to_90_se, (0)::numeric)) AS tla_days_to_90_se,
                    (max(ARRAY[(to_json(irs_focus_area_base.latest_spray_event_date) #>> '{}'::text[]), irs_focus_area_base.latest_spray_event_id]) FILTER (WHERE (irs_focus_area_base.latest_spray_event_id IS NOT NULL)))[2] AS latest_spray_event_id,
                    max(irs_focus_area_base.latest_spray_event_date) AS latest_spray_event_date,
                    (max(ARRAY[(to_json(irs_focus_area_base.latest_sa_event_date) #>> '{}'::text[]), (irs_focus_area_base.latest_sa_event_id)::text]) FILTER (WHERE (irs_focus_area_base.latest_sa_event_id IS NOT NULL)))[2] AS latest_sa_event_id,
                    max(irs_focus_area_base.latest_sa_event_date) AS latest_sa_event_date
                FROM reveal.irs_focus_area_base irs_focus_area_base
                WHERE (((irs_focus_area_base.plan_id)::text = (plans.identifier)::text) AND ((irs_jurisdictions_tree.is_virtual_jurisdiction AND irs_focus_area_base.is_virtual_jurisdiction AND (irs_focus_area_base.jurisdiction_path @> ARRAY[irs_jurisdictions_tree.jurisdiction_parent_id])) OR ((NOT irs_jurisdictions_tree.is_virtual_jurisdiction) AND (NOT irs_focus_area_base.is_virtual_jurisdiction) AND (irs_focus_area_base.jurisdiction_path @> ARRAY[irs_jurisdictions_tree.jurisdiction_id]))))
            ) irs_focus_area_base_query ON (true))
            WHERE ((irs_jurisdictions_tree.is_leaf_node = false) AND (irs_focus_area_base_query.totareas > 0))) jurisdictions_query ON (true))
    WHERE (((plans.intervention_type)::text = ANY ((ARRAY['IRS'::character varying, 'Dynamic-IRS'::character varying])::text[])) AND ((plans.status)::text <> ALL ((ARRAY['draft'::character varying, 'retired'::character varying])::text[])))) main_query
ORDER BY
    CASE
        WHEN main_query.is_virtual_jurisdiction THEN 1
        ELSE 0
    END, main_query.jurisdiction_name;

CREATE INDEX IF NOT EXISTS irs_jurisdictions_parent_id_idx ON irs_jurisdictions (jurisdiction_parent_id);
CREATE INDEX IF NOT EXISTS irs_jurisdictions_jurisdiction_depth_idx ON irs_jurisdictions (jurisdiction_depth);
