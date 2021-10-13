SET schema 'reveal';

DROP MATERIALIZED VIEW IF EXISTS irs_lite_operational_areas CASCADE;

CREATE MATERIALIZED VIEW IF NOT EXISTS irs_lite_operational_areas
AS
SELECT
    main_query.*,
    CASE
        WHEN main_query.totStruct = 0 THEN 0
        ELSE CAST(main_query.sprayed AS DECIMAL)/CAST(main_query.totStruct AS DECIMAL)
    END AS sprayCov,
    CASE
        WHEN main_query.targStruct = 0 THEN 0
        ELSE CAST(main_query.sprayed AS DECIMAL)/CAST(main_query.targStruct AS DECIMAL)
    END AS sprayCovTarg,
    CASE
        WHEN main_query.targStruct = 0 THEN 0
        ELSE CAST(main_query.found AS DECIMAL)/CAST(main_query.targStruct AS DECIMAL)
    END AS foundCoverage,
    CASE
        WHEN main_query.found = 0 THEN 0
        ELSE CAST(main_query.sprayed AS DECIMAL)/CAST(main_query.found AS DECIMAL)
    END AS spraySuccess
FROM (
    SELECT
        public.uuid_generate_v5(
            '6ba7b810-9dad-11d1-80b4-00c04fd430c8',
            concat(operational_area_query.jurisdiction_id, operational_area_query.plan_id)
        ) AS id,
        operational_area_query.plan_id as plan_id,
        lite_jurisdictions.jurisdiction_id AS jurisdiction_id,
        lite_jurisdictions.jurisdiction_parent_id AS jurisdiction_parent_id,
        lite_jurisdictions.jurisdiction_name AS jurisdiction_name,
        lite_jurisdictions.jurisdiction_depth AS jurisdiction_depth,
        lite_jurisdictions.jurisdiction_path AS jurisdiction_path,
        lite_jurisdictions.jurisdiction_name_path AS jurisdiction_name_path,
        operational_area_query.targAreas,
        operational_area_query.visitedAreas,
        operational_area_query.found,
        operational_area_query.sprayed,
        COALESCE(operational_area_query.totStruct, 0) AS totStruct,
        COALESCE(operational_area_query.targStruct, 0) AS targStruct,
        COALESCE(totAreas_query.totAreas, 0) AS totAreas
    FROM plans
    LEFT JOIN (
        SELECT
            jurisdictions.plan_id,
            jurisdictions.jurisdiction_id,
            sum(jurisdictions.targAreas) AS targAreas,
            sum(jurisdictions.visitedAreas) AS visitedAreas,
            sum(jurisdictions.totStruct) AS totStruct,
            sum(jurisdictions.targStruct) AS targStruct,
            COALESCE(sum(daily_summary.daily_found), 0) AS found,
            COALESCE(sum(daily_summary.daily_sprayed), 0) AS sprayed
        FROM (
            SELECT
                structures.plan_id,
                structures.jurisdiction_id,
                COALESCE(COUNT(structures.structure_id) , 0) AS targAreas,
                COALESCE(COUNT(structures.structure_id) FILTER (WHERE structures.task_status = 'Completed') , 0) AS visitedAreas,
                COALESCE(SUM(structures.totStruct), 0) AS totStruct,
                COALESCE(SUM(structures.totStruct) FILTER (WHERE structures.target_flag = 1), 0) AS targStruct
            FROM irs_lite_structures AS structures
            WHERE structures.plan_id IS NOT NULL
            GROUP BY structures.plan_id, structures.jurisdiction_id
        ) AS jurisdictions
        LEFT JOIN LATERAL (
            SELECT
            subq.daily_sprayed AS daily_sprayed,
            subq.daily_found AS daily_found
            FROM (
                SELECT
                    SUM(COALESCE (events.form_data -> 'sprayed' ->> 0, '0')::int) AS daily_sprayed,
                    SUM(COALESCE (events.form_data -> 'found' ->> 0, '0')::int) AS daily_found
                FROM events
                WHERE jurisdictions.jurisdiction_id  = events.location_id
                AND events.plan_id = jurisdictions.plan_id
                AND events.entity_type = 'Structure'
                AND events.event_type = 'daily_summary'
                GROUP BY jurisdictions.plan_id, jurisdictions.jurisdiction_id
            ) AS subq
        ) AS daily_summary ON true
        GROUP BY jurisdictions.plan_id, jurisdictions.jurisdiction_id
    ) AS operational_area_query ON operational_area_query.plan_id = plans.identifier
    LEFT JOIN jurisdictions_materialized_view AS lite_jurisdictions
        ON operational_area_query.jurisdiction_id = lite_jurisdictions.jurisdiction_id
    LEFT JOIN LATERAL (
        SELECT
            COALESCE(COUNT(locations.id) , 0) AS totAreas
        FROM locations
        WHERE operational_area_query.jurisdiction_id = locations.jurisdiction_id
        AND locations.geographic_level = 4
    ) AS totAreas_query ON true
    -- LEFT JOIN LATERAL (
    --     SELECT
    --         key as jurisdiction_id,
    --         COALESCE(data ->> 0, '0')::INTEGER as target
    --     FROM opensrp_settings
    --     WHERE identifier = 'jurisdiction_metadata-target'
    --     AND operational_area_query.jurisdiction_id = opensrp_settings.key
    --     ORDER BY COALESCE(data->>'serverVersion', '0')::BIGINT DESC
    --     LIMIT 1
    -- ) AS jurisdiction_target_query ON true
    WHERE plans.intervention_type IN ('IRS-Lite') AND plans.status NOT IN ('draft', 'retired')
) AS main_query;

CREATE INDEX IF NOT EXISTS irs_lite_operational_areas_path_idx_gin on irs_lite_operational_areas using GIN(jurisdiction_path);
CREATE INDEX IF NOT EXISTS irs_lite_operational_areas_plan_idx ON irs_lite_operational_areas (plan_id);
CREATE INDEX IF NOT EXISTS irs_lite_operational_areas_jurisdiction_idx ON irs_lite_operational_areas (jurisdiction_id);
CREATE INDEX IF NOT EXISTS irs_lite_operational_areas_jurisdiction_parent_idx ON irs_lite_operational_areas (jurisdiction_parent_id);
CREATE UNIQUE INDEX IF NOT EXISTS irs_lite_operational_areas_idx ON irs_lite_operational_areas (id);