DROP MATERIALIZED VIEW IF EXISTS irs_lite_jurisdictions CASCADE;

CREATE MATERIALIZED VIEW IF NOT EXISTS irs_lite_jurisdictions
AS
SELECT
    *
FROM
(
    (
        SELECT DISTINCT ON (jurisdictions_query.jurisdiction_id, plans.identifier)
            public.uuid_generate_v5(
                '6ba7b810-9dad-11d1-80b4-00c04fd430c8',
                concat(jurisdictions_query.jurisdiction_id, plans.identifier)) AS id,
            plans.identifier as plan_id,
            jurisdictions_query.jurisdiction_id AS jurisdiction_id,
            jurisdictions_query.jurisdiction_parent_id AS jurisdiction_parent_id,
            jurisdictions_query.jurisdiction_name AS jurisdiction_name,
            jurisdictions_query.jurisdiction_depth AS jurisdiction_depth,
            jurisdictions_query.jurisdiction_path AS jurisdiction_path,
            jurisdictions_query.jurisdiction_name_path AS jurisdiction_name_path,
            jurisdictions_query.totStruct AS totStruct,
            jurisdictions_query.targStruct AS targStruct,
            jurisdictions_query.sprayed AS sprayed,
            jurisdictions_query.found AS found,
            jurisdictions_query.totAreas AS totAreas,
            jurisdictions_query.targAreas AS targAreas,
            jurisdictions_query.visitedAreas AS visitedAreas,
            jurisdictions_query.sprayCovTarg AS sprayCov,
            jurisdictions_query.sprayCovTarg AS sprayCovTarg,
            jurisdictions_query.foundCoverage AS foundCoverage,
            jurisdictions_query.spraySuccess AS spraySuccess
        FROM plans
        LEFT JOIN LATERAL (
            SELECT
                lite_jurisdictions.jurisdiction_id AS jurisdiction_id,
                COALESCE(lite_jurisdictions.jurisdiction_parent_id, '') AS jurisdiction_parent_id,
                lite_jurisdictions.jurisdiction_name AS jurisdiction_name,
                lite_jurisdictions.jurisdiction_geometry AS jurisdiction_geometry,
                lite_jurisdictions.jurisdiction_depth AS jurisdiction_depth,
                lite_jurisdictions.jurisdiction_path AS jurisdiction_path,
                lite_jurisdictions.jurisdiction_name_path AS jurisdiction_name_path,
                COALESCE(jurisdiction_structure_query.structure, focus_area_irs_query.totStruct, 0) AS totStruct,
                COALESCE(jurisdiction_target_query.target, focus_area_irs_query.targStruct, 0) AS targStruct,
                focus_area_irs_query.sprayed AS sprayed,
                focus_area_irs_query.found AS found,
                COALESCE(totAreas_query.totAreas, 0) AS totAreas,
                focus_area_irs_query.targAreas AS targAreas,
                focus_area_irs_query.visitedAreas AS visitedAreas,
                CASE
                    WHEN COALESCE(jurisdiction_structure_query.structure, focus_area_irs_query.totStruct, 0) = 0 THEN 0
                    ELSE CAST(focus_area_irs_query.sprayed AS DECIMAL)/CAST(COALESCE(jurisdiction_structure_query.structure, focus_area_irs_query.totStruct, 0) AS DECIMAL)
                END AS sprayCov,
                CASE
                    WHEN COALESCE(jurisdiction_target_query.target, focus_area_irs_query.targStruct, 0) = 0 THEN 0
                    ELSE CAST(focus_area_irs_query.sprayed AS DECIMAL)/CAST(COALESCE(jurisdiction_target_query.target, focus_area_irs_query.targStruct, 0) AS DECIMAL)
                END AS sprayCovTarg,
                CASE
                    WHEN COALESCE(jurisdiction_target_query.target, focus_area_irs_query.targStruct, 0) = 0 THEN 0
                    ELSE CAST(focus_area_irs_query.found AS DECIMAL)/CAST(COALESCE(jurisdiction_target_query.target, focus_area_irs_query.targStruct, 0) AS DECIMAL)
                END AS foundCoverage,
                CASE
                    WHEN focus_area_irs_query.found = 0 THEN 0
                    ELSE CAST(focus_area_irs_query.sprayed AS DECIMAL)/CAST(focus_area_irs_query.found AS DECIMAL)
                END AS spraySuccess
            FROM jurisdictions_materialized_view AS lite_jurisdictions
            LEFT JOIN LATERAL (
                SELECT
                    COALESCE(SUM(targAreas), 0) AS targAreas,
                    COALESCE(SUM(visitedAreas), 0) AS visitedAreas,
                    COALESCE(SUM(totStruct), 0) AS totStruct,
                    COALESCE(SUM(targStruct), 0) AS targStruct,
                    COALESCE(SUM(sprayed), 0) AS sprayed,
                    COALESCE(SUM(found), 0) AS found
                FROM irs_lite_operational_areas AS zirloa
                WHERE zirloa.plan_id = plans.identifier
                AND zirloa.jurisdiction_path @> ARRAY[lite_jurisdictions.jurisdiction_id]
            ) AS focus_area_irs_query ON true
            LEFT JOIN LATERAL (
                SELECT
                    COALESCE(COUNT(locations.id) , 0) AS totAreas
                FROM locations
                LEFT JOIN jurisdictions_materialized_view
                    ON jurisdictions_materialized_view.jurisdiction_id = locations.jurisdiction_id
                WHERE jurisdictions_materialized_view.jurisdiction_path  @> ARRAY[lite_jurisdictions.jurisdiction_id]
                AND locations.geographic_level = 4
            ) AS totAreas_query ON true
            LEFT JOIN LATERAL (
                SELECT
                    key as jurisdiction_id,
                    COALESCE(data->>'value', '0')::INTEGER as target
                FROM opensrp_settings
                WHERE identifier = 'jurisdiction_metadata-target'
                AND lite_jurisdictions.jurisdiction_id = opensrp_settings.key
                ORDER BY COALESCE(data->>'serverVersion', '0')::BIGINT DESC
                LIMIT 1
            ) AS jurisdiction_target_query ON true
            LEFT JOIN LATERAL (
                SELECT
                    key as jurisdiction_id,
                    COALESCE(data->>'value', '0')::INTEGER as structure
                FROM opensrp_settings
                WHERE identifier = 'jurisdiction_metadata-structures'
                AND lite_jurisdictions.jurisdiction_id = opensrp_settings.key
                ORDER BY COALESCE(data->>'serverVersion', '0')::BIGINT DESC
                LIMIT 1
            ) AS jurisdiction_structure_query ON true
            WHERE lite_jurisdictions.jurisdiction_depth < 4
            AND focus_area_irs_query.targAreas > 0
        ) AS jurisdictions_query ON true
        WHERE plans.intervention_type IN ('IRS-Lite') AND plans.status NOT IN ('draft', 'retired')
    )
    UNION (
        SELECT
            id,
            plan_id,
            jurisdiction_id,
            jurisdiction_parent_id,
            jurisdiction_name,
            jurisdiction_depth,
            jurisdiction_path,
            jurisdiction_name_path,
            totStruct,
            targStruct,
            sprayed,
            found,
            totAreas,
            targAreas,
            visitedAreas,
            sprayCov,
            sprayCovTarg,
            foundCoverage,
            spraySuccess
        FROM irs_lite_operational_areas
    )
)
AS main_query
ORDER BY main_query.jurisdiction_name;

CREATE INDEX IF NOT EXISTS irs_lite_jurisdictions_path_idx_gin on irs_lite_jurisdictions using GIN(jurisdiction_path);
CREATE INDEX IF NOT EXISTS irs_lite_jurisdictions_plan_idx ON irs_lite_jurisdictions (plan_id);
CREATE INDEX IF NOT EXISTS irs_lite_jurisdictions_jurisdiction_idx ON irs_lite_jurisdictions (jurisdiction_id);
CREATE INDEX IF NOT EXISTS irs_lite_jurisdictions_jurisdiction_parent_idx ON irs_lite_jurisdictions (jurisdiction_parent_id);
CREATE UNIQUE INDEX IF NOT EXISTS irs_lite_jurisdictions_idx ON irs_lite_jurisdictions (id);