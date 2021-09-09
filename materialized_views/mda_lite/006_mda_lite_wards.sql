SET schema 'reveal';

DROP MATERIALIZED VIEW IF EXISTS mda_lite_wards CASCADE;

CREATE MATERIALIZED VIEW IF NOT EXISTS mda_lite_wards
AS
SELECT
    subq.*,
    COALESCE(wards_population.official_population::INTEGER, 0) AS official_population,
    COALESCE(wards_population.other_population::INTEGER, 0) AS other_pop_target,
    COALESCE(wards_population.census_target_population_6_to_59_mos_official::INTEGER, 0) AS census_target_population_6_to_59_mos_official,
    COALESCE(wards_population.census_target_population_6_to_11_mos_official::INTEGER, 0) AS census_target_population_6_to_11_mos_official,
    COALESCE(wards_population.census_target_population_12_to_59_mos_official::INTEGER, 0) AS census_target_population_12_to_59_mos_official,
    COALESCE(wards_population.other_pop_target_6_to_59_mos_trusted::INTEGER, 0) AS other_pop_target_6_to_59_mos_trusted,
    COALESCE(wards_population.other_pop_target_6_to_11_mos_trusted::INTEGER, 0) AS other_pop_target_6_to_11_mos_trusted,
    COALESCE(wards_population.other_pop_target_12_to_59_mos_trusted::INTEGER, 0) AS other_pop_target_12_to_59_mos_trusted,
    COALESCE(wards_population.census_pop_target_above_16_official::INTEGER, 0) AS census_pop_target_above_16_official,
    COALESCE(wards_population.other_pop_target_above_16_trusted::INTEGER, 0) AS other_pop_target_above_16_trusted,
    COALESCE(wards_population.census_pop_target_5_to_15_official::INTEGER, 0) AS census_pop_target_5_to_15_official,
    COALESCE(wards_population.other_pop_target_5_to_15_trusted::INTEGER, 0) AS other_pop_target_5_to_15_trusted,
    (COALESCE(wards_population.census_pop_target_5_to_15_official::INTEGER, 0) + COALESCE(wards_population.census_pop_target_above_16_official::INTEGER, 0)) AS census_pop_target_5_to_15_and_above_16_official,
    CASE
        WHEN (COALESCE(wards_population.census_pop_target_5_to_15_official::INTEGER, 0) + COALESCE(wards_population.census_pop_target_above_16_official::INTEGER, 0)) = 0 THEN 0
        ELSE CAST(subq.pzq_total_treated as DECIMAL) / CAST((COALESCE(wards_population.census_pop_target_5_to_15_official::INTEGER, 0) + COALESCE(wards_population.census_pop_target_above_16_official::INTEGER, 0)) as DECIMAL)
        END AS pzq_5_years_and_above_16_treatment_coverage,
    CASE
        WHEN COALESCE(wards_population.census_target_population_6_to_59_mos_official::INTEGER, 0) = 0 THEN 0
        ELSE CAST(subq.vita_total_treated as DECIMAL) / CAST(wards_population.census_target_population_6_to_59_mos_official as DECIMAL)
        END AS vita_6_to_59_mos_treatment_coverage,
    CASE
        WHEN COALESCE(wards_population.official_population::INTEGER, 0) = 0 THEN 0
        ELSE CAST(subq.total_all_genders as DECIMAL) / CAST(wards_population.official_population as DECIMAL)
        END AS treatment_coverage,
    CASE
        WHEN COALESCE(wards_population.other_population::INTEGER, 0) = 0 THEN 0
        ELSE CAST(subq.total_all_genders as DECIMAL) / CAST(wards_population.other_population as DECIMAL)
        END AS other_pop_coverage,
    CASE
        WHEN COALESCE(wards_population.other_pop_target_6_to_59_mos_trusted::INTEGER, 0) = 0 THEN 0
        ELSE CAST(subq.vita_total_treated as DECIMAL) / CAST(wards_population.other_pop_target_6_to_59_mos_trusted as DECIMAL)
        END AS vita_6_to_59_mos_other_pop_coverage,
    CASE
        WHEN COALESCE(wards_population.other_pop_target_6_to_11_mos_trusted::INTEGER, 0) = 0 THEN 0
        ELSE CAST(subq.vita_total_treated_6_to_11_mos as DECIMAL) / CAST(wards_population.other_pop_target_6_to_11_mos_trusted as DECIMAL)
        END AS vita_6_to_11_mos_other_pop_coverage,
    CASE
        WHEN COALESCE(wards_population.census_target_population_6_to_11_mos_official::INTEGER, 0) = 0 THEN 0
        ELSE CAST(subq.vita_total_treated_6_to_11_mos as DECIMAL) / CAST(wards_population.census_target_population_6_to_11_mos_official as DECIMAL)
        END AS vita_6_to_11_mos_treatment_coverage,
    CASE
        WHEN COALESCE(wards_population.census_target_population_12_to_59_mos_official::INTEGER, 0) = 0 THEN 0
        ELSE CAST(subq.vita_total_treated_1_4 as DECIMAL) / CAST(wards_population.census_target_population_12_to_59_mos_official as DECIMAL)
        END AS vita_treatment_coverage_1_to_4,
    CASE
        WHEN COALESCE(wards_population.other_pop_target_12_to_59_mos_trusted::INTEGER, 0) = 0 THEN 0
        ELSE CAST(subq.vita_total_treated_1_4 as DECIMAL) / CAST(wards_population.other_pop_target_12_to_59_mos_trusted as DECIMAL)
        END AS vita_1_to_4_years_other_pop_coverage,
    CASE
        WHEN COALESCE(wards_population.census_pop_target_above_16_official::INTEGER, 0) = 0 THEN 0
        ELSE CAST(subq.pzq_total_treated_above_16 as DECIMAL) / CAST(wards_population.census_pop_target_above_16_official as DECIMAL)
    END AS pzq_above_16_years_treatment_coverage,
    CASE
        WHEN COALESCE(wards_population.other_pop_target_above_16_trusted::INTEGER, 0) = 0 THEN 0
        ELSE CAST(subq.pzq_total_treated_above_16 as DECIMAL) / CAST(wards_population.other_pop_target_above_16_trusted as DECIMAL)
    END AS  pzq_above_16_years_other_pop_coverage,
    CASE
        WHEN COALESCE(wards_population.census_pop_target_5_to_15_official::INTEGER, 0) = 0 THEN 0
        ELSE CAST(subq.pzq_total_treated_5_to_15 as DECIMAL) / CAST(wards_population.census_pop_target_5_to_15_official as DECIMAL)
        END AS pzq_5_to_15_years_treatment_coverage,
    CASE
        WHEN COALESCE(wards_population.other_pop_target_5_to_15_trusted::INTEGER, 0) = 0 THEN 0
        ELSE CAST(subq.pzq_total_treated_5_to_15 as DECIMAL) / CAST(wards_population.other_pop_target_5_to_15_trusted as DECIMAL)
    END AS pzq_5_to_15_years_other_pop_coverage,
    CASE
        WHEN (COALESCE(wards_population.other_pop_target_5_to_15_trusted::INTEGER, 0) + COALESCE(wards_population.other_pop_target_above_16_trusted::INTEGER, 0)) = 0 THEN 0
        ELSE CAST(subq.pzq_total_treated as DECIMAL) / CAST((COALESCE(wards_population.other_pop_target_5_to_15_trusted::INTEGER, 0) + COALESCE(wards_population.other_pop_target_above_16_trusted::INTEGER, 0)) as DECIMAL)
        END AS pzq_5_years_above_16_other_pop_coverage,
    (COALESCE(wards_population.other_pop_target_5_to_15_trusted::INTEGER, 0) + COALESCE(wards_population.other_pop_target_above_16_trusted::INTEGER, 0)) AS other_pop_target_5_to_15_and_above_16_trusted,
    CASE
        WHEN COALESCE(wards_population.census_target_population_12_to_59_mos_official::INTEGER, 0) = 0 THEN 0
        ELSE CAST(subq.alb_meb_total_treated_1_4 as DECIMAL) / CAST(wards_population.census_target_population_12_to_59_mos_official as DECIMAL)
        END AS alb_meb_treatment_coverage_1_to_4,
    CASE
        WHEN COALESCE(wards_population.other_pop_target_12_to_59_mos_trusted::INTEGER, 0) = 0 THEN 0
        ELSE CAST(subq.alb_meb_total_treated_1_4 as DECIMAL) / CAST(wards_population.other_pop_target_12_to_59_mos_trusted as DECIMAL)
        END AS alb_meb_1_to_4_years_other_pop_coverage,
    CASE
        WHEN COALESCE(wards_population.census_pop_target_5_to_15_official::INTEGER, 0) = 0 THEN 0
        ELSE CAST(subq.alb_meb_total_treated_5_15 as DECIMAL) / CAST(wards_population.census_pop_target_5_to_15_official as DECIMAL)
        END AS alb_meb_5_to_15_years_treatment_coverage,
    CASE
        WHEN COALESCE(wards_population.other_pop_target_5_to_15_trusted::INTEGER, 0) = 0 THEN 0
        ELSE CAST(subq.alb_meb_total_treated_5_15 as DECIMAL) / CAST(wards_population.other_pop_target_5_to_15_trusted as DECIMAL)
        END AS alb_meb_5_to_15_years_other_pop_coverage,
    CASE
        WHEN COALESCE(wards_population.other_pop_target_above_16_trusted::INTEGER, 0) = 0 THEN 0
        ELSE CAST(subq.alb_meb_total_treated_above_16 as DECIMAL) / CAST(wards_population.other_pop_target_above_16_trusted as DECIMAL)
        END AS alb_meb_above_16_pop_coverage,
    CASE
        WHEN COALESCE(wards_population.census_pop_target_above_16_official::INTEGER, 0) = 0 THEN 0
        ELSE CAST(subq.alb_meb_total_treated_above_16 as DECIMAL) / CAST(wards_population.census_pop_target_above_16_official as DECIMAL)
        END AS alb_meb_above_16_treatment_coverage
FROM (
         SELECT
             public.uuid_generate_v5(
                     '6ba7b810-9dad-11d1-80b4-00c04fd430c8',
                     concat(events.base_entity_id, parents.jurisdiction_id, parents.plan_id)
                 ) AS id,
             locations.name AS ward_name,
             parents.jurisdiction_id AS parent_id,
             events.base_entity_id,
             parents.plan_id AS plan_id,
             sum(COALESCE((events.form_data -> 'health_education_above_16'::text) ->> 0, '0'::text)::bigint) AS health_education_above_16,
             sum(COALESCE((events.form_data -> 'health_education_5_to_15'::text) ->> 0, '0'::text)::bigint) AS health_education_5_to_15,
             sum(COALESCE(case when events.form_data ->> 'drugs' = 'ALB' then events.form_data ->> 'treated_male_1_to_4' end,'0')::integer) as alb_treated_male_1_4,
             sum(COALESCE(case when events.form_data ->> 'drugs' = 'MBZ' then events.form_data ->> 'treated_male_1_to_4' end,'0')::integer) as mbz_treated_male_1_4,
             sum(COALESCE(case when events.form_data ->> 'drugs' = 'MEB' then events.form_data ->> 'treated_male_1_to_4' end,'0')::integer) as meb_treated_male_1_4,
             sum(COALESCE(case when events.form_data ->> 'drugs' = 'VITA' then events.form_data ->> 'treated_male_1_to_4' end,'0')::integer) as vita_treated_male_1_4,
             SUM(COALESCE (events.form_data->'treated_male_1_to_4'->>0, '0')::int) AS treated_male_1_4,
             sum(COALESCE(case when events.form_data ->> 'drugs' = 'VITA' then events.form_data ->> 'treated_male_6_to_11_mos' end,'0')::integer) as vita_treated_male_6_to_11_mos,
             sum(COALESCE((events.form_data -> 'treated_male_6_to_11_mos'::text) ->> 0, '0'::text)::integer) AS treated_male_6_to_11_mos,
             sum(COALESCE(case when events.form_data ->> 'drugs' = 'PZQ' then events.form_data ->> 'treated_male_5_to_14' end,'0')::integer) as pzq_treated_male_5_to_14,
             sum(COALESCE(case when events.form_data ->> 'drugs' = 'ALB' then events.form_data ->> 'treated_male_5_to_14' end,'0')::integer) as alb_treated_male_5_to_14,
             sum(COALESCE(case when events.form_data ->> 'drugs' = 'MBZ' then events.form_data ->> 'treated_male_5_to_14' end,'0')::integer) as mbz_treated_male_5_to_14,
             SUM(COALESCE (events.form_data->'treated_male_5_to_14'->>0, '0')::int) AS treated_male_5_14,
             sum(COALESCE(case when events.form_data ->> 'drugs' = 'PZQ' then events.form_data ->> 'treated_female_5_to_15' end,'0')::integer) as pzq_treated_female_5_to_15,
             sum(COALESCE(case when events.form_data ->> 'drugs' = 'ALB' then events.form_data ->> 'treated_female_5_to_15' end,'0')::integer) as alb_treated_female_5_to_15,
             sum(COALESCE(case when events.form_data ->> 'drugs' = 'MEB' then events.form_data ->> 'treated_female_5_to_15' end,'0')::integer) as meb_treated_female_5_to_15,
             sum(COALESCE((events.form_data -> 'treated_female_5_to_15'::text) ->> 0, '0'::text)::integer) AS treated_female_5_15,
             sum(COALESCE(case when events.form_data ->> 'drugs' = 'PZQ' then events.form_data ->> 'treated_male_5_to_15' end,'0')::integer) as pzq_treated_male_5_to_15,
             sum(COALESCE(case when events.form_data ->> 'drugs' = 'ALB' then events.form_data ->> 'treated_male_5_to_15' end,'0')::integer) as alb_treated_male_5_to_15,
             sum(COALESCE(case when events.form_data ->> 'drugs' = 'MEB' then events.form_data ->> 'treated_male_5_to_15' end,'0')::integer) as meb_treated_male_5_to_15,
             sum(COALESCE((events.form_data -> 'treated_male_5_to_15'::text) ->> 0, '0'::text)::integer) AS treated_male_5_15,
             sum(COALESCE(case when events.form_data ->> 'drugs' = 'PZQ' then events.form_data ->> 'treated_male_above_15' end,'0')::integer) as pzq_treated_male_above_15,
             sum(COALESCE(case when events.form_data ->> 'drugs' = 'ALB' then events.form_data ->> 'treated_male_above_15' end,'0')::integer) as alb_treated_male_above_15,
             sum(COALESCE(case when events.form_data ->> 'drugs' = 'MBZ' then events.form_data ->> 'treated_male_above_15' end,'0')::integer) as mbz_treated_male_above_15,
             SUM(COALESCE (events.form_data->'treated_male_above_15'->>0, '0')::int) AS treated_male_above_15,
             sum(COALESCE(case when events.form_data ->> 'drugs' = 'PZQ' then events.form_data ->> 'treated_male_above_16' end,'0')::integer) as pzq_treated_male_above_16,
             sum(COALESCE(case when events.form_data ->> 'drugs' = 'ALB' then events.form_data ->> 'treated_male_above_16' end,'0')::integer) as alb_treated_male_above_16,
             sum(COALESCE(case when events.form_data ->> 'drugs' = 'MEB' then events.form_data ->> 'treated_male_above_16' end,'0')::integer) as meb_treated_male_above_16,
             sum(COALESCE((events.form_data -> 'treated_male_above_16'::text) ->> 0, '0'::text)::integer) AS treated_male_above_16,
             sum(COALESCE(case when events.form_data ->> 'drugs' = 'ALB' then events.form_data ->> 'treated_female_1_to_4' end,'0')::integer) as alb_treated_female_1_4,
             sum(COALESCE(case when events.form_data ->> 'drugs' = 'MBZ' then events.form_data ->> 'treated_female_1_to_4' end,'0')::integer) as mbz_treated_female_1_4,
             sum(COALESCE(case when events.form_data ->> 'drugs' = 'MEB' then events.form_data ->> 'treated_female_1_to_4' end,'0')::integer) as meb_treated_female_1_4,
             sum(COALESCE(case when events.form_data ->> 'drugs' = 'VITA' then events.form_data ->> 'treated_female_1_to_4' end,'0')::integer) as vita_treated_female_1_4,
             SUM(COALESCE (events.form_data->'treated_female_1_to_4'->>0, '0')::int) AS treated_female_1_4,
             sum(COALESCE(case when events.form_data ->> 'drugs' = 'VITA' then events.form_data ->> 'treated_female_6_to_11_mos' end,'0')::integer) as vita_treated_female_6_to_11_mos,
             sum(COALESCE((events.form_data -> 'treated_female_6_to_11_mos'::text) ->> 0, '0'::text)::integer) AS treated_female_6_to_11_mos,
             sum(COALESCE(case when events.form_data ->> 'drugs' = 'PZQ' then events.form_data ->> 'treated_female_5_to_14' end,'0')::integer) as pzq_treated_female_5_to_14,
             sum(COALESCE(case when events.form_data ->> 'drugs' = 'ALB' then events.form_data ->> 'treated_female_5_to_14' end,'0')::integer) as alb_treated_female_5_to_14,
             sum(COALESCE(case when events.form_data ->> 'drugs' = 'MBZ' then events.form_data ->> 'treated_female_5_to_14' end,'0')::integer) as mbz_treated_female_5_to_14,
             SUM(COALESCE (events.form_data->'treated_female_5_to_14'->>0, '0')::int) AS treated_female_5_14,
             sum(COALESCE(case when events.form_data ->> 'drugs' = 'PZQ' then events.form_data ->> 'treated_female_above_15' end,'0')::integer) as pzq_treated_female_above_15,
             sum(COALESCE(case when events.form_data ->> 'drugs' = 'ALB' then events.form_data ->> 'treated_female_above_15' end,'0')::integer) as alb_treated_female_above_15,
             sum(COALESCE(case when events.form_data ->> 'drugs' = 'MBZ' then events.form_data ->> 'treated_female_above_15' end,'0')::integer) as mbz_treated_female_above_15,
             SUM(COALESCE (events.form_data->'treated_female_above_15'->>0, '0')::int) AS treated_female_above_15,
             sum(COALESCE((events.form_data -> 'treated_female_above_16'::text) ->> 0, '0'::text)::integer) AS treated_female_above_16,
             sum(COALESCE(case when events.form_data ->> 'drugs' = 'PZQ' then events.form_data ->> 'treated_female_above_16' end,'0')::integer) as pzq_treated_female_above_16,
             sum(COALESCE(case when events.form_data ->> 'drugs' = 'ALB' then events.form_data ->> 'treated_female_above_16' end,'0')::integer) as alb_treated_female_above_16,
             sum(COALESCE(case when events.form_data ->> 'drugs' = 'MEB' then events.form_data ->> 'treated_female_above_16' end,'0')::integer) as meb_treated_female_above_16,
             SUM(COALESCE (events.form_data->'treated_male_1_to_4'->>0, '0')::int +
                COALESCE (events.form_data->'treated_male_5_to_14'->>0, '0')::int +
                COALESCE (events.form_data->'treated_male_above_15'->>0, '0')::int) AS total_males,
             sum(COALESCE(case when events.form_data ->> 'drugs' = 'VITA' then events.form_data ->> 'treated_male_1_to_4' end,'0')::integer +
                 COALESCE(case when events.form_data ->> 'drugs' = 'VITA' then events.form_data ->> 'treated_male_6_to_11_mos' end,'0')::integer) as vita_total_male,
             sum(COALESCE(case when events.form_data ->> 'drugs' = 'VITA' then events.form_data ->> 'treated_female_1_to_4' end,'0')::integer +
                 COALESCE(case when events.form_data ->> 'drugs' = 'VITA' then events.form_data ->> 'treated_female_6_to_11_mos' end,'0')::integer) as vita_total_female,
             sum(COALESCE(case when events.form_data ->> 'drugs' = 'VITA' then events.form_data ->> 'treated_female_1_to_4' end,'0')::integer +
                 COALESCE(case when events.form_data ->> 'drugs' = 'VITA' then events.form_data ->> 'treated_female_6_to_11_mos' end,'0')::integer +
                 COALESCE(case when events.form_data ->> 'drugs' = 'VITA' then events.form_data ->> 'treated_male_1_to_4' end,'0')::integer +
                 COALESCE(case when events.form_data ->> 'drugs' = 'VITA' then events.form_data ->> 'treated_male_6_to_11_mos' end,'0')::integer) as vita_total_treated,
             sum(COALESCE(case when events.form_data ->> 'drugs' = 'VITA' then events.form_data ->> 'treated_male_6_to_11_mos' end,'0')::integer +
                 COALESCE(case when events.form_data ->> 'drugs' = 'VITA' then events.form_data ->> 'treated_female_6_to_11_mos' end,'0')::integer) as vita_total_treated_6_to_11_mos,
             sum(COALESCE(case when events.form_data ->> 'drugs' = 'VITA' then events.form_data ->> 'treated_male_1_to_4' end,'0')::integer +
                 COALESCE(case when events.form_data ->> 'drugs' = 'VITA' then events.form_data ->> 'treated_female_1_to_4' end,'0')::integer) as vita_total_treated_1_4,
             sum(COALESCE(case when events.form_data ->> 'drugs' = 'PZQ' then events.form_data ->> 'treated_male_5_to_14' end,'0')::integer +
                 COALESCE(case when events.form_data ->> 'drugs' = 'PZQ' then events.form_data ->> 'treated_male_5_to_15' end,'0')::integer +
                 COALESCE(case when events.form_data ->> 'drugs' = 'PZQ' then events.form_data ->> 'treated_male_above_15' end,'0')::integer +
                 COALESCE(case when events.form_data ->> 'drugs' = 'PZQ' then events.form_data ->> 'treated_male_above_16' end,'0')::integer) as pzq_total_male,
             sum(COALESCE(case when events.form_data ->> 'drugs' = 'PZQ' then events.form_data ->> 'treated_female_5_to_14' end,'0')::integer +
                 COALESCE(case when events.form_data ->> 'drugs' = 'PZQ' then events.form_data ->> 'treated_female_5_to_15' end,'0')::integer +
                 COALESCE(case when events.form_data ->> 'drugs' = 'PZQ' then events.form_data ->> 'treated_female_above_15' end,'0')::integer +
                 COALESCE(case when events.form_data ->> 'drugs' = 'PZQ' then events.form_data ->> 'treated_female_above_16' end,'0')::integer) as pzq_total_female,
             sum(COALESCE(case when events.form_data ->> 'drugs' = 'PZQ' then events.form_data ->> 'treated_male_5_to_14' end,'0')::integer +
                 COALESCE(case when events.form_data ->> 'drugs' = 'PZQ' then events.form_data ->> 'treated_male_5_to_15' end,'0')::integer +
                 COALESCE(case when events.form_data ->> 'drugs' = 'PZQ' then events.form_data ->> 'treated_male_above_15' end,'0')::integer +
                 COALESCE(case when events.form_data ->> 'drugs' = 'PZQ' then events.form_data ->> 'treated_male_above_16' end,'0')::integer +
                 COALESCE(case when events.form_data ->> 'drugs' = 'PZQ' then events.form_data ->> 'treated_female_5_to_14' end,'0')::integer +
                 COALESCE(case when events.form_data ->> 'drugs' = 'PZQ' then events.form_data ->> 'treated_female_5_to_15' end,'0')::integer +
                 COALESCE(case when events.form_data ->> 'drugs' = 'PZQ' then events.form_data ->> 'treated_female_above_15' end,'0')::integer +
                COALESCE(case when events.form_data ->> 'drugs' = 'PZQ' then events.form_data ->> 'treated_female_above_16' end,'0')::integer) as pzq_total_treated,
             sum(COALESCE(case when events.form_data ->> 'drugs' = 'PZQ' then events.form_data ->> 'treated_male_above_16' end,'0')::integer +
                 COALESCE(case when events.form_data ->> 'drugs' = 'PZQ' then events.form_data ->> 'treated_female_above_16' end,'0')::integer) as pzq_total_treated_above_16,
             sum(COALESCE(case when events.form_data ->> 'drugs' = 'PZQ' then events.form_data ->> 'treated_female_5_to_15' end,'0')::integer +
                 COALESCE(case when events.form_data ->> 'drugs' = 'PZQ' then events.form_data ->> 'treated_male_5_to_15' end,'0')::integer) as pzq_total_treated_5_to_15,
             sum(COALESCE(case when events.form_data ->> 'drugs' = 'ALB' then events.form_data ->> 'treated_male_1_to_4' end,'0')::integer +
                 COALESCE(case when events.form_data ->> 'drugs' = 'ALB' then events.form_data ->> 'treated_male_5_to_14' end,'0')::integer +
                 COALESCE(case when events.form_data ->> 'drugs' = 'ALB' then events.form_data ->> 'treated_male_5_to_15' end,'0')::integer +
                 COALESCE(case when events.form_data ->> 'drugs' = 'ALB' then events.form_data ->> 'treated_male_above_16' end,'0')::integer +
                 COALESCE(case when events.form_data ->> 'drugs' = 'ALB' then events.form_data ->> 'treated_male_above_15' end,'0')::integer
                 ) as alb_total_male,
             sum(COALESCE(case when events.form_data ->> 'drugs' = 'ALB' then events.form_data ->> 'treated_female_1_to_4' end,'0')::integer +
                 COALESCE(case when events.form_data ->> 'drugs' = 'ALB' then events.form_data ->> 'treated_female_5_to_14' end,'0')::integer +
                 COALESCE(case when events.form_data ->> 'drugs' = 'ALB' then events.form_data ->> 'treated_female_5_to_15' end,'0')::integer +
                 COALESCE(case when events.form_data ->> 'drugs' = 'ALB' then events.form_data ->> 'treated_female_above_16' end,'0')::integer +
                 COALESCE(case when events.form_data ->> 'drugs' = 'ALB' then events.form_data ->> 'treated_female_above_15' end,'0')::integer
                 ) as alb_total_female,
             sum(COALESCE(case when events.form_data ->> 'drugs' = 'ALB' then events.form_data ->> 'treated_male_1_to_4' end,'0')::integer +
                 COALESCE(case when events.form_data ->> 'drugs' = 'ALB' then events.form_data ->> 'treated_male_5_to_14' end,'0')::integer +
                 COALESCE(case when events.form_data ->> 'drugs' = 'ALB' then events.form_data ->> 'treated_male_5_to_15' end,'0')::integer +
                 COALESCE(case when events.form_data ->> 'drugs' = 'ALB' then events.form_data ->> 'treated_male_above_16' end,'0')::integer +
                 COALESCE(case when events.form_data ->> 'drugs' = 'ALB' then events.form_data ->> 'treated_male_above_15' end,'0')::integer +
                 COALESCE(case when events.form_data ->> 'drugs' = 'ALB' then events.form_data ->> 'treated_female_1_to_4' end,'0')::integer +
                 COALESCE(case when events.form_data ->> 'drugs' = 'ALB' then events.form_data ->> 'treated_female_5_to_14' end,'0')::integer +
                 COALESCE(case when events.form_data ->> 'drugs' = 'ALB' then events.form_data ->> 'treated_female_5_to_15' end,'0')::integer +
                 COALESCE(case when events.form_data ->> 'drugs' = 'ALB' then events.form_data ->> 'treated_female_above_16' end,'0')::integer +
                 COALESCE(case when events.form_data ->> 'drugs' = 'ALB' then events.form_data ->> 'treated_female_above_15' end,'0')::integer
                 ) as alb_total_treated,
             sum(COALESCE(case when events.form_data ->> 'drugs' = 'MEB' then events.form_data ->> 'treated_male_1_to_4' end,'0')::integer +
                 COALESCE(case when events.form_data ->> 'drugs' = 'MEB' then events.form_data ->> 'treated_male_5_to_14' end,'0')::integer +
                 COALESCE(case when events.form_data ->> 'drugs' = 'MEB' then events.form_data ->> 'treated_male_5_to_15' end,'0')::integer +
                 COALESCE(case when events.form_data ->> 'drugs' = 'MEB' then events.form_data ->> 'treated_male_above_16' end,'0')::integer +
                 COALESCE(case when events.form_data ->> 'drugs' = 'MEB' then events.form_data ->> 'treated_male_above_15' end,'0')::integer
                 ) as meb_total_male,
             sum(COALESCE(case when events.form_data ->> 'drugs' = 'MEB' then events.form_data ->> 'treated_female_1_to_4' end,'0')::integer +
                 COALESCE(case when events.form_data ->> 'drugs' = 'MEB' then events.form_data ->> 'treated_female_5_to_14' end,'0')::integer +
                 COALESCE(case when events.form_data ->> 'drugs' = 'MEB' then events.form_data ->> 'treated_female_5_to_15' end,'0')::integer +
                 COALESCE(case when events.form_data ->> 'drugs' = 'MEB' then events.form_data ->> 'treated_female_above_16' end,'0')::integer +
                 COALESCE(case when events.form_data ->> 'drugs' = 'MEB' then events.form_data ->> 'treated_female_above_15' end,'0')::integer
                 ) as meb_total_female,
             sum(COALESCE(case when events.form_data ->> 'drugs' = 'MEB' then events.form_data ->> 'treated_male_1_to_4' end,'0')::integer +
                 COALESCE(case when events.form_data ->> 'drugs' = 'MEB' then events.form_data ->> 'treated_male_5_to_14' end,'0')::integer +
                 COALESCE(case when events.form_data ->> 'drugs' = 'MEB' then events.form_data ->> 'treated_male_5_to_15' end,'0')::integer +
                 COALESCE(case when events.form_data ->> 'drugs' = 'MEB' then events.form_data ->> 'treated_male_above_16' end,'0')::integer +
                 COALESCE(case when events.form_data ->> 'drugs' = 'MEB' then events.form_data ->> 'treated_male_above_15' end,'0')::integer +
                 COALESCE(case when events.form_data ->> 'drugs' = 'MEB' then events.form_data ->> 'treated_female_1_to_4' end,'0')::integer +
                 COALESCE(case when events.form_data ->> 'drugs' = 'MEB' then events.form_data ->> 'treated_female_5_to_14' end,'0')::integer +
                 COALESCE(case when events.form_data ->> 'drugs' = 'MEB' then events.form_data ->> 'treated_female_5_to_15' end,'0')::integer +
                 COALESCE(case when events.form_data ->> 'drugs' = 'MEB' then events.form_data ->> 'treated_female_above_16' end,'0')::integer +
                 COALESCE(case when events.form_data ->> 'drugs' = 'MEB' then events.form_data ->> 'treated_female_above_15' end,'0')::integer
                 ) as meb_total_treated,
             sum(COALESCE(case when events.form_data ->> 'drugs' = 'ALB' then events.form_data ->> 'treated_male_1_to_4' end,'0')::integer +
                 COALESCE(case when events.form_data ->> 'drugs' = 'ALB' then events.form_data ->> 'treated_male_5_to_14' end,'0')::integer +
                 COALESCE(case when events.form_data ->> 'drugs' = 'ALB' then events.form_data ->> 'treated_male_5_to_15' end,'0')::integer +
                 COALESCE(case when events.form_data ->> 'drugs' = 'ALB' then events.form_data ->> 'treated_male_above_16' end,'0')::integer +
                 COALESCE(case when events.form_data ->> 'drugs' = 'ALB' then events.form_data ->> 'treated_male_above_15' end,'0')::integer +
                 COALESCE(case when events.form_data ->> 'drugs' = 'ALB' then events.form_data ->> 'treated_female_1_to_4' end,'0')::integer +
                 COALESCE(case when events.form_data ->> 'drugs' = 'ALB' then events.form_data ->> 'treated_female_5_to_14' end,'0')::integer +
                 COALESCE(case when events.form_data ->> 'drugs' = 'ALB' then events.form_data ->> 'treated_female_5_to_15' end,'0')::integer +
                 COALESCE(case when events.form_data ->> 'drugs' = 'ALB' then events.form_data ->> 'treated_female_above_16' end,'0')::integer +
                 COALESCE(case when events.form_data ->> 'drugs' = 'ALB' then events.form_data ->> 'treated_female_above_15' end,'0')::integer +
                 COALESCE(case when events.form_data ->> 'drugs' = 'ALB' then events.form_data ->> 'treated_male_5_to_15' end,'0')::integer +
                 COALESCE(case when events.form_data ->> 'drugs' = 'ALB' then events.form_data ->> 'treated_female_1_to_4' end,'0')::integer +
                 COALESCE(case when events.form_data ->> 'drugs' = 'ALB' then events.form_data ->> 'treated_female_5_to_15' end,'0')::integer +
                 COALESCE(case when events.form_data ->> 'drugs' = 'MEB' then events.form_data ->> 'treated_male_1_to_4' end,'0')::integer +
                 COALESCE(case when events.form_data ->> 'drugs' = 'MEB' then events.form_data ->> 'treated_male_5_to_14' end,'0')::integer +
                 COALESCE(case when events.form_data ->> 'drugs' = 'MEB' then events.form_data ->> 'treated_male_5_to_15' end,'0')::integer +
                 COALESCE(case when events.form_data ->> 'drugs' = 'MEB' then events.form_data ->> 'treated_male_above_16' end,'0')::integer +
                 COALESCE(case when events.form_data ->> 'drugs' = 'MEB' then events.form_data ->> 'treated_male_above_15' end,'0')::integer +
                 COALESCE(case when events.form_data ->> 'drugs' = 'MEB' then events.form_data ->> 'treated_female_1_to_4' end,'0')::integer +
                 COALESCE(case when events.form_data ->> 'drugs' = 'MEB' then events.form_data ->> 'treated_female_5_to_14' end,'0')::integer +
                 COALESCE(case when events.form_data ->> 'drugs' = 'MEB' then events.form_data ->> 'treated_female_5_to_15' end,'0')::integer +
                 COALESCE(case when events.form_data ->> 'drugs' = 'MEB' then events.form_data ->> 'treated_female_above_16' end,'0')::integer +
                 COALESCE(case when events.form_data ->> 'drugs' = 'MEB' then events.form_data ->> 'treated_male_5_to_15' end,'0')::integer +
                 COALESCE(case when events.form_data ->> 'drugs' = 'MEB' then events.form_data ->> 'treated_female_1_to_4' end,'0')::integer +
                 COALESCE(case when events.form_data ->> 'drugs' = 'MEB' then events.form_data ->> 'treated_female_5_to_15' end,'0')::integer +
                 COALESCE(case when events.form_data ->> 'drugs' = 'MEB' then events.form_data ->> 'treated_female_above_15' end,'0')::integer
                 ) as alb_meb_total_treated,
             sum(COALESCE(case when events.form_data ->> 'drugs' = 'ALB' then events.form_data ->> 'treated_male_1_to_4' end,'0')::integer +
                 COALESCE(case when events.form_data ->> 'drugs' = 'MEB' then events.form_data ->> 'treated_male_1_to_4' end,'0')::integer +
                 COALESCE(case when events.form_data ->> 'drugs' = 'ALB' then events.form_data ->> 'treated_female_1_to_4' end,'0')::integer +
                 COALESCE(case when events.form_data ->> 'drugs' = 'MEB' then events.form_data ->> 'treated_female_1_to_4' end,'0')::integer) as alb_meb_total_treated_1_4,
             sum(COALESCE(case when events.form_data ->> 'drugs' = 'ALB' then events.form_data ->> 'treated_male_5_to_15' end,'0')::integer +
                 COALESCE(case when events.form_data ->> 'drugs' = 'ALB' then events.form_data ->> 'treated_female_5_to_15' end,'0')::integer +
                 COALESCE(case when events.form_data ->> 'drugs' = 'MEB' then events.form_data ->> 'treated_male_5_to_15' end,'0')::integer +
                 COALESCE(case when events.form_data ->> 'drugs' = 'MEB' then events.form_data ->> 'treated_female_5_to_15' end,'0')::integer) as alb_meb_total_treated_5_15,
             sum(COALESCE(case when events.form_data ->> 'drugs' = 'MEB' then events.form_data ->> 'treated_female_above_16' end,'0')::integer +
                 COALESCE(case when events.form_data ->> 'drugs' = 'ALB' then events.form_data ->> 'treated_female_above_16' end,'0')::integer +
                 COALESCE(case when events.form_data ->> 'drugs' = 'MEB' then events.form_data ->> 'treated_male_above_16' end,'0')::integer +
                 COALESCE(case when events.form_data ->> 'drugs' = 'ALB' then events.form_data ->> 'treated_male_above_16' end,'0')::integer) as alb_meb_total_treated_above_16,
            SUM(COALESCE (events.form_data->'treated_female_1_to_4'->>0, '0')::int +
                COALESCE (events.form_data->'treated_female_5_to_14'->>0, '0')::int +
                COALESCE (events.form_data->'treated_female_above_15'->>0, '0')::int) AS total_females,
             SUM(COALESCE (events.form_data->'treated_male_1_to_4'->>0, '0')::int +
                COALESCE (events.form_data->'treated_male_5_to_14'->>0, '0')::int +
                COALESCE (events.form_data->'treated_male_above_15'->>0, '0')::int +
                COALESCE (events.form_data->'treated_female_1_to_4'->>0, '0')::int +
                COALESCE (events.form_data->'treated_female_5_to_14'->>0, '0')::int +
                COALESCE (events.form_data->'treated_female_above_15'->>0, '0')::int) AS total_all_genders,
             SUM(COALESCE (events.form_data->'sum_pzq_received_and_top_up'->>0, '0')::int +
                COALESCE (events.form_data->'sum_alb_received_and_top_up'->>0, '0')::int +
                COALESCE (events.form_data->'sum_mbz_received_and_top_up'->>0, '0')::int) AS supervisor_distributed,
             sum(COALESCE((events.form_data -> 'sum_pzq_received_and_top_up'::text) ->> 0, '0'::text)::integer) AS pzq_supervisor_distributed,
             sum(COALESCE((events.form_data -> 'sum_alb_received_and_top_up'::text) ->> 0, '0'::text)::integer) AS alb_supervisor_distributed,
             sum(COALESCE((events.form_data -> 'sum_mbz_received_and_top_up'::text) ->> 0, '0'::text)::integer) AS mbz_supervisor_distributed,
             sum(COALESCE((events.form_data -> 'sum_meb_received_and_top_up'::text) ->> 0, '0'::text)::integer) AS meb_supervisor_distributed,
             sum(COALESCE((events.form_data -> 'sum_vita_received_and_top_up'::text) ->> 0, '0'::text)::integer) AS vita_supervisor_distributed,
             SUM(COALESCE (events.form_data->'received_number'->>0, '0')::int) AS received_number,
             sum(COALESCE(case when events.form_data ->> 'drugs' = 'PZQ' then events.form_data ->>'received_number' end,'0')::integer) as pzq_received,
             sum(COALESCE(case when events.form_data ->> 'drugs' = 'ALB' then events.form_data ->>'received_number' end,'0')::integer) as alb_received,
             sum(COALESCE(case when events.form_data ->> 'drugs' = 'MBZ' then events.form_data ->>'received_number' end,'0')::integer) as mbz_received,
             sum(COALESCE(case when events.form_data ->> 'drugs' = 'MEB' then events.form_data ->>'received_number' end,'0')::integer) as meb_received,
             sum(COALESCE(case when events.form_data ->> 'drugs' = 'VITA' then events.form_data ->>'received_number' end,'0')::integer) as vita_received,
             SUM(COALESCE (events.form_data->'adminstered'->>0, '0')::int) AS adminstered,
             sum(COALESCE(case when events.form_data ->> 'drugs' = 'PZQ' then events.form_data ->>'adminstered' end,'0')::integer) as pzq_administered,
             sum(COALESCE(case when events.form_data ->> 'drugs' = 'ALB' then events.form_data ->>'adminstered' end,'0')::integer) as alb_administered,
             sum(COALESCE(case when events.form_data ->> 'drugs' = 'MBZ' then events.form_data ->>'adminstered' end,'0')::integer) as mbz_administered,
             sum(COALESCE(case when events.form_data ->> 'drugs' = 'MEB' then events.form_data ->>'adminstered' end,'0')::integer) as meb_administered,
             sum(COALESCE(case when events.form_data ->> 'drugs' = 'VITA' then events.form_data ->>'adminstered' end,'0')::integer) as vita_administered,
             SUM(COALESCE (events.form_data->'damaged'->>0, '0')::int) AS damaged,
             sum(COALESCE(case when events.form_data ->> 'drugs' = 'PZQ' then events.form_data ->>'damaged' end,'0')::integer) as pzq_damaged,
             sum(COALESCE(case when events.form_data ->> 'drugs' = 'ALB' then events.form_data ->>'damaged' end,'0')::integer) as alb_damaged,
             sum(COALESCE(case when events.form_data ->> 'drugs' = 'MBZ' then events.form_data ->>'damaged' end,'0')::integer) as mbz_damaged,
             sum(COALESCE(case when events.form_data ->> 'drugs' = 'MEB' then events.form_data ->>'damaged' end,'0')::integer) as meb_damaged,
             sum(COALESCE(case when events.form_data ->> 'drugs' = 'VITA' then events.form_data ->>'damaged' end,'0')::integer) as vita_damaged,
             SUM(COALESCE (events.form_data->'adverse'->>0, '0')::int) AS adverse,
             sum(COALESCE(case when events.form_data ->> 'drugs' = 'PZQ' then events.form_data ->>'adverse' end,'0')::integer) as pzq_adverse,
             sum(COALESCE(case when events.form_data ->> 'drugs' = 'ALB' then events.form_data ->>'adverse' end,'0')::integer) as alb_adverse,
             sum(COALESCE(case when events.form_data ->> 'drugs' = 'MBZ' then events.form_data ->>'adverse' end,'0')::integer) as mbz_adverse,
             sum(COALESCE(case when events.form_data ->> 'drugs' = 'MEB' then events.form_data ->>'adverse' end,'0')::integer) as meb_adverse,
             sum(COALESCE(case when events.form_data ->> 'drugs' = 'VITA' then events.form_data ->>'adverse' end,'0')::integer) as vita_adverse,
             SUM(COALESCE (events.form_data->'received_number'->>0, '0')::int - (COALESCE (events.form_data->'adminstered'->>0, '0')::int +
                COALESCE (events.form_data->'damaged'->>0, '0')::int)) AS remaining_with_cdd,
             sum(COALESCE(case when events.form_data ->> 'drugs' = 'PZQ' then
                                       COALESCE((events.form_data -> 'received_number'::text) ->> 0, '0'::text)::integer - (COALESCE((events.form_data -> 'adminstered'::text) ->> 0, '0'::text)::integer + COALESCE((events.form_data -> 'damaged'::text) ->> 0, '0'::text)::integer)
                              end,'0')::integer) AS pzq_remaining_with_cdd,
             sum(COALESCE(case when events.form_data ->> 'drugs' = 'ALB' then
                                       COALESCE((events.form_data -> 'received_number'::text) ->> 0, '0'::text)::integer - (COALESCE((events.form_data -> 'adminstered'::text) ->> 0, '0'::text)::integer + COALESCE((events.form_data -> 'damaged'::text) ->> 0, '0'::text)::integer)
                              end,'0')::integer) AS alb_remaining_with_cdd,
             sum(COALESCE (case when events.form_data ->> 'drugs' = 'MBZ' then
                                        COALESCE((events.form_data -> 'received_number'::text) ->> 0, '0'::text)::integer - (COALESCE((events.form_data -> 'adminstered'::text) ->> 0, '0'::text)::integer + COALESCE((events.form_data -> 'damaged'::text) ->> 0, '0'::text)::integer)
                               end,'0')::integer ) AS mbz_remaining_with_cdd,
             sum(COALESCE (case when events.form_data ->> 'drugs' = 'MEB' then
                                        COALESCE((events.form_data -> 'received_number'::text) ->> 0, '0'::text)::integer - (COALESCE((events.form_data -> 'adminstered'::text) ->> 0, '0'::text)::integer + COALESCE((events.form_data -> 'damaged'::text) ->> 0, '0'::text)::integer)
                               end,'0')::integer ) AS meb_remaining_with_cdd,
             sum(COALESCE (case when events.form_data ->> 'drugs' = 'VITA' then
                                        COALESCE((events.form_data -> 'received_number'::text) ->> 0, '0'::text)::integer - (COALESCE((events.form_data -> 'adminstered'::text) ->> 0, '0'::text)::integer + COALESCE((events.form_data -> 'damaged'::text) ->> 0, '0'::text)::integer)
                               end,'0')::integer ) AS vita_remaining_with_cdd,
             SUM(COALESCE (events.form_data->'pzq_returned'->>0, '0')::int +
                COALESCE (events.form_data->'albendazole_returned'->>0, '0')::int +
                COALESCE (events.form_data->'mebendazole_returned'->>0, '0')::int) AS returned_to_supervisor,
             sum(COALESCE((events.form_data -> 'pzq_returned'::text) ->> 0, '0'::text)::integer) AS pzq_returned_to_supervisor,
             sum(COALESCE((events.form_data -> 'albendazole_returned'::text) ->> 0, '0'::text)::integer) AS alb_returned_to_supervisor,
             sum(COALESCE((events.form_data -> 'mebendazole_returned'::text) ->> 0, '0'::text)::integer) AS mbz_returned_to_supervisor,
             sum(COALESCE((events.form_data -> 'vita_returned'::text) ->> 0, '0'::text)::integer) vita_returned_to_supervisor
         FROM events
                  LEFT JOIN locations ON events.base_entity_id = locations.id
                  LEFT JOIN mda_lite_operational_areas AS parents ON locations.jurisdiction_id = parents.jurisdiction_id
         WHERE events.event_type IN ('tablet_accountability', 'cdd_supervisor_daily_summary','cell_coordinator_daily_summary')
           AND events.entity_type = 'Structure'
           AND parents.plan_id = events.plan_id
         GROUP BY events.base_entity_id, locations.name, parents.jurisdiction_id, parents.plan_id
     ) as subq
         LEFT JOIN mda_lite_wards_population as wards_population
                   ON wards_population.ward_id = subq.base_entity_id;

CREATE INDEX IF NOT EXISTS mda_lite_wards_base_entity_id_idx ON mda_lite_wards (base_entity_id);
CREATE INDEX IF NOT EXISTS mda_lite_wards_plan_id_idx ON mda_lite_wards (plan_id);
CREATE INDEX IF NOT EXISTS mda_lite_wards_parent_id_idx ON mda_lite_wards (parent_id);
CREATE UNIQUE INDEX IF NOT EXISTS mda_lite_wards_id_idx ON mda_lite_wards (id);


