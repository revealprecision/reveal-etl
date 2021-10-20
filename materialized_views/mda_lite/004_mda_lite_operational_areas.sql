SET schema 'reveal';

DROP MATERIALIZED VIEW IF EXISTS mda_lite_operational_areas CASCADE;

CREATE MATERIALIZED VIEW IF NOT EXISTS mda_lite_operational_areas
AS
SELECT
    main_query.*
FROM (
    SELECT
        public.uuid_generate_v5(
                '6ba7b810-9dad-11d1-80b4-00c04fd430c8',
                concat(operational_area_query.structure_jurisdiction_id, operational_area_query.plan_id)
            ) AS id,
        operational_area_query.plan_id AS plan_id,
        lite_jurisdictions.jurisdiction_id AS jurisdiction_id,
        lite_jurisdictions.jurisdiction_parent_id AS jurisdiction_parent_id,
        lite_jurisdictions.jurisdiction_name AS jurisdiction_name,
        lite_jurisdictions.jurisdiction_depth AS jurisdiction_depth,
        lite_jurisdictions.jurisdiction_path AS jurisdiction_path,
        lite_jurisdictions.jurisdiction_name_path AS jurisdiction_name_path,
        operational_area_query.health_education_above_16 AS health_education_above_16,
        operational_area_query.health_education_5_to_15 AS health_education_5_to_15,
        operational_area_query.alb_treated_male_1_4 AS alb_treated_male_1_4,
        operational_area_query.mbz_treated_male_1_4 AS mbz_treated_male_1_4,
        operational_area_query.meb_treated_male_1_4 AS meb_treated_male_1_4,
        operational_area_query.vita_treated_male_1_4 AS vita_treated_male_1_4,
        operational_area_query.treated_male_1_4 AS treated_male_1_4,
        operational_area_query.treated_male_6_to_11_mos AS treated_male_6_to_11_mos,
        operational_area_query.vita_treated_male_6_to_11_mos AS vita_treated_male_6_to_11_mos,
        operational_area_query.pzq_treated_male_5_14 AS pzq_treated_male_5_14,
        operational_area_query.alb_treated_male_5_14 AS alb_treated_male_5_14,
        operational_area_query.mbz_treated_male_5_14 AS mbz_treated_male_5_14,
        operational_area_query.treated_male_5_14 AS treated_male_5_14,
        operational_area_query.pzq_treated_male_5_to_15 AS pzq_treated_male_5_15,
        operational_area_query.alb_treated_male_5_to_15 AS alb_treated_male_5_15,
        operational_area_query.meb_treated_male_5_to_15 AS meb_treated_male_5_15,
        operational_area_query.treated_male_5_15 AS treated_male_5_15,
        operational_area_query.pzq_treated_male_above_15 AS pzq_treated_male_above_15,
        operational_area_query.alb_treated_male_above_15 AS alb_treated_male_above_15,
        operational_area_query.mbz_treated_male_above_15 AS mbz_treated_male_above_15,
        operational_area_query.treated_male_above_15 AS treated_male_above_15,
        operational_area_query.pzq_treated_male_above_16 AS pzq_treated_male_above_16,
        operational_area_query.alb_treated_male_above_16 AS alb_treated_male_above_16,
        operational_area_query.meb_treated_male_above_16 AS meb_treated_male_above_16,
        operational_area_query.treated_male_above_16 AS treated_male_above_16,
        operational_area_query.alb_treated_female_1_4 AS alb_treated_female_1_4,
        operational_area_query.mbz_treated_female_1_4 AS mbz_treated_female_1_4,
        operational_area_query.meb_treated_female_1_4 AS meb_treated_female_1_4,
        operational_area_query.vita_treated_female_1_4 AS vita_treated_female_1_4,
        operational_area_query.treated_female_1_4 AS treated_female_1_4,
        operational_area_query.treated_female_6_to_11_mos AS treated_female_6_to_11_mos,
        operational_area_query.vita_treated_female_6_to_11_mos AS vita_treated_female_6_to_11_mos,
        operational_area_query.pzq_treated_female_5_14 AS pzq_treated_female_5_14,
        operational_area_query.alb_treated_female_5_14 AS alb_treated_female_5_14,
        operational_area_query.mbz_treated_female_5_14 AS mbz_treated_female_5_14,
        operational_area_query.treated_female_5_14 AS treated_female_5_14,
        operational_area_query.pzq_treated_female_5_to_15 AS pzq_treated_female_5_15,
        operational_area_query.alb_treated_female_5_to_15 AS alb_treated_female_5_15,
        operational_area_query.meb_treated_female_5_to_15 AS meb_treated_female_5_15,
        operational_area_query.treated_female_5_15 AS treated_female_5_15,
        operational_area_query.pzq_treated_female_above_15 AS pzq_treated_female_above_15,
        operational_area_query.alb_treated_female_above_15 AS alb_treated_female_above_15,
        operational_area_query.mbz_treated_female_above_15 AS mbz_treated_female_above_15,
        operational_area_query.treated_female_above_15 AS treated_female_above_15,
        operational_area_query.pzq_treated_female_above_16 AS pzq_treated_female_above_16,
        operational_area_query.alb_treated_female_above_16 AS alb_treated_female_above_16,
        operational_area_query.meb_treated_female_above_16 AS meb_treated_female_above_16,
        operational_area_query.alb_meb_total_treated_above_16 AS alb_meb_total_treated_above_16,
        operational_area_query.treated_female_above_16 AS treated_female_above_16,
        operational_area_query.total_males AS total_males,
        operational_area_query.vita_total_male AS vita_total_male,
        operational_area_query.vita_total_female AS vita_total_female,
        operational_area_query.alb_total_male AS alb_total_male,
        operational_area_query.alb_total_female AS alb_total_female,
        operational_area_query.meb_total_male AS meb_total_male,
        operational_area_query.meb_total_female AS meb_total_female,
        operational_area_query.vita_total_treated AS vita_total_treated,
        operational_area_query.pzq_total_treated AS pzq_total_treated,
        operational_area_query.alb_total_treated AS alb_total_treated,
        operational_area_query.meb_total_treated AS meb_total_treated,
        operational_area_query.alb_meb_total_treated AS alb_meb_total_treated,
        operational_area_query.alb_meb_total_treated_5_15 AS alb_meb_total_treated_5_15,
        operational_area_query.alb_meb_total_treated_1_4 AS alb_meb_total_treated_1_4,
        operational_area_query.pzq_total_treated_above_16 AS pzq_total_treated_above_16,
        operational_area_query.pzq_total_treated_5_to_15 AS pzq_total_treated_5_to_15,
        operational_area_query.pzq_total_male AS pzq_total_male,
        operational_area_query.pzq_total_female AS pzq_total_female,
        operational_area_query.vita_total_treated_6_to_11_mos AS vita_total_treated_6_to_11_mos,
        operational_area_query.vita_total_treated_1_4 AS vita_total_treated_1_4,
        operational_area_query.total_females AS total_females,
        operational_area_query.total_all_genders AS total_all_genders,
        operational_area_query.supervisor_distributed AS supervisor_distributed,
        operational_area_query.pzq_supervisor_distributed AS pzq_supervisor_distributed,
        operational_area_query.alb_supervisor_distributed AS alb_supervisor_distributed,
        operational_area_query.mbz_supervisor_distributed AS mbz_supervisor_distributed,
        operational_area_query.meb_supervisor_distributed AS meb_supervisor_distributed,
        operational_area_query.vita_supervisor_distributed AS vita_supervisor_distributed,
        operational_area_query.received_number AS received_number,
        operational_area_query.pzq_received AS pzq_received,
        operational_area_query.alb_received AS alb_received,
        operational_area_query.mbz_received AS mbz_received,
        operational_area_query.meb_received AS meb_received,
        operational_area_query.vita_received AS vita_received,
        operational_area_query.adminstered AS adminstered,
        operational_area_query.pzq_administered AS pzq_administered,
        operational_area_query.alb_administered AS alb_administered,
        operational_area_query.mbz_administered AS mbz_administered,
        operational_area_query.meb_administered AS meb_administered,
        operational_area_query.vita_administered AS vita_administered,
        operational_area_query.pzq_damaged AS pzq_damaged,
        operational_area_query.alb_damaged AS alb_damaged,
        operational_area_query.mbz_damaged AS mbz_damaged,
        operational_area_query.meb_damaged AS meb_damaged,
        operational_area_query.vita_damaged AS vita_damaged,
        operational_area_query.damaged AS damaged,
        operational_area_query.adverse AS adverse,
        operational_area_query.pzq_adverse AS pzq_adverse,
        operational_area_query.alb_adverse AS alb_adverse,
        operational_area_query.mbz_adverse AS mbz_adverse,
        operational_area_query.meb_adverse AS meb_adverse,
        operational_area_query.vita_adverse AS vita_adverse,
        operational_area_query.remaining_with_cdd AS remaining_with_cdd,
        operational_area_query.pzq_remaining_with_cdd AS pzq_remaining_with_cdd,
        operational_area_query.alb_remaining_with_cdd AS alb_remaining_with_cdd,
        operational_area_query.mbz_remaining_with_cdd AS mbz_remaining_with_cdd,
        operational_area_query.meb_remaining_with_cdd AS meb_remaining_with_cdd,
        operational_area_query.vita_remaining_with_cdd AS vita_remaining_with_cdd,
        operational_area_query.returned_to_supervisor AS returned_to_supervisor,
        operational_area_query.pzq_returned_to_supervisor AS pzq_returned_to_supervisor,
        operational_area_query.alb_returned_to_supervisor AS alb_returned_to_supervisor,
        operational_area_query.mbz_returned_to_supervisor AS mbz_returned_to_supervisor,
        operational_area_query.vita_returned_to_supervisor AS vita_returned_to_supervisor,
        COALESCE(wards_population.official_population, 0) AS official_population,
        CASE
            WHEN COALESCE(wards_population.official_population, 0) = 0 THEN 0
            ELSE CAST(operational_area_query.total_all_genders AS DECIMAL) / CAST(wards_population.official_population AS DECIMAL)
            END AS treatment_coverage,
        CASE
            WHEN COALESCE(wards_population.official_population, 0) = 0 THEN 0
            ELSE CAST(operational_area_query.pzq_total_treated AS DECIMAL) / CAST(wards_population.official_population AS DECIMAL)
            END AS pzq_treatment_coverage,
--- HERE THIS ONE NEEDS TO BE DONE
CASE
    WHEN COALESCE(wards_population.official_population, 0) = 0 THEN 0
    ELSE CAST(operational_area_query.alb_total_treated AS DECIMAL) / CAST(wards_population.official_population AS DECIMAL)
    END AS alb_treatment_coverage,
CASE
    WHEN COALESCE(wards_population.official_population, 0) = 0 THEN 0
    ELSE CAST(operational_area_query.meb_total_treated AS DECIMAL) / CAST(wards_population.official_population AS DECIMAL)
    END AS mbz_treatment_coverage,
--- END
        COALESCE(wards_population.other_population, 0) AS other_pop_target,
        COALESCE(wards_population.other_pop_target_6_to_59_mos_trusted, 0) AS other_pop_target_6_to_59_mos_trusted,
        COALESCE(wards_population.other_pop_target_6_to_11_mos_trusted, 0) AS other_pop_target_6_to_11_mos_trusted,
        COALESCE(wards_population.census_pop_target_above_16_official, 0) AS census_pop_target_above_16_official,
        COALESCE(wards_population.other_pop_target_above_16_trusted, 0) AS other_pop_target_above_16_trusted,
        CASE
            WHEN COALESCE(wards_population.other_population, 0) = 0 THEN 0
            ELSE CAST(operational_area_query.total_all_genders AS DECIMAL) / CAST(wards_population.other_population AS DECIMAL)
            END AS other_pop_coverage,
        CASE
            WHEN COALESCE(wards_population.other_population, 0) = 0 THEN 0
            ELSE CAST(operational_area_query.pzq_total_treated AS DECIMAL) / CAST(wards_population.other_population AS DECIMAL)
            END AS pzq_other_pop_coverage,
--- HERE THIS ONE NEEDS TO BE DONE
CASE
    WHEN COALESCE(wards_population.other_population, 0) = 0 THEN 0
    ELSE CAST(operational_area_query.alb_total_treated AS DECIMAL) / CAST(wards_population.other_population AS DECIMAL)
    END AS alb_other_pop_coverage,
CASE
    WHEN COALESCE(wards_population.other_population, 0) = 0 THEN 0
    ELSE CAST(operational_area_query.meb_total_treated AS DECIMAL) / CAST(wards_population.other_population AS DECIMAL)
    END AS mbz_other_pop_coverage,
--- END
        COALESCE(wards_population.census_target_population_6_to_59_mos_official, 0) AS census_target_population_6_to_59_mos_official,
        CASE
            WHEN COALESCE(wards_population.census_target_population_6_to_59_mos_official, 0) = 0 THEN 0
            ELSE CAST(operational_area_query.vita_total_treated AS DECIMAL) / CAST(wards_population.census_target_population_6_to_59_mos_official AS DECIMAL)
            END AS vita_6_to_59_mos_treatment_coverage,
        CASE
            WHEN COALESCE(wards_population.other_pop_target_6_to_59_mos_trusted, 0) = 0 THEN 0
            ELSE CAST(operational_area_query.vita_total_treated AS DECIMAL) / CAST(wards_population.other_pop_target_6_to_59_mos_trusted AS DECIMAL)
            END AS vita_6_to_59_mos_other_pop_coverage,
        CASE
            WHEN COALESCE(wards_population.other_pop_target_6_to_11_mos_trusted, 0) = 0 THEN 0
            ELSE CAST(operational_area_query.vita_total_treated_6_to_11_mos AS DECIMAL) / CAST(wards_population.other_pop_target_6_to_11_mos_trusted AS DECIMAL)
            END AS vita_6_to_11_mos_other_pop_coverage,
        CASE
            WHEN COALESCE(wards_population.census_target_population_6_to_11_mos_official, 0) = 0 THEN 0
            ELSE CAST(operational_area_query.vita_total_treated_6_to_11_mos AS DECIMAL) / CAST(wards_population.census_target_population_6_to_11_mos_official AS DECIMAL)
            END AS vita_6_to_11_mos_treatment_coverage,
        CASE
            WHEN COALESCE(wards_population.census_target_population_12_to_59_mos_official, 0) = 0 THEN 0
            ELSE CAST(operational_area_query.vita_total_treated_1_4 AS DECIMAL) / CAST(wards_population.census_target_population_12_to_59_mos_official AS DECIMAL)
            END AS vita_treatment_coverage_1_to_4,
        CASE
            WHEN COALESCE(wards_population.census_target_population_12_to_59_mos_official, 0) = 0 THEN 0
            ELSE CAST(operational_area_query.alb_meb_total_treated_1_4 AS DECIMAL) / CAST(wards_population.census_target_population_12_to_59_mos_official AS DECIMAL)
            END AS alb_meb_treatment_coverage_1_to_4,
        CASE
            WHEN COALESCE(wards_population.other_pop_target_12_to_59_mos_trusted, 0) = 0 THEN 0
            ELSE CAST(operational_area_query.vita_total_treated_1_4 AS DECIMAL) / CAST(wards_population.other_pop_target_12_to_59_mos_trusted AS DECIMAL)
            END AS vita_1_to_4_years_other_pop_coverage,
        CASE
            WHEN COALESCE(wards_population.other_pop_target_12_to_59_mos_trusted, 0) = 0 THEN 0
            ELSE CAST(operational_area_query.alb_meb_total_treated_1_4 AS DECIMAL) / CAST(wards_population.other_pop_target_12_to_59_mos_trusted AS DECIMAL)
            END AS alb_meb_1_to_4_years_other_pop_coverage,
        CASE
            WHEN COALESCE(wards_population.census_pop_target_above_16_official, 0) = 0 THEN 0
            ELSE CAST(operational_area_query.pzq_total_treated_above_16 AS DECIMAL) / CAST(wards_population.census_pop_target_above_16_official AS DECIMAL)
            END AS pzq_above_16_years_treatment_coverage,
        CASE
            WHEN COALESCE(wards_population.other_pop_target_above_16_trusted, 0) = 0 THEN 0
            ELSE CAST(operational_area_query.pzq_total_treated_above_16 AS DECIMAL) / CAST(wards_population.other_pop_target_above_16_trusted AS DECIMAL)
            END AS pzq_above_16_years_other_pop_coverage,
        CASE
            WHEN COALESCE(wards_population.census_pop_target_5_to_15_official, 0) = 0 THEN 0
            ELSE CAST(operational_area_query.pzq_total_treated_5_to_15 AS DECIMAL) / CAST(wards_population.census_pop_target_5_to_15_official AS DECIMAL)
            END AS pzq_5_to_15_years_treatment_coverage,
        CASE
            WHEN COALESCE(wards_population.census_pop_target_5_to_15_official, 0) = 0 THEN 0
            ELSE CAST(operational_area_query.alb_meb_total_treated_5_15 AS DECIMAL) / CAST(wards_population.census_pop_target_5_to_15_official AS DECIMAL)
            END AS alb_meb_5_to_15_years_treatment_coverage,
        CASE
            WHEN COALESCE(wards_population.other_pop_target_5_to_15_trusted, 0) = 0 THEN 0
            ELSE CAST(operational_area_query.pzq_total_treated_5_to_15 AS DECIMAL) / CAST(wards_population.other_pop_target_5_to_15_trusted AS DECIMAL)
            END AS pzq_5_to_15_years_other_pop_coverage,
        CASE
            WHEN COALESCE(wards_population.other_pop_target_5_to_15_trusted, 0) = 0 THEN 0
            ELSE CAST(operational_area_query.alb_meb_total_treated_5_15 AS DECIMAL) / CAST(wards_population.other_pop_target_5_to_15_trusted AS DECIMAL)
            END AS alb_meb_5_to_15_years_other_pop_coverage,
        CASE
            WHEN COALESCE(wards_population.other_pop_target_above_16_trusted, 0) = 0 THEN 0
            ELSE CAST(operational_area_query.alb_meb_total_treated_above_16 AS DECIMAL) / CAST(wards_population.other_pop_target_above_16_trusted AS DECIMAL)
            END AS alb_meb_above_16_pop_coverage,
        CASE
            WHEN COALESCE(wards_population.census_pop_target_above_16_official, 0) = 0 THEN 0
            ELSE CAST(operational_area_query.alb_meb_total_treated_above_16 AS DECIMAL) / CAST(wards_population.census_pop_target_above_16_official AS DECIMAL)
            END AS alb_meb_above_16_treatment_coverage,
        CASE
            WHEN (COALESCE(wards_population.census_pop_target_5_to_15_official, 0) + COALESCE(wards_population.census_pop_target_above_16_official, 0)) = 0 THEN 0
            ELSE CAST(operational_area_query.pzq_total_treated AS DECIMAL) / CAST((COALESCE(wards_population.census_pop_target_5_to_15_official, 0) + COALESCE(wards_population.census_pop_target_above_16_official, 0)) AS DECIMAL)
            END AS pzq_5_years_and_above_16_treatment_coverage,
        CASE
            WHEN (COALESCE(wards_population.other_pop_target_5_to_15_trusted, 0) + COALESCE(wards_population.other_pop_target_above_16_trusted, 0)) = 0 THEN 0
            ELSE CAST(operational_area_query.pzq_total_treated AS DECIMAL) / CAST((COALESCE(wards_population.other_pop_target_5_to_15_trusted, 0) + COALESCE(wards_population.other_pop_target_above_16_trusted, 0)) AS DECIMAL)
            END AS pzq_5_years_above_16_other_pop_coverage,
        (COALESCE(wards_population.other_pop_target_5_to_15_trusted, 0) + COALESCE(wards_population.other_pop_target_above_16_trusted, 0)) AS other_pop_target_5_to_15_and_above_16_trusted,
        COALESCE(wards_population.census_target_population_6_to_11_mos_official, 0) AS census_target_population_6_to_11_mos_official,
        COALESCE(wards_population.census_target_population_12_to_59_mos_official, 0) AS census_target_population_12_to_59_mos_official,
        COALESCE(wards_population.other_pop_target_12_to_59_mos_trusted, 0) AS other_pop_target_12_to_59_mos_trusted,
        COALESCE(wards_population.census_pop_target_5_to_15_official, 0) AS census_pop_target_5_to_15_official,
        COALESCE(wards_population.other_pop_target_5_to_15_trusted, 0) AS other_pop_target_5_to_15_trusted,
        (COALESCE(wards_population.census_pop_target_5_to_15_official, 0) + COALESCE(wards_population.census_pop_target_above_16_official, 0)) AS census_pop_target_5_to_15_and_above_16_official
    FROM plans
    LEFT JOIN (
        SELECT
            jurisdictions.plan_id,
            jurisdictions.structure_jurisdiction_id,
            COALESCE(SUM(daily_summary.health_education_above_16), 0) AS health_education_above_16,
            COALESCE(SUM(daily_summary.health_education_5_to_15), 0) AS health_education_5_to_15,
            COALESCE(SUM(daily_summary.alb_treated_male_1_4), 0) AS alb_treated_male_1_4,
            COALESCE(SUM(daily_summary.mbz_treated_male_1_4), 0) AS mbz_treated_male_1_4,
            COALESCE(SUM(daily_summary.meb_treated_male_1_4), 0) AS meb_treated_male_1_4,
            COALESCE(SUM(daily_summary.vita_treated_male_1_4), 0) AS vita_treated_male_1_4,
            COALESCE(SUM(daily_summary.treated_male_1_4), 0) AS treated_male_1_4,
            COALESCE(SUM(daily_summary.treated_male_6_to_11_mos), 0) AS treated_male_6_to_11_mos,
            COALESCE(SUM(daily_summary.vita_treated_male_6_to_11_mos), 0) AS vita_treated_male_6_to_11_mos,
            COALESCE(SUM(daily_summary.pzq_treated_male_5_14), 0) AS pzq_treated_male_5_14,
            COALESCE(SUM(daily_summary.alb_treated_male_5_14), 0) AS alb_treated_male_5_14,
            COALESCE(SUM(daily_summary.mbz_treated_male_5_14), 0) AS mbz_treated_male_5_14,
            COALESCE(SUM(daily_summary.treated_male_5_14), 0) AS treated_male_5_14,
            COALESCE(SUM(daily_summary.pzq_treated_male_5_to_15), 0) AS pzq_treated_male_5_to_15,
            COALESCE(SUM(daily_summary.alb_treated_male_5_to_15), 0) AS alb_treated_male_5_to_15,
            COALESCE(SUM(daily_summary.meb_treated_male_5_to_15), 0) AS meb_treated_male_5_to_15,
            COALESCE(SUM(daily_summary.treated_male_5_15), 0) AS treated_male_5_15,
            COALESCE(SUM(daily_summary.pzq_treated_male_above_15), 0) AS pzq_treated_male_above_15,
            COALESCE(SUM(daily_summary.alb_treated_male_above_15), 0) AS alb_treated_male_above_15,
            COALESCE(SUM(daily_summary.mbz_treated_male_above_15), 0) AS mbz_treated_male_above_15,
            COALESCE(SUM(daily_summary.treated_male_above_15), 0) AS treated_male_above_15,
            COALESCE(SUM(daily_summary.pzq_treated_male_above_16), 0) AS pzq_treated_male_above_16,
            COALESCE(SUM(daily_summary.alb_treated_male_above_16), 0) AS alb_treated_male_above_16,
            COALESCE(SUM(daily_summary.meb_treated_male_above_16), 0) AS meb_treated_male_above_16,
            COALESCE(SUM(daily_summary.treated_male_above_16), 0) AS treated_male_above_16,
            COALESCE(SUM(daily_summary.alb_treated_female_1_4), 0) AS alb_treated_female_1_4,
            COALESCE(SUM(daily_summary.mbz_treated_female_1_4), 0) AS mbz_treated_female_1_4,
            COALESCE(SUM(daily_summary.meb_treated_female_1_4), 0) AS meb_treated_female_1_4,
            COALESCE(SUM(daily_summary.vita_treated_female_1_4), 0) AS vita_treated_female_1_4,
            COALESCE(SUM(daily_summary.treated_female_1_4), 0) AS treated_female_1_4,
            COALESCE(SUM(daily_summary.treated_female_6_to_11_mos), 0) AS treated_female_6_to_11_mos,
            COALESCE(SUM(daily_summary.vita_treated_female_6_to_11_mos), 0) AS vita_treated_female_6_to_11_mos,
            COALESCE(SUM(daily_summary.pzq_treated_female_5_14), 0) AS pzq_treated_female_5_14,
            COALESCE(SUM(daily_summary.alb_treated_female_5_14), 0) AS alb_treated_female_5_14,
            COALESCE(SUM(daily_summary.mbz_treated_female_5_14), 0) AS mbz_treated_female_5_14,
            COALESCE(SUM(daily_summary.treated_female_5_14), 0) AS treated_female_5_14,
            COALESCE(SUM(daily_summary.pzq_treated_female_5_to_15), 0) AS pzq_treated_female_5_to_15,
            COALESCE(SUM(daily_summary.alb_treated_female_5_to_15), 0) AS alb_treated_female_5_to_15,
            COALESCE(SUM(daily_summary.meb_treated_female_5_to_15), 0) AS meb_treated_female_5_to_15,
            COALESCE(SUM(daily_summary.treated_female_5_15), 0) AS treated_female_5_15,
            COALESCE(SUM(daily_summary.pzq_treated_female_above_15), 0) AS pzq_treated_female_above_15,
            COALESCE(SUM(daily_summary.alb_treated_female_above_15), 0) AS alb_treated_female_above_15,
            COALESCE(SUM(daily_summary.mbz_treated_female_above_15), 0) AS mbz_treated_female_above_15,
            COALESCE(SUM(daily_summary.treated_female_above_15), 0) AS treated_female_above_15,
            COALESCE(SUM(daily_summary.pzq_treated_female_above_16), 0) AS pzq_treated_female_above_16,
            COALESCE(SUM(daily_summary.alb_treated_female_above_16), 0) AS alb_treated_female_above_16,
            COALESCE(SUM(daily_summary.meb_treated_female_above_16), 0) AS meb_treated_female_above_16,
            COALESCE(SUM(daily_summary.treated_female_above_16), 0) AS treated_female_above_16,
            COALESCE(SUM(daily_summary.total_males), 0) AS total_males,
            COALESCE(SUM(daily_summary.vita_total_male), 0) AS vita_total_male,
            COALESCE(SUM(daily_summary.vita_total_female), 0) AS vita_total_female,
            COALESCE(SUM(daily_summary.alb_total_female), 0) AS alb_total_female,
            COALESCE(SUM(daily_summary.alb_total_male), 0) AS alb_total_male,
            COALESCE(SUM(daily_summary.meb_total_male), 0) AS meb_total_male,
            COALESCE(SUM(daily_summary.meb_total_female), 0) AS meb_total_female,
            COALESCE(SUM(daily_summary.pzq_total_male), 0) AS pzq_total_male,
            COALESCE(SUM(daily_summary.pzq_total_female), 0) AS pzq_total_female,
            COALESCE(SUM(daily_summary.vita_total_treated), 0) AS vita_total_treated,
            COALESCE(SUM(daily_summary.pzq_total_treated), 0) AS pzq_total_treated,
            COALESCE(SUM(daily_summary.alb_total_treated), 0) AS alb_total_treated,
            COALESCE(SUM(daily_summary.meb_total_treated), 0) AS meb_total_treated,
            COALESCE(SUM(daily_summary.alb_meb_total_treated), 0) AS alb_meb_total_treated,
            COALESCE(SUM(daily_summary.alb_meb_total_treated_5_15), 0) AS alb_meb_total_treated_5_15,
            COALESCE(SUM(daily_summary.alb_meb_total_treated_1_4), 0) AS alb_meb_total_treated_1_4,
            COALESCE(SUM(daily_summary.pzq_total_treated_above_16), 0) AS pzq_total_treated_above_16,
            COALESCE(SUM(daily_summary.alb_meb_total_treated_above_16), 0) AS alb_meb_total_treated_above_16,
            COALESCE(SUM(daily_summary.pzq_total_treated_5_to_15), 0) AS pzq_total_treated_5_to_15,
            COALESCE(SUM(daily_summary.vita_total_treated_6_to_11_mos), 0) AS vita_total_treated_6_to_11_mos,
            COALESCE(SUM(daily_summary.vita_total_treated_1_4), 0) AS vita_total_treated_1_4,
            COALESCE(SUM(daily_summary.total_females), 0) AS total_females,
            COALESCE(SUM(daily_summary.total_all_genders), 0) AS total_all_genders,
            COALESCE(SUM(daily_summary.supervisor_distributed), 0) AS supervisor_distributed,
            COALESCE(SUM(daily_summary.pzq_supervisor_distributed), 0) AS pzq_supervisor_distributed,
            COALESCE(SUM(daily_summary.alb_supervisor_distributed), 0) AS alb_supervisor_distributed,
            COALESCE(SUM(daily_summary.mbz_supervisor_distributed), 0) AS mbz_supervisor_distributed,
            COALESCE(SUM(daily_summary.meb_supervisor_distributed), 0) AS meb_supervisor_distributed,
            COALESCE(SUM(daily_summary.vita_supervisor_distributed), 0) AS vita_supervisor_distributed,
            COALESCE(SUM(daily_summary.received_number), 0) AS received_number,
            COALESCE(SUM(daily_summary.pzq_received), 0) AS pzq_received,
            COALESCE(SUM(daily_summary.alb_received), 0) AS alb_received,
            COALESCE(SUM(daily_summary.mbz_received), 0) AS mbz_received,
            COALESCE(SUM(daily_summary.meb_received), 0) AS meb_received,
            COALESCE(SUM(daily_summary.vita_received), 0) AS vita_received,
            COALESCE(SUM(daily_summary.adminstered), 0) AS adminstered,
            COALESCE(SUM(daily_summary.pzq_administered), 0) AS pzq_administered,
            COALESCE(SUM(daily_summary.alb_administered), 0) AS alb_administered,
            COALESCE(SUM(daily_summary.mbz_administered), 0) AS mbz_administered,
            COALESCE(SUM(daily_summary.meb_administered), 0) AS meb_administered,
            COALESCE(SUM(daily_summary.vita_administered), 0) AS vita_administered,
            COALESCE(SUM(daily_summary.pzq_damaged), 0) AS pzq_damaged,
            COALESCE(SUM(daily_summary.alb_damaged), 0) AS alb_damaged,
            COALESCE(SUM(daily_summary.mbz_damaged), 0) AS mbz_damaged,
            COALESCE(SUM(daily_summary.meb_damaged), 0) AS meb_damaged,
            COALESCE(SUM(daily_summary.vita_damaged), 0) AS vita_damaged,
            COALESCE(SUM(daily_summary.damaged), 0) AS damaged,
            COALESCE(SUM(daily_summary.adverse), 0) AS adverse,
            COALESCE(SUM(daily_summary.pzq_adverse), 0) AS pzq_adverse,
            COALESCE(SUM(daily_summary.alb_adverse), 0) AS alb_adverse,
            COALESCE(SUM(daily_summary.mbz_adverse), 0) AS mbz_adverse,
            COALESCE(SUM(daily_summary.meb_adverse), 0) AS meb_adverse,
            COALESCE(SUM(daily_summary.vita_adverse), 0) AS vita_adverse,
            COALESCE(SUM(daily_summary.remaining_with_cdd), 0) AS remaining_with_cdd,
            COALESCE(SUM(daily_summary.pzq_remaining_with_cdd), 0) AS pzq_remaining_with_cdd,
            COALESCE(SUM(daily_summary.alb_remaining_with_cdd), 0) AS alb_remaining_with_cdd,
            COALESCE(SUM(daily_summary.mbz_remaining_with_cdd), 0) AS mbz_remaining_with_cdd,
            COALESCE(SUM(daily_summary.meb_remaining_with_cdd), 0) AS meb_remaining_with_cdd,
            COALESCE(SUM(daily_summary.vita_remaining_with_cdd), 0) AS vita_remaining_with_cdd,
            COALESCE(SUM(daily_summary.returned_to_supervisor), 0) AS returned_to_supervisor,
            COALESCE(SUM(daily_summary.pzq_returned_to_supervisor), 0) AS pzq_returned_to_supervisor,
            COALESCE(SUM(daily_summary.alb_returned_to_supervisor), 0) AS alb_returned_to_supervisor,
            COALESCE(SUM(daily_summary.mbz_returned_to_supervisor), 0) AS mbz_returned_to_supervisor,
            COALESCE(SUM(daily_summary.vita_returned_to_supervisor), 0) AS vita_returned_to_supervisor
        FROM (
            SELECT
                structures.plan_id,
                structures.structure_jurisdiction_id
            FROM mda_lite_structures AS structures
            WHERE structures.plan_id IS NOT NULL
            GROUP BY structures.plan_id, structures.structure_jurisdiction_id
        ) AS jurisdictions
        LEFT JOIN LATERAL (
            SELECT
                subq.health_education_5_to_15 AS health_education_5_to_15,
                subq.health_education_above_16 AS health_education_above_16,
                subq.alb_treated_male_1_4 AS alb_treated_male_1_4,
                subq.mbz_treated_male_1_4 AS mbz_treated_male_1_4,
                subq.meb_treated_male_1_4 AS meb_treated_male_1_4,
                subq.vita_treated_male_1_4 AS vita_treated_male_1_4,
                subq.treated_male_1_4 AS treated_male_1_4,
                subq.treated_male_6_to_11_mos AS treated_male_6_to_11_mos,
                subq.vita_treated_male_6_to_11_mos AS vita_treated_male_6_to_11_mos,
                subq.pzq_treated_male_5_to_14 AS pzq_treated_male_5_14,
                subq.alb_treated_male_5_to_14 AS alb_treated_male_5_14,
                subq.mbz_treated_male_5_to_14 AS mbz_treated_male_5_14,
                subq.treated_male_5_14 AS treated_male_5_14,
                subq.pzq_treated_male_5_to_15 AS pzq_treated_male_5_to_15,
                subq.alb_treated_male_5_to_15 AS alb_treated_male_5_to_15,
                subq.meb_treated_male_5_to_15 AS meb_treated_male_5_to_15,
                subq.treated_male_5_15 AS treated_male_5_15,
                subq.pzq_treated_male_above_15 AS pzq_treated_male_above_15,
                subq.alb_treated_male_above_15 AS alb_treated_male_above_15,
                subq.mbz_treated_male_above_15 AS mbz_treated_male_above_15,
                subq.treated_male_above_15 AS treated_male_above_15,
                subq.pzq_treated_male_above_16 AS pzq_treated_male_above_16,
                subq.alb_treated_male_above_16 AS alb_treated_male_above_16,
                subq.meb_treated_male_above_16 AS meb_treated_male_above_16,
                subq.treated_male_above_16 AS treated_male_above_16,
                subq.alb_treated_female_1_4 AS alb_treated_female_1_4,
                subq.mbz_treated_female_1_4 AS mbz_treated_female_1_4,
                subq.meb_treated_female_1_4 AS meb_treated_female_1_4,
                subq.vita_treated_female_1_4 AS vita_treated_female_1_4,
                subq.treated_female_1_4 AS treated_female_1_4,
                subq.treated_female_6_to_11_mos AS treated_female_6_to_11_mos,
                subq.vita_treated_female_6_to_11_mos AS vita_treated_female_6_to_11_mos,
                subq.pzq_treated_female_5_to_14 AS pzq_treated_female_5_14,
                subq.alb_treated_female_5_to_14 AS alb_treated_female_5_14,
                subq.mbz_treated_female_5_to_14 AS mbz_treated_female_5_14,
                subq.treated_female_5_14 AS treated_female_5_14,
                subq.pzq_treated_female_5_to_15 AS pzq_treated_female_5_to_15,
                subq.alb_treated_female_5_to_15 AS alb_treated_female_5_to_15,
                subq.meb_treated_female_5_to_15 AS meb_treated_female_5_to_15,
                subq.treated_female_5_15 AS treated_female_5_15,
                subq.pzq_treated_female_above_15 AS pzq_treated_female_above_15,
                subq.alb_treated_female_above_15 AS alb_treated_female_above_15,
                subq.mbz_treated_female_above_15 AS mbz_treated_female_above_15,
                subq.treated_female_above_15 AS treated_female_above_15,
                subq.pzq_treated_female_above_16 AS pzq_treated_female_above_16,
                subq.alb_treated_female_above_16 AS alb_treated_female_above_16,
                subq.meb_treated_female_above_16 AS meb_treated_female_above_16,
                subq.treated_female_above_16 AS treated_female_above_16,
                subq.vita_total_male AS vita_total_male,
                subq.vita_total_female AS vita_total_female,
                subq.alb_total_male AS alb_total_male,
                subq.alb_total_female AS alb_total_female,
                subq.meb_total_male AS meb_total_male,
                subq.meb_total_female AS meb_total_female,
                subq.pzq_total_male AS pzq_total_male,
                subq.pzq_total_female AS pzq_total_female,
                subq.vita_total_treated AS vita_total_treated,
                subq.pzq_total_treated AS pzq_total_treated,
                subq.alb_total_treated AS alb_total_treated,
                subq.meb_total_treated AS meb_total_treated,
                subq.alb_meb_total_treated AS alb_meb_total_treated,
                subq.alb_meb_total_treated_1_4 AS alb_meb_total_treated_1_4,
                subq.alb_meb_total_treated_5_15 AS alb_meb_total_treated_5_15,
                subq.pzq_total_treated_above_16 AS pzq_total_treated_above_16,
                subq.pzq_total_treated_5_to_15 AS pzq_total_treated_5_to_15,
                subq.vita_total_treated_6_to_11_mos AS vita_total_treated_6_to_11_mos,
                subq.vita_total_treated_1_4 AS vita_total_treated_1_4,
                subq.alb_meb_total_treated_above_16 AS alb_meb_total_treated_above_16,
                subq.total_males AS total_males,
                subq.total_females AS total_females,
                subq.total_all_genders AS total_all_genders,
                subq.supervisor_distributed AS supervisor_distributed,
                subq.pzq_supervisor_distributed AS pzq_supervisor_distributed,
                subq.alb_supervisor_distributed AS alb_supervisor_distributed,
                subq.mbz_supervisor_distributed AS mbz_supervisor_distributed,
                subq.meb_supervisor_distributed AS meb_supervisor_distributed,
                subq.vita_supervisor_distributed AS vita_supervisor_distributed,
                subq.received_number AS received_number,
                subq.pzq_received AS pzq_received,
                subq.alb_received AS alb_received,
                subq.mbz_received AS mbz_received,
                subq.meb_received AS meb_received,
                subq.vita_received AS vita_received,
                subq.adminstered AS adminstered,
                subq.pzq_administered AS pzq_administered,
                subq.alb_administered AS alb_administered,
                subq.mbz_administered AS mbz_administered,
                subq.meb_administered AS meb_administered,
                subq.vita_administered AS vita_administered,
                subq.pzq_damaged AS pzq_damaged,
                subq.alb_damaged AS alb_damaged,
                subq.mbz_damaged AS mbz_damaged,
                subq.meb_damaged AS meb_damaged,
                subq.vita_damaged AS vita_damaged,
                subq.damaged AS damaged,
                subq.adverse AS adverse,
                subq.pzq_adverse AS pzq_adverse,
                subq.alb_adverse AS alb_adverse,
                subq.mbz_adverse AS mbz_adverse,
                subq.meb_adverse AS meb_adverse,
                subq.vita_adverse AS vita_adverse,
                subq.remaining_with_cdd AS remaining_with_cdd,
                subq.pzq_remaining_with_cdd AS pzq_remaining_with_cdd,
                subq.alb_remaining_with_cdd AS alb_remaining_with_cdd,
                subq.mbz_remaining_with_cdd AS mbz_remaining_with_cdd,
                subq.meb_remaining_with_cdd AS meb_remaining_with_cdd,
                subq.vita_remaining_with_cdd AS vita_remaining_with_cdd,
                subq.returned_to_supervisor AS returned_to_supervisor,
                subq.pzq_returned_to_supervisor AS pzq_returned_to_supervisor,
                subq.alb_returned_to_supervisor AS alb_returned_to_supervisor,
                subq.mbz_returned_to_supervisor AS mbz_returned_to_supervisor,
                subq.vita_returned_to_supervisor AS vita_returned_to_supervisor
            FROM (
                SELECT
                    SUM(COALESCE((events.form_data -> 'health_education_above_16'::TEXT) ->> 0, '0'::TEXT)::bigint) AS health_education_above_16,
                    SUM(COALESCE((events.form_data -> 'health_education_5_to_15'::TEXT) ->> 0, '0'::TEXT)::bigint) AS health_education_5_to_15,
                    SUM(COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'ALB' THEN events.form_data ->> 'treated_male_1_to_4' END,'0')::INTEGER) AS alb_treated_male_1_4,
                    SUM(COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'MBZ' THEN events.form_data ->> 'treated_male_1_to_4' END,'0')::INTEGER) AS mbz_treated_male_1_4,
                    SUM(COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'MEB' THEN events.form_data ->> 'treated_male_1_to_4' END,'0')::INTEGER) AS meb_treated_male_1_4,
                    SUM(COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'VITA' THEN events.form_data ->> 'treated_male_1_to_4' END,'0')::INTEGER) AS vita_treated_male_1_4,
                    SUM(COALESCE (events.form_data -> 'treated_male_1_to_4' ->> 0, '0')::INTEGER) AS treated_male_1_4,
                    SUM(COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'VITA' THEN events.form_data ->> 'treated_male_6_to_11_mos' END,'0')::INTEGER) AS vita_treated_male_6_to_11_mos,
                    SUM(COALESCE((events.form_data -> 'treated_male_6_to_11_mos'::TEXT) ->> 0, '0'::TEXT)::INTEGER) AS treated_male_6_to_11_mos,
                    SUM(COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'PZQ' THEN events.form_data ->> 'treated_male_5_to_14' END,'0')::INTEGER) AS pzq_treated_male_5_to_14,
                    SUM(COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'ALB' THEN events.form_data ->> 'treated_male_5_to_14' END,'0')::INTEGER) AS alb_treated_male_5_to_14,
                    SUM(COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'MBZ' THEN events.form_data ->> 'treated_male_5_to_14' END,'0')::INTEGER) AS mbz_treated_male_5_to_14,
                    SUM(COALESCE (events.form_data -> 'treated_male_5_to_14' ->> 0, '0')::INTEGER) AS treated_male_5_14,
                    SUM(COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'PZQ' THEN events.form_data ->> 'treated_male_5_to_15' END,'0')::INTEGER) AS pzq_treated_male_5_to_15,
                    SUM(COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'ALB' THEN events.form_data ->> 'treated_male_5_to_15' END,'0')::INTEGER) AS alb_treated_male_5_to_15,
                    SUM(COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'MEB' THEN events.form_data ->> 'treated_male_5_to_15' END,'0')::INTEGER) AS meb_treated_male_5_to_15,
                    SUM(COALESCE((events.form_data -> 'treated_male_5_to_15'::TEXT) ->> 0, '0'::TEXT)::INTEGER) AS treated_male_5_15,
                    SUM(COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'PZQ' THEN events.form_data ->> 'treated_male_above_15' END,'0')::INTEGER) AS pzq_treated_male_above_15,
                    SUM(COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'ALB' THEN events.form_data ->> 'treated_male_above_15' END,'0')::INTEGER) AS alb_treated_male_above_15,
                    SUM(COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'MBZ' THEN events.form_data ->> 'treated_male_above_15' END,'0')::INTEGER) AS mbz_treated_male_above_15,
                    SUM(COALESCE (events.form_data -> 'treated_male_above_15' ->> 0, '0')::INTEGER) AS treated_male_above_15,
                    SUM(COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'PZQ' THEN events.form_data ->> 'treated_male_above_16' END,'0')::INTEGER) AS pzq_treated_male_above_16,
                    SUM(COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'ALB' THEN events.form_data ->> 'treated_male_above_16' END,'0')::INTEGER) AS alb_treated_male_above_16,
                    SUM(COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'MEB' THEN events.form_data ->> 'treated_male_above_16' END,'0')::INTEGER) AS meb_treated_male_above_16,
                    SUM(COALESCE((events.form_data -> 'treated_male_above_16'::TEXT) ->> 0, '0'::TEXT)::INTEGER) AS treated_male_above_16,
                    SUM(COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'ALB' THEN events.form_data ->> 'treated_female_1_to_4' END,'0')::INTEGER) AS alb_treated_female_1_4,
                    SUM(COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'MBZ' THEN events.form_data ->> 'treated_female_1_to_4' END,'0')::INTEGER) AS mbz_treated_female_1_4,
                    SUM(COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'MEB' THEN events.form_data ->> 'treated_female_1_to_4' END,'0')::INTEGER) AS meb_treated_female_1_4,
                    SUM(COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'VITA' THEN events.form_data ->> 'treated_female_1_to_4' END,'0')::INTEGER) AS vita_treated_female_1_4,
                    SUM(COALESCE (events.form_data -> 'treated_female_1_to_4' ->> 0, '0')::INTEGER) AS treated_female_1_4,
                    SUM(COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'VITA' THEN events.form_data ->> 'treated_female_6_to_11_mos' END,'0')::INTEGER) AS vita_treated_female_6_to_11_mos,
                    SUM(COALESCE((events.form_data -> 'treated_female_6_to_11_mos'::TEXT) ->> 0, '0'::TEXT)::INTEGER) AS treated_female_6_to_11_mos,
                    SUM(COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'PZQ' THEN events.form_data ->> 'treated_female_5_to_14' END,'0')::INTEGER) AS pzq_treated_female_5_to_14,
                    SUM(COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'ALB' THEN events.form_data ->> 'treated_female_5_to_14' END,'0')::INTEGER) AS alb_treated_female_5_to_14,
                    SUM(COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'MBZ' THEN events.form_data ->> 'treated_female_5_to_14' END,'0')::INTEGER) AS mbz_treated_female_5_to_14,
                    SUM(COALESCE (events.form_data -> 'treated_female_5_to_14' ->> 0, '0')::INTEGER) AS treated_female_5_14,
                    SUM(COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'PZQ' THEN events.form_data ->> 'treated_female_5_to_15' END,'0')::INTEGER) AS pzq_treated_female_5_to_15,
                    SUM(COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'ALB' THEN events.form_data ->> 'treated_female_5_to_15' END,'0')::INTEGER) AS alb_treated_female_5_to_15,
                    SUM(COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'MEB' THEN events.form_data ->> 'treated_female_5_to_15' END,'0')::INTEGER) AS meb_treated_female_5_to_15,
                    SUM(COALESCE((events.form_data -> 'treated_female_5_to_15'::TEXT) ->> 0, '0'::TEXT)::INTEGER) AS treated_female_5_15,
                    SUM(COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'PZQ' THEN events.form_data ->> 'treated_female_above_15' END,'0')::INTEGER) AS pzq_treated_female_above_15,
                    SUM(COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'ALB' THEN events.form_data ->> 'treated_female_above_15' END,'0')::INTEGER) AS alb_treated_female_above_15,
                    SUM(COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'MBZ' THEN events.form_data ->> 'treated_female_above_15' END,'0')::INTEGER) AS mbz_treated_female_above_15,
                    SUM(COALESCE (events.form_data -> 'treated_female_above_15' ->> 0, '0')::INTEGER) AS treated_female_above_15,
                    SUM(COALESCE((events.form_data -> 'treated_female_above_16'::TEXT) ->> 0, '0'::TEXT)::INTEGER) AS treated_female_above_16,
                    SUM(COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'PZQ' THEN events.form_data ->> 'treated_female_above_16' END,'0')::INTEGER) AS pzq_treated_female_above_16,
                    SUM(COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'ALB' THEN events.form_data ->> 'treated_female_above_16' END,'0')::INTEGER) AS alb_treated_female_above_16,
                    SUM(COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'MEB' THEN events.form_data ->> 'treated_female_above_16' END,'0')::INTEGER) AS meb_treated_female_above_16,

                    SUM(COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'VITA' THEN events.form_data ->> 'treated_male_1_to_4' END,'0')::INTEGER +
                        COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'VITA' THEN events.form_data ->> 'treated_male_6_to_11_mos' END,'0')::INTEGER
                    ) AS vita_total_male,

                    SUM(COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'VITA' THEN events.form_data ->> 'treated_female_1_to_4' END,'0')::INTEGER +
                        COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'VITA' THEN events.form_data ->> 'treated_female_6_to_11_mos' END,'0')::INTEGER
                    ) AS vita_total_female,

                    SUM(COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'VITA' THEN events.form_data ->> 'treated_female_1_to_4' END,'0')::INTEGER +
                        COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'VITA' THEN events.form_data ->> 'treated_female_6_to_11_mos' END,'0')::INTEGER +
                        COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'VITA' THEN events.form_data ->> 'treated_male_1_to_4' END,'0')::INTEGER +
                        COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'VITA' THEN events.form_data ->> 'treated_male_6_to_11_mos' END,'0')::INTEGER
                    ) AS vita_total_treated,

                    SUM(COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'VITA' THEN events.form_data ->> 'treated_male_6_to_11_mos' END,'0')::INTEGER +
                        COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'VITA' THEN events.form_data ->> 'treated_female_6_to_11_mos' END,'0')::INTEGER
                    ) AS vita_total_treated_6_to_11_mos,

                    SUM(COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'VITA' THEN events.form_data ->> 'treated_male_1_to_4' END,'0')::INTEGER +
                        COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'VITA' THEN events.form_data ->> 'treated_female_1_to_4' END,'0')::INTEGER
                    ) AS vita_total_treated_1_4,

                    SUM(COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'PZQ' THEN events.form_data ->> 'treated_male_5_to_14' END,'0')::INTEGER +
                        COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'PZQ' THEN events.form_data ->> 'treated_male_5_to_15' END,'0')::INTEGER +
                        COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'PZQ' THEN events.form_data ->> 'treated_male_above_15' END,'0')::INTEGER +
                        COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'PZQ' THEN events.form_data ->> 'treated_male_above_16' END,'0')::INTEGER
                    ) AS pzq_total_male,

                    SUM(COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'PZQ' THEN events.form_data ->> 'treated_female_5_to_14' END,'0')::INTEGER +
                        COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'PZQ' THEN events.form_data ->> 'treated_female_5_to_15' END,'0')::INTEGER +
                        COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'PZQ' THEN events.form_data ->> 'treated_female_above_15' END,'0')::INTEGER +
                        COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'PZQ' THEN events.form_data ->> 'treated_female_above_16' END,'0')::INTEGER
                    ) AS pzq_total_female,

                    SUM(COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'PZQ' THEN events.form_data ->> 'treated_male_5_to_14' END,'0')::INTEGER +
                        COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'PZQ' THEN events.form_data ->> 'treated_male_5_to_15' END,'0')::INTEGER +
                        COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'PZQ' THEN events.form_data ->> 'treated_male_above_15' END,'0')::INTEGER +
                        COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'PZQ' THEN events.form_data ->> 'treated_male_above_16' END,'0')::INTEGER +
                        COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'PZQ' THEN events.form_data ->> 'treated_female_5_to_14' END,'0')::INTEGER +
                        COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'PZQ' THEN events.form_data ->> 'treated_female_5_to_15' END,'0')::INTEGER +
                        COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'PZQ' THEN events.form_data ->> 'treated_female_above_15' END,'0')::INTEGER +
                        COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'PZQ' THEN events.form_data ->> 'treated_female_above_16' END,'0')::INTEGER
                    ) AS pzq_total_treated,

                    SUM(COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'PZQ' THEN events.form_data ->> 'treated_male_above_16' END,'0')::INTEGER +
                        COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'PZQ' THEN events.form_data ->> 'treated_female_above_16' END,'0')::INTEGER
                    ) AS pzq_total_treated_above_16,

                    SUM(COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'PZQ' THEN events.form_data ->> 'treated_female_5_to_15' END,'0')::INTEGER +
                        COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'PZQ' THEN events.form_data ->> 'treated_male_5_to_15' END,'0')::INTEGER
                    ) AS pzq_total_treated_5_to_15,

                    SUM(COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'ALB' THEN events.form_data ->> 'treated_male_1_to_4' END,'0')::INTEGER +
                        COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'ALB' THEN events.form_data ->> 'treated_male_5_to_14' END,'0')::INTEGER +
                        COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'ALB' THEN events.form_data ->> 'treated_male_5_to_15' END,'0')::INTEGER +
                        COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'ALB' THEN events.form_data ->> 'treated_male_above_16' END,'0')::INTEGER +
                        COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'ALB' THEN events.form_data ->> 'treated_male_above_15' END,'0')::INTEGER
                    ) AS alb_total_male,

                    SUM(COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'ALB' THEN events.form_data ->> 'treated_female_1_to_4' END,'0')::INTEGER +
                        COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'ALB' THEN events.form_data ->> 'treated_female_5_to_14' END,'0')::INTEGER +
                        COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'ALB' THEN events.form_data ->> 'treated_female_5_to_15' END,'0')::INTEGER +
                        COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'ALB' THEN events.form_data ->> 'treated_female_above_16' END,'0')::INTEGER +
                        COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'ALB' THEN events.form_data ->> 'treated_female_above_15' END,'0')::INTEGER
                    ) AS alb_total_female,

                    SUM(COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'ALB' THEN events.form_data ->> 'treated_male_1_to_4' END,'0')::INTEGER +
                        COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'ALB' THEN events.form_data ->> 'treated_male_5_to_14' END,'0')::INTEGER +
                        COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'ALB' THEN events.form_data ->> 'treated_male_5_to_15' END,'0')::INTEGER +
                        COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'ALB' THEN events.form_data ->> 'treated_male_above_16' END,'0')::INTEGER +
                        COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'ALB' THEN events.form_data ->> 'treated_male_above_15' END,'0')::INTEGER +
                        COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'ALB' THEN events.form_data ->> 'treated_female_1_to_4' END,'0')::INTEGER +
                        COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'ALB' THEN events.form_data ->> 'treated_female_5_to_14' END,'0')::INTEGER +
                        COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'ALB' THEN events.form_data ->> 'treated_female_5_to_15' END,'0')::INTEGER +
                        COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'ALB' THEN events.form_data ->> 'treated_female_above_16' END,'0')::INTEGER +
                        COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'ALB' THEN events.form_data ->> 'treated_female_above_15' END,'0')::INTEGER
                    ) AS alb_total_treated,

                    SUM(COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'MEB' THEN events.form_data ->> 'treated_male_1_to_4' END,'0')::INTEGER +
                        COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'MEB' THEN events.form_data ->> 'treated_male_5_to_14' END,'0')::INTEGER +
                        COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'MEB' THEN events.form_data ->> 'treated_male_5_to_15' END,'0')::INTEGER +
                        COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'MEB' THEN events.form_data ->> 'treated_male_above_16' END,'0')::INTEGER +
                        COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'MEB' THEN events.form_data ->> 'treated_male_above_15' END,'0')::INTEGER +
                        COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'MBZ' THEN events.form_data ->> 'treated_male_1_to_4' END,'0')::INTEGER +
                        COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'MBZ' THEN events.form_data ->> 'treated_male_5_to_14' END,'0')::INTEGER +
                        COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'MBZ' THEN events.form_data ->> 'treated_male_5_to_15' END,'0')::INTEGER +
                        COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'MBZ' THEN events.form_data ->> 'treated_male_above_16' END,'0')::INTEGER +
                        COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'MBZ' THEN events.form_data ->> 'treated_male_above_15' END,'0')::INTEGER
                    ) AS meb_total_male,

                    SUM(COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'MEB' THEN events.form_data ->> 'treated_female_1_to_4' END,'0')::INTEGER +
                        COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'MEB' THEN events.form_data ->> 'treated_female_5_to_14' END,'0')::INTEGER +
                        COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'MEB' THEN events.form_data ->> 'treated_female_5_to_15' END,'0')::INTEGER +
                        COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'MEB' THEN events.form_data ->> 'treated_female_above_16' END,'0')::INTEGER +
                        COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'MEB' THEN events.form_data ->> 'treated_female_above_15' END,'0')::INTEGER +
                        COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'MBZ' THEN events.form_data ->> 'treated_female_1_to_4' END,'0')::INTEGER +
                        COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'MBZ' THEN events.form_data ->> 'treated_female_5_to_14' END,'0')::INTEGER +
                        COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'MBZ' THEN events.form_data ->> 'treated_female_5_to_15' END,'0')::INTEGER +
                        COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'MBZ' THEN events.form_data ->> 'treated_female_above_16' END,'0')::INTEGER +
                        COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'MBZ' THEN events.form_data ->> 'treated_female_above_15' END,'0')::INTEGER
                    ) AS meb_total_female,

                    SUM(COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'MEB' THEN events.form_data ->> 'treated_male_1_to_4' END,'0')::INTEGER +
                        COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'MEB' THEN events.form_data ->> 'treated_male_5_to_14' END,'0')::INTEGER +
                        COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'MEB' THEN events.form_data ->> 'treated_male_5_to_15' END,'0')::INTEGER +
                        COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'MEB' THEN events.form_data ->> 'treated_male_above_16' END,'0')::INTEGER +
                        COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'MEB' THEN events.form_data ->> 'treated_male_above_15' END,'0')::INTEGER +
                        COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'MEB' THEN events.form_data ->> 'treated_female_1_to_4' END,'0')::INTEGER +
                        COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'MEB' THEN events.form_data ->> 'treated_female_5_to_14' END,'0')::INTEGER +
                        COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'MEB' THEN events.form_data ->> 'treated_female_5_to_15' END,'0')::INTEGER +
                        COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'MEB' THEN events.form_data ->> 'treated_female_above_16' END,'0')::INTEGER +
                        COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'MEB' THEN events.form_data ->> 'treated_female_above_15' END,'0')::INTEGER +
                        COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'MBZ' THEN events.form_data ->> 'treated_male_1_to_4' END,'0')::INTEGER +
                        COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'MBZ' THEN events.form_data ->> 'treated_male_5_to_14' END,'0')::INTEGER +
                        COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'MBZ' THEN events.form_data ->> 'treated_male_5_to_15' END,'0')::INTEGER +
                        COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'MBZ' THEN events.form_data ->> 'treated_male_above_16' END,'0')::INTEGER +
                        COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'MBZ' THEN events.form_data ->> 'treated_male_above_15' END,'0')::INTEGER +
                        COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'MBZ' THEN events.form_data ->> 'treated_female_1_to_4' END,'0')::INTEGER +
                        COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'MBZ' THEN events.form_data ->> 'treated_female_5_to_14' END,'0')::INTEGER +
                        COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'MBZ' THEN events.form_data ->> 'treated_female_5_to_15' END,'0')::INTEGER +
                        COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'MBZ' THEN events.form_data ->> 'treated_female_above_16' END,'0')::INTEGER +
                        COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'MBZ' THEN events.form_data ->> 'treated_female_above_15' END,'0')::INTEGER
                    ) AS meb_total_treated,

                    SUM(COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'ALB' THEN events.form_data ->> 'treated_male_1_to_4' END,'0')::INTEGER +
                        COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'ALB' THEN events.form_data ->> 'treated_male_5_to_14' END,'0')::INTEGER +
                        COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'ALB' THEN events.form_data ->> 'treated_male_5_to_15' END,'0')::INTEGER +
                        COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'ALB' THEN events.form_data ->> 'treated_male_above_16' END,'0')::INTEGER +
                        COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'ALB' THEN events.form_data ->> 'treated_male_above_15' END,'0')::INTEGER +
                        COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'ALB' THEN events.form_data ->> 'treated_female_1_to_4' END,'0')::INTEGER +
                        COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'ALB' THEN events.form_data ->> 'treated_female_5_to_14' END,'0')::INTEGER +
                        COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'ALB' THEN events.form_data ->> 'treated_female_5_to_15' END,'0')::INTEGER +
                        COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'ALB' THEN events.form_data ->> 'treated_female_above_16' END,'0')::INTEGER +
                        COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'ALB' THEN events.form_data ->> 'treated_female_above_15' END,'0')::INTEGER +
                        COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'MEB' THEN events.form_data ->> 'treated_male_1_to_4' END,'0')::INTEGER +
                        COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'MEB' THEN events.form_data ->> 'treated_male_5_to_14' END,'0')::INTEGER +
                        COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'MEB' THEN events.form_data ->> 'treated_male_5_to_15' END,'0')::INTEGER +
                        COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'MEB' THEN events.form_data ->> 'treated_male_above_16' END,'0')::INTEGER +
                        COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'MEB' THEN events.form_data ->> 'treated_male_above_15' END,'0')::INTEGER +
                        COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'MEB' THEN events.form_data ->> 'treated_female_1_to_4' END,'0')::INTEGER +
                        COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'MEB' THEN events.form_data ->> 'treated_female_5_to_14' END,'0')::INTEGER +
                        COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'MEB' THEN events.form_data ->> 'treated_female_5_to_15' END,'0')::INTEGER +
                        COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'MEB' THEN events.form_data ->> 'treated_female_above_16' END,'0')::INTEGER+
                        COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'MBZ' THEN events.form_data ->> 'treated_male_1_to_4' END,'0')::INTEGER +
                        COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'MBZ' THEN events.form_data ->> 'treated_male_5_to_14' END,'0')::INTEGER +
                        COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'MBZ' THEN events.form_data ->> 'treated_male_5_to_15' END,'0')::INTEGER +
                        COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'MBZ' THEN events.form_data ->> 'treated_male_above_16' END,'0')::INTEGER +
                        COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'MBZ' THEN events.form_data ->> 'treated_male_above_15' END,'0')::INTEGER +
                        COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'MBZ' THEN events.form_data ->> 'treated_female_1_to_4' END,'0')::INTEGER +
                        COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'MBZ' THEN events.form_data ->> 'treated_female_5_to_14' END,'0')::INTEGER +
                        COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'MBZ' THEN events.form_data ->> 'treated_female_5_to_15' END,'0')::INTEGER +
                        COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'MBZ' THEN events.form_data ->> 'treated_female_above_16' END,'0')::INTEGER +
                        COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'MBZ' THEN events.form_data ->> 'treated_female_above_15' END,'0')::INTEGER
                    ) AS alb_meb_total_treated,

                    SUM(COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'ALB' THEN events.form_data ->> 'treated_male_1_to_4' END,'0')::INTEGER +
                        COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'MEB' THEN events.form_data ->> 'treated_male_1_to_4' END,'0')::INTEGER +
                        COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'ALB' THEN events.form_data ->> 'treated_female_1_to_4' END,'0')::INTEGER +
                        COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'MEB' THEN events.form_data ->> 'treated_female_1_to_4' END,'0')::INTEGER
                    ) AS alb_meb_total_treated_1_4,

                    SUM(COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'ALB' THEN events.form_data ->> 'treated_male_5_to_15' END,'0')::INTEGER +
                        COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'ALB' THEN events.form_data ->> 'treated_female_5_to_15' END,'0')::INTEGER +
                        COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'MEB' THEN events.form_data ->> 'treated_male_5_to_15' END,'0')::INTEGER +
                        COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'MEB' THEN events.form_data ->> 'treated_female_5_to_15' END,'0')::INTEGER
                    ) AS alb_meb_total_treated_5_15,

                    SUM(COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'MEB' THEN events.form_data ->> 'treated_female_above_16' END,'0')::INTEGER +
                        COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'ALB' THEN events.form_data ->> 'treated_female_above_16' END,'0')::INTEGER +
                        COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'MEB' THEN events.form_data ->> 'treated_male_above_16' END,'0')::INTEGER +
                        COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'ALB' THEN events.form_data ->> 'treated_male_above_16' END,'0')::INTEGER
                    ) AS alb_meb_total_treated_above_16,
                    SUM(COALESCE(events.form_data -> 'treated_male_1_to_4' ->> 0, '0')::INTEGER +
                        COALESCE(events.form_data -> 'treated_male_5_to_14' ->> 0, '0')::INTEGER +
                        COALESCE(events.form_data -> 'treated_male_above_15' ->> 0, '0')::INTEGER) AS total_males,

                    SUM(COALESCE (events.form_data -> 'treated_female_1_to_4' ->> 0, '0')::INTEGER +
                    COALESCE(events.form_data -> 'treated_female_5_to_14' ->> 0, '0')::INTEGER +
                    COALESCE (events.form_data -> 'treated_female_above_15' ->> 0, '0')::INTEGER) AS total_females,

                    SUM(COALESCE (events.form_data -> 'treated_male_1_to_4' ->> 0, '0')::INTEGER +
                    COALESCE (events.form_data -> 'treated_male_5_to_14' ->> 0, '0')::INTEGER +
                    COALESCE (events.form_data -> 'treated_male_above_15' ->> 0, '0')::INTEGER +
                    COALESCE (events.form_data -> 'treated_female_1_to_4' ->> 0, '0')::INTEGER +
                    COALESCE (events.form_data -> 'treated_female_5_to_14' ->> 0, '0')::INTEGER +
                    COALESCE (events.form_data -> 'treated_female_above_15' ->> 0, '0')::INTEGER) AS total_all_genders,

                    SUM(COALESCE (events.form_data -> 'sum_pzq_received_and_top_up' ->> 0, '0')::INTEGER +
                    COALESCE (events.form_data -> 'sum_alb_received_and_top_up' ->> 0, '0')::INTEGER +
                    COALESCE (events.form_data -> 'sum_mbz_received_and_top_up' ->> 0, '0')::INTEGER) AS supervisor_distributed,

                    SUM(COALESCE((events.form_data -> 'sum_pzq_received_and_top_up'::TEXT) ->> 0, '0'::TEXT)::INTEGER) AS pzq_supervisor_distributed,
                    SUM(COALESCE((events.form_data -> 'sum_alb_received_and_top_up'::TEXT) ->> 0, '0'::TEXT)::INTEGER) AS alb_supervisor_distributed,
                    SUM(COALESCE((events.form_data -> 'sum_mbz_received_and_top_up'::TEXT) ->> 0, '0'::TEXT)::INTEGER) AS mbz_supervisor_distributed,
                    SUM(COALESCE((events.form_data -> 'sum_meb_received_and_top_up'::TEXT) ->> 0, '0'::TEXT)::INTEGER) AS meb_supervisor_distributed,
                    SUM(COALESCE((events.form_data -> 'sum_vita_received_and_top_up'::TEXT) ->> 0, '0'::TEXT)::INTEGER) AS vita_supervisor_distributed,
                    SUM(COALESCE (events.form_data -> 'received_number' ->> 0, '0')::INTEGER) AS received_number,
                    SUM(COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'PZQ' THEN events.form_data ->> 'received_number' END,'0')::INTEGER) AS pzq_received,
                    SUM(COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'ALB' THEN events.form_data ->> 'received_number' END,'0')::INTEGER) AS alb_received,
                    SUM(COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'MBZ' THEN events.form_data ->> 'received_number' END,'0')::INTEGER) AS mbz_received,
                    SUM(COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'MEB' THEN events.form_data ->> 'received_number' END,'0')::INTEGER) AS meb_received,
                    SUM(COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'VITA' THEN events.form_data ->> 'received_number' END,'0')::INTEGER) AS vita_received,
                    SUM(COALESCE (events.form_data -> 'adminstered' ->> 0, '0')::INTEGER) AS adminstered,
                    SUM(COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'PZQ' THEN events.form_data ->> 'adminstered' END,'0')::INTEGER) AS pzq_administered,
                    SUM(COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'ALB' THEN events.form_data ->> 'adminstered' END,'0')::INTEGER) AS alb_administered,
                    SUM(COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'MBZ' THEN events.form_data ->> 'adminstered' END,'0')::INTEGER) AS mbz_administered,
                    SUM(COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'MEB' THEN events.form_data ->> 'adminstered' END,'0')::INTEGER) AS meb_administered,
                    SUM(COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'VITA' THEN events.form_data ->> 'adminstered' END,'0')::INTEGER) AS vita_administered,
                    SUM(COALESCE (events.form_data -> 'damaged' ->> 0, '0')::INTEGER) AS damaged,
                    SUM(COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'PZQ' THEN events.form_data ->> 'damaged' END,'0')::INTEGER) AS pzq_damaged,
                    SUM(COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'ALB' THEN events.form_data ->> 'damaged' END,'0')::INTEGER) AS alb_damaged,
                    SUM(COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'MBZ' THEN events.form_data ->> 'damaged' END,'0')::INTEGER) AS mbz_damaged,
                    SUM(COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'MEB' THEN events.form_data ->> 'damaged' END,'0')::INTEGER) AS meb_damaged,
                    SUM(COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'VITA' THEN events.form_data ->> 'damaged' END,'0')::INTEGER) AS vita_damaged,
                    SUM(COALESCE (events.form_data -> 'adverse' ->> 0, '0')::INTEGER) AS adverse,
                    SUM(COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'PZQ' THEN events.form_data ->> 'adverse' END,'0')::INTEGER) AS pzq_adverse,
                    SUM(COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'ALB' THEN events.form_data ->> 'adverse' END,'0')::INTEGER) AS alb_adverse,
                    SUM(COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'MBZ' THEN events.form_data ->> 'adverse' END,'0')::INTEGER) AS mbz_adverse,
                    SUM(COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'MEB' THEN events.form_data ->> 'adverse' END,'0')::INTEGER) AS meb_adverse,
                    SUM(COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'VITA' THEN events.form_data ->> 'adverse' END,'0')::INTEGER) AS vita_adverse,

                    SUM(COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'PZQ' THEN COALESCE((events.form_data -> 'received_number'::TEXT) ->> 0, '0'::TEXT)::INTEGER -
                        (COALESCE((events.form_data -> 'adminstered'::TEXT) ->> 0, '0'::TEXT)::INTEGER +
                        COALESCE((events.form_data -> 'damaged'::TEXT) ->> 0, '0'::TEXT)::INTEGER) END,'0')::INTEGER
                    ) AS pzq_remaining_with_cdd,

                    SUM(COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'ALB' THEN COALESCE((events.form_data -> 'received_number'::TEXT) ->> 0, '0'::TEXT)::INTEGER -
                        (COALESCE((events.form_data -> 'adminstered'::TEXT) ->> 0, '0'::TEXT)::INTEGER +
                        COALESCE((events.form_data -> 'damaged'::TEXT) ->> 0, '0'::TEXT)::INTEGER) END,'0')::INTEGER
                    ) AS alb_remaining_with_cdd,

                    SUM(COALESCE (CASE WHEN events.form_data ->> 'drugs' = 'MBZ' THEN COALESCE((events.form_data -> 'received_number'::TEXT) ->> 0, '0'::TEXT)::INTEGER -
                        (COALESCE((events.form_data -> 'adminstered'::TEXT) ->> 0, '0'::TEXT)::INTEGER +
                        COALESCE((events.form_data -> 'damaged'::TEXT) ->> 0, '0'::TEXT)::INTEGER) END,'0')::INTEGER
                    ) AS mbz_remaining_with_cdd,

                    SUM(COALESCE (CASE WHEN events.form_data ->> 'drugs' = 'MEB' THEN COALESCE((events.form_data -> 'received_number'::TEXT) ->> 0, '0'::TEXT)::INTEGER -
                        (COALESCE((events.form_data -> 'adminstered'::TEXT) ->> 0, '0'::TEXT)::INTEGER +
                        COALESCE((events.form_data -> 'damaged'::TEXT) ->> 0, '0'::TEXT)::INTEGER) END,'0')::INTEGER
                    ) AS meb_remaining_with_cdd,

                    SUM(COALESCE (CASE WHEN events.form_data ->> 'drugs' = 'VITA' THEN COALESCE((events.form_data -> 'received_number'::TEXT) ->> 0, '0'::TEXT)::INTEGER -
                        (COALESCE((events.form_data -> 'adminstered'::TEXT) ->> 0, '0'::TEXT)::INTEGER +
                        COALESCE((events.form_data -> 'damaged'::TEXT) ->> 0, '0'::TEXT)::INTEGER) END,'0')::INTEGER
                    ) AS vita_remaining_with_cdd,

                    SUM(COALESCE (events.form_data -> 'received_number' ->> 0, '0')::INTEGER -
                        (COALESCE (events.form_data -> 'adminstered' ->> 0, '0')::INTEGER +
                        COALESCE (events.form_data -> 'damaged' ->> 0, '0')::INTEGER)
                    ) AS remaining_with_cdd,

                    SUM(COALESCE (events.form_data -> 'pzq_returned' ->> 0, '0')::INTEGER +
                        COALESCE (events.form_data -> 'albendazole_returned' ->> 0, '0')::INTEGER +
                        COALESCE (events.form_data -> 'mebendazole_returned' ->> 0, '0')::INTEGER
                    ) AS returned_to_supervisor,

                    SUM(COALESCE((events.form_data -> 'pzq_returned'::TEXT) ->> 0, '0'::TEXT)::INTEGER) AS pzq_returned_to_supervisor,
                    SUM(COALESCE((events.form_data -> 'albendazole_returned'::TEXT) ->> 0, '0'::TEXT)::INTEGER) AS alb_returned_to_supervisor,
                    SUM(COALESCE((events.form_data -> 'mebendazole_returned'::TEXT) ->> 0, '0'::TEXT)::INTEGER) AS mbz_returned_to_supervisor,
                    SUM(COALESCE((events.form_data -> 'vita_returned'::TEXT) ->> 0, '0'::TEXT)::INTEGER) vita_returned_to_supervisor
                FROM events
                WHERE jurisdictions.structure_jurisdiction_id  = events.location_id
                AND events.plan_id = jurisdictions.plan_id
                AND events.entity_type = 'Structure'
                AND events.event_type IN ('cdd_supervisor_daily_summary', 'tablet_accountability','cell_coordinator_daily_summary')
                GROUP BY jurisdictions.plan_id, jurisdictions.structure_jurisdiction_id
            ) AS subq
        ) AS daily_summary ON TRUE
        GROUP BY jurisdictions.plan_id, jurisdictions.structure_jurisdiction_id
    ) AS operational_area_query ON operational_area_query.plan_id = plans.identifier
    LEFT JOIN jurisdictions_tree AS lite_jurisdictions ON operational_area_query.structure_jurisdiction_id = lite_jurisdictions.jurisdiction_id
    LEFT JOIN LATERAL (
        SELECT
            SUM(COALESCE(official_population, '0')::INTEGER) AS official_population,
            SUM(COALESCE(other_population, '0')::INTEGER) AS other_population,
            SUM(COALESCE(census_target_population_6_to_59_mos_official, '0')::INTEGER) AS census_target_population_6_to_59_mos_official,
            SUM(COALESCE(other_pop_target_6_to_59_mos_trusted, '0')::INTEGER) AS other_pop_target_6_to_59_mos_trusted,
            SUM(COALESCE(census_target_population_6_to_11_mos_official, '0')::INTEGER) AS census_target_population_6_to_11_mos_official,
            SUM(COALESCE(other_pop_target_6_to_11_mos_trusted, '0')::INTEGER) AS other_pop_target_6_to_11_mos_trusted,
            SUM(COALESCE(census_target_population_12_to_59_mos_official, '0')::INTEGER) AS census_target_population_12_to_59_mos_official,
            SUM(COALESCE(other_pop_target_12_to_59_mos_trusted, '0')::INTEGER) AS other_pop_target_12_to_59_mos_trusted,
            SUM(COALESCE(census_pop_target_above_16_official, '0')::INTEGER) AS census_pop_target_above_16_official,
            SUM(COALESCE(other_pop_target_above_16_trusted, '0')::INTEGER) AS other_pop_target_above_16_trusted,
            SUM(COALESCE(census_pop_target_5_to_15_official, '0')::INTEGER) AS census_pop_target_5_to_15_official,
            SUM(COALESCE(other_pop_target_5_to_15_trusted, '0')::INTEGER) AS other_pop_target_5_to_15_trusted
        FROM mda_lite_wards_population
        WHERE mda_lite_wards_population.jurisdiction_id = lite_jurisdictions.jurisdiction_id
        GROUP BY mda_lite_wards_population.jurisdiction_id
    ) AS wards_population ON TRUE
) AS main_query
WHERE main_query.jurisdiction_depth = 2;

CREATE INDEX IF NOT EXISTS mda_lite_operational_areas_path_idx_gin on mda_lite_operational_areas using GIN(jurisdiction_path);
CREATE INDEX IF NOT EXISTS mda_lite_operational_areas_plan_idx ON mda_lite_operational_areas (plan_id);
CREATE INDEX IF NOT EXISTS mda_lite_operational_areas_jurisdiction_idx ON mda_lite_operational_areas (jurisdiction_id);
CREATE INDEX IF NOT EXISTS mda_lite_operational_areas_jurisdiction_parent_idx ON mda_lite_operational_areas (jurisdiction_parent_id);
CREATE UNIQUE INDEX IF NOT EXISTS mda_lite_operational_areas_idx ON mda_lite_operational_areas (id);