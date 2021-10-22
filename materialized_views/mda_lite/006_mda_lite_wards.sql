SET schema 'reveal';

DROP MATERIALIZED VIEW IF EXISTS mda_lite_wards CASCADE;

CREATE MATERIALIZED VIEW IF NOT EXISTS mda_lite_wards
AS
SELECT
    public.uuid_generate_v5(
        '6ba7b810-9dad-11d1-80b4-00c04fd430c8',
        concat(subq.base_entity_id, subq.parent_id, subq.plan_id)
    ) AS id,
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
        ELSE CAST(subq.pzq_total_treated AS DECIMAL) / CAST((COALESCE(wards_population.census_pop_target_5_to_15_official::INTEGER, 0) + COALESCE(wards_population.census_pop_target_above_16_official::INTEGER, 0)) AS DECIMAL)
        END AS pzq_5_years_and_above_16_treatment_coverage,
    CASE
        WHEN COALESCE(wards_population.census_target_population_6_to_59_mos_official::INTEGER, 0) = 0 THEN 0
        ELSE CAST(subq.vita_total_treated AS DECIMAL) / CAST(wards_population.census_target_population_6_to_59_mos_official AS DECIMAL)
        END AS vita_6_to_59_mos_treatment_coverage,
    CASE
        WHEN COALESCE(wards_population.official_population::INTEGER, 0) = 0 THEN 0
        ELSE CAST(subq.total_all_genders AS DECIMAL) / CAST(wards_population.official_population AS DECIMAL)
        END AS treatment_coverage,
    CASE
        WHEN COALESCE(wards_population.official_population::INTEGER, 0) = 0 THEN 0
        ELSE CAST(subq.pzq_total_treated AS DECIMAL) / CAST(wards_population.official_population AS DECIMAL)
        END AS pzq_treatment_coverage,
    CASE
        WHEN COALESCE(wards_population.official_population::INTEGER, 0) = 0 THEN 0
        ELSE CAST(subq.alb_total_treated AS DECIMAL) / CAST(wards_population.official_population AS DECIMAL)
        END AS alb_treatment_coverage,
    CASE
        WHEN COALESCE(wards_population.official_population::INTEGER, 0) = 0 THEN 0
        ELSE CAST(subq.meb_total_treated AS DECIMAL) / CAST(wards_population.official_population AS DECIMAL)
        END AS mbz_treatment_coverage,
    CASE
        WHEN COALESCE(wards_population.other_population::INTEGER, 0) = 0 THEN 0
        ELSE CAST(subq.total_all_genders AS DECIMAL) / CAST(wards_population.other_population AS DECIMAL)
        END AS other_pop_coverage,
    CASE
        WHEN COALESCE(wards_population.other_population::INTEGER, 0) = 0 THEN 0
        ELSE CAST(subq.pzq_total_treated AS DECIMAL) / CAST(wards_population.other_population AS DECIMAL)
        END AS pzq_other_pop_coverage,
    CASE
        WHEN COALESCE(wards_population.other_population::INTEGER, 0) = 0 THEN 0
        ELSE CAST(subq.alb_total_treated AS DECIMAL) / CAST(wards_population.other_population AS DECIMAL)
        END AS alb_other_pop_coverage,
    CASE
        WHEN COALESCE(wards_population.other_population::INTEGER, 0) = 0 THEN 0
        ELSE CAST(subq.meb_total_treated AS DECIMAL) / CAST(wards_population.other_population AS DECIMAL)
        END AS mbz_other_pop_coverage,
    CASE
        WHEN COALESCE(wards_population.other_pop_target_6_to_59_mos_trusted::INTEGER, 0) = 0 THEN 0
        ELSE CAST(subq.vita_total_treated AS DECIMAL) / CAST(wards_population.other_pop_target_6_to_59_mos_trusted AS DECIMAL)
        END AS vita_6_to_59_mos_other_pop_coverage,
    CASE
        WHEN COALESCE(wards_population.other_pop_target_6_to_11_mos_trusted::INTEGER, 0) = 0 THEN 0
        ELSE CAST(subq.vita_total_treated_6_to_11_mos AS DECIMAL) / CAST(wards_population.other_pop_target_6_to_11_mos_trusted AS DECIMAL)
        END AS vita_6_to_11_mos_other_pop_coverage,
    CASE
        WHEN COALESCE(wards_population.census_target_population_6_to_11_mos_official::INTEGER, 0) = 0 THEN 0
        ELSE CAST(subq.vita_total_treated_6_to_11_mos AS DECIMAL) / CAST(wards_population.census_target_population_6_to_11_mos_official AS DECIMAL)
        END AS vita_6_to_11_mos_treatment_coverage,
    CASE
        WHEN COALESCE(wards_population.census_target_population_12_to_59_mos_official::INTEGER, 0) = 0 THEN 0
        ELSE CAST(subq.vita_total_treated_1_4 AS DECIMAL) / CAST(wards_population.census_target_population_12_to_59_mos_official AS DECIMAL)
        END AS vita_treatment_coverage_1_to_4,
    CASE
        WHEN COALESCE(wards_population.other_pop_target_12_to_59_mos_trusted::INTEGER, 0) = 0 THEN 0
        ELSE CAST(subq.vita_total_treated_1_4 AS DECIMAL) / CAST(wards_population.other_pop_target_12_to_59_mos_trusted AS DECIMAL)
        END AS vita_1_to_4_years_other_pop_coverage,
    CASE
        WHEN COALESCE(wards_population.census_pop_target_above_16_official::INTEGER, 0) = 0 THEN 0
        ELSE CAST(subq.pzq_total_treated_above_16 AS DECIMAL) / CAST(wards_population.census_pop_target_above_16_official AS DECIMAL)
    END AS pzq_above_16_years_treatment_coverage,
    CASE
        WHEN COALESCE(wards_population.other_pop_target_above_16_trusted::INTEGER, 0) = 0 THEN 0
        ELSE CAST(subq.pzq_total_treated_above_16 AS DECIMAL) / CAST(wards_population.other_pop_target_above_16_trusted AS DECIMAL)
    END AS  pzq_above_16_years_other_pop_coverage,
    CASE
        WHEN COALESCE(wards_population.census_pop_target_5_to_15_official::INTEGER, 0) = 0 THEN 0
        ELSE CAST(subq.pzq_total_treated_5_to_15 AS DECIMAL) / CAST(wards_population.census_pop_target_5_to_15_official AS DECIMAL)
        END AS pzq_5_to_15_years_treatment_coverage,
    CASE
        WHEN COALESCE(wards_population.other_pop_target_5_to_15_trusted::INTEGER, 0) = 0 THEN 0
        ELSE CAST(subq.pzq_total_treated_5_to_15 AS DECIMAL) / CAST(wards_population.other_pop_target_5_to_15_trusted AS DECIMAL)
        END AS pzq_5_to_15_years_other_pop_coverage,
    CASE
        WHEN (COALESCE(wards_population.other_pop_target_5_to_15_trusted::INTEGER, 0) + COALESCE(wards_population.other_pop_target_above_16_trusted::INTEGER, 0)) = 0 THEN 0
        ELSE CAST(subq.pzq_total_treated AS DECIMAL) / CAST((COALESCE(wards_population.other_pop_target_5_to_15_trusted::INTEGER, 0) + COALESCE(wards_population.other_pop_target_above_16_trusted::INTEGER, 0)) AS DECIMAL)
        END AS pzq_5_years_above_16_other_pop_coverage,
    (COALESCE(wards_population.other_pop_target_5_to_15_trusted::INTEGER, 0) + COALESCE(wards_population.other_pop_target_above_16_trusted::INTEGER, 0)) AS other_pop_target_5_to_15_and_above_16_trusted,
    CASE
        WHEN COALESCE(wards_population.census_target_population_12_to_59_mos_official::INTEGER, 0) = 0 THEN 0
        ELSE CAST(subq.alb_meb_total_treated_1_4 AS DECIMAL) / CAST(wards_population.census_target_population_12_to_59_mos_official AS DECIMAL)
        END AS alb_meb_treatment_coverage_1_to_4,
    CASE
        WHEN COALESCE(wards_population.other_pop_target_12_to_59_mos_trusted::INTEGER, 0) = 0 THEN 0
        ELSE CAST(subq.alb_meb_total_treated_1_4 AS DECIMAL) / CAST(wards_population.other_pop_target_12_to_59_mos_trusted AS DECIMAL)
        END AS alb_meb_1_to_4_years_other_pop_coverage,
    CASE
        WHEN COALESCE(wards_population.census_pop_target_5_to_15_official::INTEGER, 0) = 0 THEN 0
        ELSE CAST(subq.alb_meb_total_treated_5_15 AS DECIMAL) / CAST(wards_population.census_pop_target_5_to_15_official AS DECIMAL)
        END AS alb_meb_5_to_15_years_treatment_coverage,
    CASE
        WHEN COALESCE(wards_population.other_pop_target_5_to_15_trusted::INTEGER, 0) = 0 THEN 0
        ELSE CAST(subq.alb_meb_total_treated_5_15 AS DECIMAL) / CAST(wards_population.other_pop_target_5_to_15_trusted AS DECIMAL)
        END AS alb_meb_5_to_15_years_other_pop_coverage,
    CASE
        WHEN COALESCE(wards_population.other_pop_target_above_16_trusted::INTEGER, 0) = 0 THEN 0
        ELSE CAST(subq.alb_meb_total_treated_above_16 AS DECIMAL) / CAST(wards_population.other_pop_target_above_16_trusted AS DECIMAL)
        END AS alb_meb_above_16_pop_coverage,
    CASE
        WHEN COALESCE(wards_population.census_pop_target_above_16_official::INTEGER, 0) = 0 THEN 0
        ELSE CAST(subq.alb_meb_total_treated_above_16 AS DECIMAL) / CAST(wards_population.census_pop_target_above_16_official AS DECIMAL)
        END AS alb_meb_above_16_treatment_coverage
FROM (
        SELECT
            locations.name AS ward_name,
            parents.jurisdiction_id AS parent_id,
            parents.plan_id AS plan_id,
            COALESCE(events_query.base_entity_id, locations.code)::TEXT AS base_entity_id,
            COALESCE(events_query.health_education_above_16, '0')::INTEGER AS health_education_above_16,
            COALESCE(events_query.health_education_5_to_15, '0')::INTEGER AS health_education_5_to_15,
            COALESCE(events_query.alb_treated_male_1_4, '0')::INTEGER AS alb_treated_male_1_4,
            COALESCE(events_query.mbz_treated_male_1_4, '0')::INTEGER AS mbz_treated_male_1_4,
            COALESCE(events_query.meb_treated_male_1_4, '0')::INTEGER AS meb_treated_male_1_4,
            COALESCE(events_query.vita_treated_male_1_4, '0')::INTEGER AS vita_treated_male_1_4,
            COALESCE(events_query.treated_male_1_4, '0')::INTEGER AS treated_male_1_4,
            COALESCE(events_query.vita_treated_male_6_to_11_mos, '0')::INTEGER AS vita_treated_male_6_to_11_mos,
            COALESCE(events_query.treated_male_6_to_11_mos, '0')::INTEGER AS treated_male_6_to_11_mos,
            COALESCE(events_query.pzq_treated_male_5_14, '0')::INTEGER AS pzq_treated_male_5_14,
            COALESCE(events_query.alb_treated_male_5_14, '0')::INTEGER AS alb_treated_male_5_14,
            COALESCE(events_query.mbz_treated_male_5_14, '0')::INTEGER AS mbz_treated_male_5_14,
            COALESCE(events_query.treated_male_5_14, '0')::INTEGER AS treated_male_5_14,
            COALESCE(events_query.pzq_treated_female_5_15, '0')::INTEGER AS pzq_treated_female_5_15,
            COALESCE(events_query.alb_treated_female_5_15, '0')::INTEGER AS alb_treated_female_5_15,
            COALESCE(events_query.meb_treated_female_5_15, '0')::INTEGER AS meb_treated_female_5_15,
            COALESCE(events_query.treated_female_5_15, '0')::INTEGER AS treated_female_5_15,
            COALESCE(events_query.pzq_treated_male_5_15, '0')::INTEGER AS pzq_treated_male_5_15,
            COALESCE(events_query.alb_treated_male_5_15, '0')::INTEGER AS alb_treated_male_5_15,
            COALESCE(events_query.meb_treated_male_5_15, '0')::INTEGER AS meb_treated_male_5_15,
            COALESCE(events_query.treated_male_5_15, '0')::INTEGER AS treated_male_5_15,
            COALESCE(events_query.pzq_treated_male_above_15, '0')::INTEGER AS pzq_treated_male_above_15,
            COALESCE(events_query.alb_treated_male_above_15, '0')::INTEGER AS alb_treated_male_above_15,
            COALESCE(events_query.mbz_treated_male_above_15, '0')::INTEGER AS mbz_treated_male_above_15,
            COALESCE(events_query.treated_male_above_15, '0')::INTEGER AS treated_male_above_15,
            COALESCE(events_query.pzq_treated_male_above_16, '0')::INTEGER AS pzq_treated_male_above_16,
            COALESCE(events_query.alb_treated_male_above_16, '0')::INTEGER AS alb_treated_male_above_16,
            COALESCE(events_query.meb_treated_male_above_16, '0')::INTEGER AS meb_treated_male_above_16,
            COALESCE(events_query.treated_male_above_16, '0')::INTEGER AS treated_male_above_16,
            COALESCE(events_query.alb_treated_female_1_4, '0')::INTEGER AS alb_treated_female_1_4,
            COALESCE(events_query.mbz_treated_female_1_4, '0')::INTEGER AS mbz_treated_female_1_4,
            COALESCE(events_query.meb_treated_female_1_4, '0')::INTEGER AS meb_treated_female_1_4,
            COALESCE(events_query.vita_treated_female_1_4, '0')::INTEGER AS vita_treated_female_1_4,
            COALESCE(events_query.treated_female_1_4, '0')::INTEGER AS treated_female_1_4,
            COALESCE(events_query.vita_treated_female_6_to_11_mos, '0')::INTEGER AS vita_treated_female_6_to_11_mos,
            COALESCE(events_query.treated_female_6_to_11_mos, '0')::INTEGER AS treated_female_6_to_11_mos,
            COALESCE(events_query.pzq_treated_female_5_14, '0')::INTEGER AS pzq_treated_female_5_14,
            COALESCE(events_query.alb_treated_female_5_14, '0')::INTEGER AS alb_treated_female_5_14,
            COALESCE(events_query.mbz_treated_female_5_14, '0')::INTEGER AS mbz_treated_female_5_14,
            COALESCE(events_query.treated_female_5_14, '0')::INTEGER AS treated_female_5_14,
            COALESCE(events_query.pzq_treated_female_above_15, '0')::INTEGER AS pzq_treated_female_above_15,
            COALESCE(events_query.alb_treated_female_above_15, '0')::INTEGER AS alb_treated_female_above_15,
            COALESCE(events_query.mbz_treated_female_above_15, '0')::INTEGER AS mbz_treated_female_above_15,
            COALESCE(events_query.treated_female_above_15, '0')::INTEGER AS treated_female_above_15,
            COALESCE(events_query.treated_female_above_16, '0')::INTEGER AS treated_female_above_16,
            COALESCE(events_query.pzq_treated_female_above_16, '0')::INTEGER AS pzq_treated_female_above_16,
            COALESCE(events_query.alb_treated_female_above_16, '0')::INTEGER AS alb_treated_female_above_16,
            COALESCE(events_query.meb_treated_female_above_16, '0')::INTEGER AS meb_treated_female_above_16,
            COALESCE(events_query.total_males, '0')::INTEGER AS total_males,
            COALESCE(events_query.vita_total_male, '0')::INTEGER AS vita_total_male,
            COALESCE(events_query.vita_total_female, '0')::INTEGER AS vita_total_female,
            COALESCE(events_query.vita_total_treated, '0')::INTEGER AS vita_total_treated,
            COALESCE(events_query.vita_total_treated_6_to_11_mos, '0')::INTEGER AS vita_total_treated_6_to_11_mos,
            COALESCE(events_query.vita_total_treated_1_4, '0')::INTEGER AS vita_total_treated_1_4,
            COALESCE(events_query.pzq_total_male, '0')::INTEGER AS pzq_total_male,
            COALESCE(events_query.pzq_total_female, '0')::INTEGER AS pzq_total_female,
            COALESCE(events_query.pzq_total_treated, '0')::INTEGER AS pzq_total_treated,
            COALESCE(events_query.pzq_total_treated_above_16, '0')::INTEGER AS pzq_total_treated_above_16,
            COALESCE(events_query.pzq_total_treated_5_to_15, '0')::INTEGER AS pzq_total_treated_5_to_15,
            COALESCE(events_query.alb_total_male, '0')::INTEGER AS alb_total_male,
            COALESCE(events_query.alb_total_female, '0')::INTEGER AS alb_total_female,
            COALESCE(events_query.alb_total_treated, '0')::INTEGER AS alb_total_treated,
            COALESCE(events_query.meb_total_male, '0')::INTEGER AS meb_total_male,
            COALESCE(events_query.meb_total_female, '0')::INTEGER AS meb_total_female,
            COALESCE(events_query.meb_total_treated, '0')::INTEGER AS meb_total_treated,
            COALESCE(events_query.alb_meb_total_treated, '0')::INTEGER AS alb_meb_total_treated,
            COALESCE(events_query.alb_meb_total_treated_1_4, '0')::INTEGER AS alb_meb_total_treated_1_4,
            COALESCE(events_query.alb_meb_total_treated_5_15, '0')::INTEGER AS alb_meb_total_treated_5_15,
            COALESCE(events_query.alb_meb_total_treated_above_16, '0')::INTEGER AS alb_meb_total_treated_above_16,
            COALESCE(events_query.total_females, '0')::INTEGER AS total_females,
            COALESCE(events_query.total_all_genders, '0')::INTEGER AS total_all_genders,
            COALESCE(events_query.supervisor_distributed, '0')::INTEGER AS supervisor_distributed,
            COALESCE(events_query.pzq_supervisor_distributed, '0')::INTEGER AS pzq_supervisor_distributed,
            COALESCE(events_query.alb_supervisor_distributed, '0')::INTEGER AS alb_supervisor_distributed,
            COALESCE(events_query.mbz_supervisor_distributed, '0')::INTEGER AS mbz_supervisor_distributed,
            COALESCE(events_query.meb_supervisor_distributed, '0')::INTEGER AS meb_supervisor_distributed,
            COALESCE(events_query.vita_supervisor_distributed, '0')::INTEGER AS vita_supervisor_distributed,
            COALESCE(events_query.received_number, '0')::INTEGER AS received_number,
            COALESCE(events_query.pzq_received, '0')::INTEGER AS pzq_received,
            COALESCE(events_query.alb_received, '0')::INTEGER AS alb_received,
            COALESCE(events_query.mbz_received, '0')::INTEGER AS mbz_received,
            COALESCE(events_query.meb_received, '0')::INTEGER AS meb_received,
            COALESCE(events_query.vita_received, '0')::INTEGER AS vita_received,
            COALESCE(events_query.adminstered, '0')::INTEGER AS adminstered,
            COALESCE(events_query.pzq_administered, '0')::INTEGER AS pzq_administered,
            COALESCE(events_query.alb_administered, '0')::INTEGER AS alb_administered,
            COALESCE(events_query.mbz_administered, '0')::INTEGER AS mbz_administered,
            COALESCE(events_query.meb_administered, '0')::INTEGER AS meb_administered,
            COALESCE(events_query.vita_administered, '0')::INTEGER AS vita_administered,
            COALESCE(events_query.damaged, '0')::INTEGER AS damaged,
            COALESCE(events_query.pzq_damaged, '0')::INTEGER AS pzq_damaged,
            COALESCE(events_query.alb_damaged, '0')::INTEGER AS alb_damaged,
            COALESCE(events_query.mbz_damaged, '0')::INTEGER AS mbz_damaged,
            COALESCE(events_query.meb_damaged, '0')::INTEGER AS meb_damaged,
            COALESCE(events_query.vita_damaged, '0')::INTEGER AS vita_damaged,
            COALESCE(events_query.adverse, '0')::INTEGER AS adverse,
            COALESCE(events_query.pzq_adverse, '0')::INTEGER AS pzq_adverse,
            COALESCE(events_query.alb_adverse, '0')::INTEGER AS alb_adverse,
            COALESCE(events_query.mbz_adverse, '0')::INTEGER AS mbz_adverse,
            COALESCE(events_query.meb_adverse, '0')::INTEGER AS meb_adverse,
            COALESCE(events_query.vita_adverse, '0')::INTEGER AS vita_adverse,
            COALESCE(events_query.remaining_with_cdd, '0')::INTEGER AS remaining_with_cdd,
            COALESCE(events_query.pzq_remaining_with_cdd, '0')::INTEGER AS pzq_remaining_with_cdd,
            COALESCE(events_query.alb_remaining_with_cdd, '0')::INTEGER AS alb_remaining_with_cdd,
            COALESCE(events_query.mbz_remaining_with_cdd, '0')::INTEGER AS mbz_remaining_with_cdd,
            COALESCE(events_query.meb_remaining_with_cdd, '0')::INTEGER AS meb_remaining_with_cdd,
            COALESCE(events_query.vita_remaining_with_cdd, '0')::INTEGER AS vita_remaining_with_cdd,
            COALESCE(events_query.returned_to_supervisor, '0')::INTEGER AS returned_to_supervisor,
            COALESCE(events_query.pzq_returned_to_supervisor, '0')::INTEGER AS pzq_returned_to_supervisor,
            COALESCE(events_query.alb_returned_to_supervisor, '0')::INTEGER AS alb_returned_to_supervisor,
            COALESCE(events_query.mbz_returned_to_supervisor, '0')::INTEGER AS mbz_returned_to_supervisor,
            COALESCE(events_query.vita_returned_to_supervisor, '0')::INTEGER AS vita_returned_to_supervisor
--- HERE
        FROM locations
        JOIN mda_lite_operational_areas AS parents ON locations.jurisdiction_id = parents.jurisdiction_id
        LEFT JOIN LATERAL (
            SELECT
                events.base_entity_id,
                SUM(COALESCE((events.form_data -> 'health_education_above_16'::TEXT) ->> 0, '0'::TEXT)::bigint) AS health_education_above_16,
                SUM(COALESCE((events.form_data -> 'health_education_5_to_15'::TEXT) ->> 0, '0'::TEXT)::bigint) AS health_education_5_to_15,
                SUM(COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'ALB' THEN events.form_data ->> 'treated_male_1_to_4' END, '0')::INTEGER) AS alb_treated_male_1_4,
                SUM(COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'MBZ' THEN events.form_data ->> 'treated_male_1_to_4' END, '0')::INTEGER) AS mbz_treated_male_1_4,
                SUM(COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'MEB' THEN events.form_data ->> 'treated_male_1_to_4' END, '0')::INTEGER) AS meb_treated_male_1_4,
                SUM(COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'VITA' THEN events.form_data ->> 'treated_male_1_to_4' END, '0')::INTEGER) AS vita_treated_male_1_4,
                SUM(COALESCE (events.form_data->'treated_male_1_to_4'->> 0, '0')::INTEGER) AS treated_male_1_4,
                SUM(COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'VITA' THEN events.form_data ->> 'treated_male_6_to_11_mos' END, '0')::INTEGER) AS vita_treated_male_6_to_11_mos,
                SUM(COALESCE((events.form_data -> 'treated_male_6_to_11_mos'::TEXT) ->> 0, '0'::TEXT)::INTEGER) AS treated_male_6_to_11_mos,
                SUM(COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'PZQ' THEN events.form_data ->> 'treated_male_5_to_14' END, '0')::INTEGER) AS pzq_treated_male_5_14,
                SUM(COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'ALB' THEN events.form_data ->> 'treated_male_5_to_14' END, '0')::INTEGER) AS alb_treated_male_5_14,
                SUM(COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'MBZ' THEN events.form_data ->> 'treated_male_5_to_14' END, '0')::INTEGER) AS mbz_treated_male_5_14,
                SUM(COALESCE (events.form_data->'treated_male_5_to_14'->> 0, '0')::INTEGER) AS treated_male_5_14,
                SUM(COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'PZQ' THEN events.form_data ->> 'treated_female_5_to_15' END, '0')::INTEGER) AS pzq_treated_female_5_15,
                SUM(COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'ALB' THEN events.form_data ->> 'treated_female_5_to_15' END, '0')::INTEGER) AS alb_treated_female_5_15,
                SUM(COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'MEB' THEN events.form_data ->> 'treated_female_5_to_15' END, '0')::INTEGER) AS meb_treated_female_5_15,
                SUM(COALESCE((events.form_data -> 'treated_female_5_to_15'::TEXT) ->> 0, '0'::TEXT)::INTEGER) AS treated_female_5_15,
                SUM(COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'PZQ' THEN events.form_data ->> 'treated_male_5_to_15' END, '0')::INTEGER) AS pzq_treated_male_5_15,
                SUM(COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'ALB' THEN events.form_data ->> 'treated_male_5_to_15' END, '0')::INTEGER) AS alb_treated_male_5_15,
                SUM(COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'MEB' THEN events.form_data ->> 'treated_male_5_to_15' END, '0')::INTEGER) AS meb_treated_male_5_15,
                SUM(COALESCE((events.form_data -> 'treated_male_5_to_15'::TEXT) ->> 0, '0'::TEXT)::INTEGER) AS treated_male_5_15,
                SUM(COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'PZQ' THEN events.form_data ->> 'treated_male_above_15' END, '0')::INTEGER) AS pzq_treated_male_above_15,
                SUM(COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'ALB' THEN events.form_data ->> 'treated_male_above_15' END, '0')::INTEGER) AS alb_treated_male_above_15,
                SUM(COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'MBZ' THEN events.form_data ->> 'treated_male_above_15' END, '0')::INTEGER) AS mbz_treated_male_above_15,
                SUM(COALESCE (events.form_data->'treated_male_above_15'->> 0, '0')::INTEGER) AS treated_male_above_15,
                SUM(COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'PZQ' THEN events.form_data ->> 'treated_male_above_16' END, '0')::INTEGER) AS pzq_treated_male_above_16,
                SUM(COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'ALB' THEN events.form_data ->> 'treated_male_above_16' END, '0')::INTEGER) AS alb_treated_male_above_16,
                SUM(COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'MEB' THEN events.form_data ->> 'treated_male_above_16' END, '0')::INTEGER) AS meb_treated_male_above_16,
                SUM(COALESCE((events.form_data -> 'treated_male_above_16'::TEXT) ->> 0, '0'::TEXT)::INTEGER) AS treated_male_above_16,
                SUM(COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'ALB' THEN events.form_data ->> 'treated_female_1_to_4' END, '0')::INTEGER) AS alb_treated_female_1_4,
                SUM(COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'MBZ' THEN events.form_data ->> 'treated_female_1_to_4' END, '0')::INTEGER) AS mbz_treated_female_1_4,
                SUM(COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'MEB' THEN events.form_data ->> 'treated_female_1_to_4' END, '0')::INTEGER) AS meb_treated_female_1_4,
                SUM(COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'VITA' THEN events.form_data ->> 'treated_female_1_to_4' END, '0')::INTEGER) AS vita_treated_female_1_4,
                SUM(COALESCE (events.form_data->'treated_female_1_to_4'->> 0, '0')::INTEGER) AS treated_female_1_4,
                SUM(COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'VITA' THEN events.form_data ->> 'treated_female_6_to_11_mos' END, '0')::INTEGER) AS vita_treated_female_6_to_11_mos,
                SUM(COALESCE((events.form_data -> 'treated_female_6_to_11_mos'::TEXT) ->> 0, '0'::TEXT)::INTEGER) AS treated_female_6_to_11_mos,
                SUM(COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'PZQ' THEN events.form_data ->> 'treated_female_5_to_14' END, '0')::INTEGER) AS pzq_treated_female_5_14,
                SUM(COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'ALB' THEN events.form_data ->> 'treated_female_5_to_14' END, '0')::INTEGER) AS alb_treated_female_5_14,
                SUM(COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'MBZ' THEN events.form_data ->> 'treated_female_5_to_14' END, '0')::INTEGER) AS mbz_treated_female_5_14,
                SUM(COALESCE (events.form_data->'treated_female_5_to_14'->> 0, '0')::INTEGER) AS treated_female_5_14,
                SUM(COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'PZQ' THEN events.form_data ->> 'treated_female_above_15' END, '0')::INTEGER) AS pzq_treated_female_above_15,
                SUM(COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'ALB' THEN events.form_data ->> 'treated_female_above_15' END, '0')::INTEGER) AS alb_treated_female_above_15,
                SUM(COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'MBZ' THEN events.form_data ->> 'treated_female_above_15' END, '0')::INTEGER) AS mbz_treated_female_above_15,
                SUM(COALESCE (events.form_data->'treated_female_above_15'->> 0, '0')::INTEGER) AS treated_female_above_15,
                SUM(COALESCE((events.form_data -> 'treated_female_above_16'::TEXT) ->> 0, '0'::TEXT)::INTEGER) AS treated_female_above_16,
                SUM(COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'PZQ' THEN events.form_data ->> 'treated_female_above_16' END, '0')::INTEGER) AS pzq_treated_female_above_16,
                SUM(COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'ALB' THEN events.form_data ->> 'treated_female_above_16' END, '0')::INTEGER) AS alb_treated_female_above_16,
                SUM(COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'MEB' THEN events.form_data ->> 'treated_female_above_16' END, '0')::INTEGER) AS meb_treated_female_above_16,

                SUM(COALESCE (events.form_data->'treated_male_1_to_4'->> 0, '0')::INTEGER +
                    COALESCE (events.form_data->'treated_male_5_to_14'->> 0, '0')::INTEGER +
                    COALESCE (events.form_data->'treated_male_above_15'->> 0, '0')::INTEGER
                ) AS total_males,

                SUM(COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'VITA' THEN events.form_data ->> 'treated_male_1_to_4' END, '0')::INTEGER +
                    COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'VITA' THEN events.form_data ->> 'treated_male_6_to_11_mos' END, '0')::INTEGER
                ) AS vita_total_male,

                SUM(COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'VITA' THEN events.form_data ->> 'treated_female_1_to_4' END, '0')::INTEGER +
                    COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'VITA' THEN events.form_data ->> 'treated_female_6_to_11_mos' END, '0')::INTEGER
                ) AS vita_total_female,

                SUM(COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'VITA' THEN events.form_data ->> 'treated_female_1_to_4' END, '0')::INTEGER +
                    COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'VITA' THEN events.form_data ->> 'treated_female_6_to_11_mos' END, '0')::INTEGER +
                    COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'VITA' THEN events.form_data ->> 'treated_male_1_to_4' END, '0')::INTEGER +
                    COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'VITA' THEN events.form_data ->> 'treated_male_6_to_11_mos' END, '0')::INTEGER
                ) AS vita_total_treated,

                SUM(COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'VITA' THEN events.form_data ->> 'treated_male_6_to_11_mos' END, '0')::INTEGER +
                    COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'VITA' THEN events.form_data ->> 'treated_female_6_to_11_mos' END, '0')::INTEGER
                ) AS vita_total_treated_6_to_11_mos,

                SUM(COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'VITA' THEN events.form_data ->> 'treated_male_1_to_4' END, '0')::INTEGER +
                    COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'VITA' THEN events.form_data ->> 'treated_female_1_to_4' END, '0')::INTEGER
                ) AS vita_total_treated_1_4,

                SUM(COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'PZQ' THEN events.form_data ->> 'treated_male_5_to_14' END, '0')::INTEGER +
                    COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'PZQ' THEN events.form_data ->> 'treated_male_5_to_15' END, '0')::INTEGER +
                    COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'PZQ' THEN events.form_data ->> 'treated_male_above_15' END, '0')::INTEGER +
                    COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'PZQ' THEN events.form_data ->> 'treated_male_above_16' END, '0')::INTEGER
                ) AS pzq_total_male,

                SUM(COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'PZQ' THEN events.form_data ->> 'treated_female_5_to_14' END, '0')::INTEGER +
                    COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'PZQ' THEN events.form_data ->> 'treated_female_5_to_15' END, '0')::INTEGER +
                    COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'PZQ' THEN events.form_data ->> 'treated_female_above_15' END, '0')::INTEGER +
                    COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'PZQ' THEN events.form_data ->> 'treated_female_above_16' END, '0')::INTEGER
                ) AS pzq_total_female,

                SUM(COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'PZQ' THEN events.form_data ->> 'treated_male_5_to_14' END, '0')::INTEGER +
                    COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'PZQ' THEN events.form_data ->> 'treated_male_5_to_15' END, '0')::INTEGER +
                    COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'PZQ' THEN events.form_data ->> 'treated_male_above_15' END, '0')::INTEGER +
                    COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'PZQ' THEN events.form_data ->> 'treated_male_above_16' END, '0')::INTEGER +
                    COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'PZQ' THEN events.form_data ->> 'treated_female_5_to_14' END, '0')::INTEGER +
                    COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'PZQ' THEN events.form_data ->> 'treated_female_5_to_15' END, '0')::INTEGER +
                    COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'PZQ' THEN events.form_data ->> 'treated_female_above_15' END, '0')::INTEGER +
                    COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'PZQ' THEN events.form_data ->> 'treated_female_above_16' END, '0')::INTEGER
                ) AS pzq_total_treated,

                SUM(COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'PZQ' THEN events.form_data ->> 'treated_male_above_16' END, '0')::INTEGER +
                    COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'PZQ' THEN events.form_data ->> 'treated_female_above_16' END, '0')::INTEGER
                ) AS pzq_total_treated_above_16,

                SUM(COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'PZQ' THEN events.form_data ->> 'treated_female_5_to_15' END, '0')::INTEGER +
                    COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'PZQ' THEN events.form_data ->> 'treated_male_5_to_15' END, '0')::INTEGER
                ) AS pzq_total_treated_5_to_15,

                SUM(COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'ALB' THEN events.form_data ->> 'treated_male_1_to_4' END, '0')::INTEGER +
                    COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'ALB' THEN events.form_data ->> 'treated_male_5_to_14' END, '0')::INTEGER +
                    COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'ALB' THEN events.form_data ->> 'treated_male_5_to_15' END, '0')::INTEGER +
                    COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'ALB' THEN events.form_data ->> 'treated_male_above_16' END, '0')::INTEGER +
                    COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'ALB' THEN events.form_data ->> 'treated_male_above_15' END, '0')::INTEGER
                ) AS alb_total_male,

                SUM(COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'ALB' THEN events.form_data ->> 'treated_female_1_to_4' END, '0')::INTEGER +
                    COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'ALB' THEN events.form_data ->> 'treated_female_5_to_14' END, '0')::INTEGER +
                    COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'ALB' THEN events.form_data ->> 'treated_female_5_to_15' END, '0')::INTEGER +
                    COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'ALB' THEN events.form_data ->> 'treated_female_above_16' END, '0')::INTEGER +
                    COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'ALB' THEN events.form_data ->> 'treated_female_above_15' END, '0')::INTEGER
                ) AS alb_total_female,

                SUM(COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'ALB' THEN events.form_data ->> 'treated_male_1_to_4' END, '0')::INTEGER +
                    COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'ALB' THEN events.form_data ->> 'treated_male_5_to_14' END, '0')::INTEGER +
                    COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'ALB' THEN events.form_data ->> 'treated_male_5_to_15' END, '0')::INTEGER +
                    COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'ALB' THEN events.form_data ->> 'treated_male_above_16' END, '0')::INTEGER +
                    COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'ALB' THEN events.form_data ->> 'treated_male_above_15' END, '0')::INTEGER +
                    COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'ALB' THEN events.form_data ->> 'treated_female_1_to_4' END, '0')::INTEGER +
                    COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'ALB' THEN events.form_data ->> 'treated_female_5_to_14' END, '0')::INTEGER +
                    COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'ALB' THEN events.form_data ->> 'treated_female_5_to_15' END, '0')::INTEGER +
                    COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'ALB' THEN events.form_data ->> 'treated_female_above_16' END, '0')::INTEGER +
                    COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'ALB' THEN events.form_data ->> 'treated_female_above_15' END, '0')::INTEGER
                ) AS alb_total_treated,

                SUM(COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'MEB' THEN events.form_data ->> 'treated_male_1_to_4' END, '0')::INTEGER +
                    COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'MEB' THEN events.form_data ->> 'treated_male_5_to_14' END, '0')::INTEGER +
                    COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'MEB' THEN events.form_data ->> 'treated_male_5_to_15' END, '0')::INTEGER +
                    COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'MEB' THEN events.form_data ->> 'treated_male_above_16' END, '0')::INTEGER +
                    COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'MEB' THEN events.form_data ->> 'treated_male_above_15' END, '0')::INTEGER +
                    COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'MBZ' THEN events.form_data ->> 'treated_male_1_to_4' END, '0')::INTEGER +
                    COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'MBZ' THEN events.form_data ->> 'treated_male_5_to_14' END, '0')::INTEGER +
                    COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'MBZ' THEN events.form_data ->> 'treated_male_5_to_15' END, '0')::INTEGER +
                    COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'MBZ' THEN events.form_data ->> 'treated_male_above_16' END, '0')::INTEGER +
                    COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'MBZ' THEN events.form_data ->> 'treated_male_above_15' END, '0')::INTEGER
                ) AS meb_total_male,

                SUM(COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'MEB' THEN events.form_data ->> 'treated_female_1_to_4' END, '0')::INTEGER +
                    COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'MEB' THEN events.form_data ->> 'treated_female_5_to_14' END, '0')::INTEGER +
                    COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'MEB' THEN events.form_data ->> 'treated_female_5_to_15' END, '0')::INTEGER +
                    COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'MEB' THEN events.form_data ->> 'treated_female_above_16' END, '0')::INTEGER +
                    COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'MEB' THEN events.form_data ->> 'treated_female_above_15' END, '0')::INTEGER +
                    COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'MBZ' THEN events.form_data ->> 'treated_female_1_to_4' END, '0')::INTEGER +
                    COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'MBZ' THEN events.form_data ->> 'treated_female_5_to_14' END, '0')::INTEGER +
                    COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'MBZ' THEN events.form_data ->> 'treated_female_5_to_15' END, '0')::INTEGER +
                    COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'MBZ' THEN events.form_data ->> 'treated_female_above_16' END, '0')::INTEGER +
                    COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'MBZ' THEN events.form_data ->> 'treated_female_above_15' END, '0')::INTEGER
                ) AS meb_total_female,

                SUM(COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'MEB' THEN events.form_data ->> 'treated_male_1_to_4' END, '0')::INTEGER +
                    COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'MEB' THEN events.form_data ->> 'treated_male_5_to_14' END, '0')::INTEGER +
                    COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'MEB' THEN events.form_data ->> 'treated_male_5_to_15' END, '0')::INTEGER +
                    COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'MEB' THEN events.form_data ->> 'treated_male_above_16' END, '0')::INTEGER +
                    COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'MEB' THEN events.form_data ->> 'treated_male_above_15' END, '0')::INTEGER +
                    COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'MEB' THEN events.form_data ->> 'treated_female_1_to_4' END, '0')::INTEGER +
                    COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'MEB' THEN events.form_data ->> 'treated_female_5_to_14' END, '0')::INTEGER +
                    COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'MEB' THEN events.form_data ->> 'treated_female_5_to_15' END, '0')::INTEGER +
                    COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'MEB' THEN events.form_data ->> 'treated_female_above_16' END, '0')::INTEGER +
                    COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'MEB' THEN events.form_data ->> 'treated_female_above_15' END, '0')::INTEGER
                ) AS meb_total_treated,

                SUM(COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'ALB' THEN events.form_data ->> 'treated_male_1_to_4' END, '0')::INTEGER +
                    COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'ALB' THEN events.form_data ->> 'treated_male_5_to_14' END, '0')::INTEGER +
                    COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'ALB' THEN events.form_data ->> 'treated_male_5_to_15' END, '0')::INTEGER +
                    COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'ALB' THEN events.form_data ->> 'treated_male_above_16' END, '0')::INTEGER +
                    COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'ALB' THEN events.form_data ->> 'treated_male_above_15' END, '0')::INTEGER +
                    COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'ALB' THEN events.form_data ->> 'treated_female_1_to_4' END, '0')::INTEGER +
                    COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'ALB' THEN events.form_data ->> 'treated_female_5_to_14' END, '0')::INTEGER +
                    COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'ALB' THEN events.form_data ->> 'treated_female_5_to_15' END, '0')::INTEGER +
                    COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'ALB' THEN events.form_data ->> 'treated_female_above_16' END, '0')::INTEGER +
                    COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'ALB' THEN events.form_data ->> 'treated_female_above_15' END, '0')::INTEGER +
                    COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'MEB' THEN events.form_data ->> 'treated_male_1_to_4' END, '0')::INTEGER +
                    COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'MEB' THEN events.form_data ->> 'treated_male_5_to_14' END, '0')::INTEGER +
                    COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'MEB' THEN events.form_data ->> 'treated_male_5_to_15' END, '0')::INTEGER +
                    COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'MEB' THEN events.form_data ->> 'treated_male_above_16' END, '0')::INTEGER +
                    COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'MEB' THEN events.form_data ->> 'treated_male_above_15' END, '0')::INTEGER +
                    COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'MEB' THEN events.form_data ->> 'treated_female_1_to_4' END, '0')::INTEGER +
                    COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'MEB' THEN events.form_data ->> 'treated_female_5_to_14' END, '0')::INTEGER +
                    COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'MEB' THEN events.form_data ->> 'treated_female_5_to_15' END, '0')::INTEGER +
                    COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'MEB' THEN events.form_data ->> 'treated_female_above_16' END, '0')::INTEGER +
                    COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'MBZ' THEN events.form_data ->> 'treated_male_1_to_4' END, '0')::INTEGER +
                    COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'MBZ' THEN events.form_data ->> 'treated_male_5_to_14' END, '0')::INTEGER +
                    COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'MBZ' THEN events.form_data ->> 'treated_male_5_to_15' END, '0')::INTEGER +
                    COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'MBZ' THEN events.form_data ->> 'treated_male_above_16' END, '0')::INTEGER +
                    COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'MBZ' THEN events.form_data ->> 'treated_male_above_15' END, '0')::INTEGER +
                    COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'MBZ' THEN events.form_data ->> 'treated_female_1_to_4' END, '0')::INTEGER +
                    COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'MBZ' THEN events.form_data ->> 'treated_female_5_to_14' END, '0')::INTEGER +
                    COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'MBZ' THEN events.form_data ->> 'treated_female_5_to_15' END, '0')::INTEGER +
                    COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'MBZ' THEN events.form_data ->> 'treated_female_above_16' END, '0')::INTEGER +
                    COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'MBZ' THEN events.form_data ->> 'treated_female_above_15' END, '0')::INTEGER
                ) AS alb_meb_total_treated,

                SUM(COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'ALB' THEN events.form_data ->> 'treated_male_1_to_4' END, '0')::INTEGER +
                    COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'MEB' THEN events.form_data ->> 'treated_male_1_to_4' END, '0')::INTEGER +
                    COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'ALB' THEN events.form_data ->> 'treated_female_1_to_4' END, '0')::INTEGER +
                    COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'MEB' THEN events.form_data ->> 'treated_female_1_to_4' END, '0')::INTEGER
                ) AS alb_meb_total_treated_1_4,

                SUM(COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'ALB' THEN events.form_data ->> 'treated_male_5_to_15' END, '0')::INTEGER +
                    COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'ALB' THEN events.form_data ->> 'treated_female_5_to_15' END, '0')::INTEGER +
                    COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'MEB' THEN events.form_data ->> 'treated_male_5_to_15' END, '0')::INTEGER +
                    COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'MEB' THEN events.form_data ->> 'treated_female_5_to_15' END, '0')::INTEGER
                ) AS alb_meb_total_treated_5_15,

                SUM(COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'MEB' THEN events.form_data ->> 'treated_female_above_16' END, '0')::INTEGER +
                    COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'ALB' THEN events.form_data ->> 'treated_female_above_16' END, '0')::INTEGER +
                    COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'MEB' THEN events.form_data ->> 'treated_male_above_16' END, '0')::INTEGER +
                    COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'ALB' THEN events.form_data ->> 'treated_male_above_16' END, '0')::INTEGER
                ) AS alb_meb_total_treated_above_16,

                SUM(COALESCE (events.form_data->'treated_female_1_to_4'->> 0, '0')::INTEGER +
                    COALESCE (events.form_data->'treated_female_5_to_14'->> 0, '0')::INTEGER +
                    COALESCE (events.form_data->'treated_female_above_15'->> 0, '0')::INTEGER
                ) AS total_females,

                SUM(COALESCE (events.form_data->'treated_male_1_to_4'->> 0, '0')::INTEGER +
                    COALESCE (events.form_data->'treated_male_5_to_14'->> 0, '0')::INTEGER +
                    COALESCE (events.form_data->'treated_male_above_15'->> 0, '0')::INTEGER +
                    COALESCE (events.form_data->'treated_female_1_to_4'->> 0, '0')::INTEGER +
                    COALESCE (events.form_data->'treated_female_5_to_14'->> 0, '0')::INTEGER +
                    COALESCE (events.form_data->'treated_female_above_15'->> 0, '0')::INTEGER
                ) AS total_all_genders,

                SUM(COALESCE (events.form_data->'sum_pzq_received_and_top_up'->> 0, '0')::INTEGER +
                    COALESCE (events.form_data->'sum_alb_received_and_top_up'->> 0, '0')::INTEGER +
                    COALESCE (events.form_data->'sum_mbz_received_and_top_up'->> 0, '0')::INTEGER
                ) AS supervisor_distributed,

                SUM(COALESCE((events.form_data -> 'sum_pzq_received_and_top_up'::TEXT) ->> 0, '0'::TEXT)::INTEGER) AS pzq_supervisor_distributed,
                SUM(COALESCE((events.form_data -> 'sum_alb_received_and_top_up'::TEXT) ->> 0, '0'::TEXT)::INTEGER) AS alb_supervisor_distributed,
                SUM(COALESCE((events.form_data -> 'sum_mbz_received_and_top_up'::TEXT) ->> 0, '0'::TEXT)::INTEGER) AS mbz_supervisor_distributed,
                SUM(COALESCE((events.form_data -> 'sum_meb_received_and_top_up'::TEXT) ->> 0, '0'::TEXT)::INTEGER) AS meb_supervisor_distributed,
                SUM(COALESCE((events.form_data -> 'sum_vita_received_and_top_up'::TEXT) ->> 0, '0'::TEXT)::INTEGER) AS vita_supervisor_distributed,
                SUM(COALESCE (events.form_data->'received_number'->> 0, '0')::INTEGER) AS received_number,
                SUM(COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'PZQ' THEN events.form_data ->> 'received_number' END, '0')::INTEGER) AS pzq_received,
                SUM(COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'ALB' THEN events.form_data ->> 'received_number' END, '0')::INTEGER) AS alb_received,
                SUM(COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'MBZ' THEN events.form_data ->> 'received_number' END, '0')::INTEGER) AS mbz_received,
                SUM(COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'MEB' THEN events.form_data ->> 'received_number' END, '0')::INTEGER) AS meb_received,
                SUM(COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'VITA' THEN events.form_data ->> 'received_number' END, '0')::INTEGER) AS vita_received,
                SUM(COALESCE (events.form_data->'adminstered'->> 0, '0')::INTEGER) AS adminstered,
                SUM(COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'PZQ' THEN events.form_data ->> 'adminstered' END, '0')::INTEGER) AS pzq_administered,
                SUM(COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'ALB' THEN events.form_data ->> 'adminstered' END, '0')::INTEGER) AS alb_administered,
                SUM(COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'MBZ' THEN events.form_data ->> 'adminstered' END, '0')::INTEGER) AS mbz_administered,
                SUM(COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'MEB' THEN events.form_data ->> 'adminstered' END, '0')::INTEGER) AS meb_administered,
                SUM(COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'VITA' THEN events.form_data ->> 'adminstered' END, '0')::INTEGER) AS vita_administered,
                SUM(COALESCE (events.form_data->'damaged'->> 0, '0')::INTEGER) AS damaged,
                SUM(COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'PZQ' THEN events.form_data ->> 'damaged' END, '0')::INTEGER) AS pzq_damaged,
                SUM(COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'ALB' THEN events.form_data ->> 'damaged' END, '0')::INTEGER) AS alb_damaged,
                SUM(COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'MBZ' THEN events.form_data ->> 'damaged' END, '0')::INTEGER) AS mbz_damaged,
                SUM(COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'MEB' THEN events.form_data ->> 'damaged' END, '0')::INTEGER) AS meb_damaged,
                SUM(COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'VITA' THEN events.form_data ->> 'damaged' END, '0')::INTEGER) AS vita_damaged,
                SUM(COALESCE(events.form_data->'adverse'->> 0, '0')::INTEGER) AS adverse,
                SUM(COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'PZQ' THEN events.form_data ->> 'adverse' END, '0')::INTEGER) AS pzq_adverse,
                SUM(COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'ALB' THEN events.form_data ->> 'adverse' END, '0')::INTEGER) AS alb_adverse,
                SUM(COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'MBZ' THEN events.form_data ->> 'adverse' END, '0')::INTEGER) AS mbz_adverse,
                SUM(COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'MEB' THEN events.form_data ->> 'adverse' END, '0')::INTEGER) AS meb_adverse,
                SUM(COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'VITA' THEN events.form_data ->> 'adverse' END, '0')::INTEGER) AS vita_adverse,
                SUM(COALESCE (events.form_data->'received_number'->> 0, '0')::INTEGER - (COALESCE (events.form_data->'adminstered'->> 0, '0')::INTEGER +
                COALESCE (events.form_data->'damaged'->> 0, '0')::INTEGER)) AS remaining_with_cdd,

                SUM(COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'PZQ' THEN
                    COALESCE((events.form_data -> 'received_number'::TEXT) ->> 0, '0'::TEXT)::INTEGER -
                    (COALESCE((events.form_data -> 'adminstered'::TEXT) ->> 0, '0'::TEXT)::INTEGER +
                    COALESCE((events.form_data -> 'damaged'::TEXT) ->> 0, '0'::TEXT)::INTEGER) END, '0')::INTEGER
                ) AS pzq_remaining_with_cdd,

                SUM(COALESCE(CASE WHEN events.form_data ->> 'drugs' = 'ALB' THEN
                    COALESCE((events.form_data -> 'received_number'::TEXT) ->> 0, '0'::TEXT)::INTEGER -
                    (COALESCE((events.form_data -> 'adminstered'::TEXT) ->> 0, '0'::TEXT)::INTEGER +
                    COALESCE((events.form_data -> 'damaged'::TEXT) ->> 0, '0'::TEXT)::INTEGER) END, '0')::INTEGER
                ) AS alb_remaining_with_cdd,

                SUM(COALESCE (CASE WHEN events.form_data ->> 'drugs' = 'MBZ' THEN
                    COALESCE((events.form_data -> 'received_number'::TEXT) ->> 0, '0'::TEXT)::INTEGER -
                    (COALESCE((events.form_data -> 'adminstered'::TEXT) ->> 0, '0'::TEXT)::INTEGER +
                    COALESCE((events.form_data -> 'damaged'::TEXT) ->> 0, '0'::TEXT)::INTEGER) END, '0')::INTEGER
                ) AS mbz_remaining_with_cdd,

                SUM(COALESCE (CASE WHEN events.form_data ->> 'drugs' = 'MEB' THEN
                    COALESCE((events.form_data -> 'received_number'::TEXT) ->> 0, '0'::TEXT)::INTEGER -
                    (COALESCE((events.form_data -> 'adminstered'::TEXT) ->> 0, '0'::TEXT)::INTEGER +
                    COALESCE((events.form_data -> 'damaged'::TEXT) ->> 0, '0'::TEXT)::INTEGER) END, '0')::INTEGER
                ) AS meb_remaining_with_cdd,

                SUM(COALESCE (CASE WHEN events.form_data ->> 'drugs' = 'VITA' THEN
                    COALESCE((events.form_data -> 'received_number'::TEXT) ->> 0, '0'::TEXT)::INTEGER -
                    (COALESCE((events.form_data -> 'adminstered'::TEXT) ->> 0, '0'::TEXT)::INTEGER +
                    COALESCE((events.form_data -> 'damaged'::TEXT) ->> 0, '0'::TEXT)::INTEGER) END, '0')::INTEGER
                ) AS vita_remaining_with_cdd,

                SUM(COALESCE (events.form_data->'pzq_returned'->> 0, '0')::INTEGER +
                    COALESCE (events.form_data->'albendazole_returned'->> 0, '0')::INTEGER +
                    COALESCE (events.form_data->'mebendazole_returned'->> 0, '0')::INTEGER
                ) AS returned_to_supervisor,
                SUM(COALESCE((events.form_data -> 'pzq_returned'::TEXT) ->> 0, '0'::TEXT)::INTEGER) AS pzq_returned_to_supervisor,
                SUM(COALESCE((events.form_data -> 'albendazole_returned'::TEXT) ->> 0, '0'::TEXT)::INTEGER) AS alb_returned_to_supervisor,
                SUM(COALESCE((events.form_data -> 'mebendazole_returned'::TEXT) ->> 0, '0'::TEXT)::INTEGER) AS mbz_returned_to_supervisor,
                SUM(COALESCE((events.form_data -> 'vita_returned'::TEXT) ->> 0, '0'::TEXT)::INTEGER) vita_returned_to_supervisor
            FROM events
            WHERE events.base_entity_id = locations.id
            AND events.event_type IN ('tablet_accountability', 'cdd_supervisor_daily_summary','cell_coordinator_daily_summary')
            AND events.entity_type = 'Structure'
            AND parents.plan_id = events.plan_id
            GROUP BY events.base_entity_id, locations.name, parents.jurisdiction_id, parents.plan_id
        ) events_query ON TRUE
    ) AS subq
LEFT JOIN mda_lite_wards_population AS wards_population ON wards_population.ward_id = subq.base_entity_id;

CREATE INDEX IF NOT EXISTS mda_lite_wards_base_entity_id_idx ON mda_lite_wards (base_entity_id);
CREATE INDEX IF NOT EXISTS mda_lite_wards_plan_id_idx ON mda_lite_wards (plan_id);
CREATE INDEX IF NOT EXISTS mda_lite_wards_parent_id_idx ON mda_lite_wards (parent_id);
CREATE UNIQUE INDEX IF NOT EXISTS mda_lite_wards_id_idx ON mda_lite_wards (id);


