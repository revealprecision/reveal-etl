SET schema 'reveal';

DROP MATERIALIZED VIEW IF EXISTS mda_lite_jurisdictions CASCADE;

CREATE MATERIALIZED VIEW IF NOT EXISTS mda_lite_jurisdictions
AS
SELECT DISTINCT ON (jurisdictions_query.jurisdiction_id, plans.plan_id)
    public.uuid_generate_v5(
    '6ba7b810-9dad-11d1-80b4-00c04fd430c8',
    concat(jurisdictions_query.jurisdiction_id, plans.plan_id)) AS id,
    plans.plan_id as plan_id,
    jurisdictions_query.*,
    COALESCE(ward_population.official_population, 0) AS official_population,
    COALESCE(ward_population.other_pop_target, 0) AS other_pop_target,
    COALESCE(ward_population.census_target_population_6_to_59_mos_official, 0) AS census_target_population_6_to_59_mos_official,
    COALESCE(ward_population.census_target_population_6_to_11_mos_official, 0) AS census_target_population_6_to_11_mos_official,
    COALESCE(ward_population.census_target_population_12_to_59_mos_official, 0) AS census_target_population_12_to_59_mos_official,
    COALESCE(ward_population.other_pop_target_6_to_59_mos_trusted, 0) AS other_pop_target_6_to_59_mos_trusted,
    COALESCE(ward_population.other_pop_target_6_to_11_mos_trusted, 0) AS other_pop_target_6_to_11_mos_trusted,
    COALESCE(ward_population.other_pop_target_12_to_59_mos_trusted, 0) AS other_pop_target_12_to_59_mos_trusted,
    COALESCE(ward_population.census_pop_target_above_16_official, 0) AS census_pop_target_above_16_official,
    COALESCE(ward_population.other_pop_target_above_16_trusted, 0) AS other_pop_target_above_16_trusted,
    COALESCE(ward_population.census_pop_target_5_to_15_official, 0) AS census_pop_target_5_to_15_official,
    COALESCE(ward_population.other_pop_target_5_to_15_trusted, 0) AS other_pop_target_5_to_15_trusted,
    (COALESCE(ward_population.census_pop_target_5_to_15_official, 0) + COALESCE(ward_population.census_pop_target_above_16_official, 0)) AS census_pop_target_5_to_15_and_above_16_official,
    CASE
    WHEN (COALESCE(ward_population.census_pop_target_5_to_15_official, 0) + COALESCE(ward_population.census_pop_target_above_16_official, 0)) = 0 THEN 0
    ELSE CAST(jurisdictions_query.pzq_total_treated as DECIMAL) / CAST((COALESCE(ward_population.census_pop_target_5_to_15_official, 0) + COALESCE(ward_population.census_pop_target_above_16_official, 0)) as DECIMAL)
    END AS pzq_5_years_and_above_16_treatment_coverage,
    CASE
    WHEN COALESCE(ward_population.census_target_population_6_to_59_mos_official, 0) = 0 THEN 0
    ELSE CAST(jurisdictions_query.vita_total_treated AS DECIMAL)/CAST(ward_population.census_target_population_6_to_59_mos_official AS DECIMAL)
    END as vita_6_to_59_mos_treatment_coverage,
    CASE
    WHEN COALESCE(ward_population.official_population, 0) = 0 THEN 0
    ELSE CAST(jurisdictions_query.total_all_genders AS DECIMAL)/CAST(ward_population.official_population AS DECIMAL)
    END as treatment_coverage,
    CASE
        WHEN COALESCE(ward_population.official_population, 0) = 0 THEN 0
        ELSE CAST(jurisdictions_query.pzq_total_treated as DECIMAL) / CAST(ward_population.official_population as DECIMAL)
        END AS pzq_treatment_coverage,
    CASE
        WHEN COALESCE(ward_population.official_population, 0) = 0 THEN 0
        ELSE CAST(jurisdictions_query.alb_meb_total_treated as DECIMAL) / CAST(ward_population.official_population as DECIMAL)
        END AS alb_mbz_treatment_coverage,
    CASE
    WHEN COALESCE(ward_population.other_pop_target, 0) = 0 THEN 0
    ELSE CAST(jurisdictions_query.total_all_genders AS DECIMAL)/CAST(ward_population.other_pop_target AS DECIMAL)
    END as other_pop_coverage,
    CASE
        WHEN COALESCE(ward_population.other_pop_target, 0) = 0 THEN 0
        ELSE CAST(jurisdictions_query.pzq_total_treated as DECIMAL) / CAST(ward_population.other_pop_target as DECIMAL)
        END AS pzq_other_pop_coverage,
    CASE
        WHEN COALESCE(ward_population.other_pop_target, 0) = 0 THEN 0
        ELSE CAST(jurisdictions_query.alb_meb_total_treated as DECIMAL) / CAST(ward_population.other_pop_target as DECIMAL)
        END AS alb_mbz_other_pop_coverage,
    CASE
    WHEN COALESCE(ward_population.other_pop_target_6_to_59_mos_trusted, 0) = 0 THEN 0
    ELSE CAST(jurisdictions_query.vita_total_treated AS DECIMAL)/CAST(ward_population.other_pop_target_6_to_59_mos_trusted AS DECIMAL)
    END as vita_6_to_59_mos_other_pop_coverage,
    CASE
        WHEN COALESCE(ward_population.other_pop_target_6_to_11_mos_trusted, 0) = 0 THEN 0
        ELSE CAST(jurisdictions_query.vita_total_treated_6_to_11_mos AS DECIMAL)/CAST(ward_population.other_pop_target_6_to_11_mos_trusted AS DECIMAL)
        END as vita_6_to_11_mos_other_pop_coverage,
    CASE
    WHEN COALESCE(ward_population.census_target_population_6_to_11_mos_official, 0) = 0 THEN 0
    ELSE CAST(jurisdictions_query.vita_total_treated_6_to_11_mos AS DECIMAL)/CAST(ward_population.census_target_population_6_to_11_mos_official AS DECIMAL)
    END as vita_6_to_11_mos_treatment_coverage,
    CASE
        WHEN COALESCE(ward_population.census_target_population_12_to_59_mos_official, 0) = 0 THEN 0
        ELSE CAST(jurisdictions_query.vita_total_treated_1_4 AS DECIMAL)/CAST(ward_population.census_target_population_12_to_59_mos_official AS DECIMAL)
        END as vita_treatment_coverage_1_to_4,
    CASE
        WHEN COALESCE(ward_population.other_pop_target_12_to_59_mos_trusted, 0) = 0 THEN 0
        ELSE CAST(jurisdictions_query.vita_total_treated_1_4 AS DECIMAL)/CAST(ward_population.other_pop_target_12_to_59_mos_trusted AS DECIMAL)
        END as vita_1_to_4_years_other_pop_coverage,
    CASE
        WHEN COALESCE(ward_population.census_pop_target_above_16_official, 0) = 0 THEN 0
        ELSE CAST(jurisdictions_query.pzq_total_treated_above_16 AS DECIMAL)/CAST(ward_population.census_pop_target_above_16_official AS DECIMAL)
        END as pzq_above_16_years_treatment_coverage,
    CASE
        WHEN COALESCE(ward_population.other_pop_target_above_16_trusted, 0) = 0 THEN 0
        ELSE CAST(jurisdictions_query.pzq_total_treated_above_16 AS DECIMAL)/CAST(ward_population.other_pop_target_above_16_trusted AS DECIMAL)
    END as pzq_above_16_years_other_pop_coverage,
    CASE
        WHEN COALESCE(ward_population.census_pop_target_5_to_15_official, 0) = 0 THEN 0
        ELSE CAST(jurisdictions_query.pzq_total_treated_5_to_15 AS DECIMAL)/CAST(ward_population.census_pop_target_5_to_15_official AS DECIMAL)
        END as pzq_5_to_15_years_treatment_coverage,
    CASE
        WHEN COALESCE(ward_population.other_pop_target_5_to_15_trusted, 0) = 0 THEN 0
        ELSE CAST(jurisdictions_query.pzq_total_treated_5_to_15 AS DECIMAL)/CAST(ward_population.other_pop_target_5_to_15_trusted AS DECIMAL)
    END as pzq_5_to_15_years_other_pop_coverage,
    CASE
         WHEN (COALESCE(ward_population.other_pop_target_5_to_15_trusted, 0) + COALESCE(ward_population.other_pop_target_above_16_trusted, 0)) = 0 THEN 0
         ELSE CAST(jurisdictions_query.pzq_total_treated as DECIMAL) / CAST((COALESCE(ward_population.other_pop_target_5_to_15_trusted, 0) + COALESCE(ward_population.other_pop_target_above_16_trusted, 0)) as DECIMAL)
    END AS pzq_5_years_above_16_other_pop_coverage,
     (COALESCE(ward_population.other_pop_target_5_to_15_trusted, 0) + COALESCE(ward_population.other_pop_target_above_16_trusted, 0)) AS other_pop_target_5_to_15_and_above_16_trusted,
    CASE
        WHEN COALESCE(ward_population.census_target_population_12_to_59_mos_official, 0) = 0 THEN 0
        ELSE CAST(jurisdictions_query.alb_meb_total_treated_1_4 as DECIMAL) / CAST(ward_population.census_target_population_12_to_59_mos_official as DECIMAL)
        END AS alb_meb_treatment_coverage_1_to_4,
    CASE
        WHEN COALESCE(ward_population.other_pop_target_12_to_59_mos_trusted, 0) = 0 THEN 0
        ELSE CAST(jurisdictions_query.alb_meb_total_treated_1_4 as DECIMAL) / CAST(ward_population.other_pop_target_12_to_59_mos_trusted as DECIMAL)
        END AS alb_meb_1_to_4_years_other_pop_coverage,
    CASE
        WHEN COALESCE(ward_population.census_pop_target_5_to_15_official, 0) = 0 THEN 0
        ELSE CAST(jurisdictions_query.alb_meb_total_treated_5_15 as DECIMAL) / CAST(ward_population.census_pop_target_5_to_15_official as DECIMAL)
        END AS alb_meb_5_to_15_years_treatment_coverage,
    CASE
        WHEN COALESCE(ward_population.other_pop_target_5_to_15_trusted, 0) = 0 THEN 0
        ELSE CAST(jurisdictions_query.alb_meb_total_treated_5_15 as DECIMAL) / CAST(ward_population.other_pop_target_5_to_15_trusted as DECIMAL)
        END AS alb_meb_5_to_15_years_other_pop_coverage,
    CASE
        WHEN COALESCE(ward_population.other_pop_target_above_16_trusted, 0) = 0 THEN 0
        ELSE CAST(jurisdictions_query.alb_meb_total_treated_above_16 as DECIMAL) / CAST(ward_population.other_pop_target_above_16_trusted as DECIMAL)
        END AS alb_meb_above_16_pop_coverage,
    CASE
        WHEN COALESCE(ward_population.census_pop_target_above_16_official, 0) = 0 THEN 0
        ELSE CAST(jurisdictions_query.alb_meb_total_treated_above_16 as DECIMAL) / CAST(ward_population.census_pop_target_above_16_official as DECIMAL)
        END AS alb_meb_above_16_treatment_coverage
FROM mda_lite_plans as plans
LEFT JOIN LATERAL (
    SELECT
      lite_jurisdictions.*
    FROM mda_lite_operational_areas as jurisdictions
    LEFT JOIN LATERAL (
      SELECT
          jurisdiction_id AS jurisdiction_id,
          COALESCE(jurisdiction_parent_id, '') AS jurisdiction_parent_id,
          jurisdiction_name AS jurisdiction_name,
          jurisdiction_depth AS jurisdiction_depth,
          jurisdiction_path AS jurisdiction_path,
          jurisdiction_name_path AS jurisdiction_name_path,
          op_areas.*
      FROM jurisdictions_tree
      LEFT JOIN LATERAL (
        SELECT
            COALESCE(sum(health_education_5_to_15), 0) AS health_education_5_to_15,
            COALESCE(sum(health_education_above_16), 0) AS health_education_above_16,
            COALESCE(sum(alb_treated_male_1_4), 0) AS alb_treated_male_1_4,
            COALESCE(sum(mbz_treated_male_1_4), 0) AS mbz_treated_male_1_4,
            COALESCE(sum(meb_treated_male_1_4), 0) AS meb_treated_male_1_4,
            COALESCE(sum(vita_treated_male_1_4), 0) AS vita_treated_male_1_4,
            COALESCE(sum(treated_male_1_4), 0) AS treated_male_1_4,
            COALESCE(sum(treated_male_6_to_11_mos), 0) AS treated_male_6_to_11_mos,
            COALESCE(sum(vita_treated_male_6_to_11_mos), 0) AS vita_treated_male_6_to_11_mos,
            COALESCE(sum(pzq_treated_male_5_14), 0) AS pzq_treated_male_5_14,
            COALESCE(sum(alb_treated_male_5_14), 0) AS alb_treated_male_5_14,
            COALESCE(sum(mbz_treated_male_5_14), 0) AS mbz_treated_male_5_14,
            COALESCE(sum(treated_male_5_14), 0) AS treated_male_5_14,
            COALESCE(sum(pzq_treated_male_5_15), 0) AS pzq_treated_male_5_15,
            COALESCE(sum(alb_treated_male_5_15), 0) AS alb_treated_male_5_15,
            COALESCE(sum(meb_treated_male_5_15), 0) AS meb_treated_male_5_15,
            COALESCE(sum(treated_male_5_15), 0) AS treated_male_5_15,
            COALESCE(sum(pzq_treated_male_above_15), 0) AS pzq_treated_male_above_15,
            COALESCE(sum(alb_treated_male_above_15), 0) AS alb_treated_male_above_15,
            COALESCE(sum(mbz_treated_male_above_15), 0) AS mbz_treated_male_above_15,
            COALESCE(sum(treated_male_above_15), 0) AS treated_male_above_15,
            COALESCE(sum(pzq_treated_male_above_16), 0) AS pzq_treated_male_above_16,
            COALESCE(sum(alb_treated_male_above_16), 0) AS alb_treated_male_above_16,
            COALESCE(sum(meb_treated_male_above_16), 0) AS meb_treated_male_above_16,
            COALESCE(sum(treated_male_above_16), 0) AS treated_male_above_16,
            COALESCE(sum(alb_treated_female_1_4), 0) AS alb_treated_female_1_4,
            COALESCE(sum(mbz_treated_female_1_4), 0) AS mbz_treated_female_1_4,
            COALESCE(sum(meb_treated_female_1_4), 0) AS meb_treated_female_1_4,
            COALESCE(sum(vita_treated_female_1_4), 0) AS vita_treated_female_1_4,
            COALESCE(sum(treated_female_1_4), 0) AS treated_female_1_4,
            COALESCE(sum(treated_female_6_to_11_mos), 0) AS treated_female_6_to_11_mos,
            COALESCE(sum(vita_treated_female_6_to_11_mos), 0) AS vita_treated_female_6_to_11_mos,
            COALESCE(sum(pzq_treated_female_5_14), 0) AS pzq_treated_female_5_14,
            COALESCE(sum(alb_treated_female_5_14), 0) AS alb_treated_female_5_14,
            COALESCE(sum(mbz_treated_female_5_14), 0) AS mbz_treated_female_5_14,
            COALESCE(sum(treated_female_5_14), 0) AS treated_female_5_14,
            COALESCE(sum(pzq_treated_female_5_15), 0) AS pzq_treated_female_5_15,
            COALESCE(sum(alb_treated_female_5_15), 0) AS alb_treated_female_5_15,
            COALESCE(sum(meb_treated_female_5_15), 0) AS meb_treated_female_5_15,
            COALESCE(sum(treated_female_5_15), 0) AS treated_female_5_15,
            COALESCE(sum(pzq_treated_female_above_15), 0) AS pzq_treated_female_above_15,
            COALESCE(sum(alb_treated_female_above_15), 0) AS alb_treated_female_above_15,
            COALESCE(sum(mbz_treated_female_above_15), 0) AS mbz_treated_female_above_15,
            COALESCE(sum(treated_female_above_15), 0) AS treated_female_above_15,
            COALESCE(sum(pzq_treated_female_above_16), 0) AS pzq_treated_female_above_16,
            COALESCE(sum(alb_treated_female_above_16), 0) AS alb_treated_female_above_16,
            COALESCE(sum(meb_treated_female_above_16), 0) AS meb_treated_female_above_16,
            COALESCE(sum(treated_female_above_16), 0) AS treated_female_above_16,
            COALESCE(sum(total_males), 0) AS total_males,
            COALESCE(sum(vita_total_male), 0) AS vita_total_male,
            COALESCE(sum(vita_total_female), 0) AS vita_total_female,
            COALESCE(sum(pzq_total_male), 0) AS pzq_total_male,
            COALESCE(sum(pzq_total_female), 0) AS pzq_total_female,
            COALESCE(sum(alb_total_male), 0) AS alb_total_male,
            COALESCE(sum(alb_total_female), 0) AS alb_total_female,
            COALESCE(sum(meb_total_male), 0) AS meb_total_male,
            COALESCE(sum(meb_total_female), 0) AS meb_total_female,
            COALESCE(sum(alb_total_treated), 0) AS alb_total_treated,
            COALESCE(sum(meb_total_treated), 0) AS meb_total_treated,
            COALESCE(sum(alb_meb_total_treated), 0) AS alb_meb_total_treated,
            COALESCE(sum(alb_meb_total_treated_5_15), 0) AS alb_meb_total_treated_5_15,
            COALESCE(sum(alb_meb_total_treated_1_4), 0) AS alb_meb_total_treated_1_4,
            COALESCE(sum(vita_total_treated), 0) AS vita_total_treated,
            COALESCE(sum(pzq_total_treated), 0) AS pzq_total_treated,
            COALESCE(sum(pzq_total_treated_above_16), 0) AS pzq_total_treated_above_16,
            COALESCE(sum(alb_meb_total_treated_above_16), 0) AS alb_meb_total_treated_above_16,
            COALESCE(sum(pzq_total_treated_5_to_15), 0) AS pzq_total_treated_5_to_15,
            COALESCE(sum(vita_total_treated_6_to_11_mos), 0) AS vita_total_treated_6_to_11_mos,
            COALESCE(sum(vita_total_treated_1_4), 0) AS vita_total_treated_1_4,
            COALESCE(sum(total_females), 0) AS total_females,
            COALESCE(sum(total_all_genders), 0) AS total_all_genders,
            COALESCE(sum(supervisor_distributed), 0) AS supervisor_distributed,
            COALESCE(sum(pzq_supervisor_distributed), 0) AS pzq_supervisor_distributed,
            COALESCE(sum(alb_supervisor_distributed), 0) AS alb_supervisor_distributed,
            COALESCE(sum(mbz_supervisor_distributed), 0) AS mbz_supervisor_distributed,
            COALESCE(sum(vita_supervisor_distributed), 0) AS vita_supervisor_distributed,
            COALESCE(sum(meb_supervisor_distributed), 0) AS meb_supervisor_distributed,
            COALESCE(sum(received_number), 0) AS received_number,
            COALESCE(sum(pzq_received), 0) AS pzq_received,
            COALESCE(sum(alb_received), 0) AS alb_received,
            COALESCE(sum(mbz_received), 0) AS mbz_received,
            COALESCE(sum(meb_received), 0) AS meb_received,
            COALESCE(sum(vita_received), 0) AS vita_received,
            COALESCE(sum(adminstered), 0) AS adminstered,
            COALESCE(sum(pzq_administered), 0) AS pzq_administered,
            COALESCE(sum(alb_administered), 0) AS alb_administered,
            COALESCE(sum(mbz_administered), 0) AS mbz_administered,
            COALESCE(sum(meb_administered), 0) AS meb_administered,
            COALESCE(sum(vita_administered), 0) AS vita_administered,
            COALESCE(sum(damaged), 0) AS damaged,
            COALESCE(sum(pzq_damaged), 0) AS pzq_damaged,
            COALESCE(sum(alb_damaged), 0) AS alb_damaged,
            COALESCE(sum(mbz_damaged), 0) AS mbz_damaged,
            COALESCE(sum(meb_damaged), 0) AS meb_damaged,
            COALESCE(sum(vita_damaged), 0) AS vita_damaged,
            COALESCE(sum(adverse), 0) AS adverse,
            COALESCE(sum(pzq_adverse), 0) AS pzq_adverse,
            COALESCE(sum(alb_adverse), 0) AS alb_adverse,
            COALESCE(sum(mbz_adverse), 0) AS mbz_adverse,
            COALESCE(sum(meb_adverse), 0) AS meb_adverse,
            COALESCE(sum(vita_adverse), 0) AS vita_adverse,
            COALESCE(sum(remaining_with_cdd), 0) AS remaining_with_cdd,
            COALESCE(sum(pzq_remaining_with_cdd), 0) AS pzq_remaining_with_cdd,
            COALESCE(sum(alb_remaining_with_cdd), 0) AS alb_remaining_with_cdd,
            COALESCE(sum(mbz_remaining_with_cdd), 0) AS mbz_remaining_with_cdd,
            COALESCE(sum(meb_remaining_with_cdd), 0) AS meb_remaining_with_cdd,
            COALESCE(sum(vita_remaining_with_cdd), 0) AS vita_remaining_with_cdd,
            COALESCE(sum(returned_to_supervisor), 0) AS returned_to_supervisor,
            COALESCE(sum(pzq_returned_to_supervisor), 0) AS pzq_returned_to_supervisor,
            COALESCE(sum(alb_returned_to_supervisor), 0) AS alb_returned_to_supervisor,
            COALESCE(sum(mbz_returned_to_supervisor), 0) AS mbz_returned_to_supervisor,
            COALESCE(sum(vita_returned_to_supervisor), 0) AS vita_returned_to_supervisor
        FROM mda_lite_operational_areas AS op_area
        WHERE op_area.plan_id = plans.plan_id
        AND op_area.jurisdiction_path @> ARRAY[jurisdictions_tree.jurisdiction_id]
      ) AS op_areas ON TRUE
      WHERE jurisdictions.jurisdiction_path @> ARRAY[jurisdictions_tree.jurisdiction_id]
      AND jurisdictions_tree.jurisdiction_depth < 2
    ) AS lite_jurisdictions ON TRUE
) AS jurisdictions_query ON TRUE
LEFT JOIN LATERAL (
    SELECT
        COALESCE(sum(official_population::INTEGER), 0) AS official_population,
        COALESCE(sum(other_population::INTEGER), 0) AS other_pop_target,
        COALESCE(sum(census_target_population_6_to_59_mos_official::INTEGER), 0) AS census_target_population_6_to_59_mos_official,
        COALESCE(sum(other_pop_target_6_to_59_mos_trusted::INTEGER), 0) AS other_pop_target_6_to_59_mos_trusted,
        COALESCE(sum(census_target_population_6_to_11_mos_official::INTEGER), 0) AS census_target_population_6_to_11_mos_official,
        COALESCE(sum(other_pop_target_6_to_11_mos_trusted::INTEGER), 0) AS other_pop_target_6_to_11_mos_trusted,
        COALESCE(sum(census_target_population_12_to_59_mos_official::INTEGER), 0) AS census_target_population_12_to_59_mos_official,
        COALESCE(sum(other_pop_target_12_to_59_mos_trusted::INTEGER), 0) AS other_pop_target_12_to_59_mos_trusted,
        COALESCE(sum(census_pop_target_above_16_official::INTEGER), 0) AS census_pop_target_above_16_official,
        COALESCE(sum(other_pop_target_above_16_trusted::INTEGER), 0) AS other_pop_target_above_16_trusted,
        COALESCE(sum(census_pop_target_5_to_15_official::INTEGER), 0) AS census_pop_target_5_to_15_official,
        COALESCE(sum(other_pop_target_5_to_15_trusted::INTEGER), 0) AS other_pop_target_5_to_15_trusted
    FROM mda_lite_wards_population
    WHERE mda_lite_wards_population.jurisdiction_path @> ARRAY[jurisdictions_query.jurisdiction_id]
) as ward_population ON TRUE;

CREATE INDEX IF NOT EXISTS mda_lite_jurisdictions_plan_idx ON mda_lite_jurisdictions (plan_id);
CREATE INDEX IF NOT EXISTS mda_lite_jurisdictions_jurisdiction_idx ON mda_lite_jurisdictions (jurisdiction_id);
CREATE INDEX IF NOT EXISTS mda_lite_jurisdictions_jurisdiction_parent_idx ON mda_lite_jurisdictions (jurisdiction_parent_id);
CREATE UNIQUE INDEX IF NOT EXISTS mda_lite_jurisdictions_idx ON mda_lite_jurisdictions (id);
