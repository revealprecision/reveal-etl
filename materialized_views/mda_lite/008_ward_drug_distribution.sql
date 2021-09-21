SET schema 'reveal';
DROP MATERIALIZED VIEW IF EXISTS ward_drug_distribution CASCADE;
CREATE MATERIALIZED VIEW IF NOT EXISTS ward_drug_distribution AS
SELECT
    subq.*,
    (subq.total_all_genders / subq.days_worked) as average_per_day
FROM (
         SELECT
             public.uuid_generate_v5(
                     '6ba7b810-9dad-11d1-80b4-00c04fd430c8',
                     concat(events.base_entity_id, events.plan_id, events.form_data -> 'cdd_name', events.form_data -> 'health_worker_supervisor')
                 ) AS id,
             public.uuid_generate_v5(
                     '6ba7b810-9dad-11d1-80b4-00c04fd430c8',
                     concat(events.base_entity_id, events.plan_id, events.form_data -> 'health_worker_supervisor')
                 ) AS supervisor_id,
             events.form_data -> 'health_worker_supervisor'::text AS supervisor_name,
             events.plan_id,
             events.base_entity_id,
             events.form_data -> 'cdd_name'::text AS cdd_name,
             count(DISTINCT TO_CHAR(date_created :: DATE, 'dd/mm/yyyy')) as days_worked,
             sum(COALESCE((events.form_data -> 'health_education_above_16'::text) ->> 0, '0'::text)::bigint) AS health_education_above_16,
             sum(COALESCE((events.form_data -> 'health_education_5_to_15'::text) ->> 0, '0'::text)::bigint) AS health_education_5_to_15,
             sum(COALESCE(case when events.form_data ->> 'drugs' = 'ALB' then events.form_data ->> 'treated_male_1_to_4' end,'0')::integer) as alb_treated_male_1_4,
             sum(COALESCE(case when events.form_data ->> 'drugs' = 'MBZ' then events.form_data ->> 'treated_male_1_to_4' end,'0')::integer) as mbz_treated_male_1_4,
             sum(COALESCE(case when events.form_data ->> 'drugs' = 'MEB' then events.form_data ->> 'treated_male_1_to_4' end,'0')::integer) as meb_treated_male_1_4,
             sum(COALESCE(case when events.form_data ->> 'drugs' = 'VITA' then events.form_data ->> 'treated_male_1_to_4' end,'0')::integer) as vita_treated_male_1_4,
             sum(COALESCE((events.form_data -> 'treated_male_1_to_4'::text) ->> 0, '0'::text)::integer) AS treated_male_1_4,
             sum(COALESCE(case when events.form_data ->> 'drugs' = 'VITA' then events.form_data ->> 'treated_male_6_to_11_mos' end,'0')::integer) as vita_treated_male_6_to_11_mos,
             sum(COALESCE((events.form_data -> 'treated_male_6_to_11_mos'::text) ->> 0, '0'::text)::integer) AS treated_male_6_to_11_mos,
             sum(COALESCE(case when events.form_data ->> 'drugs' = 'PZQ' then events.form_data ->> 'treated_male_5_to_14' end,'0')::integer) as pzq_treated_male_5_to_14,
             sum(COALESCE(case when events.form_data ->> 'drugs' = 'ALB' then events.form_data ->> 'treated_male_5_to_14' end,'0')::integer) as alb_treated_male_5_to_14,
             sum(COALESCE(case when events.form_data ->> 'drugs' = 'MBZ' then events.form_data ->> 'treated_male_5_to_14' end,'0')::integer) as mbz_treated_male_5_to_14,
             sum(COALESCE((events.form_data -> 'treated_male_5_to_14'::text) ->> 0, '0'::text)::integer) AS treated_male_5_14,
             sum(COALESCE(case when events.form_data ->> 'drugs' = 'PZQ' then events.form_data ->> 'treated_male_5_to_15' end,'0')::integer) as pzq_treated_male_5_to_15,
             sum(COALESCE(case when events.form_data ->> 'drugs' = 'ALB' then events.form_data ->> 'treated_male_5_to_15' end,'0')::integer) as alb_treated_male_5_to_15,
             sum(COALESCE(case when events.form_data ->> 'drugs' = 'MEB' then events.form_data ->> 'treated_male_5_to_15' end,'0')::integer) as meb_treated_male_5_to_15,
             sum(COALESCE((events.form_data -> 'treated_male_5_to_15'::text) ->> 0, '0'::text)::integer) AS treated_male_5_15,
             sum(COALESCE(case when events.form_data ->> 'drugs' = 'PZQ' then events.form_data ->> 'treated_male_above_15' end,'0')::integer) as pzq_treated_male_above_15,
             sum(COALESCE(case when events.form_data ->> 'drugs' = 'ALB' then events.form_data ->> 'treated_male_above_15' end,'0')::integer) as alb_treated_male_above_15,
             sum(COALESCE(case when events.form_data ->> 'drugs' = 'MBZ' then events.form_data ->> 'treated_male_above_15' end,'0')::integer) as mbz_treated_male_above_15,
             sum(COALESCE((events.form_data -> 'treated_male_above_15'::text) ->> 0, '0'::text)::integer) AS treated_male_above_15,
             sum(COALESCE(case when events.form_data ->> 'drugs' = 'PZQ' then events.form_data ->> 'treated_male_above_16' end,'0')::integer) as pzq_treated_male_above_16,
             sum(COALESCE(case when events.form_data ->> 'drugs' = 'ALB' then events.form_data ->> 'treated_male_above_16' end,'0')::integer) as alb_treated_male_above_16,
             sum(COALESCE(case when events.form_data ->> 'drugs' = 'MEB' then events.form_data ->> 'treated_male_above_16' end,'0')::integer) as meb_treated_male_above_16,
             sum(COALESCE((events.form_data -> 'treated_male_above_16'::text) ->> 0, '0'::text)::integer) AS treated_male_above_16,
             sum(COALESCE(case when events.form_data ->> 'drugs' = 'ALB' then events.form_data ->> 'treated_female_1_to_4' end,'0')::integer) as alb_treated_female_1_4,
             sum(COALESCE(case when events.form_data ->> 'drugs' = 'MBZ' then events.form_data ->> 'treated_female_1_to_4' end,'0')::integer) as mbz_treated_female_1_4,
             sum(COALESCE(case when events.form_data ->> 'drugs' = 'MEB' then events.form_data ->> 'treated_female_1_to_4' end,'0')::integer) as meb_treated_female_1_4,
             sum(COALESCE(case when events.form_data ->> 'drugs' = 'VITA' then events.form_data ->> 'treated_female_1_to_4' end,'0')::integer) as vita_treated_female_1_4,
             sum(COALESCE((events.form_data -> 'treated_female_1_to_4'::text) ->> 0, '0'::text)::integer) AS treated_female_1_4,
             sum(COALESCE(case when events.form_data ->> 'drugs' = 'VITA' then events.form_data ->> 'treated_female_6_to_11_mos' end,'0')::integer) as vita_treated_female_6_to_11_mos,
             sum(COALESCE((events.form_data -> 'treated_female_6_to_11_mos'::text) ->> 0, '0'::text)::integer) AS treated_female_6_to_11_mos,
             sum(COALESCE(case when events.form_data ->> 'drugs' = 'PZQ' then events.form_data ->> 'treated_female_5_to_14' end,'0')::integer) as pzq_treated_female_5_to_14,
             sum(COALESCE(case when events.form_data ->> 'drugs' = 'ALB' then events.form_data ->> 'treated_female_5_to_14' end,'0')::integer) as alb_treated_female_5_to_14,
             sum(COALESCE(case when events.form_data ->> 'drugs' = 'MBZ' then events.form_data ->> 'treated_female_5_to_14' end,'0')::integer) as mbz_treated_female_5_to_14,
             sum(COALESCE((events.form_data -> 'treated_female_5_to_14'::text) ->> 0, '0'::text)::integer) AS treated_female_5_14,
             sum(COALESCE(case when events.form_data ->> 'drugs' = 'PZQ' then events.form_data ->> 'treated_female_5_to_15' end,'0')::integer) as pzq_treated_female_5_to_15,
             sum(COALESCE(case when events.form_data ->> 'drugs' = 'ALB' then events.form_data ->> 'treated_female_5_to_15' end,'0')::integer) as alb_treated_female_5_to_15,
             sum(COALESCE(case when events.form_data ->> 'drugs' = 'MEB' then events.form_data ->> 'treated_female_5_to_15' end,'0')::integer) as meb_treated_female_5_to_15,
             sum(COALESCE((events.form_data -> 'treated_female_5_to_15'::text) ->> 0, '0'::text)::integer) AS treated_female_5_15,
             sum(COALESCE(case when events.form_data ->> 'drugs' = 'PZQ' then events.form_data ->> 'treated_female_above_15' end,'0')::integer) as pzq_treated_female_above_15,
             sum(COALESCE(case when events.form_data ->> 'drugs' = 'ALB' then events.form_data ->> 'treated_female_above_15' end,'0')::integer) as alb_treated_female_above_15,
             sum(COALESCE(case when events.form_data ->> 'drugs' = 'MBZ' then events.form_data ->> 'treated_female_above_15' end,'0')::integer) as mbz_treated_female_above_15,
             sum(COALESCE((events.form_data -> 'treated_female_above_15'::text) ->> 0, '0'::text)::integer) AS treated_female_above_15,
             sum(COALESCE((events.form_data -> 'treated_female_above_16'::text) ->> 0, '0'::text)::integer) AS treated_female_above_16,
             sum(COALESCE(case when events.form_data ->> 'drugs' = 'PZQ' then events.form_data ->> 'treated_female_above_16' end,'0')::integer) as pzq_treated_female_above_16,
             sum(COALESCE(case when events.form_data ->> 'drugs' = 'ALB' then events.form_data ->> 'treated_female_above_16' end,'0')::integer) as alb_treated_female_above_16,
             sum(COALESCE(case when events.form_data ->> 'drugs' = 'MEB' then events.form_data ->> 'treated_female_above_16' end,'0')::integer) as meb_treated_female_above_16,
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
                 COALESCE(case when events.form_data ->> 'drugs' = 'MEB' then events.form_data ->> 'treated_male_above_15' end,'0')::integer +
                 COALESCE(case when events.form_data ->> 'drugs' = 'MBZ' then events.form_data ->> 'treated_male_1_to_4' end,'0')::integer +
                 COALESCE(case when events.form_data ->> 'drugs' = 'MBZ' then events.form_data ->> 'treated_male_5_to_14' end,'0')::integer +
                 COALESCE(case when events.form_data ->> 'drugs' = 'MBZ' then events.form_data ->> 'treated_male_5_to_15' end,'0')::integer +
                 COALESCE(case when events.form_data ->> 'drugs' = 'MBZ' then events.form_data ->> 'treated_male_above_16' end,'0')::integer +
                 COALESCE(case when events.form_data ->> 'drugs' = 'MBZ' then events.form_data ->> 'treated_male_above_15' end,'0')::integer
                 ) as meb_total_male,
             sum(COALESCE(case when events.form_data ->> 'drugs' = 'MEB' then events.form_data ->> 'treated_female_1_to_4' end,'0')::integer +
                 COALESCE(case when events.form_data ->> 'drugs' = 'MEB' then events.form_data ->> 'treated_female_5_to_14' end,'0')::integer +
                 COALESCE(case when events.form_data ->> 'drugs' = 'MEB' then events.form_data ->> 'treated_female_5_to_15' end,'0')::integer +
                 COALESCE(case when events.form_data ->> 'drugs' = 'MEB' then events.form_data ->> 'treated_female_above_16' end,'0')::integer +
                 COALESCE(case when events.form_data ->> 'drugs' = 'MEB' then events.form_data ->> 'treated_female_above_15' end,'0')::integer +
                 COALESCE(case when events.form_data ->> 'drugs' = 'MBZ' then events.form_data ->> 'treated_female_1_to_4' end,'0')::integer +
                 COALESCE(case when events.form_data ->> 'drugs' = 'MBZ' then events.form_data ->> 'treated_female_5_to_14' end,'0')::integer +
                 COALESCE(case when events.form_data ->> 'drugs' = 'MBZ' then events.form_data ->> 'treated_female_5_to_15' end,'0')::integer +
                 COALESCE(case when events.form_data ->> 'drugs' = 'MBZ' then events.form_data ->> 'treated_female_above_16' end,'0')::integer +
                 COALESCE(case when events.form_data ->> 'drugs' = 'MBZ' then events.form_data ->> 'treated_female_above_15' end,'0')::integer
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
                 COALESCE(case when events.form_data ->> 'drugs' = 'MEB' then events.form_data ->> 'treated_male_1_to_4' end,'0')::integer +
                 COALESCE(case when events.form_data ->> 'drugs' = 'MEB' then events.form_data ->> 'treated_male_5_to_14' end,'0')::integer +
                 COALESCE(case when events.form_data ->> 'drugs' = 'MEB' then events.form_data ->> 'treated_male_5_to_15' end,'0')::integer +
                 COALESCE(case when events.form_data ->> 'drugs' = 'MEB' then events.form_data ->> 'treated_male_above_16' end,'0')::integer +
                 COALESCE(case when events.form_data ->> 'drugs' = 'MEB' then events.form_data ->> 'treated_male_above_15' end,'0')::integer +
                 COALESCE(case when events.form_data ->> 'drugs' = 'MEB' then events.form_data ->> 'treated_female_1_to_4' end,'0')::integer +
                 COALESCE(case when events.form_data ->> 'drugs' = 'MEB' then events.form_data ->> 'treated_female_5_to_14' end,'0')::integer +
                 COALESCE(case when events.form_data ->> 'drugs' = 'MEB' then events.form_data ->> 'treated_female_5_to_15' end,'0')::integer +
                 COALESCE(case when events.form_data ->> 'drugs' = 'MEB' then events.form_data ->> 'treated_female_above_16' end,'0')::integer
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
             sum(COALESCE((events.form_data -> 'treated_male_1_to_4'::text) ->> 0, '0'::text)::integer + COALESCE((events.form_data -> 'treated_male_5_to_14'::text) ->> 0, '0'::text)::integer + COALESCE((events.form_data -> 'treated_male_above_15'::text) ->> 0, '0'::text)::integer) AS total_males,
             sum(COALESCE((events.form_data -> 'treated_female_1_to_4'::text) ->> 0, '0'::text)::integer + COALESCE((events.form_data -> 'treated_female_5_to_14'::text) ->> 0, '0'::text)::integer + COALESCE((events.form_data -> 'treated_female_above_15'::text) ->> 0, '0'::text)::integer) AS total_females,
             sum(COALESCE((events.form_data -> 'treated_male_1_to_4'::text) ->> 0, '0'::text)::integer + COALESCE((events.form_data -> 'treated_male_5_to_14'::text) ->> 0, '0'::text)::integer + COALESCE((events.form_data -> 'treated_male_above_15'::text) ->> 0, '0'::text)::integer + COALESCE((events.form_data -> 'treated_female_1_to_4'::text) ->> 0, '0'::text)::integer + COALESCE((events.form_data -> 'treated_female_5_to_14'::text) ->> 0, '0'::text)::integer + COALESCE((events.form_data -> 'treated_female_above_15'::text) ->> 0, '0'::text)::integer) AS total_all_genders,
             sum(COALESCE((events.form_data -> 'sum_pzq_received_and_top_up'::text) ->> 0, '0'::text)::integer + COALESCE((events.form_data -> 'sum_alb_received_and_top_up'::text) ->> 0, '0'::text)::integer + COALESCE((events.form_data -> 'sum_mbz_received_and_top_up'::text) ->> 0, '0'::text)::integer) AS supervisor_distributed,
             sum(COALESCE((events.form_data -> 'sum_pzq_received_and_top_up'::text) ->> 0, '0'::text)::integer) AS pzq_supervisor_distributed,
             sum(COALESCE((events.form_data -> 'sum_alb_received_and_top_up'::text) ->> 0, '0'::text)::integer) AS alb_supervisor_distributed,
             sum(COALESCE((events.form_data -> 'sum_mbz_received_and_top_up'::text) ->> 0, '0'::text)::integer) AS mbz_supervisor_distributed,
             sum(COALESCE((events.form_data -> 'sum_meb_received_and_top_up'::text) ->> 0, '0'::text)::integer) AS meb_supervisor_distributed,
             sum(COALESCE((events.form_data -> 'sum_vita_received_and_top_up'::text) ->> 0, '0'::text)::integer) AS vita_supervisor_distributed,
             sum(COALESCE((events.form_data -> 'received_number'::text) ->> 0, '0'::text)::integer) AS received_number,
             sum(COALESCE(case when events.form_data ->> 'drugs' = 'PZQ' then events.form_data ->>'received_number' end,'0')::integer) as pzq_received,
             sum(COALESCE(case when events.form_data ->> 'drugs' = 'ALB' then events.form_data ->>'received_number' end,'0')::integer) as alb_received,
             sum(COALESCE(case when events.form_data ->> 'drugs' = 'MBZ' then events.form_data ->>'received_number' end,'0')::integer) as mbz_received,
             sum(COALESCE(case when events.form_data ->> 'drugs' = 'MEB' then events.form_data ->>'received_number' end,'0')::integer) as meb_received,
             sum(COALESCE(case when events.form_data ->> 'drugs' = 'VITA' then events.form_data ->>'received_number' end,'0')::integer) as vita_received,
             sum(COALESCE((events.form_data -> 'adminstered'::text) ->> 0, '0'::text)::integer) AS adminstered,
             sum(COALESCE(case when events.form_data ->> 'drugs' = 'PZQ' then events.form_data ->>'adminstered' end,'0')::integer) as pzq_administered,
             sum(COALESCE(case when events.form_data ->> 'drugs' = 'ALB' then events.form_data ->>'adminstered' end,'0')::integer) as alb_administered,
             sum(COALESCE(case when events.form_data ->> 'drugs' = 'MBZ' then events.form_data ->>'adminstered' end,'0')::integer) as mbz_administered,
             sum(COALESCE(case when events.form_data ->> 'drugs' = 'MEB' then events.form_data ->>'adminstered' end,'0')::integer) as meb_administered,
             sum(COALESCE(case when events.form_data ->> 'drugs' = 'VITA' then events.form_data ->>'adminstered' end,'0')::integer) as vita_administered,
             sum(COALESCE((events.form_data -> 'damaged'::text) ->> 0, '0'::text)::integer) AS damaged,
             sum(COALESCE(case when events.form_data ->> 'drugs' = 'PZQ' then events.form_data ->>'damaged' end,'0')::integer) as pzq_damaged,
             sum(COALESCE(case when events.form_data ->> 'drugs' = 'ALB' then events.form_data ->>'damaged' end,'0')::integer) as alb_damaged,
             sum(COALESCE(case when events.form_data ->> 'drugs' = 'MBZ' then events.form_data ->>'damaged' end,'0')::integer) as mbz_damaged,
             sum(COALESCE(case when events.form_data ->> 'drugs' = 'MEB' then events.form_data ->>'damaged' end,'0')::integer) as meb_damaged,
             sum(COALESCE(case when events.form_data ->> 'drugs' = 'VITA' then events.form_data ->>'damaged' end,'0')::integer) as vita_damaged,
             sum(COALESCE((events.form_data -> 'adverse'::text) ->> 0, '0'::text)::integer) AS adverse,
             sum(COALESCE(case when events.form_data ->> 'drugs' = 'PZQ' then events.form_data ->>'adverse' end,'0')::integer) as pzq_adverse,
             sum(COALESCE(case when events.form_data ->> 'drugs' = 'ALB' then events.form_data ->>'adverse' end,'0')::integer) as alb_adverse,
             sum(COALESCE(case when events.form_data ->> 'drugs' = 'MBZ' then events.form_data ->>'adverse' end,'0')::integer) as mbz_adverse,
             sum(COALESCE(case when events.form_data ->> 'drugs' = 'MEB' then events.form_data ->>'adverse' end,'0')::integer) as meb_adverse,
             sum(COALESCE(case when events.form_data ->> 'drugs' = 'VITA' then events.form_data ->>'adverse' end,'0')::integer) as vita_adverse,
             sum(COALESCE((events.form_data -> 'received_number'::text) ->> 0, '0'::text)::integer - (COALESCE((events.form_data -> 'adminstered'::text) ->> 0, '0'::text)::integer + COALESCE((events.form_data -> 'damaged'::text) ->> 0, '0'::text)::integer)) AS remaining_with_cdd,
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
             sum(COALESCE((events.form_data -> 'pzq_returned'::text) ->> 0, '0'::text)::integer + COALESCE((events.form_data -> 'albendazole_returned'::text) ->> 0, '0'::text)::integer + COALESCE((events.form_data -> 'mebendazole_returned'::text) ->> 0, '0'::text)::integer) AS returned_to_supervisor,
             sum(COALESCE((events.form_data -> 'pzq_returned'::text) ->> 0, '0'::text)::integer) AS pzq_returned_to_supervisor,
             sum(COALESCE((events.form_data -> 'albendazole_returned'::text) ->> 0, '0'::text)::integer) AS alb_returned_to_supervisor,
             sum(COALESCE((events.form_data -> 'mebendazole_returned'::text) ->> 0, '0'::text)::integer) AS mbz_returned_to_supervisor,
             sum(COALESCE((events.form_data -> 'vita_returned'::text) ->> 0, '0'::text)::integer) vita_returned_to_supervisor

         FROM events
         WHERE events.event_type::text = ANY (ARRAY['tablet_accountability'::character varying, 'cdd_supervisor_daily_summary'::character varying,'cell_coordinator_daily_summary'::character varying]::text[])
  AND events.entity_type = 'Structure'
         GROUP BY (events.form_data -> 'cdd_name'::text), (events.form_data -> 'health_worker_supervisor'::text), events.plan_id, events.base_entity_id
     ) as subq;

CREATE INDEX IF NOT EXISTS ward_drug_distribution_base_entity_id_idx ON ward_drug_distribution (base_entity_id);
CREATE INDEX IF NOT EXISTS ward_drug_distribution_supervisor_id_idx ON ward_drug_distribution (supervisor_id);
CREATE INDEX IF NOT EXISTS ward_drug_distribution_plan_id_idx ON ward_drug_distribution (plan_id);
CREATE UNIQUE INDEX IF NOT EXISTS ward_drug_distribution_id_idx ON ward_drug_distribution (id);