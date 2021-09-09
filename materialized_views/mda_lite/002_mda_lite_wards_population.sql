SET schema 'reveal';

DROP MATERIALIZED VIEW IF EXISTS mda_lite_wards_population CASCADE;

CREATE MATERIALIZED VIEW IF NOT EXISTS mda_lite_wards_population
AS
SELECT
    locations.code as ward_id,
    locations.jurisdiction_id,
    locations.name,
    jurisdiction_other_population.data ->> 0 as official_population,
    jurisdiction_population_query.data ->> 0 as other_population,
    census_target_population_6_to_59_mos_official.data ->> 0 as census_target_population_6_to_59_mos_official,
    other_pop_target_6_to_59_mos_trusted.data ->> 0 as other_pop_target_6_to_59_mos_trusted,
    census_target_population_6_to_11_mos_official.data ->> 0 as census_target_population_6_to_11_mos_official,
    other_pop_target_6_to_11_mos_trusted.data ->> 0 as other_pop_target_6_to_11_mos_trusted,
    census_target_population_12_to_59_mos_official.data ->> 0 as census_target_population_12_to_59_mos_official,
    other_pop_target_12_to_59_mos_trusted.data ->> 0 as other_pop_target_12_to_59_mos_trusted,
    census_pop_target_above_16_official.data ->> 0 as census_pop_target_above_16_official,
    other_pop_target_above_16_trusted.data ->> 0 as other_pop_target_above_16_trusted,
    census_pop_target_5_to_15_official.data ->> 0 as census_pop_target_5_to_15_official,
    other_pop_target_5_to_15_trusted.data ->> 0 as other_pop_target_5_to_15_trusted,
    jurisdictions_materialized_view.jurisdiction_name_path,
    jurisdictions_materialized_view.jurisdiction_path
FROM locations as locations
LEFT JOIN jurisdictions_tree as jurisdictions_materialized_view
    ON locations.jurisdiction_id = jurisdictions_materialized_view.jurisdiction_id
LEFT JOIN opensrp_settings as jurisdiction_population_query
    ON jurisdiction_population_query.key = locations.code
    AND jurisdiction_population_query.identifier = 'structure_metadata-other-population'
LEFT JOIN opensrp_settings as jurisdiction_other_population
    ON jurisdiction_other_population.key = locations.code
    AND jurisdiction_other_population.identifier = 'structure_metadata-population'
LEFT JOIN opensrp_settings as census_target_population_6_to_59_mos_official
          ON census_target_population_6_to_59_mos_official.key = locations.code
              AND census_target_population_6_to_59_mos_official.identifier = 'census_pop_target_6_to_59_mos_official'
LEFT JOIN opensrp_settings as other_pop_target_6_to_59_mos_trusted
          ON other_pop_target_6_to_59_mos_trusted.key = locations.code
              AND other_pop_target_6_to_59_mos_trusted.identifier = 'other_pop_target_6_to_59_mos_trusted'
LEFT JOIN opensrp_settings as census_target_population_6_to_11_mos_official
          ON census_target_population_6_to_11_mos_official.key = locations.code
              AND census_target_population_6_to_11_mos_official.identifier = 'census_pop_target_6_to_11_mos_official'
LEFT JOIN opensrp_settings as other_pop_target_6_to_11_mos_trusted
          ON other_pop_target_6_to_11_mos_trusted.key = locations.code
              AND other_pop_target_6_to_11_mos_trusted.identifier = 'other_pop_target_6_to_11_mos_trusted'
LEFT JOIN opensrp_settings as census_target_population_12_to_59_mos_official
          ON census_target_population_12_to_59_mos_official.key = locations.code
              AND census_target_population_12_to_59_mos_official.identifier = 'census_pop_target_12_to_59_mos_official'
LEFT JOIN opensrp_settings as other_pop_target_12_to_59_mos_trusted
          ON other_pop_target_12_to_59_mos_trusted.key = locations.code
              AND other_pop_target_12_to_59_mos_trusted.identifier = 'other_pop_target_12_to_59_mos_trusted'
LEFT JOIN opensrp_settings as census_pop_target_above_16_official
          ON census_pop_target_above_16_official.key = locations.code
              AND census_pop_target_above_16_official.identifier = 'census_pop_target_above_16_official'
LEFT JOIN opensrp_settings as other_pop_target_above_16_trusted
          ON other_pop_target_above_16_trusted.key = locations.code
              AND other_pop_target_above_16_trusted.identifier = 'other_pop_target_above_16_trusted'
LEFT JOIN opensrp_settings as census_pop_target_5_to_15_official
          ON census_pop_target_5_to_15_official.key = locations.code
              AND census_pop_target_5_to_15_official.identifier = 'census_pop_target_5_to_15_official'
LEFT JOIN opensrp_settings as other_pop_target_5_to_15_trusted
          ON other_pop_target_5_to_15_trusted.key = locations.code
              AND other_pop_target_5_to_15_trusted.identifier = 'other_pop_target_5_to_15_trusted'
WHERE jurisdiction_population_query.data IS NOT NULL
OR jurisdiction_other_population.data IS NOT NULL
OR census_target_population_6_to_59_mos_official.data IS NOT NULL
OR census_target_population_6_to_11_mos_official.data IS NOT NULL
OR other_pop_target_6_to_59_mos_trusted.data IS NOT NULL
OR other_pop_target_6_to_11_mos_trusted.data IS NOT NULL
OR census_target_population_12_to_59_mos_official.data IS NOT NULL
OR other_pop_target_12_to_59_mos_trusted.data IS NOT NULL
OR census_pop_target_above_16_official.data IS NOT NULL
OR other_pop_target_above_16_trusted.data IS NOT NULL
OR census_pop_target_5_to_15_official.data IS NOT NULL
OR other_pop_target_5_to_15_trusted.data IS NOT NULL;
CREATE INDEX IF NOT EXISTS mda_lite_wards_population_jurisdiction_idx ON mda_lite_wards_population (jurisdiction_id);
CREATE UNIQUE INDEX IF NOT EXISTS mda_lite_wards_population_idx ON mda_lite_wards_population (ward_id);
