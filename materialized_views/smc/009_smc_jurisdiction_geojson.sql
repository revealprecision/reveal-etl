SET schema 'reveal';

DROP MATERIALIZED VIEW IF EXISTS smc_jurisdiction_geojson CASCADE;

CREATE MATERIALIZED VIEW IF NOT EXISTS smc_jurisdiction_geojson
AS
SELECT
  "row".jurisdiction_id,
  "row".plan_id,
  jsonb_build_object('type', 'Feature', 'id', "row".jurisdiction_id, 'geometry', (public.st_asgeojson("row".jurisdiction_geometry))::jsonb, 'properties', ((to_jsonb("row".*) - 'jurisdiction_id'::text) - 'jurisdiction_geometry'::text)) AS geojson
FROM (
  SELECT
      DISTINCT jurisdictions.id AS jurisdiction_id,
      jurisdictions.geometry AS jurisdiction_geometry,
      plan_jurisdiction.plan_id
  FROM reveal.plan_jurisdiction plan_jurisdiction,
             reveal.jurisdictions jurisdictions
  WHERE ((plan_jurisdiction.jurisdiction_id)::text = (jurisdictions.id)::text)
  ) "row";