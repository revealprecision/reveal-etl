SET schema 'reveal';

DROP MATERIALIZED VIEW IF EXISTS dashboard_area_view CASCADE;

CREATE MATERIALIZED VIEW IF NOT EXISTS dashboard_area_view
AS
SELECT
    irs_focus_area_base.plan_id AS plan_id,
    plans.title AS plan_title,
    irs_focus_area_base.jurisdiction_name_path[1] AS country,
    irs_focus_area_base.jurisdiction_name_path[2] AS province,
    irs_focus_area_base.jurisdiction_name_path[3] AS district,
    irs_focus_area_base.jurisdiction_name_path[4] AS catchment,
    irs_focus_area_base.jurisdiction_name AS jurisdiction,
    irs_focus_area_base.jurisdiction_id AS jurisdiction_id,
    irs_focus_area_base.totstruct AS totstruct,
    irs_focus_area_base.targstruct AS targstruct,
    irs_focus_area_base.foundstruct AS foundstruct,
    irs_focus_area_base.sprayedstruct AS sprayedstruct,
    irs_focus_area_base.notsprayed AS notsprayed,
    irs_focus_area_base.noteligible AS noteligible,
    irs_focus_area_base.rooms_eligible AS rooms_eligible,
    irs_focus_area_base.rooms_sprayed AS rooms_sprayed,
    irs_focus_area_base.sprayed_duplicates AS sprayed_duplicates,
    irs_focus_area_base.notsprayed_reasons_counts AS notsprayed_reasons_counts,
    irs_focus_area_base.sprayed_totalpop,
    irs_focus_area_base.sprayed_totalmale,
    irs_focus_area_base.sprayed_totalfemale,
    irs_focus_area_base.sprayed_males,
    irs_focus_area_base.sprayed_females,
    irs_focus_area_base.sprayed_pregwomen,
    irs_focus_area_base.sprayed_childrenU5,
    irs_focus_area_base.notsprayed_totalpop,
    irs_focus_area_base.notsprayed_males,
    irs_focus_area_base.notsprayed_females,
    irs_focus_area_base.notsprayed_pregwomen,
    irs_focus_area_base.notsprayed_childrenU5
FROM irs_focus_area_base irs_focus_area_base
LEFT JOIN reveal.plans plans ON irs_focus_area_base.plan_id = plans.identifier;

CREATE INDEX IF NOT EXISTS dashboard_area_view_plan_id_idx ON dashboard_area_view (plan_id);
CREATE INDEX IF NOT EXISTS dashboard_area_view_jurisdiction_id_idx ON dashboard_area_view (jurisdiction_id);