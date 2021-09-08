SET schema 'reveal';

DROP VIEW IF EXISTS irs_focus_area;

DROP MATERIALIZED VIEW IF EXISTS irs_focus_area CASCADE;

CREATE MATERIALIZED VIEW IF NOT EXISTS irs_focus_area
AS
SELECT
    irs_focus_area_base.id,
    irs_focus_area_base.plan_id,
    irs_focus_area_base.jurisdiction_id,
    irs_focus_area_base.jurisdiction_parent_id,
    irs_focus_area_base.jurisdiction_name,
    irs_focus_area_base.jurisdiction_depth,
    irs_focus_area_base.jurisdiction_path,
    irs_focus_area_base.jurisdiction_name_path,
    irs_focus_area_base.is_virtual_jurisdiction,
    irs_focus_area_base.is_leaf_node,
    irs_focus_area_base.health_center_jurisdiction_id,
    irs_focus_area_base.health_center_jurisdiction_name,
    irs_focus_area_base.totstruct,
    irs_focus_area_base.targstruct,
    irs_focus_area_base.rooms_eligible,
    irs_focus_area_base.rooms_sprayed,
    irs_focus_area_base.sprayed_rooms_eligible,
    irs_focus_area_base.sprayed_rooms_sprayed,
    irs_focus_area_base.foundstruct,
    irs_focus_area_base.notsprayed,
    irs_focus_area_base.noteligible,
    irs_focus_area_base.notasks,
    irs_focus_area_base.sprayedstruct,
    irs_focus_area_base.duplicates,
    irs_focus_area_base.sprayed_duplicates,
    irs_focus_area_base.notsprayed_reasons,
    irs_focus_area_base.notsprayed_reasons_counts,
    irs_focus_area_base.latest_spray_event_id,
    irs_focus_area_base.latest_spray_event_date,
    irs_focus_area_base.spraycov,
    irs_focus_area_base.spraytarg,
    irs_focus_area_base.spraysuccess,
    irs_focus_area_base.spray_effectiveness,
    irs_focus_area_base.structures_remaining_to_90_se,
    irs_focus_area_base.tla_days_to_90_se,
    irs_focus_area_base.roomcov,
    irs_focus_area_base.reviewed_with_decision,
    irs_focus_area_base.latest_sa_event_id,
    irs_focus_area_base.latest_sa_event_date,
    irs_focus_area_base.rooms_on_ground
FROM irs_focus_area_base
WHERE irs_focus_area_base.is_leaf_node = true;
