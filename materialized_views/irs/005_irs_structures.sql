SET schema 'reveal';

DROP MATERIALIZED VIEW IF EXISTS irs_structures CASCADE;

CREATE MATERIALIZED VIEW IF NOT EXISTS irs_structures
AS
SELECT
    DISTINCT ON (irs_plans.plan_id, locations.id) public.uuid_generate_v5('6ba7b810-9dad-11d1-80b4-00c04fd430c8'::uuid, concat(irs_plans.plan_id, locations.id)) AS id,
    irs_plans.plan_id,
    locations.id AS structure_id,
    locations.status AS structure_status,
    locations.jurisdiction_id AS old_jurisdiction_id,
    irs_structure_jurisdictions.jurisdiction_depth AS old_jurisdiction_depth,
    irs_structure_jurisdictions.geo_jurisdiction_id,
    irs_structure_jurisdictions.geo_jurisdiction_depth,
    irs_structure_jurisdictions.zambia_jurisdiction_id,
    irs_structure_jurisdictions.zambia_jurisdiction_depth,
    COALESCE(events_query.village_name, '') AS village_name,
    public.st_astext(public.st_centroid(locations.geometry)) AS lat_lon,
    COALESCE(irs_structure_jurisdictions.zambia_jurisdiction_id, locations.jurisdiction_id) AS structure_jurisdiction_id,
    COALESCE(events_query.task_id, tasks_query.identifier) AS task_id,
    COALESCE(events_query.task_status, tasks_query.status) AS task_status,
    events_query.event_id,
    events_query.event_date,
    COALESCE(events_query.rooms_eligible, 0) AS rooms_eligible,
    COALESCE(events_query.rooms_sprayed, 0) AS rooms_sprayed,
    COALESCE(events_query.sprayed_totalpop, 0) AS sprayed_totalpop,
    COALESCE(events_query.sprayed_totalmale, 0) AS sprayed_totalmale,
    COALESCE(events_query.sprayed_totalfemale, 0) AS sprayed_totalfemale,
    COALESCE(events_query.sprayed_males, 0) AS sprayed_males,
    COALESCE(events_query.sprayed_females, 0) AS sprayed_females,
    COALESCE(events_query.sprayed_pregwomen, 0) AS sprayed_pregwomen,
    COALESCE(events_query.sprayed_childrenU5, 0) AS sprayed_childrenU5,
    COALESCE(events_query.notsprayed_totalpop, 0) AS notsprayed_totalpop,
    COALESCE(events_query.notsprayed_males, 0) AS notsprayed_males,
    COALESCE(events_query.notsprayed_females, 0) AS notsprayed_females,
    COALESCE(events_query.notsprayed_pregwomen, 0) AS notsprayed_pregwomen,
    COALESCE(events_query.notsprayed_childrenU5, 0) AS notsprayed_childrenU5,
    COALESCE(events_query.eligibility, 'Eligible'::text) AS eligibility,
    COALESCE(events_query.structure_sprayed, 'No'::text) AS structure_sprayed,
    COALESCE(events_query.business_status, (tasks_query.business_status)::text, 'No Tasks'::text) AS business_status,
    COALESCE(events_query.sprayed_values, (ARRAY[]::character varying[])::text[]) AS sprayed_values,
    COALESCE(events_query.notsprayed_reasons, (ARRAY[]::character varying[])::text[]) AS notsprayed_reasons,
    COALESCE(events_query.duplicate, false) AS duplicate,
    COALESCE(events_query.sprayed_duplicate, false) AS sprayed_duplicate
FROM (
    (
        (
            (
                (
                    (reveal.irs_plans
                    JOIN reveal.plan_jurisdiction ON (((plan_jurisdiction.plan_id)::text = (irs_plans.plan_id)::text)))
                    JOIN reveal.jurisdictions_tree jurisdictions_tree ON (((plan_jurisdiction.jurisdiction_id)::text = (jurisdictions_tree.jurisdiction_id)::text)))
                    JOIN reveal.locations ON ((((plan_jurisdiction.jurisdiction_id)::text = (locations.jurisdiction_id)::text) OR ((locations.jurisdiction_id)::text = ANY ((jurisdictions_tree.jurisdiction_path)::text[])))))
                    JOIN reveal.irs_structure_jurisdictions irs_structure_jurisdictions ON (((irs_structure_jurisdictions.id)::text = (locations.id)::text)))
                    LEFT JOIN LATERAL (
                        SELECT
                            subq.task_id,
                            subq.task_status,
                            subq.event_id,
                            subq.event_date,
                            subq.plan_id,
                            subq.village_name,
                            subq.rooms_eligible,
                            subq.rooms_sprayed,
                            subq.eligibility,
                            subq.structure_sprayed,
                            subq.business_status,
                            subq.notsprayed_reason,
                            subq.sprayed_totalpop,
                            subq.sprayed_totalmale,
                            subq.sprayed_totalfemale,
                            subq.sprayed_males,
                            subq.sprayed_females,
                            subq.sprayed_pregwomen,
                            subq.sprayed_childrenU5,
                            subq.notsprayed_totalpop,

                            subq.notsprayed_males AS notsprayed_males,
                            subq.notsprayed_females AS notsprayed_females,
                            subq.notsprayed_pregwomen AS notsprayed_pregwomen,
                            subq.notsprayed_childrenU5 AS notsprayed_childrenU5,

                            array_agg(subq.structure_sprayed) OVER (PARTITION BY subq.structure_sprayed) AS sprayed_values,
                            array_agg(subq.notsprayed_reason) FILTER (WHERE (subq.notsprayed_reason <> ''::text)) OVER (PARTITION BY subq.notsprayed_reason) AS notsprayed_reasons,
                            (array_length(array_agg(subq.structure_sprayed) OVER (PARTITION BY subq.structure_sprayed), 1) > 1) AS duplicate,
                            (('yes'::text = ANY (array_agg(subq.structure_sprayed) OVER (PARTITION BY subq.structure_sprayed))) AND (array_length(array_agg(subq.structure_sprayed) OVER (PARTITION BY subq.structure_sprayed), 1) > 1)) AS sprayed_duplicate
                        FROM (
                            SELECT
                                events.id AS event_id,
                                events.task_id,
                                events.event_date,
                                tasks.plan_identifier AS plan_id,
                                tasks.status AS task_status,
                                COALESCE(form_data ->> 'location_zone', ''::text) AS village_name,
                                (COALESCE(((events.form_data -> 'rooms_eligible'::text) ->> 0), '0'::text))::integer AS rooms_eligible,
                                (COALESCE(((events.form_data -> 'rooms_sprayed'::text) ->> 0), '0'::text))::integer AS rooms_sprayed,
                                COALESCE(((events.form_data -> 'eligibility'::text) ->> 0), 'Eligible'::text) AS eligibility,
                                COALESCE(((events.form_data -> 'structure_sprayed'::text) ->> 0), 'No'::text) AS structure_sprayed,
                                (events.form_data ->> 'start_time'::text) AS form_start_time,
                                (events.form_data ->> 'end_time'::text) AS form_end_time,
                                ((events.form_data -> 'business_status'::text) ->> 0) AS business_status,
                                COALESCE((events.form_data ->> 'sprayed_totalpop')::text, '0'::text)::integer AS sprayed_totalpop,
                                COALESCE((events.form_data ->> 'sprayed_totalmale')::text, '0'::text)::integer AS sprayed_totalmale,
                                COALESCE((events.form_data ->> 'sprayed_totalfemale')::text, '0'::text)::integer AS sprayed_totalfemale,
                                COALESCE((events.form_data ->> 'sprayed_males')::text, '0'::text)::integer AS sprayed_males,
                                COALESCE((events.form_data ->> 'sprayed_females')::text, '0'::text)::integer AS sprayed_females,
                                COALESCE((events.form_data ->> 'sprayed_pregwomen')::text, '0'::text)::integer AS sprayed_pregwomen,
                                COALESCE((events.form_data ->> 'sprayed_childrenU5')::text, '0'::text)::integer AS sprayed_childrenU5,
                                COALESCE((events.form_data ->> 'notsprayed_totalpop')::text, '0'::text)::integer AS notsprayed_totalpop,
                                COALESCE((events.form_data ->> 'notsprayed_males')::text, '0'::text)::integer AS notsprayed_males,
                                COALESCE((events.form_data ->> 'notsprayed_females')::text, '0'::text)::integer AS notsprayed_females,
                                COALESCE((events.form_data ->> 'notsprayed_pregwomen')::text, '0'::text)::integer AS notsprayed_pregwomen,
                                COALESCE((events.form_data ->> 'notsprayed_childrenU5')::text, '0'::text)::integer AS notsprayed_childrenU5,
                                COALESCE(((events.form_data -> 'notsprayed_reason'::text) ->> 0), ''::text) AS notsprayed_reason
                            FROM (reveal.events
                            LEFT JOIN reveal.tasks ON (((tasks.identifier)::text = (events.task_id)::text)))
                            WHERE (((locations.id)::text = (events.base_entity_id)::text)
                            AND ((events.entity_type)::text = 'Structure'::text)
                            AND ((events.event_type)::text = 'Spray'::text)
                            AND ((tasks.plan_identifier)::text = (irs_plans.plan_id)::text))
                            ORDER BY (events.form_data ->> 'end_time'::text) DESC
                        ) subq
                        ORDER BY subq.form_end_time DESC, subq.structure_sprayed DESC, subq.rooms_sprayed DESC, subq.rooms_eligible DESC
                        LIMIT 1
                    ) events_query ON (true))
                    LEFT JOIN LATERAL (
                        SELECT
                            tasks.identifier,
                            tasks.server_version,
                            tasks.plan_identifier,
                            tasks.status,
                            tasks.business_status
                        FROM reveal.tasks
                        WHERE (((tasks.task_for)::text = (locations.id)::text)
                        AND ((tasks.status)::text <> 'Cancelled'::text)
                        AND ((tasks.plan_identifier)::text = (irs_plans.plan_id)::text))
                        ORDER BY tasks.server_version DESC
                        LIMIT 1
                    ) tasks_query ON (true))
                    WHERE ((locations.status)::text <> 'Inactive'::text);

CREATE INDEX IF NOT EXISTS irs_structures_structure_sprayed_idx ON irs_structures (structure_sprayed);
CREATE INDEX IF NOT EXISTS irs_structures_business_status_idx ON irs_structures (business_status);
CREATE INDEX IF NOT EXISTS irs_structures_sprayed_duplicate_idx ON irs_structures (sprayed_duplicate);
CREATE INDEX IF NOT EXISTS irs_structures_duplicate_idx ON irs_structures (duplicate);
CREATE INDEX IF NOT EXISTS irs_structures_notsprayed_reasons_idx ON irs_structures using GIN(notsprayed_reasons);
CREATE INDEX IF NOT EXISTS irs_structures_event_date_idx ON irs_structures (event_date);
CREATE INDEX IF NOT EXISTS irs_structures_task_id_idx ON irs_structures (task_id);
CREATE INDEX IF NOT EXISTS irs_structures_plan_id_idx ON irs_structures (plan_id);
CREATE INDEX IF NOT EXISTS irs_structures_plan_jurisdiction_id_idx ON irs_structures (plan_id, structure_jurisdiction_id);
CREATE INDEX IF NOT EXISTS irs_structures_structure_jurisdiction_idx ON irs_structures (structure_jurisdiction_id);
CREATE INDEX IF NOT EXISTS irs_structures_old_jurisdiction_idx ON irs_structures (old_jurisdiction_id);
CREATE UNIQUE INDEX IF NOT EXISTS irs_structures_structure_task_idx ON irs_structures (structure_id, task_id);
CREATE UNIQUE INDEX IF NOT EXISTS irs_structures_idx ON irs_structures (id);