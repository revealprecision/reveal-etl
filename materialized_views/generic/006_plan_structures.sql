SET schema 'reveal';

DROP MATERIALIZED VIEW IF EXISTS plan_structures CASCADE;

CREATE MATERIALIZED VIEW IF NOT EXISTS plan_structures
AS
SELECT DISTINCT ON (locations.id, tasks_query.task_id)
    public.uuid_generate_v5('6ba7b810-9dad-11d1-80b4-00c04fd430c8', concat(locations.id, tasks_query.task_id)) AS id,
    locations.id AS structure_id,
    locations.jurisdiction_id AS jurisdiction_id,
    plan_jurisdictions.plan_id AS plan_id,
    tasks_query.task_id AS task_id
FROM locations AS locations
RIGHT JOIN plan_jurisdictions plan_jurisdictions ON locations.jurisdiction_id = plan_jurisdictions.jurisdiction_id AND is_focus_area = 't'
LEFT JOIN LATERAL (
    SELECT
        subq.task_id AS task_id,
        subq.plan_id AS plan_id
    FROM (
        SELECT
            DISTINCT ON (tasks.identifier)
            tasks.identifier AS task_id,
            tasks.plan_identifier AS plan_id
        FROM tasks as tasks
        WHERE locations.id = tasks.task_for
        AND tasks.status != 'Cancelled'
    ) AS subq
) AS tasks_query ON true
WHERE locations.status != 'Inactive';

CREATE INDEX IF NOT EXISTS plan_structures_task_id_idx ON plan_structures (task_id);
CREATE INDEX IF NOT EXISTS plan_structures_plan_id_idx ON plan_structures (plan_id);
CREATE INDEX IF NOT EXISTS plan_structures_plan_jurisdiction_id_idx ON plan_structures (plan_id, jurisdiction_id);
CREATE INDEX IF NOT EXISTS plan_structures_jurisdiction_idx ON plan_structures (jurisdiction_id);
CREATE UNIQUE INDEX IF NOT EXISTS plan_structures_structure_task_idx ON plan_structures (structure_id, task_id);
CREATE UNIQUE INDEX IF NOT EXISTS plan_structures_idx ON plan_structures (id);