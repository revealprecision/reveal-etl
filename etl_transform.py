"""Runs various transformations from the raw data into the reveal warehouse"""

import json
import uuid

import etl_global
import etl_database

def transform_reveal_plan_data():
    """tranforms the data of the plan extract into the reveal warehouse"""

    sql = "SELECT full_json ->> 'identifier' AS identifier, full_json ->> 'date' AS created_at, full_json ->> 'version' AS version, full_json ->> 'name' AS name, full_json ->> 'title' AS title, full_json ->> 'status' AS status, full_json ->> 'fi_status' AS fi_status, full_json ->> 'fi_reason' AS fi_reason, full_json -> 'useContext' -> 0 ->> 'valueCodableConcept'  AS intervention_type, full_json ->> 'date' AS date, full_json -> 'effectivePeriod' ->> 'start' AS effective_period_start, full_json -> 'effectivePeriod' ->> 'end' AS effective_period_end, full_json ->> 'jurisdiction' AS jurisdiction_object FROM reveal.raw_plans;"

    data = etl_database.fetch_reveal_query(sql)

    for data_line in data:
        sql = "INSERT INTO reveal.plans (identifier, created_at, version, name, title, status, fi_status, fi_reason, intervention_type, date, effective_period_start, effective_period_end) VALUES ('" + str(data_line[0]) + "', '" + str(data_line[1]) + "', '" + str(data_line[2]) + "', '" + str(data_line[3]) + "', '" + str(data_line[4]) + "', '" + str(data_line[5]) + "', '" + str(data_line[6]) + "', '" + str(data_line[7]) + "', '" + str(data_line[8]) + "', '" + str(data_line[9]) + "', '" + str(data_line[10]) + "', '" + str(data_line[11]) + "')  ON CONFLICT (identifier) DO UPDATE SET version = '" + str(data_line[2]) + "', name = '" + str(data_line[3]) + "', title = '" + str(data_line[4]) + "', status = '" + str(data_line[5]) + "', effective_period_start = '" + str(data_line[10]) + "', effective_period_end = '" + str(data_line[11]) + "'"

        etl_database.store_reveal_query(sql)

        sql = "DELETE FROM reveal.plan_jurisdiction WHERE plan_id = '" + str(data_line[0]) + "'"
        etl_database.store_reveal_query(sql)

        for j in json.loads(data_line[12]):
            #line_id = uuid.uuid5(uuid.NAMESPACE_DNS,(str(j['code']) + str(data_line[0])))
            sql = "INSERT INTO reveal.plan_jurisdiction (id, jurisdiction_id, plan_id) VALUES ('" + str(uuid.uuid4()) + "', '" + str(j['code']) + "', '" + str(data_line[0]) + "')"
            etl_database.store_reveal_query(sql)


def transform_reveal_jurisdiction_data():
    """tranforms the data of the jurisdiction extract into the reveal warehouse"""

    sql = "SELECT full_json ->> 'id' AS id, date_created AS created_at, full_json ->> 'id' AS uid, COALESCE(full_json -> 'properties' ->> 'parentId','') AS parent_id, full_json ->> 'id' AS code, full_json ->> 'type' AS type, full_json -> 'properties' ->> 'name' AS name, full_json -> 'properties' ->> 'status' AS status, COALESCE(ST_GeomFromGeoJSON(full_json ->> 'geometry'),'0101000020E610000000000000000000803BDF4F8D976E823F') AS geometry, full_json -> 'properties' ->> 'geographicLevel' AS geographic_level, COALESCE(full_json ->> 'effective_start_date','infinity') AS effective_start_date, COALESCE(full_json ->> 'effective_end_date','infinity') AS effective_end_date, full_json -> 'properties' ->> 'version' AS version, full_json ->> 'serverVersion' AS server_version FROM reveal.raw_jurisdictions;"

    data = etl_database.fetch_reveal_query(sql)

    for data_line in data:
        sql = "INSERT INTO reveal.jurisdictions (id, created_at, uid, parent_id, code, type, name, status, geometry, geographic_level, effective_start_date, effective_end_date, version, server_version ) VALUES ('" + str(data_line[0]) + "','" + str(data_line[1]) + "','" + str(data_line[2]) + "','" + str(data_line[3]) + "','" + str(data_line[4]) + "','" + str(data_line[5]) + "','" + str(data_line[6]).replace("'",r"''") + "','" + str(data_line[7]) + "','" + data_line[8] + "','" + str(data_line[9]) + "','" + data_line[10] + "','" + data_line[11] + "','" + str(data_line[12]) + "','" + str(data_line[13]) + "') ON CONFLICT (id) DO UPDATE SET status = '" + str(data_line[7]) + "', geometry = '" + data_line[8] + "', geographic_level = '" + str(data_line[9]) + "', version = '" + str(data_line[12]) + "', server_version = '" + str(data_line[13]) + "';"

        etl_database.store_reveal_query(sql)


def transform_reveal_location_data(server_version = 0):
    """tranforms the data of the location extract into the reveal warehouse"""

    sql = "SELECT data -> 'id' AS id, COALESCE(data ->> 'created_at','infinity') AS created_at, data -> 'id' AS uid, data -> 'properties' -> 'parentId' AS jurisdiction_id, data -> 'id' AS code, data -> 'type' AS type, data -> 'properties' -> 'name' AS name, data -> 'properties' -> 'status' AS status, COALESCE(ST_GeomFromGeoJSON(data ->> 'geometry')) AS geometry, data -> 'properties' -> 'geographicLevel' AS geographic_level, COALESCE(data ->> 'effective_start_date','infinity') AS effective_start_date, COALESCE(data ->> 'effective_end_date','infinity') AS effective_end_date, data -> 'properties' -> 'version' AS version, data -> 'serverVersion' AS server_version FROM reveal.raw_locations where (data ->> 'serverVersion')::int >= (" + str(server_version) + ")::int;"

    data = etl_database.fetch_reveal_query(sql)

    for data_line in data:
        sql = "INSERT INTO reveal.locations (id, created_at, uid, jurisdiction_id, code, type, name, status, geometry, geographic_level, effective_start_date, effective_end_date, version, server_version) VALUES ('" + str(data_line[0]) + "','" + str(data_line[1]) + "','" + str(data_line[2]) + "','" + str(data_line[3]) + "','" + str(data_line[4]) + "','" + str(data_line[5]) + "','" + str(data_line[6]).replace("'",r"''") + "','" + str(data_line[7]) + "','" + data_line[8] + "','" + str(data_line[9]) + "','" + data_line[10] + "','" + data_line[11] + "','" + str(data_line[12]) + "','" + str(data_line[13]) + "') ON CONFLICT (id) DO UPDATE SET status = '" + str(data_line[7]) + "', geometry = '" + data_line[8] + "', geographic_level = '" + str(data_line[9]) + "', version = '" + str(data_line[12]) + "', server_version = '" + str(data_line[13]) + "';"

        etl_database.store_reveal_query(sql)


def transform_reveal_client_data():
    """tranforms the data of the client extract into the reveal warehouse"""

    sql = "SELECT full_json -> '_id' AS id, COALESCE(date_created,'infinity') AS created_at, full_json -> 'baseEntityId' AS baseentityid, full_json -> 'dateCreated' AS datecreated, COALESCE(full_json ->> 'datevoided','infinity') AS datevoided, full_json -> 'firstName' AS firstname, COALESCE(full_json ->> 'middleName','') AS middlename, full_json -> 'lastName' AS lastname, full_json -> 'gender' AS gender, COALESCE(full_json ->> 'birthdate','infinity') AS birthdate, full_json -> 'identifiers' AS identifiers, full_json -> 'attributes' AS attributes, full_json -> 'relationships' AS relationships, full_json -> 'addresses' AS addresses, full_json -> 'attributes' -> 'residence' AS residence, full_json -> 'birthdateApprox' AS birthdateapprox, full_json -> 'deathdateApprox' AS deathdateapprox, full_json -> 'clientApplicationVersion' AS clientapplicationversion, full_json -> 'clientDatabaseVersion' AS clientdatabaseversion, COALESCE(full_json -> 'server_version','0') AS server_version FROM reveal.raw_clients WHERE (full_json ->> 'dateCreated')::timestamp > (NOW() - interval '" + etl_global.data_pull_interval + " HOUR')::timestamp;"

    data = etl_database.fetch_reveal_query(sql)

    for data_line in data:
        sql = "INSERT INTO reveal.clients (id, created_at, baseentityid, datecreated, datevoided, firstname, middlename, lastname, gender, birthdate, identifiers, attributes, relationships, addresses, residence, birthdateapprox, deathdateapprox, clientapplicationversion, clientdatabaseversion, server_version) VALUES ('" + str(data_line[0]) + "','" + str(data_line[1]) + "','" + str(data_line[2]) + "','" + str(data_line[3]) + "','" + str(data_line[4]) + "','" + str(data_line[5]) + "','" + str(data_line[6]) + "','" + str(data_line[7]) + "','" + str(data_line[8]) + "','" + str(data_line[9]) + "','" + json.dumps(data_line[10]).replace("'",r"''") + "','" + json.dumps(data_line[11]).replace("'",r"''") + "','" + json.dumps(data_line[12]).replace("'",r"''") + "','" + str(data_line[13]) + "','" + str(data_line[14]) + "','" + str(data_line[15]) + "','" + str(data_line[16]) + "','" + str(data_line[17]) + "','" + str(data_line[18]) + "','" + str(data_line[19]) + "') ON CONFLICT (id) DO UPDATE SET firstName = '" + str(data_line[5]) + "', middlename = '" + str(data_line[6]) + "', lastName = '" + str(data_line[7]) + "', gender = '" + str(data_line[8]) + "', birthdate = '" + str(data_line[9]) + "', identifiers = '" + json.dumps(data_line[10]).replace("'",r"''") + "', attributes = '" + json.dumps(data_line[11]).replace("'",r"''") + "', relationships = '" + json.dumps(data_line[12]).replace("'",r"''") + "', addresses = '" + str(data_line[13]) + "', residence = '" + str(data_line[14]) + "', birthdateApprox = '" + str(data_line[15]) + "', deathdateApprox = '" + str(data_line[16]) + "', clientApplicationVersion = '" + str(data_line[17]) + "', clientDatabaseVersion = '" + str(data_line[18]) + "', server_version = '" + str(data_line[19]) + "';"

        etl_database.store_reveal_query(sql)


def transform_reveal_task_data():
    """tranforms the data of the task extract into the reveal warehouse"""

    sql = "SELECT full_json -> 'identifier' AS identifier, date_created AS created_at, full_json -> 'planIdentifier' AS plan_identifier, full_json -> 'groupIdentifier' AS group_identifier, full_json -> 'status' AS status, full_json -> 'businessStatus' AS business_status, full_json -> 'priority' AS priority, full_json -> 'code' AS code, full_json -> 'description' AS description, full_json -> 'focus' AS focus, full_json -> 'for' AS task_for, COALESCE(full_json -> 'executionPeriod' ->> 'start','infinity') AS execution_start_date, COALESCE(full_json -> 'executionPeriod' ->> 'end','infinity') AS execution_end_date, full_json -> 'owner' AS owner, COALESCE(full_json -> 'note','{}') AS note, server_version AS server_version FROM reveal.raw_tasks WHERE (full_json ->>  'lastModified')::timestamp > (NOW() - interval '" + etl_global.data_pull_interval + " HOUR')::timestamp;"

    data = etl_database.fetch_reveal_query(sql)

    for data_line in data:
        sql = "INSERT INTO reveal.tasks (identifier, created_at, plan_identifier, group_identifier, status, business_status, priority, code, description, focus, task_for, execution_start_date, execution_end_date, owner, note, server_version) VALUES ('" + str(data_line[0]) + "','" + str(data_line[1]) + "','" + str(data_line[2]) + "','" + str(data_line[3]) + "','" + str(data_line[4]) + "','" + str(data_line[5]) + "','" + str(data_line[6]) + "','" + str(data_line[7]).replace("'",r"''") + "','" + str(data_line[8]).replace("'",r"''") + "','" + str(data_line[9]) + "','" + data_line[10] + "','" + data_line[11] + "','" + str(data_line[12]) + "','" + str(data_line[13]) + "','" + str(data_line[14]) + "','" + str(data_line[15]) + "') ON CONFLICT (identifier) DO UPDATE SET status = '" + str(data_line[4]) + "', business_status = '" + data_line[5] + "', priority = '" + str(data_line[6]) + "', code = '" + str(data_line[7]).replace("'",r"''") + "', description = '" + str(data_line[8]).replace("'",r"''") + "', focus = '" + str(data_line[9]) + "', task_for = '" + str(data_line[10]) + "', execution_start_date = '" + str(data_line[11]) + "', execution_end_date = '" + str(data_line[12]) + "', owner = '" + str(data_line[13]) + "';"

        etl_database.store_reveal_query(sql)


def transform_reveal_event_data():
    """tranforms the data of the event extract into the reveal warehouse"""

    sql = "SELECT full_json -> '_id' AS id, date_created AS created_at, full_json -> 'baseEntityId' AS base_entity_id, full_json -> 'locationId' AS location_id, full_json -> 'eventType' AS event_type, full_json -> 'providerId' AS provider_id, full_json -> 'dateCreated' AS date_created, full_json -> 'eventDate' AS event_date, full_json -> 'entityType' AS entity_type, full_json -> 'details' ->> 'taskIdentifier' AS task_id, full_json -> 'teamId' AS team_id, full_json -> 'serverVersion' AS server_version, COALESCE(full_json -> 'details' ->> 'location_id', full_json -> 'details' ->> 'locationUUID', full_json ->> 'baseEntityId') AS structure_id, full_json -> 'details' ->> 'planIdentifier' AS plan_id, full_json -> 'details' AS details FROM reveal.raw_events WHERE (full_json ->>  'dateCreated')::timestamp > (NOW() - interval '" + etl_global.data_pull_interval + " HOUR')::timestamp;"

    data = etl_database.fetch_reveal_query(sql)

    for data_line in data:
        #0 id, 1created_at, 2base_entity_id, 3location_id, 4event_type, 5provider_id, 6date_created, 7event_date, 8entity_type, 9task_id, 10team_id, 11server_version, 12structure_id, 13plan_id,14details

        sql = "INSERT INTO reveal.events (id,created_at,base_entity_id,location_id,event_type,provider_id,date_created,event_date,entity_type,task_id,team_id,server_version,structure_id,plan_id,details) VALUES ('" + str(data_line[0]) + "','" + str(data_line[1]) + "','" + str(data_line[2]) + "','" + str(data_line[3]) + "','" + str(data_line[4]) + "','" + str(data_line[5]) + "','" + str(data_line[6]) + "','" + str(data_line[7]) + "','" + str(data_line[8]) + "','" + str(data_line[9]) + "','" + str(data_line[10]) + "','" + str(data_line[11]) + "','" + str(data_line[12]) + "','" + str(data_line[13]) + "','" + json.dumps(data_line[14]).replace("'",r"''") + "') ON CONFLICT (id) DO NOTHING;"

        etl_database.store_reveal_query(sql)

        sql = "SELECT json_object_agg(fieldCode, response) AS data_object FROM ( SELECT CASE WHEN (line_data ->> 'fieldCode' = '163137AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA') THEN 'start_time' WHEN (line_data ->> 'fieldCode' = '163138AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA') THEN 'end_time' WHEN (line_data ->> 'fieldCode' = '163152AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA') THEN 'device_phone_number' ELSE (line_data ->> 'fieldCode') END AS fieldCode, regexp_replace((line_data ->> 'values')::text, '[''\"\[\]]+', '', 'gi') AS response FROM ( SELECT jsonb_array_elements(full_json -> 'obs') AS line_data FROM reveal.raw_events WHERE ((id)::text = '" + str(data_line[0]) + "')) status_line ) as form_object;"

        data_object = etl_database.fetch_reveal_query(sql)

        sql = "UPDATE reveal.events SET form_data = '" + json.dumps(data_object[0][0]).replace("'",r"''") + "' WHERE id = '" + str(data_line[0]) + "'"
        etl_database.store_reveal_query(sql)

        # print("for GCE extract this stays useful")
        # sql = "SELECT id AS event_id, line_data ->> 'fieldCode' AS fieldCode, line_data -> 'values' ->> 0 AS response FROM ( SELECT id, jsonb_array_elements(full_json -> 'obs') AS line_data FROM reveal.raw_events WHERE (id)::text = '" + str(data_line[0]) + "') status_line;"

        # data_split = etl_database.fetch_reveal_query(sql)

        # for s in data_split:
        #    sql = "INSERT INTO reveal.form_data (id,event_id,field_code,response) VALUES ('" + str(uuid.uuid4()) + "','" + str(s[0]) + "','" + str(s[1]) + "','" + str(s[2]) + "') ON CONFLICT (event_id,field_code) DO NOTHING"

        #    etl_database.store_reveal_query(sql)


def transform_reveal_settings_data():
    """tranforms the data of the settings extract into the reveal warehouse"""

    sql = "SELECT data -> 'uuid' AS uuid, data -> 'key' AS key, data -> 'settingIdentifier' AS identifier, COALESCE(data -> 'value', data -> 'values') as data from reveal.raw_settings;"

    data = etl_database.fetch_reveal_query(sql)

    for data_line in data:
        sql = "INSERT INTO opensrp_settings (uuid,key,identifier,data) VALUES ('" + str(data_line[0]) + "','" + str(data_line[1]) + "','" + str(data_line[2]) + "','" +  json.dumps(data_line[3]).replace("'",r"''") + "') ON CONFLICT (uuid) DO UPDATE SET data = '" + json.dumps(data_line[3]).replace("'",r"''") + "';"
        etl_database.store_reveal_query(sql)
