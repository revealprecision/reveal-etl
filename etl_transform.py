import json
import uuid

import etl_global
import etl_database

def transform_reveal_plan_data():
    sql = "SELECT full_json ->> 'identifier' AS identifier, full_json ->> 'date' AS created_at, full_json ->> 'version' AS version, full_json ->> 'name' AS name, full_json ->> 'title' AS title, full_json ->> 'status' AS status, full_json ->> 'fi_status' AS fi_status, full_json ->> 'fi_reason' AS fi_reason, full_json -> 'useContext' -> 0 ->> 'valueCodableConcept'  AS intervention_type, full_json ->> 'date' AS date, full_json -> 'effectivePeriod' ->> 'start' AS effective_period_start, full_json -> 'effectivePeriod' ->> 'end' AS effective_period_end, full_json ->> 'jurisdiction' AS jurisdiction_object FROM reveal.raw_plans;"

    data = etl_database.fetch_reveal_query(sql)

    for d in data:
        sql = "INSERT INTO reveal.plans (identifier, created_at, version, name, title, status, fi_status, fi_reason, intervention_type, date, effective_period_start, effective_period_end) VALUES ('" + str(d[0]) + "', '" + str(d[1]) + "', '" + str(d[2]) + "', '" + str(d[3]) + "', '" + str(d[4]) + "', '" + str(d[5]) + "', '" + str(d[6]) + "', '" + str(d[7]) + "', '" + str(d[8]) + "', '" + str(d[9]) + "', '" + str(d[10]) + "', '" + str(d[11]) + "')  ON CONFLICT (identifier) DO UPDATE SET version = '" + str(d[2]) + "', name = '" + str(d[3]) + "', title = '" + str(d[4]) + "', status = '" + str(d[5]) + "', effective_period_start = '" + str(d[10]) + "', effective_period_end = '" + str(d[11]) + "'"

        etl_database.store_reveal_query(sql)

        sql = "DELETE FROM reveal.plan_jurisdiction WHERE plan_id = '" + str(d[0]) + "'"
        etl_database.store_reveal_query(sql)

        for j in json.loads(d[12]):
            id = uuid.uuid5(uuid.NAMESPACE_DNS,(str(j['code']) + str(d[0])))
            sql = "INSERT INTO reveal.plan_jurisdiction (id, jurisdiction_id, plan_id) VALUES ('" + str(uuid.uuid4()) + "', '" + str(j['code']) + "', '" + str(d[0]) + "')"
            etl_database.store_reveal_query(sql)


def transform_reveal_jurisdiction_data():
    sql = "SELECT full_json ->> 'id' AS id, date_created AS created_at, full_json ->> 'id' AS uid, COALESCE(full_json -> 'properties' ->> 'parentId','') AS parent_id, full_json ->> 'id' AS code, full_json ->> 'type' AS type, full_json -> 'properties' ->> 'name' AS name, full_json -> 'properties' ->> 'status' AS status, COALESCE(ST_GeomFromGeoJSON(full_json ->> 'geometry'),'0101000020E610000000000000000000803BDF4F8D976E823F') AS geometry, full_json -> 'properties' ->> 'geographicLevel' AS geographic_level, COALESCE(full_json ->> 'effective_start_date','infinity') AS effective_start_date, COALESCE(full_json ->> 'effective_end_date','infinity') AS effective_end_date, full_json -> 'properties' ->> 'version' AS version, full_json ->> 'serverVersion' AS server_version FROM reveal.raw_jurisdictions;"

    data = etl_database.fetch_reveal_query(sql)

    for d in data:
        sql = "INSERT INTO reveal.jurisdictions (id, created_at, uid, parent_id, code, type, name, status, geometry, geographic_level, effective_start_date, effective_end_date, version, server_version ) VALUES ('" + str(d[0]) + "','" + str(d[1]) + "','" + str(d[2]) + "','" + str(d[3]) + "','" + str(d[4]) + "','" + str(d[5]) + "','" + str(d[6]).replace("'",r"''") + "','" + str(d[7]) + "','" + d[8] + "','" + str(d[9]) + "','" + d[10] + "','" + d[11] + "','" + str(d[12]) + "','" + str(d[13]) + "') ON CONFLICT (id) DO UPDATE SET status = '" + str(d[7]) + "', geometry = '" + d[8] + "', geographic_level = '" + str(d[9]) + "', version = '" + str(d[12]) + "', server_version = '" + str(d[13]) + "';"

        etl_database.store_reveal_query(sql)


def transform_reveal_location_data(serverVersion = 0):
    sql = "SELECT data -> 'id' AS id, COALESCE(data ->> 'created_at','infinity') AS created_at, data -> 'id' AS uid, data -> 'properties' -> 'parentId' AS jurisdiction_id, data -> 'id' AS code, data -> 'type' AS type, data -> 'properties' -> 'name' AS name, data -> 'properties' -> 'status' AS status, COALESCE(ST_GeomFromGeoJSON(data ->> 'geometry')) AS geometry, data -> 'properties' -> 'geographicLevel' AS geographic_level, COALESCE(data ->> 'effective_start_date','infinity') AS effective_start_date, COALESCE(data ->> 'effective_end_date','infinity') AS effective_end_date, data -> 'properties' -> 'version' AS version, data -> 'serverVersion' AS server_version FROM reveal.raw_locations where (data ->> 'serverVersion')::int >= (" + str(serverVersion) + ")::int;"

    data = etl_database.fetch_reveal_query(sql)

    for d in data:
        sql = "INSERT INTO reveal.locations (id, created_at, uid, jurisdiction_id, code, type, name, status, geometry, geographic_level, effective_start_date, effective_end_date, version, server_version) VALUES ('" + str(d[0]) + "','" + str(d[1]) + "','" + str(d[2]) + "','" + str(d[3]) + "','" + str(d[4]) + "','" + str(d[5]) + "','" + str(d[6]).replace("'",r"''") + "','" + str(d[7]) + "','" + d[8] + "','" + str(d[9]) + "','" + d[10] + "','" + d[11] + "','" + str(d[12]) + "','" + str(d[13]) + "') ON CONFLICT (id) DO UPDATE SET status = '" + str(d[7]) + "', geometry = '" + d[8] + "', geographic_level = '" + str(d[9]) + "', version = '" + str(d[12]) + "', server_version = '" + str(d[13]) + "';"

        etl_database.store_reveal_query(sql)


def transform_reveal_client_data():
    sql = "SELECT full_json -> '_id' AS id, COALESCE(date_created,'infinity') AS created_at, full_json -> 'baseEntityId' AS baseentityid, full_json -> 'dateCreated' AS datecreated, COALESCE(full_json ->> 'datevoided','infinity') AS datevoided, full_json -> 'firstName' AS firstname, COALESCE(full_json ->> 'middleName','') AS middlename, full_json -> 'lastName' AS lastname, full_json -> 'gender' AS gender, COALESCE(full_json ->> 'birthdate','infinity') AS birthdate, full_json -> 'identifiers' AS identifiers, full_json -> 'attributes' AS attributes, full_json -> 'relationships' AS relationships, full_json -> 'addresses' AS addresses, full_json -> 'attributes' -> 'residence' AS residence, full_json -> 'birthdateApprox' AS birthdateapprox, full_json -> 'deathdateApprox' AS deathdateapprox, full_json -> 'clientApplicationVersion' AS clientapplicationversion, full_json -> 'clientDatabaseVersion' AS clientdatabaseversion, COALESCE(full_json -> 'server_version','0') AS server_version FROM reveal.raw_clients WHERE (full_json ->> 'dateCreated')::timestamp > (NOW() - interval '" + etl_global.data_pull_interval + " HOUR')::timestamp;"

    data = etl_database.fetch_reveal_query(sql)

    for d in data:
        sql = "INSERT INTO reveal.clients (id, created_at, baseentityid, datecreated, datevoided, firstname, middlename, lastname, gender, birthdate, identifiers, attributes, relationships, addresses, residence, birthdateapprox, deathdateapprox, clientapplicationversion, clientdatabaseversion, server_version) VALUES ('" + str(d[0]) + "','" + str(d[1]) + "','" + str(d[2]) + "','" + str(d[3]) + "','" + str(d[4]) + "','" + str(d[5]) + "','" + str(d[6]) + "','" + str(d[7]) + "','" + str(d[8]) + "','" + str(d[9]) + "','" + json.dumps(d[10]).replace("'",r"''") + "','" + json.dumps(d[11]).replace("'",r"''") + "','" + json.dumps(d[12]).replace("'",r"''") + "','" + str(d[13]) + "','" + str(d[14]) + "','" + str(d[15]) + "','" + str(d[16]) + "','" + str(d[17]) + "','" + str(d[18]) + "','" + str(d[19]) + "') ON CONFLICT (id) DO UPDATE SET firstName = '" + str(d[5]) + "', middlename = '" + str(d[6]) + "', lastName = '" + str(d[7]) + "', gender = '" + str(d[8]) + "', birthdate = '" + str(d[9]) + "', identifiers = '" + json.dumps(d[10]).replace("'",r"''") + "', attributes = '" + json.dumps(d[11]).replace("'",r"''") + "', relationships = '" + json.dumps(d[12]).replace("'",r"''") + "', addresses = '" + str(d[13]) + "', residence = '" + str(d[14]) + "', birthdateApprox = '" + str(d[15]) + "', deathdateApprox = '" + str(d[16]) + "', clientApplicationVersion = '" + str(d[17]) + "', clientDatabaseVersion = '" + str(d[18]) + "', server_version = '" + str(d[19]) + "';"

        etl_database.store_reveal_query(sql)


def transform_reveal_task_data():
    sql = "SELECT full_json -> 'identifier' AS identifier, date_created AS created_at, full_json -> 'planIdentifier' AS plan_identifier, full_json -> 'groupIdentifier' AS group_identifier, full_json -> 'status' AS status, full_json -> 'businessStatus' AS business_status, full_json -> 'priority' AS priority, full_json -> 'code' AS code, full_json -> 'description' AS description, full_json -> 'focus' AS focus, full_json -> 'for' AS task_for, COALESCE(full_json -> 'executionPeriod' ->> 'start','infinity') AS execution_start_date, COALESCE(full_json -> 'executionPeriod' ->> 'end','infinity') AS execution_end_date, full_json -> 'owner' AS owner, COALESCE(full_json -> 'note','{}') AS note, server_version AS server_version FROM reveal.raw_tasks WHERE (full_json ->>  'lastModified')::timestamp > (NOW() - interval '" + etl_global.data_pull_interval + " HOUR')::timestamp;"

    data = etl_database.fetch_reveal_query(sql)

    for d in data:
        sql = "INSERT INTO reveal.tasks (identifier, created_at, plan_identifier, group_identifier, status, business_status, priority, code, description, focus, task_for, execution_start_date, execution_end_date, owner, note, server_version) VALUES ('" + str(d[0]) + "','" + str(d[1]) + "','" + str(d[2]) + "','" + str(d[3]) + "','" + str(d[4]) + "','" + str(d[5]) + "','" + str(d[6]) + "','" + str(d[7]).replace("'",r"''") + "','" + str(d[8]).replace("'",r"''") + "','" + str(d[9]) + "','" + d[10] + "','" + d[11] + "','" + str(d[12]) + "','" + str(d[13]) + "','" + str(d[14]) + "','" + str(d[15]) + "') ON CONFLICT (identifier) DO UPDATE SET status = '" + str(d[4]) + "', business_status = '" + d[5] + "', priority = '" + str(d[6]) + "', code = '" + str(d[7]).replace("'",r"''") + "', description = '" + str(d[8]).replace("'",r"''") + "', focus = '" + str(d[9]) + "', task_for = '" + str(d[10]) + "', execution_start_date = '" + str(d[11]) + "', execution_end_date = '" + str(d[12]) + "', owner = '" + str(d[13]) + "';"

        etl_database.store_reveal_query(sql)


def transform_reveal_event_data():
    sql = "SELECT full_json -> '_id' AS id, date_created AS created_at, full_json -> 'baseEntityId' AS base_entity_id, full_json -> 'locationId' AS location_id, full_json -> 'eventType' AS event_type, full_json -> 'providerId' AS provider_id, full_json -> 'dateCreated' AS date_created, full_json -> 'eventDate' AS event_date, full_json -> 'entityType' AS entity_type, full_json -> 'details' ->> 'taskIdentifier' AS task_id, full_json -> 'teamId' AS team_id, full_json -> 'serverVersion' AS server_version, COALESCE(full_json -> 'details' ->> 'location_id', full_json -> 'details' ->> 'locationUUID', full_json ->> 'baseEntityId') AS structure_id, full_json -> 'details' ->> 'planIdentifier' AS plan_id, full_json -> 'details' AS details FROM reveal.raw_events WHERE (full_json ->>  'dateCreated')::timestamp > (NOW() - interval '" + etl_global.data_pull_interval + " HOUR')::timestamp;"

    data = etl_database.fetch_reveal_query(sql)

    for d in data:
        #0 id, 1created_at, 2base_entity_id, 3location_id, 4event_type, 5provider_id, 6date_created, 7event_date, 8entity_type, 9task_id, 10team_id, 11server_version, 12structure_id, 13plan_id,14details

        sql = "INSERT INTO reveal.events (id,created_at,base_entity_id,location_id,event_type,provider_id,date_created,event_date,entity_type,task_id,team_id,server_version,structure_id,plan_id,details) VALUES ('" + str(d[0]) + "','" + str(d[1]) + "','" + str(d[2]) + "','" + str(d[3]) + "','" + str(d[4]) + "','" + str(d[5]) + "','" + str(d[6]) + "','" + str(d[7]) + "','" + str(d[8]) + "','" + str(d[9]) + "','" + str(d[10]) + "','" + str(d[11]) + "','" + str(d[12]) + "','" + str(d[13]) + "','" + json.dumps(d[14]).replace("'",r"''") + "') ON CONFLICT (id) DO NOTHING;"

        etl_database.store_reveal_query(sql)

        sql = "SELECT json_object_agg(fieldCode, response) AS data_object FROM ( SELECT CASE WHEN (line_data ->> 'fieldCode' = '163137AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA') THEN 'start_time' WHEN (line_data ->> 'fieldCode' = '163138AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA') THEN 'end_time' WHEN (line_data ->> 'fieldCode' = '163152AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA') THEN 'device_phone_number' ELSE (line_data ->> 'fieldCode') END AS fieldCode, regexp_replace((line_data ->> 'values')::text, '[''\"\[\]]+', '', 'gi') AS response FROM ( SELECT jsonb_array_elements(full_json -> 'obs') AS line_data FROM reveal.raw_events WHERE ((id)::text = '" + str(d[0]) + "')) status_line ) as form_object;"

        data_object = etl_database.fetch_reveal_query(sql)

        sql = "UPDATE reveal.events SET form_data = '" + json.dumps(data_object[0][0]).replace("'",r"''") + "' WHERE id = '" + str(d[0]) + "'"
        etl_database.store_reveal_query(sql)

        # print("for GCE extract this stays useful")
        # sql = "SELECT id AS event_id, line_data ->> 'fieldCode' AS fieldCode, line_data -> 'values' ->> 0 AS response FROM ( SELECT id, jsonb_array_elements(full_json -> 'obs') AS line_data FROM reveal.raw_events WHERE (id)::text = '" + str(d[0]) + "') status_line;"

        # data_split = etl_database.fetch_reveal_query(sql)

        # for s in data_split:
        #    sql = "INSERT INTO reveal.form_data (id,event_id,field_code,response) VALUES ('" + str(uuid.uuid4()) + "','" + str(s[0]) + "','" + str(s[1]) + "','" + str(s[2]) + "') ON CONFLICT (event_id,field_code) DO NOTHING"

        #    etl_database.store_reveal_query(sql)


def transform_reveal_settings_data():
    sql = "SELECT data -> 'uuid' AS uuid, data -> 'key' AS key, data -> 'settingIdentifier' AS identifier, COALESCE(data -> 'value', data -> 'values') as data from reveal.raw_settings;"

    data = etl_database.fetch_reveal_query(sql)

    for d in data:
        sql = "INSERT INTO opensrp_settings (uuid,key,identifier,data) VALUES ('" + str(d[0]) + "','" + str(d[1]) + "','" + str(d[2]) + "','" +  json.dumps(d[3]).replace("'",r"''") + "') ON CONFLICT (uuid) DO UPDATE SET data = '" + json.dumps(d[3]).replace("'",r"''") + "';"
        etl_database.store_reveal_query(sql)