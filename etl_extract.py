"""Runs various extracts from the opensrp database into the reveal warehouse raw tables"""

import json
from datetime import datetime
import configparser
import logging
import etl_global
from etl_database import store_reveal_query,fetch_opensrp_query,fetch_reveal_query
from etl_transform import transform_reveal_location_data

config = configparser.ConfigParser()
config.read('./etl_processor.conf')

def extract_opensrp_data(src,dst):
    """extracts the data from opensrp into the reveal warehouse"""

    if src == "core.task":
        sql = "SELECT json FROM " + src + " WHERE (json ->>  'lastModified')::timestamp > (NOW() - INTERVAL '" + etl_global.data_pull_interval + " HOUR')::timestamp ORDER BY json ->>  'lastModified'"
    elif src == "core.event":
        sql = "select max(full_json ->> 'dateCreated') as max_date from reveal.raw_events;"
        max_date = fetch_reveal_query(sql)
        if max_date[0][0] is None:
            sql = "SELECT json FROM " + src + " WHERE (json ->> 'dateCreated')::timestamp > (NOW() - INTERVAL '" + etl_global.data_pull_interval + " HOUR')::timestamp order by json ->>  'dateCreated';"
        else:
            sql = "SELECT json FROM " + src + " WHERE (json ->> 'dateCreated')::timestamp > (('" + max_date[0][0] + "')::date - INTERVAL '" + etl_global.data_pull_interval + " HOUR')::timestamp order by json ->>  'dateCreated';"
    elif src == "core.settings_metadata":
        sql = "select max((data ->> 'serverVersion')::integer) as server_version from reveal.raw_settings;"
        server_version = fetch_reveal_query(sql)
        if server_version[0][0] is None:
            sql = "SELECT json FROM " + src
        else:
            sql = "SELECT json FROM " + src + " WHERE (json ->> 'serverVersion')::integer > " + str(server_version[0][0])
    else:
        sql = "SELECT json FROM " + src

    data = fetch_opensrp_query(sql)
    record_count = len(data)
    logging.info('Found records in %s: %s',src,record_count)

    ### adding in structures that have been added
    for data_line in data:
        if ((src == "core.event") and (data_line[0]['eventType'] == 'Register_Structure')):
            sql = "select json from core.structure where json ->> 'id' = '" + data_line[0]['baseEntityId'] + "'"
            location = fetch_opensrp_query(sql)
            if len(location) == 0:
                logging.debug("no location found, adding from event")
                #sql = "insert into core.structure (id,json,server_version) values (" + data_line[0]['serverVersion'] + ",," + data_line[0]['serverVersion'] + "");"
                #{"id": "e643dd22-68fd-434f-8af5-cb3d4d787ce2", "type": "Feature", "geometry": {"type": "Point", "coordinates": [-15.116157084727519, 15.394756859242316]}, "properties": {"uid": "368f64f1-c15b-4a92-a050-fd206dd79de4", "type": "Residential Structure", "status": "Pending Review", "version": 0, "parentId": "0ca477ab-746b-43dd-8d12-c6d960a16ee6", "geographicLevel": 0, "effectiveStartDate": "2021-06-20T1258"}, "serverVersion": 130017}

            try:
                logging.info('New structures found, extracting new structures')
                create_reveal_raw_data("core.structure","reveal.raw_locations",location[0])
                #transform_reveal_location_data(location[0][0]['serverVersion'])
                #sql = "SELECT process_structure_geo_hierarchy_structure_queue();"
                #store_reveal_query(sql)
            except (Exception,) as error:
                logging.error('DATALINE missing location information: %s', error)
                #break THIS NEEDS TO BE TURNED ON AS THE ABOVE IS FIXED

        record_count = record_count - 1
        print("records left: " + str(record_count))
        create_reveal_raw_data(src,dst,data_line)

    if src in ("core.event","core.structures"):
        sql = "SELECT process_structure_geo_hierarchy_structure_queue();"
        store_reveal_query(sql)


def create_reveal_raw_data(src,dst,data_json):
    """inserts the data from opensrp into the reveal warehouse"""

    if src in ("core.location", "core.structure"):
        identifier = (data_json[0]['id'])
    elif src in ("core.client", "core.event"):
        identifier = (data_json[0]['_id'])
    elif src in ("core.settings_metadata"):
        identifier = (data_json[0]['uuid'])
    else:
        identifier = (data_json[0]['identifier'])

    full_json = json.dumps(data_json[0])

    if dst in ("reveal.raw_locations", "reveal.raw_settings"):
        sql = "INSERT INTO " + dst + " (id,server_version,data,synced) VALUES ('" + identifier + "',0,'" + full_json.replace("'",r"''") + "','true') ON CONFLICT (id) DO UPDATE SET data = '" + full_json.replace("'",r"''") + "';"
    elif dst in ("reveal.raw_structures"):
        sql = "INSERT INTO " + dst + " (id,full_json,date_created,last_updated) VALUES ('" + identifier + "','" + full_json.replace("'",r"''") + "','" + str(datetime.now()) + "','" + str(datetime.now()) + "') ON CONFLICT (id) DO UPDATE full_json = '" + full_json.replace("'",r"''") + "', last_updated = '" + str(datetime.now()) + "'"
    else:
        sql = "INSERT INTO " + dst + " (id,server_version,full_json,synced,date_created,last_updated) VALUES ('" + identifier + "',0,'" + full_json.replace("'",r"''") + "','true','" + str(datetime.now()) + "','" + str(datetime.now()) + "') ON CONFLICT (id) DO UPDATE SET full_json = '" + full_json.replace("'",r"''") + "', last_updated = '" + str(datetime.now()) + "'"

    store_reveal_query(sql)
