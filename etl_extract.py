import json
from datetime import datetime
import configparser
import logging

config = configparser.ConfigParser()
config.read('./etl_processor.conf')

import etl_global
from etl_database import store_reveal_query,fetch_opensrp_query
from etl_transform import transform_reveal_location_data

def extract_opensrp_data(src,dst):
    ##ToDo here we need to split this into a more targeted select by looking at the data in the dst
    if (src == "core.task"):
        sql = "SELECT json FROM " + src + " WHERE (json ->>  'lastModified')::timestamp > (NOW() - INTERVAL '" + etl_global.data_pull_interval + " HOUR')::timestamp"
    elif (src == "core.event"):
        sql = "SELECT json FROM " + src + " WHERE (json ->>  'dateCreated')::timestamp > (NOW() - INTERVAL '" + etl_global.data_pull_interval + " HOUR')::timestamp"
    else:
        sql = "SELECT json FROM " + src

    data = fetch_opensrp_query(sql)
    logging.info('Found records in {0}: {1}'.format(src,len(data)))

    ### adding in structures that have been added
    for data_line in data:
        if ((src == "core.event") and (data_line[0]['eventType'] == 'Register_Structure')):
            print(data_line[0]['serverVersion'])
            sql = "select json from core.structure where json ->> 'id' = '" + data_line[0]['baseEntityId'] + "'"
            location = fetch_opensrp_query(sql)
            if (len(location) == 0):
                logging.debug("no location found, adding from event")
                #sql = "insert into core.structure (id,json,server_version) values (" + data_line[0]['serverVersion'] + ",," + data_line[0]['serverVersion'] + "");"
                #{"id": "e643dd22-68fd-434f-8af5-cb3d4d787ce2", "type": "Feature", "geometry": {"type": "Point", "coordinates": [-15.116157084727519, 15.394756859242316]}, "properties": {"uid": "368f64f1-c15b-4a92-a050-fd206dd79de4", "type": "Residential Structure", "status": "Pending Review", "version": 0, "parentId": "0ca477ab-746b-43dd-8d12-c6d960a16ee6", "geographicLevel": 0, "effectiveStartDate": "2021-06-20T1258"}, "serverVersion": 130017}

            try:
                logging.info('New structures found, loading new structures')
                create_reveal_raw_data("core.structure","reveal.raw_locations",location[0])
                transform_reveal_location_data(location[0][0]['serverVersion'])
                sql = "SELECT process_structure_geo_hierarchy_structure_queue();"
                store_reveal_query(sql)
            except (Exception,) as error:
                logging.error('DATALINE missing location information: {0}'.format(error))
                #break THIS NEEDS TO BE TURNED ON AS THE ABOVE IS FIXED

        create_reveal_raw_data(src,dst,data_line)


def create_reveal_raw_data(src,dst,data_json):
    if (src == "core.location" or src == "core.structure"):
        identifier = (data_json[0]['id'])
    elif (src == "core.client" or src == "core.event"):
        identifier = (data_json[0]['_id'])
    elif (src == "core.settings_metadata"):
        identifier = (data_json[0]['uuid'])
    else:
        identifier = (data_json[0]['identifier'])

    full_json = json.dumps(data_json[0])

    if (dst == "reveal.raw_locations" or dst == "reveal.raw_settings"):
        sql = "INSERT INTO " + dst + " (id,server_version,data,synced) VALUES ('" + identifier + "',0,'" + full_json.replace("'",r"''") + "','true') ON CONFLICT (id) DO UPDATE SET data = '" + full_json.replace("'",r"''") + "';"
    elif (dst == "reveal.raw_structures"):
        sql = "INSERT INTO " + dst + " (id,full_json,date_created,last_updated) VALUES ('" + identifier + "','" + full_json.replace("'",r"''") + "','" + str(datetime.now()) + "','" + str(datetime.now()) + "') ON CONFLICT (id) DO UPDATE full_json = '" + full_json.replace("'",r"''") + "', last_updated = '" + str(datetime.now()) + "'"
    else:
        sql = "INSERT INTO " + dst + " (id,server_version,full_json,synced,date_created,last_updated) VALUES ('" + identifier + "',0,'" + full_json.replace("'",r"''") + "','true','" + str(datetime.now()) + "','" + str(datetime.now()) + "') ON CONFLICT (id) DO UPDATE SET full_json = '" + full_json.replace("'",r"''") + "', last_updated = '" + str(datetime.now()) + "'"

    store_reveal_query(sql)