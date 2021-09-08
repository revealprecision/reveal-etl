#!/usr/bin/python3.8

# Things still to do
# * add lockfile process
# * add thrading
# * finish rest of raw

import configparser
import sys
import inspect
import logging
import getopt
import json
from time import sleep
from importlib import reload
from filelock import Timeout, FileLock

import etl_global
import etl_database
from etl_extract import extract_opensrp_data
import etl_transform

reload(sys)


def main(argv):
    config = configparser.ConfigParser()
    config.read('./etl_processor.conf')

    logging.basicConfig(level=logging.DEBUG, filename=config['default']['logfile'], filemode='a', format='%(asctime)s - %(levelname)s - %(message)s')

    try:
        opts, args = getopt.getopt(argv, "hc:e:f:i:", ["help","check", "element", "function", "interval"])
    except getopt.GetoptError as err:
        logging.info('Use -h for help')
        logging.info(err)
        sys.exit(2)
    for opt, arg in opts:
        if opt in ("-h", "--help"):
            print(
'''
Check the etl_processor.conf for settings

-h or --help [this menu]
-c or --check [the ability to run SELECT but not INSERT]
-f= or --function= [options: all extract transform load flush]
-e= or --element= [options: plans jurisdictions locations structures settings clients tasks events]
-i= or --interval [amount of hours to pull a delta of data]
'''
            )
            exit(1)
        if opt in ("-c", "--check"):
            logging.info('Checking only not inserting')
            checkonly = arg
        if opt in ("-e", "--element"):
            etl_global.element = arg
        if opt in ("-f", "--function"):
            etl_global.function = arg
        if opt in ("-i", "--interval"):
            etl_global.data_pull_interval = arg
            logging.info('data_pull_interval set to: ' + data_pull_interval )
            exit(1)
        else:
            etl_global.data_pull_interval = config['process']['data_pull_interval']

    logging.info("Start process")

    logging.info("function set to {0}".format(etl_global.function))
    logging.info("element set to {0}".format(etl_global.element))

    if (etl_global.function == "all" or etl_global.function == "extract"):
        logging.info("starting extract")
        if (etl_global.element == "all" or etl_global.element == 'std' or etl_global.element == "plans"):
            logging.info("running extract of plans")
            extract_opensrp_data('core.plan','reveal.raw_plans')
        if (etl_global.element == 'all' or etl_global.element == 'locations'):
            logging.info("running extract of locations")
            extract_opensrp_data('core.location','reveal.raw_jurisdictions') ## Note the stuff up here where language changes
        if (etl_global.element == 'all' or etl_global.element == 'structures'):
            logging.info("running extract of structure")
            extract_opensrp_data('core.structure','reveal.raw_locations') ## Note the stuff up here where language changes
        if (etl_global.element == 'all' or etl_global.element == 'std' or etl_global.element == 'settings'):
            logging.info("running extract of settings")
            extract_opensrp_data('core.settings_metadata','reveal.raw_settings')
        if (etl_global.element == 'all' or etl_global.element == 'std' or etl_global.element == 'clients'):
            logging.info("running extract of clients")
            extract_opensrp_data('core.client','reveal.raw_clients')
        if (etl_global.element == 'all' or etl_global.element == 'std' or etl_global.element == 'tasks'):
            logging.info("running extract of tasks")
            extract_opensrp_data('core.task','reveal.raw_tasks')
        if (etl_global.element == 'all' or etl_global.element == 'std' or etl_global.element == 'events'):
            logging.info("running extract of events")
            extract_opensrp_data('core.event','reveal.raw_events')


    if (etl_global.function == "all" or etl_global.function == "transform"):
        if (etl_global.element == "all" or etl_global.element == 'std' or etl_global.element == "plans"):
            logging.info("running transform of plans")
            etl_transform.transform_reveal_plan_data()
        if (etl_global.element == 'all' or etl_global.element == 'jurisdictions'):
            logging.info("running transform of jurisdictions")
            etl_transform.transform_reveal_jurisdiction_data()
        if (etl_global.element == 'all' or etl_global.element == 'locations'):
            logging.info("running transform of locations")
            etl_transform.transform_reveal_location_data()
        if (etl_global.element == 'all' or etl_global.element == 'std' or etl_global.element == 'settings'):
            logging.info("running transform of settings")
            etl_transform.transform_reveal_settings_data()
        if (etl_global.element == 'all' or etl_global.element == 'std' or etl_global.element == 'clients'):
            logging.info("running transform of clients")
            etl_transform.transform_reveal_client_data()
        if (etl_global.element == 'all' or etl_global.element == 'std' or etl_global.element == 'tasks'):
            logging.info("running transform of tasks")
            etl_transform.transform_reveal_task_data()
        if (etl_global.element == 'all' or etl_global.element == 'std' or etl_global.element == 'events'):
            logging.info("running transform of events")
            etl_transform.transform_reveal_event_data()


    if (etl_global.function == "all" or etl_global.function == "load"):
        views = json.loads(config['materialized_views_load']['generic'])
        for view in views:
            logging.info("running load of {0}".format(view))
            etl_database.load_materialised_view_uncur(str(view))

        views = json.loads(config['materialized_views_load']['implementation'])
        for view in views:
            logging.info("running load of {0}".format(view))
            etl_database.load_materialised_view_uncur(str(view))


    if (etl_global.function == "all" or etl_global.function == "flush"):
        logging.info("flushing redis db")
        etl_database.flush_redis_database()

    logging.info("closing database connections")
    etl_database.close_connections()

    logging.info("End process")


if __name__ == '__main__':
    main(sys.argv[1:])
