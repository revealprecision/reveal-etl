#!/usr/bin/python3.8

"""main ETL thread"""

import configparser
import sys
import logging
import getopt
import json
from importlib import reload

import etl_global
import etl_database
from etl_extract import extract_opensrp_data
import etl_transform

reload(sys)


def main(argv):
    """ main ETL function"""

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
            sys.exit()
        if opt in ("-e", "--element"):
            etl_global.element = arg
        if opt in ("-f", "--function"):
            etl_global.function = arg
        if opt in ("-i", "--interval"):
            etl_global.data_pull_interval = arg
            logging.info('data_pull_interval set to: %s', etl_global.data_pull_interval)
            sys.exit()
        else:
            etl_global.data_pull_interval = config['process']['data_pull_interval']

    logging.info("Start process")

    logging.info("function set to %s", etl_global.function)
    logging.info("element set to %s", etl_global.element)

    if etl_global.function in ("all", "extract"):
        logging.info("starting extract")
        if etl_global.element in ("all", 'std', "plans"):
            logging.info("running extract of plans")
            extract_opensrp_data('core.plan','reveal.raw_plans')
        if etl_global.element in ('all', 'locations'):
            logging.info("running extract of locations")
            extract_opensrp_data('core.location','reveal.raw_jurisdictions') ## Note the stuff up here where language changes
        if etl_global.element in ('all', 'structures'):
            logging.info("running extract of structure")
            extract_opensrp_data('core.structure','reveal.raw_locations') ## Note the stuff up here where language changes
        if etl_global.element in ('all', 'std', 'settings'):
            logging.info("running extract of settings")
            extract_opensrp_data('core.settings_metadata','reveal.raw_settings')
        if etl_global.element in ('all', 'std', 'clients'):
            logging.info("running extract of clients")
            extract_opensrp_data('core.client','reveal.raw_clients')
        if etl_global.element in ('all', 'std', 'tasks'):
            logging.info("running extract of tasks")
            extract_opensrp_data('core.task','reveal.raw_tasks')
        if etl_global.element in ('all', 'std', 'events'):
            logging.info("running extract of events")
            extract_opensrp_data('core.event','reveal.raw_events')


    if etl_global.function in ("all", "transform"):
        if etl_global.element in ('all', 'std', "plans"):
            logging.info("running transform of plans")
            etl_transform.transform_reveal_plan_data()
        if etl_global.element in ('all', 'jurisdictions'):
            logging.info("running transform of jurisdictions")
            etl_transform.transform_reveal_jurisdiction_data()
        if etl_global.element in ('all', 'locations'):
            logging.info("running transform of locations")
            etl_transform.transform_reveal_location_data()
        if etl_global.element in ('all', 'std', 'settings'):
            logging.info("running transform of settings")
            etl_transform.transform_reveal_settings_data()
        if etl_global.element in ('all', 'std', 'clients'):
            logging.info("running transform of clients")
            etl_transform.transform_reveal_client_data()
        if etl_global.element in ('all', 'std', 'tasks'):
            logging.info("running transform of tasks")
            etl_transform.transform_reveal_task_data()
        if etl_global.element in ('all', 'std', 'events'):
            logging.info("running transform of events")
            etl_transform.transform_reveal_event_data()


    if etl_global.function in ('all', "load"):
        views = json.loads(config['materialized_views_load']['generic'])
        for view in views:
            logging.info("running load of %s", view)
            etl_database.load_materialised_view_uncur(str(view))

        views = json.loads(config['materialized_views_load']['implementation'])
        for view in views:
            logging.info("running load of %s", view)
            etl_database.load_materialised_view_uncur(str(view))


    if etl_global.function in ('all', "flush"):
        logging.info("flushing redis db")
        etl_database.flush_redis_database()

    logging.info("closing database connections")
    etl_database.close_connections()

    logging.info("End process")


if __name__ == '__main__':
    main(sys.argv[1:])
