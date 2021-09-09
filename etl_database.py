import psycopg2
import redis
import configparser
import logging
import os,fnmatch
from etl_global import drop_views_and_recreate

config = configparser.ConfigParser()
config.read('./etl_processor.conf')

try:
    conn_o = psycopg2.connect(host=config['db_opensrp']['host'], port=config['db_opensrp']['port'], database=config['db_opensrp']['database'], user=config['db_opensrp']['user'], password=config['db_opensrp']['password'])
except psycopg2.OperationalError as error:
    logging.error('connection failure on opensrp {0}'.format(error))
    exit(1)

try:
    conn_r = psycopg2.connect(host=config['db_reveal']['host'], port=config['db_reveal']['port'], database=config['db_reveal']['database'], user=config['db_reveal']['user'], password=config['db_reveal']['password'])
except psycopg2.OperationalError as error:
    logging.error('connection failure on reveal {0}'.format(error))
    exit(1)

try:
    conn_d = redis.Redis(host=config['redis']['host'], port=config['redis']['port'], password=config['redis']['password'], db=config['redis']['database'])
except psycopg2.OperationalError as error:
    logging.error('connection failure on redis {0}'.format(error))
    exit(1)


def fetch_reveal_query(sql):
    cur = conn_r.cursor()
    cur.execute("set schema 'public';")
    cur.execute(sql)
    data = cur.fetchall()
    cur.close()
    return (data)


def fetch_opensrp_query(sql):
    cur = conn_o.cursor()
    cur.execute(sql)
    data = cur.fetchall()
    cur.close()
    return (data)


def store_reveal_query(sql):
    try:
        with conn_r:
            with conn_r.cursor() as cur:
                cur.execute("set schema 'reveal';")
                cur.execute(sql)
    except (Exception, psycopg2.DatabaseError) as error:
        logging.error("insert failure {0}".format(error))


def load_materialised_view_concur(name):
    try:
        with conn_r:
            with conn_r.cursor() as cur:
                cur.execute("set schema 'reveal';")
                cur.execute("SELECT refresh_mat_view('" + name + "',TRUE)")
    except (Exception, psycopg2.DatabaseError) as error:
        logging.error("insert failure {0}".format(error))


def load_materialised_view_uncur(name):
    if (drop_views_and_recreate == "True"):
        try:
            with conn_r:
                with conn_r.cursor() as cur_drop:
                    cur_drop.execute("set schema 'reveal';")
                    cur_drop.execute("DROP MATERIALIZED VIEW IF EXISTS " + name + " CASCADE;")
                    logging.debug("view {0} has been dropped".format(name))
        except (Exception, psycopg2.DatabaseError) as error:
             logging.error("drop failure {0}".format(error))

    try:
        with conn_r:
            with conn_r.cursor() as cur:
                cur.execute("set schema 'reveal';")
                cur.execute("SELECT refresh_mat_view('" + name + "',FALSE);")
    except (Exception, psycopg2.DatabaseError) as error:
        if error.pgcode == '42P01':
            logging.debug("missing view {0} attempting to create".format(name))
            sql_file = find_and_load_materialized_view(name)
            try:
                with conn_r:
                    with conn_r.cursor() as cur:
                        cur.execute(open(sql_file, "r").read())
                        logging.debug("creating view {0} successful".format(name))
            except (Exception, psycopg2.DatabaseError) as error:
                logging.error("creating materialized_view failure {0}".format(error))
        else:
            logging.error("insert failure {0}".format(error))


def find_and_load_materialized_view(name):
    # force scan custom first
    scan_folder = os.listdir('custom')
    logging.debug("scanning custom folder")
    for file in scan_folder:
        if fnmatch.fnmatch(file, "*" + name + ".sql"):
            logging.debug("found materilized_view {0}".format(file))
            return('custom/' + file)
            break

    list_of_folders =  os.listdir('materialized_views')
    for folder in list_of_folders:
        scan_folder = os.listdir('materialized_views/' + folder)
        for file in scan_folder:
            if fnmatch.fnmatch(file, "*" + name + ".sql"):
                logging.debug("found materilized_view {0}".format(file))
                return('materialized_views/' + folder + '/' + file)
                break


def close_connections():
    conn_o.close()
    conn_r.close()
    conn_d.close()


def flush_redis_database():
    conn_d.flushdb()