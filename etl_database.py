"""Database class"""

import os
import sys
import fnmatch
import logging
import configparser
import psycopg2
import redis
from etl_global import drop_views_and_recreate

config = configparser.ConfigParser()
config.read('./etl_processor.conf')

try:
    conn_o = psycopg2.connect(host=config['db_opensrp']['host'], port=config['db_opensrp']['port'], database=config['db_opensrp']['database'], user=config['db_opensrp']['user'], password=config['db_opensrp']['password'])
except psycopg2.OperationalError as err:
    logging.error('connection failure on opensrp %s', err)
    sys.exit()

try:
    conn_r = psycopg2.connect(host=config['db_reveal']['host'], port=config['db_reveal']['port'], database=config['db_reveal']['database'], user=config['db_reveal']['user'], password=config['db_reveal']['password'])
except psycopg2.OperationalError as err:
    logging.error('connection failure on reveal %s', err)
    sys.exit()

try:
    conn_d = redis.Redis(host=config['redis']['host'], port=config['redis']['port'], password=config['redis']['password'], db=config['redis']['database'])
except psycopg2.OperationalError as err:
    logging.error('connection failure on redis %s', err)
    sys.exit()


def fetch_reveal_query(sql):
    """executes SELECT against the reveal database"""

    cur = conn_r.cursor()
    cur.execute("set schema 'public';")
    cur.execute(sql)
    data = cur.fetchall()
    cur.close()
    return data


def fetch_opensrp_query(sql):
    """executes SELECT against the opensrp database"""

    cur = conn_o.cursor()
    cur.execute(sql)
    data = cur.fetchall()
    cur.close()
    return data


def store_reveal_query(sql):
    """executes INSERT against the reveal database"""

    try:
        with conn_r:
            with conn_r.cursor() as cur:
                cur.execute("set schema 'reveal';")
                cur.execute(sql)
    except (Exception, psycopg2.DatabaseError) as exc:
        logging.error("insert failure %s", exc)


def load_materialised_view_concur(name):
    """calles REFRESH on materialized views in the reveal database with recurrsion"""

    try:
        with conn_r:
            with conn_r.cursor() as cur:
                cur.execute("set schema 'reveal';")
                cur.execute("SELECT refresh_mat_view('" + name + "',TRUE)")
    except (Exception, psycopg2.DatabaseError) as exc:
        logging.error("insert failure %s", exc)


def load_materialised_view_uncur(name):
    """calles REFRESH on materialized views in the reveal database with recurrsion"""

    if drop_views_and_recreate == "True":
        try:
            with conn_r:
                with conn_r.cursor() as cur_drop:
                    cur_drop.execute("set schema 'reveal';")
                    cur_drop.execute("DROP MATERIALIZED VIEW IF EXISTS " + name + " CASCADE;")
                    logging.debug("view %s has been dropped", name)
        except (Exception, psycopg2.DatabaseError) as exc:
            logging.error("drop failure %s", exc)

    try:
        with conn_r:
            with conn_r.cursor() as cur:
                cur.execute("set schema 'reveal';")
                cur.execute("SELECT refresh_mat_view('" + name + "',FALSE);")
    except (Exception, psycopg2.DatabaseError) as error:
        if error.pgcode == '42P01':
            logging.debug("missing view %s attempting to create", name)
            sql_file = find_and_load_materialized_view(name)
            try:
                with conn_r:
                    with conn_r.cursor() as cur:
                        cur.execute(open(sql_file, "r").read())
                        logging.debug("creating view %s successful", name)
            except (Exception, psycopg2.DatabaseError) as exc:
                logging.error("creating materialized_view failure %s", exc)
        else:
            logging.error("insert failure %s", exc)


def find_and_load_materialized_view(name):
    """calles REFRESH on materialized views in the reveal database with recurrsion"""

    # force scan custom first
    scan_folder = os.listdir('custom')
    logging.debug("scanning custom folder")
    for file in scan_folder:
        if fnmatch.fnmatch(file, "*" + name + ".sql"):
            logging.debug("found materilized_view %s", file)
            return 'custom/' + file

    list_of_folders =  os.listdir('materialized_views')
    for folder in list_of_folders:
        scan_folder = os.listdir('materialized_views/' + folder)
        for file in scan_folder:
            if fnmatch.fnmatch(file, "*" + name + ".sql"):
                logging.debug("found materilized_view %s", file)
                return 'materialized_views/' + folder + '/' + file


def close_connections():
    """closed the database connections"""

    conn_o.close()
    conn_r.close()
    conn_d.close()


def flush_redis_database():
    """flushes the redis cache"""

    conn_d.flushdb()
