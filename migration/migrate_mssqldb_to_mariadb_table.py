import argparse
import boto3
import configparser
import json
import logging
import pymssql
import pymysql
import os
import sys
import time
import threading

from datetime import datetime, timedelta


def make_parser():
    """ Creates an ArgumentParser to parse the command line options. """
    parser = argparse.ArgumentParser(description='Migrate table from sql server to mariadb.')
    parser.add_argument('-p', '--profile', dest='profile', type=str, required=True)
    parser.add_argument('-t', '--table', dest='table', type=str, required=True)
    parser.add_argument('-o', '--offset', dest='offset', type=int, required=False, default=-1)
    parser.add_argument('-f', '--folder', dest='folder', type=str, required=False, default='.')
    return parser

def get_current_datetime():
    now = datetime.now()
    return now.strftime("%Y-%m-%d %H:%M:%S")

def fetch_mssql_connection(config, profile):
    mssql_server = config[profile]['mssql_server']
    mssql_database = config[profile]['mssql_database']
    username = config[profile]['username']
    password = config[profile]['password']

    mssql_conn = pymssql.connect(
            server=mssql_server,
            user=username,
            password=password,
            database=mssql_database)
    return mssql_conn

def fetch_maria_connection(config, profile):
    maria_server = config[profile]['maria_server']
    maria_database = config[profile]['maria_database']
    username = config[profile]['username']
    password = config[profile]['password']

    maria_conn = pymysql.connect(
        host=maria_server,
        user=username,
        password=password,
        db=maria_database,
        charset='utf8mb4')
    return maria_conn

def fetch_tables_config(table):
    file = 'migrate_mssqldb_to_mariadb_table_configs.json'
    with open(file, "r") as stream:
            config = json.load(stream)
            stream.close()
    return config

def fetch_source_table_count(envConfig, profile, sql):
    table_rows_count = 0
    mssql_conn = fetch_mssql_connection(envConfig, profile)
    with mssql_conn.cursor(as_dict=True) as mssql_cursor:
        mssql_cursor.execute(sql)
        row = mssql_cursor.fetchone()
        table_rows_count = int(row['row_count'])
    mssql_conn.close()
    return table_rows_count

def fetch_target_table_count(envConfig, profile, sql):
    table_rows_count = 0
    maria_conn = fetch_maria_connection(envConfig, profile)
    with maria_conn.cursor() as maria_cursor:
        maria_cursor.execute(sql)
        row = maria_cursor.fetchone()
        table_rows_count = int(row[0])
    maria_conn.close()
    return table_rows_count

def fetch_migration_data(mssql_cursor, sql):
    logging.info('fetch_migration_data_sql: %s', sql)
    mssql_cursor.execute(sql)
    rows = mssql_cursor.fetchall()
    return rows

def batch_save(maria_conn, maria_cursor, sql, batchDataList):
    maria_cursor.executemany(sql, batchDataList)
    maria_conn.commit()
    logging.info('commit into mariaDB')

def migrate_data(profile, envConfig, table, migrationConfigs, folder, offset):
    migrationStartTime = int(round(time.time()))
    migrationStartTimeStr = get_current_datetime()
    logging.info('%s, migrate %s in %s: start...', migrationStartTimeStr, table, profile)
    process_start_time = time.perf_counter()

    table_config = migrationConfigs[table]
    limit = migrationConfigs['limit']
    huge_query_limit = migrationConfigs['huge_query_limit']

    mssql_table = table_config['mssql_table']
    maria_table = table_config['maria_table']
    mssql_table_fields = table_config['fields'] + table_config['cast_fields']
    maria_table_fields = table_config['fields'] + table_config['non_cast_fields']
    mssql_fetch_sql = table_config['mssql_fetch_sql'].replace('FIELDS', mssql_table_fields).replace('TABLE', mssql_table)
    maria_batch_insert_sql = table_config['maria_insert_sql'].replace('FIELDS', maria_table_fields).replace('TABLE', maria_table)

    mssql_rows_count_sql = migrationConfigs['table_rows_count_sql'].replace('TABLE', table_config['mssql_table'])
    maria_rows_count_sql = migrationConfigs['table_rows_count_sql'].replace('TABLE', table_config['maria_table'])
    mssql_table_rows_count = fetch_source_table_count(envConfig, profile, mssql_rows_count_sql)
    maria_table_rows_count = fetch_target_table_count(envConfig, profile, maria_rows_count_sql)
    
    if mssql_table_rows_count == maria_table_rows_count:
        logging.info('%s is up-to-date, mssql size: %s, mariadb size: %s', maria_table, mssql_table_rows_count, maria_table_rows_count)
        return

    logging.info('%s, migrate totla row count of %s in %s, mssql size: %s, mariadb size: %s', get_current_datetime(), table, profile, mssql_table_rows_count, maria_table_rows_count)

    data_batch_list = []
    if offset < 0:
        offset = maria_table_rows_count
    processedCount = offset
    counter = 0
    
    mssql_conn = fetch_mssql_connection(envConfig, profile)
    maria_conn = fetch_maria_connection(envConfig, profile)

    try:
        with mssql_conn.cursor(as_dict=True) as mssql_cursor:
            while offset < mssql_table_rows_count or processedCount < mssql_table_rows_count:
                maria_conn.ping()
                with maria_conn.cursor() as maria_cursor:
                    fatch_sql = mssql_fetch_sql.replace('OFFSET_NUM', str(offset)).replace('LIMIT_NUM', str(huge_query_limit))
                    rows = fetch_migration_data(mssql_cursor, fatch_sql)
                    offset += huge_query_limit
                    processedCount += len(rows)

                    for row in rows:        
                        data = []
                        column_names = maria_table_fields.split(',')
                        for name in column_names:
                            data.append(row[name.strip()])
                        data_batch_list.append(data)
                        counter += 1
                        if counter >= limit:
                            batch_save(maria_conn, maria_cursor, maria_batch_insert_sql, data_batch_list)
                            process_spent_time = int((time.perf_counter() - process_start_time) * 1000)
                            logging.info('migrated %s records... time elapsed %s msecs' % (limit, process_spent_time))
                            data_batch_list.clear()
                            counter = 0

            maria_conn.ping()
            with maria_conn.cursor() as maria_cursor:
                batch_save(maria_conn, maria_cursor, maria_batch_insert_sql, data_batch_list)
                process_spent_time = int((time.perf_counter() - process_start_time) * 1000)
                logging.info('migrated %s records... time elapsed %s msecs' % (len(data_batch_list), process_spent_time))
                data_batch_list.clear()
                counter = 0
                
    finally:
        maria_conn.close()
        mssql_conn.close()

    migrationEndTime = int(round(time.time()))
    migrationEndTimeStr = get_current_datetime()
    logging.info('migration start time of %s in %s: %s', table, profile, migrationStartTimeStr)
    logging.info('migration end time of %s in %s: %s', table, profile, migrationEndTimeStr)
    logging.info('migration total processed row count of %s in %s: %s', table, profile, processedCount)
    logging.info('migration time cost of %s in %s: %s seconds', table, profile, str(migrationEndTime - migrationStartTime))
        

def main():
    logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
    logging.getLogger('boto3').setLevel(logging.WARNING)
    logging.getLogger('botocore').setLevel(logging.WARNING)
    logging.getLogger('urllib3').setLevel(logging.WARNING)

    logging.info('%s, process start!!!!', get_current_datetime())

    parser = make_parser()
    arguments = parser.parse_args()
    logging.info('arguments: %s' % arguments)

    profile = arguments.profile
    folder = arguments.folder
    offset = arguments.offset
    config = configparser.ConfigParser()
    config.read('env.ini')

    table = arguments.table
    migrationConfigs = fetch_tables_config(table)
    if migrationConfigs[table] is not None:
        migrate_data(profile, config, table, migrationConfigs, folder, offset)
    else:
        logging.info('table %s config is missing!!!! skip current migration process.', table)


if __name__ == "__main__":
    main()
