import argparse
import configparser
import json
import logging
import sys
import os
import time

from datetime import datetime, timedelta


def make_parser():
    """ Creates an ArgumentParser to parse the command line options. """
    parser = argparse.ArgumentParser(description='Migrate subscription product list from sql server to mariadb.')
    parser.add_argument('-p', '--profile', dest='profile', type=str, required=False, default='dev')
    return parser

def get_current_datetime():
    now = datetime.now()
    dt = now.strftime("%Y%m%d%H%M%S")
    return dt

def make_log_dir(currentDateTime):
    os.system("mkdir migration.log." + currentDateTime)

def execute(profile, table_list, executionDateTime):
    command = ''
    loggingFolder = './migration.log.' + executionDateTime
    isFirst = True
    
    for table in table_list:
        if isFirst != True:
            command += ' && sleep 3s && python migrate_mssqldb_to_mariadb_table.py -p ' + profile + ' -t ' + table + ' -o 0 -f ' + loggingFolder + ' &> ' + loggingFolder + '/' + table + '.log'
        else:
            command += ' python migrate_mssqldb_to_mariadb_table.py -p ' + profile + '  -t ' + table + ' -o 0 -f ' + loggingFolder + ' &> ' + loggingFolder + '/' + table + '.log'
            isFirst = False
    
    if len(command) > 0:
        command += ' &'
    logging.info(command)
    os.system(command)

def main():
    logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
    logging.info('process start!!!!')

    parser = make_parser()
    arguments = parser.parse_args()
    logging.info('arguments: %s' % arguments)

    profile = arguments.profile
    table_list = []

    try:
        executionDateTime = get_current_datetime()
        make_log_dir(executionDateTime)

        # table dependencies block start
        table_list.append('tabla_config_key_name')
        # table dependencies block end
        
        execute(profile, table_list, executionDateTime)
        table_list.clear()
    finally:
        logging.info('process done!!!!')


if __name__ == "__main__":
    main()
