import argparse, json, logging, datetime
from connections import Connection
from helpers import time_and_log


logging.basicConfig(
    filename="log.log",
    level=logging.DEBUG,
    format="%(asctime)s:%(levelname)s:%(message)s"
)

@time_and_log(logger=logging)
def execute_and_log_raw_sql(connection, raw_sql_to_execute):
    connection.execute_sql_from_string(raw_sql_to_execute)
    return None


@time_and_log(logger=logging)
def execute_and_log_sql_file(connection, file_location):
    connection.execute_sql_from_file(file_location)
    return None


@time_and_log(logger=logging)
def execute_and_log_stored_procedure(connection, stored_procedure_name):
    connection.execute_stored_procedure(stored_procedure_name)
    return None


def execute_raw_sql_from_settings_dict(connection, build_settings):
    raw_sql_to_execute = build_settings.get('raw_sql_to_execute')
    if raw_sql_to_execute:
        execute_and_log_raw_sql(connection, raw_sql_to_execute)
    return None


def execute_sql_file_list_from_settings_dict(connection, build_settings):
    sql_file_list = build_settings.get('raw_sql_file_locations', [])
    for sql_file in sql_file_list:
        execute_and_log_sql_file(connection, sql_file)
    return None


def execute_stored_procedure_list_from_settings_dict(connection, build_settings):
    stored_procedure_list = build_settings.get('stored_procedure_names', [])
    for sp_name in stored_procedure_list:
        execute_and_log_stored_procedure(connection, sp_name)
    return None


@time_and_log(logger=logging)
def build_db(build_name, credentials_file_location='config.ini', build_settings_file_location='settings.json'):
    with open(build_settings_file_location) as f:
        build_settings_list = json.load(f)
    build_settings = [d for d in build_settings_list if d['build_name'] == build_name][0]
    connection_name = build_settings['connection_name']
    
    if not build_settings:
        logging.error(f'Provided build_name not found in {build_settings_file_location}')
        raise Exception(f'Provided build_name not found in {build_settings_file_location}')
    
    connection = Connection(connection_name, credentials_file_location)
    
    execute_raw_sql_from_settings_dict(connection, build_settings)
    execute_sql_file_list_from_settings_dict(connection, build_settings)
    execute_stored_procedure_list_from_settings_dict(connection, build_settings)

    return f'Database built for connection {connection_name}.'


def get_parser():
    parser = argparse.ArgumentParser(description="Simple-DB-Builder")
    parser.add_argument("-n", "--build_name", type=str,
                        help="location of file with database build settings")
    return parser


if __name__ == '__main__':
    parser = get_parser()
    args = parser.parse_args()
    build_db(args.build_name)
