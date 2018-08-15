'''
1. Connect to DB
2. Execute raw SQL
3. Execute stored procedures
4. Close connection
'''

import argparse, json, logging, datetime
import sqlalchemy as sa 
from configparser import ConfigParser


logging.basicConfig(
    filename="log.log",
    level=logging.DEBUG,
    format="%(asctime)s:%(levelname)s:%(message)s"
)

class Connection():
    def __init__(self, credentials_file_location, connection_name):
        credentials_config = ConfigParser()
        credentials_config.read(credentials_file_location)
        self.host = credentials_config.get(connection_name, "host")
        self.port = credentials_config.get(connection_name, "port")
        self.default_schema = credentials_config.get(connection_name, "default_schema")
        self.username = credentials_config.get(connection_name, "username")
        self.password = credentials_config.get(connection_name, "password")
        self.db_type = credentials_config.get(connection_name, "db_type")
        self.engine = self.create_engine()

    def generate_connection_string(self):
        if self.db_type == 'mysql':
            return f'mysql+mysqlconnector://{self.username}:{self.password}@{self.host}/{self.default_schema}'
        else: 
            logging.error('Unrecognized db_type in Connection instance.')
            raise Exception('Unrecognized db_type')

    def create_engine(self):
        connection_string = self.generate_connection_string()
        engine = sa.create_engine(connection_string)
        return engine

    def execute_sql_from_string(self, raw_sql_string):
        if raw_sql_string:
            connection = self.engine.raw_connection()
            try:
                cursor = connection.cursor()
                for result in cursor.execute(raw_sql_string, multi=True):
                    logging.debug('SQL command executed. %s.', result)
                cursor.close()
                connection.commit()
            finally:
                connection.close()
            return result

    def execute_sql_from_file(self, filepath):
        with open(filepath, 'r') as sql_file:
            raw_sql_string = sql_file.read()
        result = self.execute_sql_from_string(raw_sql_string)
        return result
    
    def execute_stored_procedure(self, sp_name, proc_arguments_array=None):
        if not proc_arguments_array:
            proc_arguments_array = []
        connection = self.engine.raw_connection()
        try:
            cursor = connection.cursor()
            cursor.callproc(sp_name, proc_arguments_array)
            cursor.close()
            connection.commit()
        finally:
            connection.close()
        return None


def execute_raw_sql_from_settings_json(connection, raw_sql_to_execute):
    if raw_sql_to_execute:
        logging.info('Attempting to execute raw SQL: %s.', raw_sql_to_execute)
        start_time = datetime.datetime.now()
        connection.execute_sql_from_string(raw_sql_to_execute)
        end_time = datetime.datetime.now()
        duration = (end_time - start_time).total_seconds()
        logging.info('Raw SQL executed in %s seconds.', duration)
    return None


def execute_sql_file_list_from_settings_json(connection, sql_file_list):
    for sql_file in sql_file_list:
        logging.info('Attempting to execute SQL from file: %s.', sql_file)
        start_time = datetime.datetime.now()
        connection.execute_sql_from_file(sql_file)
        end_time = datetime.datetime.now()
        duration = (end_time - start_time).total_seconds()
        logging.info('SQL in file %s executed in %s seconds.', sql_file, duration)
    if sql_file_list:
        logging.info('All SQL files executed.')
    return None


def execute_stored_procedure_list_from_settings_json(connection, stored_procedure_list):
    for sp_name in stored_procedure_list:
        logging.info('Attempting to execute stored procedure %s.', sp_name)
        start_time = datetime.datetime.now()
        connection.execute_stored_procedure(sp_name)
        end_time = datetime.datetime.now()
        duration = (end_time - start_time).total_seconds()
        logging.info('Stored procedure %s executed in %s seconds.', sp_name, duration)
    if stored_procedure_list:
        logging.info('All stored procedures executed.')
    return None

        
def build_db(build_name, credentials_file_location='config.ini', db_settings_file_location='settings.json'):
    start_time = datetime.datetime.now()
    with open(db_settings_file_location) as f:
        data = json.load(f)
    db_settings_list = data
    if build_name:
        db_settings = [d for d in db_settings_list if d['build_name'] == build_name][0]
    else:
        logging.error(f'Provided build_name not found in {db_settings_file_location}')
        raise Exception(f'Provided build_name not found in {db_settings_file_location}')
    connection_name = db_settings['connection_name']
    raw_sql_to_execute = db_settings.get('raw_sql_to_execute')
    sql_file_list = db_settings.get('raw_sql_file_locations', [])
    stored_procedure_list = db_settings.get('stored_procedure_names', [])

    logging.info('Attempting to build %s on connection %s.', db_settings['build_name'], connection_name)
    
    connection = Connection(credentials_file_location=credentials_file_location, connection_name=connection_name)
    
    
    execute_raw_sql_from_settings_json(connection=connection, raw_sql_to_execute=raw_sql_to_execute)
    execute_sql_file_list_from_settings_json(connection=connection, sql_file_list=sql_file_list)
    execute_stored_procedure_list_from_settings_json(connection=connection, stored_procedure_list=stored_procedure_list)

    end_time = datetime.datetime.now()
    duration = (end_time - start_time).total_seconds()
    logging.info('Successfully built %s on connection %s in %s seconds.', db_settings['build_name'], connection_name, duration)
    return f'Database built for connection {connection_name}.'

# if __name__ == '__main__':
#     build_db('three_liter_monthly_pit')


def get_parser():
    parser = argparse.ArgumentParser(description="Simple-DB-Builder")
    parser.add_argument("-n", "--build_name", type=str,
                        help="location of file with database build settings")
    return parser

if __name__ == '__main__':
    parser = get_parser()
    args = parser.parse_args()
    build_db(args.build_name)
