'''
1. Connect to DB
2. Execute raw SQL
3. Execute stored procedures
4. Close connection
'''

import argparse, json
import sqlalchemy as sa 
from configparser import ConfigParser


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
            raise Exception('Unrecognized db_type')

    def create_engine(self):
        connection_string = self.generate_connection_string()
        engine = sa.create_engine(connection_string)
        return engine

    def execute_sql_from_string(self, raw_sql_string):
        connection = self.engine.raw_connection()
        try:
            cursor = connection.cursor()
            for result in cursor.execute(raw_sql_string, multi=True):
                print(f'result: {result}')
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
    
    def execute_stored_procedure(self, proc_name, proc_arguments_array=None):
        if not proc_arguments_array:
            proc_arguments_array = []
        connection = self.engine.connect()
        dbapi_connection = connection.connection
        try:
            cursor = dbapi_connection.cursor()
            cursor.callproc(proc_name, proc_arguments_array)
            result = list(cursor.fetchall())
            cursor.close()
            connection.commit()
        finally:
            connection.close()
        return result
        
def build_db(connection_name, credentials_file_location='config.ini', db_settings_file_location='settings.json'):
    connection = Connection(credentials_file_location=credentials_file_location, connection_name=connection_name)
    with open(db_settings_file_location) as f:
        data = json.load(f)
    db_settings = data[0]
    for sql_file in db_settings.get('raw_sql_file_locations', []):
        connection.execute_sql_from_file(sql_file)
    for sp_name in db_settings.get('stored_procedure_names', []):
        connection.execute_stored_procedure(sp_name)
    return f'Database built for connect {connection_name}'

if __name__ == '__main__':
    build_db('vw_reporting_test')


# def get_parser():
#     parser = argparse.ArgumentParser(description="Simple-DB-Builder")
#     parser.add_argument("--db_settings_file", type=str,
#                         help="location of file with database build settings")
#     parser.add_argument("--credentials_file", type=str,
#                         help="location of file with database connection info")
#     parser.add_argument("--connection_name", type=str,
#                         help="name of connection in credentials file")
#     return parser

# if __name__ == '__main__':
#     parser = get_parser()
#     args = parser.parse_args()
#     build_db(args.credentials_file, args.connection_name, args.db_settings_file)
