import sqlalchemy as sa 
from configparser import ConfigParser




class Connection():
    def __init__(self, connection_name, credentials_file_location='config.ini'):
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
        if raw_sql_string:
            connection = self.engine.raw_connection()
            try:
                cursor = connection.cursor()
                results = []
                for result in cursor.execute(raw_sql_string, multi=True):
                    results.append(result)
                cursor.close()
                connection.commit()
            finally:
                connection.close()
            return results

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
