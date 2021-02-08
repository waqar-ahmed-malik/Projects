import psycopg2
import io
import json
from airflow.hooks.base_hook import BaseHook


class CustomPostgresHook(BaseHook):
    def __init__(self, postgres_conn_id):
        self.connection = self.get_connection(postgres_conn_id)
        self.conn = psycopg2.connect(
                                        host=self.connection.host,
                                        database=self.connection.schema,
                                        user=self.connection.login,
                                        password=self.connection.password)

        
    def execute_query(self, query: str) -> list:
        cursor = self.conn.cursor()
        cursor.execute(query)
        self.conn.commit()
        if cursor.description:
            data = list(cursor.fetchall())
            cursor.close()

            return data
        else:
            cursor.close()

    def file_to_postgres(self, file_path: str, table: str, column_list: list, separator: str = ',', null: str = ''):
        column_list = ['"{}"'.format(column) for column in column_list]
        cursor = self.conn.cursor()
        try:
            with open(file_path, 'r') as f:
                cursor.copy_from(f, table, sep=separator, columns=(tuple(column_list)), null=null)
                self.conn.commit()
                cursor.close()
        except (Exception, psycopg2.DatabaseError) as error:
            print("Error: {}".format(error))
            self.conn.rollback()
            cursor.close()
            raise CustomException('Unable To Transfer {} to {}'.format(file_path, table))

    
    def dict_to_postgres_table(self, query: str, dictionary: dict):
        cursor = self.conn.cursor()
        try:
            cursor.execute(query, (json.dumps(dictionary),))
            self.conn.commit()
            cursor.close()
        except (Exception, psycopg2.DatabaseError) as error:
            print("Error: {}".format(error))
            self.conn.rollback()
            cursor.close()
            raise CustomException('Unable To Insert Records')
