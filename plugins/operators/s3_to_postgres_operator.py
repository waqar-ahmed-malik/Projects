import io
import datetime
import csv
import os
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd
from airflow.operators import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.hooks.S3_hook import S3Hook
from hooks.custom_postgres_hook import CustomPostgresHook



class S3ToPostgresOperator(BaseOperator):

    def __init__(self, 
                postgres_conn_id,
                landing_table,  
                s3_conn_id, 
                s3_bucket, 
                s3_file_key, 
                column_list,
                separator=',',
                source=None,
                to_be_renamed=None,
                *args,
                **kwargs):

        super().__init__(*args, **kwargs)

        self.postgres_conn_id = postgres_conn_id
        self.source = source
        self.s3_conn_id = s3_conn_id
        self.s3_bucket = s3_bucket
        self.separator = separator
        self.s3_file_key = s3_file_key
        self.column_list = column_list
        self.to_be_renamed = to_be_renamed
        self.landing_table = landing_table
        


    # def execute(self, context):
    #     postgres_hook = CustomPostgresHook(self.postgres_conn_id)

    #     create_table_sql = "DROP TABLE IF EXISTS {};\nCREATE TABLE IF NOT EXISTS {}\n(".format(self.landing_table, self.landing_table)
    #     for column in self.column_list:
    #         create_table_sql += '{} VARCHAR NULL,'.format(column)
    #     create_table_sql = create_table_sql[:-1] + ');'
    #     postgres_hook.execute_query(create_table_sql)

    #     s3_conn = S3Hook(self.s3_conn_id)
    #     if s3_conn.check_for_key(self.s3_file_key, self.s3_bucket):
    #         s3_object = s3_conn.get_key(self.s3_file_key, self.s3_bucket).get()['Body'].read()
    #         data = io.BytesIO(s3_object) 
    #     csv_file_path = os.path.join(os.path.dirname(__file__), 'temp.csv')
    #     with io.TextIOWrapper(data, encoding="ISO-8859-1") as text_file:
    #         csv_reader = csv.reader(text_file)
    #         with open(csv_file_path, 'w', newline='') as f:
    #             csv_writer = csv.writer(f, delimiter = "^")
    #             next(csv_reader)
    #             for row in csv_reader:
    #                 new_record = list()
    #                 for record_data in row:
    #                     if type(record_data) == str and record_data.__contains__("\\"):
    #                         record_data = record_data.replace("\\", "").replace('\\n', '').replace('\\r', '').replace('\n', '').replace('\r', '').replace('\\', '').replace('^', '')
    #                     new_record.append(record_data)
    #                 csv_writer.writerow(new_record)
    #         self.column_list = [column_name.lower() for column_name in self.column_list]
    #         postgres_hook.file_to_postgres(csv_file_path, self.landing_table, self.column_list, separator='^')
    #     os.remove(csv_file_path)



    def execute(self, context):
        s3_conn = S3Hook(self.s3_conn_id)
        if s3_conn.check_for_key(self.s3_file_key, self.s3_bucket):
            s3_object = s3_conn.get_key(self.s3_file_key, self.s3_bucket).get()['Body'].read()
            data = io.BytesIO(s3_object) 
            file_df = pd.read_csv(data, sep=self.separator)
            if self.source:
                file_df.insert(2, 'source', self.source)
                file_df.insert(5, 'date_entered', datetime.datetime.utcnow())
            if self.to_be_renamed:
                file_df = file_df.rename(columns=self.to_be_renamed)
            file_df = file_df[self.column_list]
            
            csv_file_path = os.path.join(os.path.dirname(__file__), 'temp.csv')
            file_df.to_csv(csv_file_path, header=False, index=False, sep=self.separator)
            self.column_list = [column_name.lower() for column_name in self.column_list]
            postgres_hook = CustomPostgresHook(self.postgres_conn_id)
            create_table_sql = "DROP TABLE IF EXISTS {};\nCREATE TABLE IF NOT EXISTS {}\n(".format(self.landing_table, self.landing_table)
            for column in self.column_list:
                create_table_sql += '{} VARCHAR NULL,'.format(column)
            create_table_sql = create_table_sql[:-1] + ');'
            postgres_hook.execute_query(create_table_sql)
            postgres_hook.file_to_postgres(csv_file_path, self.landing_table, self.column_list, separator=self.separator)
            os.remove(csv_file_path)

