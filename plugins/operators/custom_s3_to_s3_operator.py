import pathlib
import io
import os
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd
from airflow.operators import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.hooks.S3_hook import S3Hook




class CustomS3ToS3Operator(BaseOperator):
    def __init__(self,
                 s3_conn_id,
                 s3_bucket,
                 s3_from_key,
                 s3_to_key,
                 delimiter,
                 key_column_list=None,
                 *args,
                 **kwargs):
        super().__init__(*args, **kwargs)

        self.s3_conn_id = s3_conn_id
        self.s3_bucket = s3_bucket
        self.s3_from_key = s3_from_key
        self.s3_to_key = s3_to_key
        self.key_column_list = key_column_list
        self.delimiter = delimiter
        

    def execute(self, context):
        s3_conn = S3Hook(self.s3_conn_id)
        if s3_conn.check_for_key(self.s3_from_key, self.s3_bucket) and s3_conn.check_for_key(self.s3_to_key, self.s3_bucket):
            existing_s3_object = s3_conn.get_key(self.s3_to_key, self.s3_bucket).get()['Body'].read()
            data = io.BytesIO(existing_s3_object) 
            existing_table = pq.read_table(data)
            existing_df = existing_table.to_pandas()
            current_s3_object = s3_conn.get_key(self.s3_from_key, self.s3_bucket).get()['Body'].read()
            current_data = io.BytesIO(current_s3_object) 
            current_df = pd.read_csv(current_data)
            final_df = existing_df.append(current_df)
        elif s3_conn.check_for_key(self.s3_from_key, self.s3_bucket) and not s3_conn.check_for_key(self.s3_to_key, self.s3_bucket):
            current_s3_object = s3_conn.get_key(self.s3_from_key, self.s3_bucket).get()['Body'].read()
            current_data = io.BytesIO(current_s3_object) 
            final_df = pd.read_csv(current_data)

        final_df = final_df.drop_duplicates(subset=self.key_column_list)
        final_df = final_df.loc[:,~final_df.columns.duplicated()]
        self.log.info("Existing: {} | Final: {}".format(existing_df['vin'].count(), final_df['vin'].count()))
        # self.log.info("Existing: {} | Final: {}".format(existing_df[self.key_column_list[0]].count(), final_df[self.key_column_list[0]].count()))
        # final_df = final_df.astype(str)
        # parquet_file_path = os.path.join(os.path.dirname(__file__), 'file.parquet')
        parquet_file_path = os.path.join(os.path.dirname(__file__), 'file.parquet')
        table = pa.Table.from_pandas(final_df)
        pq.write_table(table, parquet_file_path)   
        s3_conn.load_file(
            parquet_file_path,
            self.s3_to_key,
            bucket_name=self.s3_bucket,
            replace=True,
            encrypt=True,
        )
        os.remove(parquet_file_path)
        self.log.info("File: {} --> {} uploaded to {} successfully!".format(parquet_file_path, self.s3_from_key, self.s3_to_key))
        
    