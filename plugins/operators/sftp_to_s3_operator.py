import io
import datetime
import csv
import os
from airflow.operators import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.hooks.S3_hook import S3Hook
from hooks.custom_sftp_hook import CustomSftpHook



class S3ToPostgresOperator(BaseOperator):

    def __init__(self,
                 s3_conn_id,
                 s3_bucket,
                 s3_key,
                 sftp_conn_id,
                 sftp_file_path,
                 sftp_delimiter,
                 sale_date,
                 delimiter,
                 *args,
                 **kwargs):
        super().__init__(*args, **kwargs)

        self.s3_conn_id = s3_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.sftp_conn_id = sftp_conn_id
        self.sftp_file_path = sftp_file_path
        self.sftp_delimiter = sftp_delimiter
        self.delimiter = delimiter
        

    def execute(self, context):
        s3_conn = S3Hook(self.s3_conn_id)
        sftp_client = CustomSftpHook(self.sftp_conn_id)
        file_content = sftp_client.get_file_data_from_sftp(self.sftp_file_path)
        file_name = '/tmp/sftp_temp.csv'
        with io.TextIOWrapper(file_content, encoding="ISO-8859-1") as text_file:
            csv_reader = csv.reader(text_file, delimiter=self.sftp_delimiter)
            with open(csv_file_path, 'w', newline='') as f:
                csv_writer = csv.writer(f, delimiter = self.delimiter)
                for row in csv_reader:
                    record = row
                    new_record = list()
                    for record_data in record:
                        if isinstance(record_data, str):
                            record_data = record_data.replace('\\n', ' ').replace('\\r', ' ').replace('\n', ' ').replace('\r', ' ').replace('\\', ' ').replace(self.delimiter, '_')
                        new_record.append(record_data)
                    csv_writer.writerow(new_record)
            
        s3_conn.load_file(file_name, self.s3_key, self.s3_bucket, True)
        os.remove(file_name)

