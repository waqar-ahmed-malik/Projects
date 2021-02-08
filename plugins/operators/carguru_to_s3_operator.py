from airflow.operators import BaseOperator
from airflow.utils.decorators import apply_defaults
from hooks.carguru_hook import CarguruHook
from airflow.hooks.S3_hook import S3Hook
import csv
import os



class CarguruToS3Operator(BaseOperator):
    def __init__(self,
                 s3_conn_id,
                 s3_bucket,
                 s3_key,
                 app_id,
                 auth_token,
                 stats_date,
                 dealer_name,
                 delimiter,
                 *args,
                 **kwargs):
        super().__init__(*args, **kwargs)

        self.s3_conn_id = s3_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.app_id = app_id
        self.auth_token = auth_token
        self.stats_date = stats_date
        self.dealer_name = dealer_name
        self.delimiter = delimiter
        

    def execute(self, context):
        s3_conn = S3Hook(self.s3_conn_id)
        carguru_hook = CarguruHook(self.app_id, self.auth_token)
        stats = carguru_hook.get_dealer_vin_insights(self.stats_date, self.dealer_name)
        field_names = [key for key, value in stats[0].items()]

        file_name = '/tmp/{}.csv'.format(self.app_id)
        with open(file_name, 'w') as f:
            csv_writer = csv.DictWriter(f, fieldnames=field_names, delimiter=self.delimiter)
            csv_writer.writeheader()
            csv_writer.writerows(stats)
            
        s3_conn.load_file(file_name, self.s3_key, self.s3_bucket, True)
        os.remove(file_name)
    