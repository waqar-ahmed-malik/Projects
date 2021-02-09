from airflow.operators import BaseOperator
from airflow.utils.decorators import apply_defaults
from hooks.cars_com_hook import CarsComHook
from airflow.hooks.S3_hook import S3Hook
import csv
import os



class CarsComToS3Operator(BaseOperator):
    def __init__(self,
                 s3_conn_id,
                 s3_bucket,
                 s3_key,
                 api_key,
                 date,
                 customer_id,
                 delimiter,
                 *args,
                 **kwargs):
        super().__init__(*args, **kwargs)

        self.s3_conn_id = s3_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.api_key = api_key
        self.date = date
        self.customer_id = customer_id
        self.delimiter = delimiter
        

    def execute(self, context):
        s3_conn = S3Hook(self.s3_conn_id)
        cars_com_hook = CarsComHook(self.api_key)
        stats = cars_com_hook.get_vehicle_report(self.customer_id, self.date)
        field_names = [key for key, value in stats[0].items()]

        file_name = '/tmp/{}.csv'.format(self.customer_id)
        with open(file_name, 'w') as f:
            csv_writer = csv.DictWriter(f, fieldnames=field_names, delimiter=self.delimiter)
            csv_writer.writeheader()
            for stat in stats:
                row = {}
                for key, value in stat.items():
                    if isinstance(value, str):
                        v = value.replace('\\n', ' ').replace('\\r', ' ').replace('\n', ' ').replace('\r', ' ').replace('\\', ' ').replace(self.delimiter, '_')
                        row.__setitem__(key, v)
                else:
                    row.__setitem__(key, value)
            csv_writer.writerow(row)
            
        s3_conn.load_file(file_name, self.s3_key, self.s3_bucket, True)
        os.remove(file_name)
    