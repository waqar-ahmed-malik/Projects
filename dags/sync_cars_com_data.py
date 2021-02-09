import os
from airflow import DAG
from operators.cars_com_to_s3_operator import CarsComToS3Operator
from operators.custom_s3_to_s3_operator import CustomS3ToS3Operator
from operators.s3_to_postgres_operator import S3ToPostgresOperator
from datetime import datetime
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2020, 11, 19),
    'retries': 0
}


schedule_interval = None

config = Variable.get("cars_com_config", deserialize_json=True)

landing_to_public_query_path = os.path.join(os.path.dirname(__file__), '../data/sources/cars_com/sql/landing_to_public.sql')
with open(landing_to_public_query_path, 'r') as f:
    landing_to_public_query = f.read()


with DAG('sync_cars_com_data', schedule_interval=schedule_interval, catchup=False, default_args=default_args) as dag:
    tasks = []

    start_node = DummyOperator(
        task_id='start',
        dag=dag
    )
    tasks.append(start_node)


    for dealer in config:
        api_to_s3 = CarsComToS3Operator(
            task_id='sync_api_to_s3_{}'.format(''.join([char if char.isalpha() else '_' for char in dealer['dealer_name']])),
            s3_conn_id = 's3',
            api_key = 'YYbMasDZAxnKJ3ixd4qzVmykB6j6RFMg',
            date = "2021-02-07",
            customer_id = dealer['dealer_id'],
            s3_key = 'temp/cars.com/{}.csv'.format(dealer['dealer_id']),
            delimiter = ',',
            s3_bucket = 'lutherstrategy',
            trigger_rule="all_done"
        )


        s3_to_s3 = CustomS3ToS3Operator(
            task_id='sync_s3_{}'.format(''.join([char if char.isalpha() else '_' for char in dealer['dealer_name']])),
            s3_conn_id = 's3',
            s3_bucket = 'lutherstrategy',
            s3_from_key = 'temp/cars.com/{}.csv'.format(dealer['dealer_id']),
            s3_to_key = 'thirdparty/cars.com/{}/vehicle_details/data.parquet'.format(dealer['dealer_id']),
            delimiter = ',',
            trigger_rule="all_done"
        )

        s3_to_postgres = S3ToPostgresOperator(
            task_id='s3_to_postgres_{}'.format(''.join([char if char.isalpha() else '_' for char in dealer['dealer_name']])),
            postgres_conn_id='postgres',
            landing_table = 'landing."views_cars_com"', 
            s3_conn_id = 's3',
            s3_bucket = 'lutherstrategy',
            s3_file_key = 'temp/cars.com/{}.csv'.format(dealer['dealer_id']),
            column_list = ['date_occurred', 'classifiedAdId', 'source', 'stockNumber', 'vehicleYear', 'date_entered', 'vehicleMake', 'vehicleModel', 'vin', 'stockType', 'vehicleStatus', 'listingPrice', 'numberOfPhotos', 'sellerNotes', 'currentAge', 'deletedAge', 'deletedLastLeadType', 'deletedLastLeadDateTime', 'srp', 'vdp', 'conversionRate', 'totalContacts'],
            source = 'cars.com',
            to_be_renamed = {
                    "detailViews": "vdp", 
                    "searchViews": "srp",
                    "date": "date_occurred"
                },
            trigger_rule="all_done"
        )


        landing_to_public = PostgresOperator(
            task_id='landing_to_public_{}'.format(''.join([char if char.isalpha() else '_' for char in dealer['dealer_name']])),
            sql=landing_to_public_query,
            postgres_conn_id='postgres',
            trigger_rule="all_done"
        )

        tasks.append(api_to_s3)
        tasks.append(s3_to_s3)
        tasks.append(s3_to_postgres)
        tasks.append(landing_to_public)
  

    end_node = DummyOperator(
        task_id='end',
        dag=dag
    )
    
    tasks.append(end_node)
    

for i in range(1, len(tasks)):
    tasks[i-1] >> tasks[i]