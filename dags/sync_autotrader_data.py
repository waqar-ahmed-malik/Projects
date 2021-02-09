import os
from airflow import DAG
from operators.auctions_to_s3_operator import AuctionsToS3Operator
from operators.custom_s3_to_s3_operator import CustomS3ToS3Operator
from operators.s3_to_postgres_operator import S3ToPostgresOperator
import datetime
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.datetime(2020, 11, 19),
    'retries': 0
}


schedule_interval = None

landing_to_public_query_path = os.path.join(os.path.dirname(__file__), '../data/sources/auctions/sql/landing_to_public.sql')
with open(landing_to_public_query_path, 'r') as f:
    landing_to_public_query = f.read()


with DAG('sync_auctions_data', schedule_interval=schedule_interval, catchup=False, default_args=default_args) as dag:


    api_to_s3 = AuctionsToS3Operator(
        task_id='sync_api_to_s3',
        s3_conn_id = 's3',
        api_key = 'qp2yz9gqsq83w83sn4h9z97a',
        sale_date = datetime.datetime.utcnow().date(),
        s3_key = 'temp/auctions/auctions.csv',
        delimiter = '^',
        s3_bucket = 'lutherstrategy',
        trigger_rule="all_done"
    )


    s3_to_s3 = CustomS3ToS3Operator(
        task_id='sync_temp_to_s3',
        s3_conn_id = 's3',
        s3_bucket = 'lutherstrategy',
        s3_from_key = 'temp/auctions/auctions.csv',
        s3_to_key = 'thirdparty/auctions/data.parquet',
        key_column_list = ['auction_id', 'auction_start_date', 'auction_end_date', 'vin', 'channel'],
        delimiter = '^',
        trigger_rule="all_done"
    )

    s3_to_postgres = S3ToPostgresOperator(
        task_id='sync_s3_to_postgres',
        postgres_conn_id='postgres',
        landing_table = 'landing."auctions"', 
        s3_conn_id = 's3',
        s3_bucket = 'lutherstrategy',
        s3_file_key = 'temp/auctions/auctions.csv',
        # column_list = ['as_is', 'buy_now_price', 'certified', 'comments', 'engine', 'exterior_color', 'frame_damage', 'fuel_type', 'has_air_conditioning', 'images', 'interior_color', 'interior_type', 'location_zip', 'make_id', 'mileage', 'model_id', 'odometer_units', 'pickup_location_state', 'pickup_location_zip', 'pickup_region', 'prior_paint', 'salvage', 'seller_types', 'title_status', 'transmission', 'vin', 'model_year', 'auction_end_date', 'auction_id', 'auction_location', 'auction_start_date', 'channel', 'sale_date', 'sale_type', 'sale_year', 'vehicle_sale_url', 'seller_name'],
        column_list = ['as_is', 'buy_now_price', 'certified', 'comments', 'engine',
       'exterior_color', 'frame_damage', 'fuel_type', 'has_air_conditioning',
       'images', 'interior_color', 'interior_type', 'location_zip', 'make_id',
       'mileage', 'model_id', 'odometer_units', 'pickup_location_state',
       'pickup_location_zip', 'pickup_region', 'prior_paint', 'salvage',
       'seller_types', 'title_status', 'transmission', 'vin', 'model_year',
       'auction_end_date', 'auction_id', 'auction_location',
       'auction_start_date', 'channel', 'sale_date', 'sale_type', 'sale_year',
       'vehicle_sale_url', 'seller_name'],
       separator = '^',
        # source = 'auctions',
        # to_be_renamed = {
        #     "vdps": "vdp",
        #     "srps": "srp",
        #     "date": "date_occurred"
        # },
        trigger_rule="all_done"
    )


    # landing_to_public = PostgresOperator(
    #     task_id='landing_to_public',
    #     sql=landing_to_public_query,
    #     postgres_conn_id='postgres',
    #     trigger_rule="all_done"
    # )

    
api_to_s3 >> s3_to_s3 >> s3_to_postgres 
# >> landing_to_public