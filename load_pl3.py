from airflow import DAG
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.contrib.operators.bigquery_check_operator import BigQueryCheckOperator
from airflow.contrib.hooks.bigquery_hook import BigQueryHook
from airflow.operators.python_operator import PythonOperator
import os
import pandas as pd
from google.cloud import storage
from io import BytesIO

# client = storage.Client()

PERSIST_BUCKET = "sasi-persist"

PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "sasiprapa-bluepi-de-exam")
GCS_BUCKET = os.environ.get("GCP_GCS_BUCKET_NAME", "sasi-staging")
PARTITION_FOLDER = datetime.today().strftime('%Y-%m-%d')
DWH_DATASET = 'USER_PERSIST_DB'

# Define default arguments
default_args = {
    'owner': 'Sasiprapa N.',
    'depends_on_past': False,
    'start_date':datetime(2020, 4, 20),
    # 'email_on_failure': False,
    # 'email_on_retry': False,
    # 'retries': 5,
    # 'retry_delay': timedelta(minutes=5),
}

dag = DAG('pl_datalake_to_bq1',
          start_date=datetime.now(),
          schedule_interval='@once',
        #   concurrency=5,
        #   max_active_runs=1,
          default_args=default_args)

start_pipeline = DummyOperator(
    task_id = 'start_pipeline',
    dag = dag
)

# Transform
def Transform_users():
    blob = storage.blob.Blob("//*.csv",GCS_BUCKET)
    content = blob.download_as_string()
    user_csv = pd.read_csv(BytesIO(content))

    user_csv['updated_at'] = pd.to_datetime(df['updated_at'], format='%Y%m%d%H%M%S') 
    user_csv['created_at'] = pd.to_datetime(df['created_at'], format='%Y%m%d%H%M%S') 

    # user_csv.to_csv("gs://"+PERSIST_BUCKET+"//user//"+PARTITION_FOLDER+"USER.csv", index=False)


transform_users_tbl = PythonOperator(
    task_id = 'transform_users_tbl',
    python_callable=Transform_users,
    dag=dag
)

# def transform_users_tbl():
#     retail = pd.read_csv("/home/airflow/gcs/data/retail_from_db.csv")
#     conversion_rate = pd.read_csv("/home/airflow/gcs/data/conversion_rate_from_api.csv")
    
#     final_df = retail.merge(conversion_rate, how="left", left_on="InvoiceDate", right_on="date")

#     final_df['THBPrice'] = final_df.apply(lambda x: x['UnitPrice'] * x['Rate'], axis=1)
#     final_df.to_csv("/home/airflow/gcs/data/result.csv", index=False)



# Load data from GCS to BQ
load_users_demo = GoogleCloudStorageToBigQueryOperator(
    task_id = 'load_users_demo',
    google_cloud_storage_conn_id='google_cloud_storage_default',
    bigquery_conn_id='bigquery_default',
    bucket = PERSIST_BUCKET,
    source_objects = [os.path.join("users", PARTITION_FOLDER,"*")],
    destination_project_dataset_table = f'{PROJECT_ID}.{DWH_DATASET}.user_users',
    source_format = 'csv',
    field_delimiter=',',
    skip_leading_rows = 1,
    write_disposition='WRITE_TRUNCATE',
    dag=dag
)

# load_user_log_demo = GoogleCloudStorageToBigQueryOperator(
#     task_id = 'load_user_log_demo',
    # # bigquery_conn_id='my_bq',
#     bucket = GCS_BUCKET,
#     source_objects = [os.path.join("user_log", PARTITION_FOLDER,"*")],
#     destination_project_dataset_table = f'{RAW_DATASET}.user_user_log',
#     write_disposition='WRITE_TRUNCATE',
#     source_format = 'csv',
#     field_delimiter=',',
#     skip_leading_rows = 1
# )


finish_pipeline = DummyOperator(
    task_id = 'finish_pipeline'
)

# Define task dependencies
start_pipeline >> transform_users_tbl >> load_users_demo >> finish_pipeline