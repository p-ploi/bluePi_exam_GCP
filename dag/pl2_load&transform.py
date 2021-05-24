from airflow import DAG
# from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.contrib.operators.bigquery_check_operator import BigQueryCheckOperator
# from airflow.operators.python_operator import PythonOperator
import os


PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "sasiprapa-bluepi-de-exam")
GCS_BUCKET = os.environ.get("GCP_GCS_BUCKET_NAME", "sasi-staging")
PARTITION_FOLDER = datetime.today().strftime('%Y-%m-%d')
RAW_DATASET = 'USER_RAW_DB'
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

# Load data from GCS to BQ
load_users_demo = GoogleCloudStorageToBigQueryOperator(
    task_id = 'load_users_demo',
    google_cloud_storage_conn_id='google_cloud_storage_default',
    bigquery_conn_id='bigquery_default',
    bucket = GCS_BUCKET,
    source_objects = [os.path.join("users", PARTITION_FOLDER,"*")],
    destination_project_dataset_table = f'{PROJECT_ID}.{RAW_DATASET}.user_users',
    source_format = 'csv',
    field_delimiter=',',
    skip_leading_rows = 1,
    write_disposition='WRITE_TRUNCATE',
    dag=dag
)

load_user_log_demo = GoogleCloudStorageToBigQueryOperator(
    task_id = 'load_user_log_demo',
    bigquery_conn_id='bigquery_default',
    bucket = GCS_BUCKET,
    source_objects = [os.path.join("user_log", PARTITION_FOLDER,"*")],
    destination_project_dataset_table = f'{RAW_DATASET}.user_user_log',
    write_disposition='WRITE_TRUNCATE',
    source_format = 'csv',
    field_delimiter=',',
    skip_leading_rows = 1
)


## Check loaded data not null
# check_users_data = BigQueryCheckOperator(
#     task_id = 'check_users_data',
#     bigquery_conn_id='bigquery_default',
#     use_legacy_sql=False,
#     sql = f'SELECT count(*) FROM `{PROJECT_ID}.{RAW_DATASET}.user_users`',
#     dag=dag
# )

# check_user_log_data = BigQueryCheckOperator(
#     task_id = 'check_user_log_data',
    # # bigquery_conn_id='my_bq',
#     use_legacy_sql=False,
#     sql = f'SELECT count(*) FROM `{RAW_DATASET}.user_user_log`'
# )

loaded_data_to_raw_db = DummyOperator(
    task_id = 'loaded_data_to_raw_db'
)


# Transform data
transform_users_tbl = BigQueryOperator(
    task_id = 'transform_users_tbl',
    use_legacy_sql = False,
    params = {
        'PROJECT_ID': PROJECT_ID,
        'STAGING_DATASET': RAW_DATASET,
        'DWH_DATASET': DWH_DATASET
    },
    sql = './sql/raw_user_users.sql',
    bigquery_conn_id='bigquery_default',
    dag=dag
)

transform_user_log_tbl = BigQueryOperator(
    task_id = 'transform_user_log_tbl',
    use_legacy_sql = False,
    params = {
        'PROJECT_ID': PROJECT_ID,
        'STAGING_DATASET': RAW_DATASET,
        'DWH_DATASET': DWH_DATASET
    },
    sql = './sql/raw_user_log.sql',
    bigquery_conn_id='bigquery_default'
)

finish_pipeline = DummyOperator(
    task_id = 'finish_pipeline'
)

# Define task dependencies
start_pipeline >> [load_users_demo, load_user_log_demo] >> loaded_data_to_raw_db >> [transform_users_tbl, transform_user_log_tbl] >> finish_pipeline