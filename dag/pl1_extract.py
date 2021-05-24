from datetime import datetime, timedelta
from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator
import os
import time



PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "sasiprapa-bluepi-de-exam")
GCS_BUCKET = os.environ.get("GCP_GCS_BUCKET_NAME", "sasi-staging")

PARTITION_FOLDER = datetime.today().strftime('%Y-%m-%d')
timestr = time.strftime("%Y%m%d-%H%M")

USERS_FILENAME = os.path.join("users", PARTITION_FOLDER, "user_"+timestr+".csv")
SQL_QUERY_USERS = "select * from users;"

USER_LOG_FILENAME = os.path.join("user_log", PARTITION_FOLDER, "user_log_"+timestr+".csv")
SQL_QUERY_USER_LOG = "select * from user_log;"

default_args = {
    'owner': 'Sasiprapa N.',
    'depend_on_past': False,
    'start_date':datetime(2021, 5, 24, 7, 00, 00),
    'retries':1,
}

dag = DAG('pl1_extract', default_args=default_args, 
            start_date=datetime(2021, 5, 24, 7, 00, 00),
            schedule_interval='0 * * * *'
            , catchup=False)

start_pipeline = DummyOperator(
    task_id = 'start_pipeline',
    dag = dag
)

export_users = PostgresToGCSOperator(
        postgres_conn_id='airflow_postgres'
        , task_id="export_users", sql=SQL_QUERY_USERS
        , bucket=GCS_BUCKET
        , filename=USERS_FILENAME 
        ,export_format='csv'
        , gzip=False, dag=dag
    )

upload_users_file = PostgresToGCSOperator(
        postgres_conn_id='airflow_postgres',
        task_id="upload_users_file",
        sql=SQL_QUERY_USERS,
        bucket=GCS_BUCKET,
        filename=USERS_FILENAME,
        export_format='csv',
        gzip=False,
        use_server_side_cursor=True,dag=dag
    )


export_user_log = PostgresToGCSOperator(
        postgres_conn_id='airflow_postgres'
        , task_id="export_user_log", sql=SQL_QUERY_USER_LOG
        , bucket=GCS_BUCKET
        , filename=USER_LOG_FILENAME 
        ,export_format='csv'
        , gzip=False, dag=dag
    )

upload_user_log_file = PostgresToGCSOperator(
        postgres_conn_id='airflow_postgres',
        task_id="upload_user_log_file",
        sql=SQL_QUERY_USER_LOG,
        bucket=GCS_BUCKET,
        filename=USER_LOG_FILENAME,
        export_format='csv',
        gzip=False,
        use_server_side_cursor=True,dag=dag
    )

finish_pipeline = DummyOperator(
    task_id = 'finish_pipeline'
)




start_pipeline >> [export_users, export_user_log]

export_users >> upload_users_file
export_user_log >> upload_user_log_file

[upload_users_file, upload_user_log_file] >> finish_pipeline