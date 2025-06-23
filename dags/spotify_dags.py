from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import time
import sys
import os
from typing import Optional
from airflow.models.taskinstance import TaskInstance
from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook

SLACK_WEBHOOK_URL= "https://hooks.slack.com/services/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"

from spotify_api.spotify_api_ import (
    get_token, 
    get_auth_header,
    search_for_genre_of_Artists
)

from spotify_api.mariadb_utils import (
    create_table, 
    importFile_from_minio_to_mariadb
)

from spotify_api.minio_utils import (
    upload_to_minio
)

from spotify_api.config_para import(
    MARIADB_TABLE_SCHEMA_ARTISTS,
    COLUMNS_ARTISTS
)

#slack alert
def alert_slack_channel(context: dict):
     
    if not SLACK_WEBHOOK_URL:
        return
    
    last_task: Optional[TaskInstance] = context.get('task_instance')
    dag_name = last_task.dag_id
    task_name= last_task.task_id
    error_message = context.get('exception') or context.get('reason')
    execution_date= context.get('execution_date')
    dag_run = context.get('dag_run')
    task_instances = dag_run.get_task_instances()
    file_and_link_template = "<{log_url}|{name}>"
    failed_tis=[file_and_link_template.format(log_url=ti.log_url, name= ti.task_id)
                for ti in task_instances
                if ti.state == 'failed']
    title= f':red_circle: Dag: *{dag_name}* has failed, with ({len(failed_tis)}) failed tasks'
    msg_parts = {
        'Execution date' : execution_date,
        'Failed Tasks'   : ', '.join(failed_tis),
        'Error': error_message
    }
    msg = "\n".join([title, *(f"*{key}*: {value}" for key, value in msg_parts.items())]).strip()
    SlackWebhookHook(
        webhook_token = SLACK_WEBHOOK_URL,
        message =msg
    ).execute()


default_args = {
    'owner': 'airflow',
    'retries': 3,
    'retry_delay': timedelta(minutes=3)
}

with DAG(
    dag_id='spotify_api',
    default_args=default_args,
    schedule='0 0,6,12,18 * * *',  # chạy mỗi ngày cách 6 tiếng
    on_failure_callback= alert_slack_channel,
    start_date=datetime(2025, 6, 1),
    catchup=False
) as dag:
        def fetch_API(**kwargs):
            token = get_token()
            data = search_for_genre_of_Artists(token, genre="v-pop")


            if not data:
                print("No artist data retrieved. Skipping upload.")
                return
            kwargs['ti'].xcom_push(key='data', value=data)
            # kwargs['ti'].xcom_push(key='my_key', value='hello')#

        def upload_file_to_minio(**kwargs):
            data = kwargs['ti'].xcom_pull(key='data', task_ids="fetch_API")
            # val = kwargs['ti'].xcom_pull(task_ids='my_task', key='my_key') #

            bucket_name = 'spotify-data'
            current_timestamp = time.strftime("%Y%m%d_%H-%M-%S") 
            genre_for_filename="v_pop"
            object_name = f"artists/{genre_for_filename}_artists_{current_timestamp}.parquet"

            upload_to_minio(data, bucket_name, object_name)
            # Lưu tên file cho task sau dùng
            kwargs['ti'].xcom_push(key='object_name', value=object_name)

        def import_data(**kwargs):
            object_name = kwargs['ti'].xcom_pull(key='object_name', task_ids="Minio")
            if not object_name:
                print("No object_name found in XCom. Skipping import.")
                return
            # đọc file từ minio vào mariaDB
            importFile_from_minio_to_mariadb(
                bucket_name='spotify-data',
                object_name=object_name,
                mariadb_table='artists_vpop',
                columns_order=COLUMNS_ARTISTS
            )

        t1_fetch_API = PythonOperator(
            task_id='fetch_API',
            python_callable=fetch_API,
            # provide_context=True
            )

        t2_minio = PythonOperator(
            task_id='Minio',
            python_callable=upload_file_to_minio,
            # provide_context=True
        )

        t3_mariadb = PythonOperator(
            task_id='Mariadb',
            python_callable=import_data,
            # provide_context=True
        )

        t1_fetch_API >> t2_minio >> t3_mariadb