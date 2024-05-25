from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import requests
from requests.auth import HTTPBasicAuth
from airflow.operators.empty import EmptyOperator
from airflow.sensors.http_sensor import HttpSensor
from airflow.hooks.http_hook import HttpHook
from airflow.exceptions import AirflowException
from airflow.utils.task_group import TaskGroup
from unit import *
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 3, 12),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0
}
with DAG(
   'ZEPPELIN_SCD_ACTION_AUDIT',
   default_args=default_args,
   description='simple dag',
   schedule_interval='07 14 * * *',
   start_date=datetime(2024, 5, 16),
   catchup=False,
   tags=['example13'],
   template_searchpath='/opt/airflow/dags/repo/'
) as dag:
    start = EmptyOperator(task_id="start")
    with TaskGroup("SCD_ACTION_AUDIT") as task_group:
       trigger_notebook_task = PythonOperator(
            task_id='trigger_notebook',
            python_callable=trigger_notebook,
            op_kwargs={'nodepadID': '2JXB29MDS'},
            dag=dag
        )
       sensor_task =  CustomHttpSensor(
            task_id='check_status_notebook',
            method='GET',
            http_conn_id='zeppelin_http_conn',  # Định nghĩa kết nối HTTP trong Airflow
            endpoint='/api/notebook/job/2JXB29MDS',  # Thay {note_id} bằng ID của notebook Zeppelin
            headers={"Content-Type": "application/json"},
            timeout=120,  # Thời gian chờ tối đa
            poke_interval=60,  # Khoảng thời gian giữa các lần kiểm tra
            dag=dag,
        )
       trigger_notebook_task >> sensor_task
    start >> task_group
