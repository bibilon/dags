from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import requests
from requests.auth import HTTPBasicAuth
from airflow.operators.empty import EmptyOperator
from airflow.sensors.http_sensor import HttpSensor
from airflow.hooks.http_hook import HttpHook
from airflow.exceptions import AirflowException
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
   'trigger_zeppelin_notebook',
   default_args=default_args,
   description='simple dag',
   schedule_interval=timedelta(days=1),
   start_date=datetime(2024, 5, 16),
   catchup=False,
   tags=['example13'],
   template_searchpath='/opt/airflow/dags/repo/'
) as dag:
   start = EmptyOperator(task_id="start")
   trigger_notebook_task = PythonOperator(
    task_id='trigger_notebook',
    python_callable=trigger_notebook,
    dag=dag
    )
   sensor_task =  CustomHttpSensor(
    task_id='zeppelin_notebook_sensor',
    method='GET',
    http_conn_id='zeppelin_http_conn',  # Định nghĩa kết nối HTTP trong Airflow
    endpoint='/api/notebook/job/2JX2D44RY',  # Thay {note_id} bằng ID của notebook Zeppelin
    headers={"Content-Type": "application/json"},
    timeout=120,  # Thời gian chờ tối đa
    poke_interval=60,  # Khoảng thời gian giữa các lần kiểm tra
    dag=dag,
    )
   start >> trigger_notebook_task >> sensor_task
