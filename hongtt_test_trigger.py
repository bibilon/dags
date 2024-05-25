from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import requests
from requests.auth import HTTPBasicAuth
from airflow.operators.empty import EmptyOperator
from airflow.sensors.http_sensor import HttpSensor
from airflow.sensors.base_sensor_operator import BaseSensorOperator
from airflow.hooks.http_hook import HttpHook
from airflow.exceptions import AirflowException
# Hàm trigger notebook trong Zeppelin
def trigger_notebook():
    url = "http://192.168.121.112:31818/api/notebook/job/2JX2D44RY"
    headers = { "Content-Type": "application/json"}
    response = requests.post(url, headers=headers)
    if response.status_code == 200:
        print("Notebook triggered successfully.")
    else:
        print(f"Failed to trigger notebook: {response.status_code}, {response.text}")
        response.raise_for_status()
#Ham check status cua notebook
def check_paragraphs(response):
    try:
        data = response.json()
        paragraphs = data['body']['paragraphs']
        for paragraph in paragraphs:
            if paragraph['status'] == 'ERROR':
                return 'error'
            elif paragraph['status'] != 'FINISHED':
                return False
        return 'success'
    except Exception as e:
        return 'error'
class ZeppelinNotebookSensor(BaseSensorOperator):
    """
    Sensor để kiểm tra trạng thái của tất cả các đoạn văn bản trong Zeppelin Notebook.
    """

    @apply_defaults
    def __init__(self, zeppelin_url, notebook_id, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.zeppelin_url = zeppelin_url
        self.notebook_id = notebook_id

    def poke(self, context):
        response = requests.get(f"{self.zeppelin_url}/api/notebook/job/{self.notebook_id}")
        status = check_paragraphs(response)
        task_instance = context['task_instance']
        task_instance.xcom_push(key='zeppelin_status', value=status)
        if status == 'error':
            raise AirflowException("Có lỗi khi kiểm tra trạng thái các đoạn văn bản.")
        elif status == False:
            return False
        else:
            return True            
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 3, 12),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1
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
   sensor_task =  ZeppelinNotebookSensor(
    task_id='notebook_sensor',
    zeppelin_url='http://zeppelin_url:port',
    notebook_id='2JX2D44RY',
    poke_interval=60,
    timeout=120,
    dag=dag
   start >> trigger_notebook_task >> sensor_task
