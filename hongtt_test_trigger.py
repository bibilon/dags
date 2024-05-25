from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import requests
from requests.auth import HTTPBasicAuth
from airflow.operators.empty import EmptyOperator
from airflow.sensors.http_sensor import HttpSensor
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
class CustomHttpSensor(HttpSensor):
    def poke(self, context):
        self.log.info('Poking: %s', self.endpoint)
        
        # Initialize HttpHook within the poke method
        http_hook = HttpHook(self.method, http_conn_id=self.http_conn_id)
        
        response = http_hook.run(self.endpoint, data=self.request_params, headers=self.headers, extra_options=self.extra_options)

        if response.status_code != 200:
            raise AirflowException(f"HTTP request failed with status code {response.status_code}")

        try:
            data = response.json()
            paragraphs = data['body']['paragraphs']
            all_finished = True
            for paragraph in paragraphs:
                status = paragraph['status']
                if status == 'ERROR':
                    raise AirflowException("One of the paragraphs has an error status")
                elif status == 'RUNNING':
                    all_finished = False
            return all_finished
        except Exception as e:
            raise AirflowException(f"Error parsing response: {str(e)}")
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
   sensor_task =  CustomHttpSensor(
    task_id='zeppelin_notebook_sensor',
    method='GET',
    http_conn_id='zeppelin_http_conn',  # Định nghĩa kết nối HTTP trong Airflow
    endpoint='/api/notebook/job/2JX2D44RY',  # Thay {note_id} bằng ID của notebook Zeppelin
    headers={"Content-Type": "application/json"},
    request_params={},  # Các tham số yêu cầu (nếu có)
    timeout=120,  # Thời gian chờ tối đa
    poke_interval=60,  # Khoảng thời gian giữa các lần kiểm tra
    dag=dag,
    )
   start >> trigger_notebook_task >> sensor_task
