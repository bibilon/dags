from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import requests
from requests.auth import HTTPBasicAuth
from airflow.operators.empty import EmptyOperator
from airflow.sensors.http_sensor import HttpSensor
from airflow.hooks.http_hook import HttpHook
from airflow.exceptions import AirflowException
from airflow.sensors.base_sensor_operator import BaseSensorOperator
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
class CustomHttpSensor(BaseSensorOperator):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def poke(self, context):
        self.log.info('Checking HTTP endpoint')
        try:
            response = requests.get(self.endpoint, headers=self.headers, params=self.request_params)
            response.raise_for_status()
            if self.response_check(response):
                return True  # Tiếp tục sensor nếu có paragraphs đang running
            else:
                return False  # Dừng sensor và trả về success nếu không có paragraphs nào đang running
        except Exception as e:
            raise AirflowException(f"Error parsing response: {str(e)}")

    def response_check(self, response):
        try:
            data = response.json()
            paragraphs = data['body']['paragraphs']
            error_paragraphs = [p for p in paragraphs if p['status'] == 'ERROR']
            if error_paragraphs:
                self.xcom_push(context, key='status', value='error')
                return False  # Trả về False nếu có ít nhất một paragraphs có trạng thái là error
            else:
                self.xcom_push(context, key='status', value='success')
                return True  # Trả về True nếu không có paragraphs nào có trạng thái là error
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
