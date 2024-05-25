from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import requests
from requests.auth import HTTPBasicAuth
from airflow.operators.empty import EmptyOperator
from airflow.sensors.http_sensor import HttpSensor
from airflow.hooks.http_hook import HttpHook
from airflow.exceptions import AirflowException
from airflow.models.baseoperator import BaseOperator
# Hàm trigger notebook trong Zeppelin
def trigger_notebook(nodepadID : str):
    url = f"http://192.168.121.112:31818/api/notebook/job/{nodepadID}"
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

class MyTaskGroup(BaseOperator):
    def __init__(self, nodepadID, endpoint, http_conn_id, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.nodepadID = nodepadID
        self.endpoint = endpoint
        self.http_conn_id = http_conn_id
        self.trigger_notebook_task = None
        self.sensor_task = None

    def execute(self, context):
        # Định nghĩa các task
        self.trigger_notebook_task = PythonOperator(
            task_id='trigger_notebook',
            python_callable=trigger_notebook,
	    op_kwargs={'nodepadID': self.nodepadID},
            dag=context['dag'],  # Sử dụng context['dag'] để truy cập đến DAG
        )
        self.sensor_task = CustomHttpSensor(
            task_id='zeppelin_notebook_sensor',
            method='GET',
            http_conn_id=self.http_conn_id,
            endpoint=self.endpoint,
            headers={"Content-Type": "application/json"},
            timeout=120,
            poke_interval=60,
            dag=context['dag'],  # Sử dụng context['dag'] để truy cập đến DAG
        )

        # Thiết lập luồng công việc
        self.trigger_notebook_task >> self.sensor_task

        # Thực thi các task
        self.trigger_notebook_task.execute(context)
        self.sensor_task.execute(context)

        # Trả về kết quả của lớp Python
        return True
