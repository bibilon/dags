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
def trigger_notebook(nodepadID : str):
    host_zeppelin = Variable.get("host_zeppelin")
    port_zeppelin = Variable.get("port_zeppelin")
    url = f"http://{host_zeppelin}:{port_zeppelin}/api/notebook/job/{nodepadID}"
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


