from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import requests
from requests.auth import HTTPBasicAuth


# Hàm trigger notebook trong Zeppelin
def trigger_notebook():
    url = "http://192.168.121.112:31818/api/notebook/job/2K16648YJ"
    headers = { "Content-Type": "application/json"}
    response = requests.post(url, headers=headers)
    if response.status_code == 200:
        print("Notebook triggered successfully.")
    else:
        print(f"Failed to trigger notebook: {response.status_code}, {response.text}")
        response.raise_for_status()

# Tạo DAG
default_args = {
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'trigger_zeppelin_notebook',
    default_args=default_args,
    description='Trigger a Zeppelin notebook from Airflow',
    schedule_interval=timedelta(days=1)
)

# Tạo task PythonOperator để trigger notebook
trigger_notebook_task = PythonOperator(
    task_id='trigger_notebook',
    python_callable=trigger_notebook,
    dag=dag
)

trigger_notebook_task 
