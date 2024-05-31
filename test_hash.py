import paramiko
import hashlib
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

def hash_file_on_sftp(hostname, port, username, password, remote_file_path, hash_algorithm='sha256', block_size=65536):
    # Kết nối tới SFTP server
    transport = paramiko.Transport((hostname, port))
    transport.connect(username=username, password=password)
    sftp = paramiko.SFTPClient.from_transport(transport)

    # Chọn thuật toán băm
    if hash_algorithm.lower() == 'sha256':
        hasher = hashlib.sha256()
    elif hash_algorithm.lower() == 'md5':
        hasher = hashlib.md5()
    else:
        raise ValueError(f"Unsupported hash algorithm: {hash_algorithm}")

    # Mở tệp tin trên SFTP server và đọc theo từng khối
    with sftp.open(remote_file_path, 'rb') as remote_file:
        while True:
            data = remote_file.read(block_size)
            if not data:
                break
            hasher.update(data)

    # Đóng kết nối SFTP
    sftp.close()
    transport.close()
    print(f"SHA-256 hash of the file on SFTP: {hasher.hexdigest()}")

    # Trả về giá trị băm dưới dạng chuỗi hex
    return hasher.hexdigest()



default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 3, 12),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0
}
with DAG(
   'test_hash',
   default_args=default_args,
   description='simple dag',
   schedule_interval='07 14 * * *',
   start_date=datetime(2024, 5, 16),
   catchup=False,
   tags=['example13'],
   template_searchpath='/opt/airflow/dags/repo/'
) as dag:
    start = EmptyOperator(task_id="start")
    with TaskGroup("test_hash") as task_group:
       trigger_notebook_task = PythonOperator(
            task_id='trigger_notebook',
            python_callable=hash_file_on_sftp,
            op_kwargs={'hostname': '14.231.238.41' , 'port': '2223', 'username': 'nguyen' , 'password': 'vwefWEHKIer#^&843VDsds' , 'remote_file_path': '/home/nguyen/thinhdv/data/SHOP.csv.' },
            dag=dag
        )


