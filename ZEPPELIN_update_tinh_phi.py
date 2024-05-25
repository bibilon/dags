from airflow import DAG
from datetime import timedelta, datetime
from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from airflow.operators.python_operator import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 3, 12),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0
}

with DAG(
        'ZEPPELIN_update-tinh-phi-dags',
        default_args=default_args,
        description='simple dag',
        schedule_interval='45 3 * * *',
        start_date=datetime(2024, 5, 20),
        catchup=False,
        tags=['example13'],
        template_searchpath='/opt/airflow/dags/repo/'
) as dag:
    start = DummyOperator(task_id="start")
    end = DummyOperator(task_id="end")
    wait_for_load_RP_SUB_PRE_dags = ExternalTaskSensor(
        task_id='wait_for_load_RP_SUB_PRE_dags',
        external_dag_id='ZEPPELIN_load_to_RP_SUB_PRE',
        mode='poke',
        timeout=600,
        dag=dag,
    )
    with TaskGroup("load_COMM_RP") as load_COMM_RP:
       trigger_notebook_task = PythonOperator(
            task_id='trigger_notebook',
            python_callable=trigger_notebook,
            op_kwargs={'nodepadID': '2JYTEPZCQ'},
            dag=dag
        )
       sensor_task =  CustomHttpSensor(
            task_id='check_status_notebook',
            method='GET',
            http_conn_id='zeppelin_http_conn',  # Định nghĩa kết nối HTTP trong Airflow
            endpoint='/api/notebook/job/2JYTEPZCQ',  # Thay {note_id} bằng ID của notebook Zeppelin
            headers={"Content-Type": "application/json"},
            timeout=120,  # Thời gian chờ tối đa
            poke_interval=60,  # Khoảng thời gian giữa các lần kiểm tra
            dag=dag,
        )
       trigger_notebook_task >> sensor_task

    with TaskGroup("update_phi") as update_phi:
       trigger_notebook_task = PythonOperator(
            task_id='trigger_notebook',
            python_callable=trigger_notebook,
            op_kwargs={'nodepadID': '2JX2D44RY'},
            dag=dag
        )
       sensor_task =  CustomHttpSensor(
            task_id='check_status_notebook',
            method='GET',
            http_conn_id='zeppelin_http_conn',  # Định nghĩa kết nối HTTP trong Airflow
            endpoint='/api/notebook/job/2JX2D44RY',  # Thay {note_id} bằng ID của notebook Zeppelin
            headers={"Content-Type": "application/json"},
            timeout=120,  # Thời gian chờ tối đa
            poke_interval=60,  # Khoảng thời gian giữa các lần kiểm tra
            dag=dag,
        )
       trigger_notebook_task >> sensor_task
    start >> wait_for_load_RP_SUB_PRE_dags >> load_COMM_RP >> update_phi

