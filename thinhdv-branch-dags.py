from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import SparkKubernetesSensor
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 3, 12),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1
}

def decide_which_path(task_instance, upstream_task_id, task_id):
    previous_task_status = task_instance.xcom_pull(task_ids=upstream_task_id)
    return task_id if previous_task_status == "success" else 'handle_error'

def push_sensor_status(ti):
    ti.xcom_push(key='return_value', value='success')

with DAG(
    'test-dags',
    default_args=default_args,
    description='simple dag',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 5, 20),
    catchup=False,
    tags=['example13'],
    template_searchpath='/opt/airflow/dags/repo/'
) as dag:

    start = DummyOperator(task_id="start")
    end = DummyOperator(task_id="end")

    handle_error_task = DummyOperator(task_id='handle_error')

    prev_task = start

    tasks = [
        {
            'task_id': 'load_RP_SUB_PRE',
            'application_file': 'rp_sub_pre.yaml',
            'sensor_task_id': 'spark_sensor_spark_load_rp_sub_pre',
            'sensor_app_name': 'spark-load-rp-sub-pre',
            'delete_task_id': 'delete_spark_application_load_rp_sub_pre'
        },
        {
            'task_id': 'load_COMM_RP',
            'application_file': 'comm_rp.yaml',
            'sensor_task_id': 'spark_sensor_load_COMM_RP',
            'sensor_app_name': 'spark-comm-rp',
            'delete_task_id': 'delete_spark_application_load_COMM_RP'
        },
        {
            'task_id': 'update_phi',
            'application_file': 'pbh.yaml',
            'sensor_task_id': 'spark_sensor_update_phi',
            'sensor_app_name': 'spark-update-phi',
            'delete_task_id': 'delete_spark_application_update_phi'
        }
    ]

    for idx, task in enumerate(tasks):
        spark_task = SparkKubernetesOperator(
            task_id=task['task_id'],
            namespace='spark-jobs',
            application_file=task['application_file'],
            kubernetes_conn_id='myk8s',
            do_xcom_push=True
        )

        spark_sensor = SparkKubernetesSensor(
            task_id=task['sensor_task_id'],
            namespace='spark-jobs',
            application_name=task['sensor_app_name'],
            kubernetes_conn_id='myk8s',
            do_xcom_push=True
        )

        push_sensor_status_task = PythonOperator(
            task_id=f'push_sensor_{idx+1}_status',
            python_callable=push_sensor_status
        )

        delete_task = KubernetesPodOperator(
            task_id=task['delete_task_id'],
            namespace='spark-jobs',
            image='bitnami/kubectl:latest',
            cmds=['kubectl', 'delete', 'sparkapplication', task['sensor_app_name'], '-n', 'spark-jobs'],
            get_logs=True
        )

        branch_task = BranchPythonOperator(
            task_id=f'branch_task_{idx+1}',
            python_callable=decide_which_path,
            op_kwargs={
                'upstream_task_id': f'push_sensor_{idx+1}_status',
                'task_id': task['delete_task_id']
            }
        )

        prev_task >> spark_task >> spark_sensor >> push_sensor_status_task >> branch_task
        branch_task >> delete_task >> end
        branch_task >> handle_error_task >> end

        prev_task = delete_task

    start >> spark_task  # Ensure the first task is connected to the start
