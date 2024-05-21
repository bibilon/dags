from airflow import DAG
from datetime import timedelta, datetime
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import SparkKubernetesSensor
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1
}

def push_sensor_status(**kwargs):
    ti = kwargs['ti']
    ti.xcom_push(key='return_value', value='success')

with DAG(
    'test_dags',
    default_args=default_args,
    description='A test DAG for SparkKubernetesSensor and XCom',
    schedule_interval=timedelta(days=1),
    catchup=False,
) as dag:

    start = DummyOperator(task_id='start')
    end = DummyOperator(task_id='end')

    t1 = SparkKubernetesOperator(
        task_id='load_rp_sub_pre',
        namespace='spark-jobs',
        application_file='rp_sub_pre.yaml',
        kubernetes_conn_id='myk8s',
        do_xcom_push=True,
    )

    def on_success_callback(context):
        task_instance = context['task_instance']
        task_instance.xcom_push(key='status', value='success')

    spark_sensor_1 = SparkKubernetesSensor(
        task_id='spark_sensor_load_rp_sub_pre',
        namespace='spark-jobs',
        application_name='spark-load-rp-sub-pre',
        kubernetes_conn_id='myk8s',
        on_success_callback=on_success_callback,
    )

    push_sensor_1_status = PythonOperator(
        task_id='push_sensor_1_status',
        python_callable=push_sensor_status,
        provide_context=True,
    )

    delete_task_1 = DummyOperator(task_id='delete_spark_application_load_rp_sub_pre')

    t1 >> spark_sensor_1 >> push_sensor_1_status >> delete_task_1 >> end
