from airflow import DAG
from datetime import timedelta, datetime
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import SparkKubernetesSensor
from airflow.models import Variable
from kubernetes.client import models as k8s
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.operators.empty import EmptyOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 3, 12),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1
}
with DAG(
   'hong-dags',
   default_args=default_args,
   description='simple dag',
   schedule_interval=timedelta(days=1),
   start_date=datetime(2024, 5, 16),
   catchup=False,
   tags=['example13'],
   template_searchpath='/opt/airflow/dags/repo/'
) as dag:
   start = EmptyOperator(task_id="start")
   t1 = SparkKubernetesOperator(
       task_id='n-spark',
       trigger_rule="all_success",
       depends_on_past=False,
       retries=0,
       namespace='spark-jobs',
       application_file="hongtt_sparkjob.yaml",
       kubernetes_conn_id="myk8s",
       do_xcom_push=True,
       dag=dag
   )
   spark_sensor = SparkKubernetesSensor(
    task_id='spark_sensor',
    namespace='spark-jobs',
    application_name='hongtt-spark-job-18',
    kubernetes_conn_id='myk8s',
    dag=dag
    )
   delete_task = KubernetesPodOperator(
    task_id='delete_spark_application',
    namespace='spark-jobs',
    image='bitnami/kubectl:latest',
    cmds=['kubectl', 'delete', 'sparkapplication', 'hongtt-spark-job-18', '-n', 'spark-jobs'],
    get_logs=True,
    dag=dag
    )
   start >> t1 >> spark_sensor >> delete_task
