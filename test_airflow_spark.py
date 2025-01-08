from __future__ import annotations
from airflow import DAG
from datetime import timedelta, datetime
from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import SparkKubernetesSensor
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from kubernetes.client import models as k8s
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.operators.empty import EmptyOperator
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

name_application = "spark-test11"
namespace = "spark-jobs"
oraclePassword = "123"
spark_template_spec = {
    "apiVersion": "sparkoperator.k8s.io/v1beta2",
    "kind": "SparkApplication",
    "metadata": {
        "name": f"{name_application}",
        "namespace": f"{namespace}",
    },
    "spec": {
        "type": "Scala",
        "mode": "cluster",
        "image": "hongtt11/spark-aws-delta:3.3.202",
        "imagePullPolicy": "Always",
        "mainClass": "com.lifesup.test",
        "mainApplicationFile": "local:///opt/spark/jars/test123.jar",
        "sparkVersion": "3.3.3",
        "restartPolicy": {"type": "Never"},
        "driver": {
            "cores": 1,
            "coreLimit": "1000m",
            "memory": "4096m",
            "labels": {"version": "3.3.3"},
            "serviceAccount": "spark",
            "envVars": {
                "oraclePassword": f"{oraclePassword}",
            },
        },
        "executor": {
            "instances": 2,
            "coreRequest": "1000m",
            "coreLimit": "1000m",
            "memory": "4096m",
            "labels": {"version": "3.3.3"},
        },
    },
}

default_params = {"start_date": "2022-01-01", "end_date": "2022-12-01"}

    

def push_sensor_status(**kwargs):
    ti = kwargs['ti']
    ti.xcom_push(key='return_value', value='success')
        
with DAG(
   'test_airflow_spark',
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

   t1 = SparkKubernetesOperator(
       task_id='trigger_job_run',
       retries=0,
       namespace=namespace,
       application_file=spark_template_spec,
       kubernetes_conn_id="myk8s",
       do_xcom_push=True,
       dag=dag
   )
   spark_sensor_1 = SparkKubernetesSensor(
    task_id='check_status_job',
    namespace=namespace,
    application_name=name_application,
    kubernetes_conn_id='myk8s',
    dag=dag
   )
   delete_task = KubernetesPodOperator(
    task_id='delete_spark_application',
    namespace=namespace,
    image='bitnami/kubectl:latest',
    cmds=['kubectl', 'delete', 'sparkapplication', f'{name_application}', '-n', 'spark-jobs'],
    get_logs=True,
    dag=dag
    )
   start >> t1 >> spark_sensor_1 >>  delete_task >> end
