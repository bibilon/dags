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


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 3, 12),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1
}


spark_image = "hongtt11/spark-aws-delta:3.3.202"  
namespace = "spark-jobs"
spark_class = "com.lifesup.test"
k8s_cluster_url = "k8s://https://192.168.121.110:6443"
spark_job_jar = "local:///opt/spark/jars/test123.jar"


spark_submit_cmd = f"""
spark-submit --class {spark_class} \
--master 	{k8s_cluster_url} \
--deploy-mode cluster \
--conf spark.executor.memory=4G \
--conf spark.executor.cores=2 \
{spark_job_jar}
"""
with DAG(
   'test_pod',
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

   t1 = KubernetesPodOperator(
    task_id="spark-submit-task",
    namespace=namespace,
    image=spark_image,
    cmds=["/bin/bash", "-c", spark_submit_cmd],  
    name="spark-submit-pod",
    is_delete_operator_pod=True,  
    get_logs=True, 
    in_cluster=True, 
    task_concurrency=1,  
    dag=dag  )
   start >> t1 >> end
