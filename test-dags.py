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

def decide_which_path(**kwargs):
    task_instance = kwargs['task_instance']
    previous_task_status = task_instance.xcom_pull(task_ids=kwargs['upstream_task_id'])
    if previous_task_status == "success":
        return 'continue_next_task'
    else:
        return 'handle_error'

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

   t1 = SparkKubernetesOperator(
       task_id='load_RP_SUB_PRE',
       retries=0,
       namespace='spark-jobs',
       application_file="rp_sub_pre.yaml",
       kubernetes_conn_id="myk8s",
       do_xcom_push=True,
       dag=dag
   )
   
   spark_sensor_1 = SparkKubernetesSensor(
    task_id='spark_sensor_spark_load_rp_sub_pre',
    namespace='spark-jobs',
    application_name='spark-load-rp-sub-pre',
    kubernetes_conn_id='myk8s',
    dag=dag
   )

   delete_task_1 = KubernetesPodOperator(
    task_id='delete_spark_application_load_rp_sub_pre',
    namespace='spark-jobs',
    image='bitnami/kubectl:latest',
    cmds=['kubectl', 'delete', 'sparkapplication', 'spark-load-rp-sub-pre', '-n', 'spark-jobs'],
    get_logs=True,
    dag=dag
   )
   
   t2 = SparkKubernetesOperator(
       task_id='load_COMM_RP',
       retries=0,
       namespace='spark-jobs',
       application_file="comm_rp.yaml",
       kubernetes_conn_id="myk8s",
       do_xcom_push=True,
       dag=dag
   )
   
   spark_sensor_2 = SparkKubernetesSensor(
    task_id='spark_sensor_load_COMM_RP',
    namespace='spark-jobs',
    application_name='spark-comm-rp',
    kubernetes_conn_id='myk8s',
    dag=dag
   )

   delete_task_2 = KubernetesPodOperator(
    task_id='delete_spark_application_load_COMM_RP',
    namespace='spark-jobs',
    image='bitnami/kubectl:latest',
    cmds=['kubectl', 'delete', 'sparkapplication', 'spark-comm-rp', '-n', 'spark-jobs'],
    get_logs=True,
    dag=dag
   )
   
   t3 = SparkKubernetesOperator(
       task_id='update_phi',
       retries=0,
       namespace='spark-jobs',
       application_file="pbh.yaml",
       kubernetes_conn_id="myk8s",
       do_xcom_push=True,
       dag=dag
   )
   
   spark_sensor_3 = SparkKubernetesSensor(
    task_id='spark_sensor_update_phi',
    namespace='spark-jobs',
    application_name='spark-update-phi',
    kubernetes_conn_id='myk8s',
    dag=dag
   )

   delete_task_3 = KubernetesPodOperator(
    task_id='delete_spark_application_update_phi',
    namespace='spark-jobs',
    image='bitnami/kubectl:latest',
    cmds=['kubectl', 'delete', 'sparkapplication', 'spark-update-phi', '-n', 'spark-jobs'],
    get_logs=True,
    dag=dag
   )

   branch_task = BranchPythonOperator(
       task_id='branch_task',
       python_callable=decide_which_path,
       provide_context=True,
       op_kwargs={'upstream_task_id': 'previous_task_id'},
       dag=dag
   )

   handle_error_task = DummyOperator(
       task_id='handle_error',
       dag=dag
   )

   start >> t1 >> spark_sensor_1 >> branch_task >> delete_task_1
   branch_task >> [t2, handle_error_task]
   t2 >> spark_sensor_2 >> delete_task_2 >> t3 >> spark_sensor_3 >> delete_task_3 >> end
   handle_error_task >> end
