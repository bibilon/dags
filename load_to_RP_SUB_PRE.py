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

default_params = {"start_date": "2022-01-01", "end_date": "2022-12-01"}
class CustomSparkKubernetesOperator(SparkKubernetesOperator):
    def manage_template_specs(self):
        print("application: ", self.application_file)
        if self.application_file:
            template_body = _load_body_to_dict(open(self.application_file))
        elif self.template_spec:
            template_body = self.template_spec
        else:
            raise AirflowException("either application_file or template_spec should be passed")
        if "spark" not in template_body:
            template_body = {"spark": template_body}
        return template_body

def decide_which_path(**kwargs):
    task_instance = kwargs['task_instance']
    previous_task_status = task_instance.xcom_pull(task_ids=kwargs['upstream_task_id'])
    if previous_task_status == "success":
        return 'delete_spark_application_load_rp_sub_pre'
    else:
        return 'handle_error'
        
def push_sensor_status(**kwargs):
    ti = kwargs['ti']
    ti.xcom_push(key='return_value', value='success')
        
with DAG(
   'load_RP_SUB_PRE_dags',
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
       task_id='load_RP_SUB_PRE',
       retries=0,
       namespace='spark-jobs',
       application_file="rp_sub_pre.yaml",
       kubernetes_conn_id="myk8s",
       do_xcom_push=True,
       dag=dag
   )

   def on_success_callback(context):
        task_instance = context['task_instance']
        task_instance.xcom_push(key='status', value='success')
   
   spark_sensor_1 = SparkKubernetesSensor(
    task_id='spark_sensor_spark_load_rp_sub_pre',
    namespace='spark-jobs',
    application_name='spark-load-rp-sub-pre',
    kubernetes_conn_id='myk8s',
    on_success_callback=on_success_callback,
    do_xcom_push=True, 
    dag=dag
   )

   push_sensor_1_status = PythonOperator(
        task_id='push_sensor_1_status',
        python_callable=push_sensor_status,
        provide_context=True,
    )

   delete_task_1 = KubernetesPodOperator(
    task_id='delete_spark_application_load_rp_sub_pre',
    namespace='spark-jobs',
    image='bitnami/kubectl:latest',
    cmds=['kubectl', 'delete', 'sparkapplication', 'spark-load-rp-sub-pre', '-n', 'spark-jobs'],
    get_logs=True,
    dag=dag
   )

   branch_task = BranchPythonOperator(
        task_id='branch_task',
        python_callable=decide_which_path,
        provide_context=True,
        op_kwargs={'upstream_task_id': 'push_sensor_1_status'},
        dag=dag
    )

   handle_error_task = DummyOperator(
       task_id='handle_error',
       dag=dag
   )

   start >> t1 >> spark_sensor_1 >> push_sensor_1_status >> branch_task >> delete_task_1 >> end
   branch_task >> handle_error_task
   handle_error_task >> end
