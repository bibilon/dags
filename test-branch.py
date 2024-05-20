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
    'start_date': datetime(2024, 3, 12),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1
}

dag = DAG(
    'branch-dags',
    default_args=default_args,
    description='simple dag',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 5, 20),
    catchup=False,
    tags=['example13'],
    template_searchpath='/opt/airflow/dags/repo/'
)

# Define tasks
start = DummyOperator(task_id="start", dag=dag)

t1 = SparkKubernetesOperator(
    task_id='load_RP_SUB_PRE',
    retries=0,
    namespace='spark-jobs',
    application_file="rp_sub_pre.yaml",
    kubernetes_conn_id="myk8s",
    do_xcom_push=True,
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

t3 = SparkKubernetesOperator(
    task_id='update_phi',
    retries=0,
    namespace='spark-jobs',
    application_file="pbh.yaml",
    kubernetes_conn_id="myk8s",
    do_xcom_push=True,
    dag=dag
)

# Define branching logic for each task
def decide_branch_t1(**kwargs):
    task_instance = kwargs['ti']
    task_id = task_instance.task_id
    if task_instance.current_state() == "failed":
        return f"{task_id}_failed"
    else:
        return f"{task_id}_success"

branching_t1 = BranchPythonOperator(
    task_id='branching_t1',
    python_callable=decide_branch_t1,
    provide_context=True,
    dag=dag
)

def decide_branch_t2(**kwargs):
    task_instance = kwargs['ti']
    task_id = task_instance.task_id
    if task_instance.current_state() == "failed":
        return f"{task_id}_failed"
    else:
        return f"{task_id}_success"

branching_t2 = BranchPythonOperator(
    task_id='branching_t2',
    python_callable=decide_branch_t2,
    provide_context=True,
    dag=dag
)

def decide_branch_t3(**kwargs):
    task_instance = kwargs['ti']
    task_id = task_instance.task_id
    if task_instance.current_state() == "failed":
        return f"{task_id}_failed"
    else:
        return f"{task_id}_success"

branching_t3 = BranchPythonOperator(
    task_id='branching_t3',
    python_callable=decide_branch_t3,
    provide_context=True,
    dag=dag
)

# Define DummyOperators for each task to handle different branches
dummy_fail_t1 = DummyOperator(task_id='t1_failed', dag=dag)
dummy_fail_t2 = DummyOperator(task_id='t2_failed', dag=dag)
dummy_fail_t3 = DummyOperator(task_id='t3_failed', dag=dag)

# Define TriggerDagRunOperator for each task to rerun the DAG with different branches
trigger_fail_t1 = TriggerDagRunOperator(
    task_id='trigger_t1_failed',
    trigger_dag_id='handle_failures',
    conf={"task_id": "load_RP_SUB_PRE"},
    dag=dag
)

trigger_fail_t2 = TriggerDagRunOperator(
    task_id='trigger_t2_failed',
    trigger_dag_id='handle_failures',
    conf={"task_id": "load_COMM_RP"},
    dag=dag
)

trigger_fail_t3 = TriggerDagRunOperator(
    task_id='trigger_t3_failed',
    trigger_dag_id='handle_failures',
    conf={"task_id": "update_phi"},
    dag=dag
)

# Define dependencies
start >> t1 >> branching_t1 >> [dummy_fail_t1, trigger_fail_t1]
start >> t2 >> branching_t2 >> [dummy_fail_t2, trigger_fail_t2]
start >> t3 >> branching_t3 >> [dummy_fail_t3, trigger_fail_t3]
