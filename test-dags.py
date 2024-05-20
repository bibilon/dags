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
    'thinhdv-dags',
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

# Define branching logic
def decide_branch(**kwargs):
    task_instance = kwargs['ti']
    task_id = task_instance.task_id
    if task_instance.current_state() == "failed":
        return f"{task_id}_failed"
    else:
        return f"{task_id}_success"

branching = BranchPythonOperator(
    task_id='branching',
    python_callable=decide_branch,
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
start >> t1 >> spark_sensor_1 >> delete_task_1 >> branching
start >> t2 >> spark_sensor_2 >> delete_task_2 >> branching
start >> t3 >> spark_sensor_3 >> delete_task_3 >> branching

branching >> [dummy_fail_t1, dummy_fail_t2, dummy_fail_t3]

for dummy_fail_task, trigger_fail_task in zip([dummy_fail_t1, dummy_fail_t2, dummy_fail_t3], [trigger_fail_t1, trigger_fail_t2, trigger_fail_t3]):
    dummy_fail_task >> trigger_fail_task
