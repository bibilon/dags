from airflow.operators.python import BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

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
    provide_context=True
)

# Define DummyOperators for each task to handle different branches
dummy_fail_t1 = DummyOperator(task_id='t1_failed')
dummy_fail_t2 = DummyOperator(task_id='t2_failed')
dummy_fail_t3 = DummyOperator(task_id='t3_failed')

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

start >> t1 >> branching
start >> t2 >> branching
start >> t3 >> branching

# Define the branching logic
branching >> [dummy_fail_t1, dummy_fail_t2, dummy_fail_t3]

# Define the failure paths
[dummy_fail_t1, dummy_fail_t2, dummy_fail_t3] >> [trigger_fail_t1, trigger_fail_t2, trigger_fail_t3]
