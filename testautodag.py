from airflow import DAG
from datetime import datetime

for i in range(1, 4):
    dag_name = f"dynamic_dag_{i}"
    
    default_args = {
        'owner': 'airflow',
        'start_date': datetime(2024, 11, 1),
    }

    with DAG(dag_id=dag_name, default_args=default_args, schedule_interval='@daily', catchup=False) as dag:
        # Define your tasks here
        pass
