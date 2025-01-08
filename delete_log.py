from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta

# Định nghĩa DAG
dag = DAG(
    'delete_airflow_logs',
    default_args={
        'owner': 'airflow',
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='DAG to delete Airflow logs older than 1 day',
    schedule_interval='@daily',  # Chạy hàng ngày
    start_date=datetime(2023, 1, 1),
    catchup=False,
)
task = BashOperator(
    task_id='find_and_list_old_files',
    bash_command='find /opt/airflow/logs -type f -mtime 1 -exec echo {} \;',
)

# Câu lệnh Bash để xóa log cũ
# Xóa các file log đã được tạo hơn 1 ngày trước
delete_logs = BashOperator(
    task_id='delete_old_logs',
    bash_command="find /opt/airflow/logs -type f -mtime +1 -exec rm -f {} \;",
    dag=dag,
)

task >> delete_logs
