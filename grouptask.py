from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.models import Variable
from datetime import datetime
import cx_Oracle
from airflow.utils.task_group import TaskGroup
from airflow.operators.python_operator import PythonOperator
from airflow.sensors.http_sensor import HttpSensor
from unit import trigger_notebook, restart_interpreter_notebook

# Hàm kết nối đến cơ sở dữ liệu Oracle và lấy tên DAG
def get_dynamic_dag_names():
    # Lấy các thông tin kết nối từ Airflow Variables
    host = Variable.get("server")
    port = Variable.get("port")
    sid = Variable.get("SID")
    user = Variable.get("user")
    password = Variable.get("pass")

    # Tạo DSN sử dụng các biến
    dsn_tns = cx_Oracle.makedsn(host, port, sid=sid)

    # Kết nối đến cơ sở dữ liệu sử dụng SID
    connection = cx_Oracle.connect(user=user, password=password, dsn=dsn_tns)
    
    try:
        cursor = connection.cursor()
        cursor.execute("SELECT dag_name, schedule_interval, notebookid FROM DAG_NAMES")  # Thay đổi câu lệnh truy vấn nếu cần
        dag_configs = cursor.fetchall()
    finally:
        cursor.close()
        connection.close()
    
    return [(config[0], config[1], config[2]) for config in dag_configs]  # Trả về danh sách cấu hình DAG

# Lấy cấu hình DAG từ cơ sở dữ liệu
dag_configs = get_dynamic_dag_names()

# Khởi tạo một DAG duy nhất
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
}
main_dag = DAG(
    dag_id='dynamic_task_groups_dag',
    default_args=default_args,
    schedule_interval=None,
    catchup=False
)

# Thêm các TaskGroup cho mỗi cấu hình từ cơ sở dữ liệu
with main_dag:
    start = DummyOperator(task_id="start")
    end = DummyOperator(task_id="end")

    previous_task_group = start  # Dùng để giữ nhóm task trước đó

    for dag_name, schedule_interval, notebookid in dag_configs:
        with TaskGroup(group_id=dag_name) as task_group:
            trigger_notebook_task = PythonOperator(
                task_id='trigger_notebook',
                python_callable=trigger_notebook,
                op_kwargs={'nodepadID': notebookid},
                dag=main_dag
            )

            sensor_task =  CustomHttpSensor(
                task_id='check_status_notebook',
                method='GET',
                http_conn_id='zeppelin_http_conn',  # Định nghĩa kết nối HTTP trong Airflow
                endpoint=f'/api/notebook/job/{notebookid}',  # Thay {note_id} bằng ID của notebook Zeppelin
                headers={"Content-Type": "application/json"},
                timeout=120,  # Thời gian chờ tối đa
                poke_interval=60,  # Khoảng thời gian giữa các lần kiểm tra
                dag=dag,
            )

            restart_interpreter = PythonOperator(
                task_id='restart_interpreter_notebook',
                python_callable=restart_interpreter_notebook,
                op_kwargs={'notebookID': notebookid},
                dag=main_dag
            )

            # Xác định thứ tự thực hiện trong TaskGroup
            trigger_notebook_task >> sensor_task >> restart_interpreter

        # Xác định thứ tự giữa các TaskGroup
        previous_task_group >> task_group
        previous_task_group = task_group  # Cập nhật task_group trước đó

    # Kết thúc DAG
    previous_task_group >> end

# Đăng ký DAG chính để Airflow có thể nhận diện
globals()['dynamic_task_groups_dag'] = main_dag
