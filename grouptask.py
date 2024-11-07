from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.models import Variable
from datetime import datetime
import cx_Oracle
from airflow.utils.task_group import TaskGroup
from datetime import timedelta, datetime
from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from airflow.operators.python_operator import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
from unit import *
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
        cursor.execute("SELECT dag_name, item_id FROM DAG_NAMES")  # Thay đổi câu lệnh truy vấn nếu cần
        dag_configs = cursor.fetchall()
    finally:
        cursor.close()
        connection.close()

    return [(config[0], config[1]) for config in dag_configs]  # Trả về danh sách cấu hình DAG

# Hàm để clone notebook và trả về ID mới
def clone_notebook(original_notebook_id: str):
    connection = BaseHook.get_connection('zeppelin_http_conn')
    base_url = connection.get_uri() 
    clone_url = f"{base_url}/api/notebook/{original_notebook_id}"
    headers = {"Content-Type": "application/json"}
    data = {"name": f"Cloned-{original_notebook_id}"}

    response = requests.post(clone_url, headers=headers, json=data)
    if response.status_code == 200:
        notebook_id = response.json().get("body")
        return notebook_id  # Trả về ID của notebook đã clone
    else:
        raise Exception("Failed to clone notebook")

#xoa notebook
def delete_notebook(notebook_id: str):
    connection = BaseHook.get_connection('zeppelin_http_conn')
    base_url = connection.get_uri() 
    delete_url = f"{base_url}/api/notebook/{notebook_id}"
    response = requests.delete(delete_url)
    if response.status_code == 200:
        print(f"Notebook {notebook_id} deleted successfully.")
    else:
        raise Exception(f"Failed to delete notebook {notebook_id}")

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

notebook = Variable.get("notebook")
# Thêm các TaskGroup cho mỗi cấu hình từ cơ sở dữ liệu
with main_dag:
    start = DummyOperator(task_id="start")
    end = DummyOperator(task_id="end")

    previous_task_group = start  # Dùng để giữ nhóm task trước đó

    for dag_name, item_id in dag_configs:
        with TaskGroup(group_id=dag_name) as task_group:
            clone_task = PythonOperator(
                task_id='clone_notebook',
                python_callable=clone_notebook,
                op_kwargs={'original_notebook_id': f"{notebook}"},
                dag=main_dag
            )

            # Task trigger notebook với ID notebook đã clone
            trigger_notebook_task = PythonOperator(
                task_id='trigger_notebook',
                python_callable=trigger_notebook,
                op_kwargs={'nodepadID': "{{ ti.xcom_pull(task_ids='" + dag_name + ".clone_notebook') }}", 'item_id': item_id},
                dag=main_dag
            )

            sensor_task =  CustomHttpSensor(
                task_id='check_status_notebook',
                method='GET',
                http_conn_id='zeppelin_http_conn',  # Định nghĩa kết nối HTTP trong Airflow
                endpoint="/api/notebook/job/{{ task_instance.xcom_pull(task_ids='" + dag_name + ".clone_notebook') }}",  # Thay {note_id} bằng ID của notebook Zeppelin
                headers={"Content-Type": "application/json"},
                timeout=120,  # Thời gian chờ tối đa
                poke_interval=60,  # Khoảng thời gian giữa các lần kiểm tra
                dag=main_dag
            )

            restart_interpreter = PythonOperator(
                task_id='restart_interpreter_notebook',
                python_callable=restart_interpreter_notebook,
                op_kwargs={'notebookID': "{{ ti.xcom_pull(task_ids='" + dag_name + ".clone_notebook') }}"},
                dag=main_dag
            )
            # Task xóa notebook sau khi thực hiện xong các tác vụ khác
            delete_notebook_task = PythonOperator(
                task_id='delete_notebook',
                python_callable=delete_notebook,
                op_kwargs={'notebook_id': "{{ task_instance.xcom_pull(task_ids='" + dag_name + ".clone_notebook') }}"},
                dag=main_dag
            )
            # Xác định thứ tự thực hiện trong TaskGroup
            clone_task >> trigger_notebook_task >> sensor_task >> restart_interpreter >> delete_notebook_task
        # Xác định thứ tự giữa các TaskGroup
        previous_task_group >> task_group
        previous_task_group = task_group  # Cập nhật task_group trước đó
    # Kết thúc DAG
    previous_task_group >> end
# Đăng ký DAG chính để Airflow có thể nhận diện
globals()['dynamic_task_groups_dag'] = main_dag
