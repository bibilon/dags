from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.models import Variable
from datetime import datetime
import cx_Oracle

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
        cursor.execute("SELECT dag_name, schedule_interval FROM DAG_NAMES")  # Thay đổi câu lệnh truy vấn nếu cần
        dag_configs = cursor.fetchall()
    finally:
        cursor.close()
        connection.close()
    
    return [(config[0], config[1]) for config in dag_configs]  # Trả về danh sách cấu hình DAG

# Lấy cấu hình DAG từ cơ sở dữ liệu
dag_configs = get_dynamic_dag_names()

# Tạo DAG cho mỗi cấu hình lấy được
for dag_name, schedule_interval in dag_configs:
    default_args = {
        'owner': 'airflow',
        'start_date': datetime(2023, 1, 1),  # Đặt start_date theo yêu cầu của bạn
    }

    # Khởi tạo DAG với lịch biểu từ cơ sở dữ liệu
    dag = DAG(
        dag_id=dag_name,
        default_args=default_args,
        schedule_interval=schedule_interval,
        catchup=False
    )

    # Thêm một DummyOperator làm nhiệm vụ mẫu
    with dag:
        start = DummyOperator(task_id='start')

    # Đăng ký DAG vào globals để Airflow có thể nhận diện
    globals()[dag_name] = dag
