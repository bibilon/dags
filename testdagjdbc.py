from airflow import DAG
from airflow.operators.dummy import DummyOperator
from datetime import datetime
import cx_Oracle

# Hàm kết nối đến cơ sở dữ liệu Oracle và lấy tên DAG cùng với lịch biểu
def get_dynamic_dag_configs():
    # Thay thế các thông tin kết nối theo cấu hình của bạn
    dsn_tns = cx_Oracle.makedsn('192.168.1.100', '1521', sid='ORCLCDB')

    # Kết nối đến cơ sở dữ liệu sử dụng SID
    connection = cx_Oracle.connect(user='pbh_app', password='admin@123', dsn=dsn_tns)
    
    try:
        cursor = connection.cursor()
        # Thực hiện truy vấn để lấy tên DAG và lịch biểu
        cursor.execute("SELECT dag_name, schedule_interval FROM DAG_NAMES")  # Cập nhật truy vấn nếu cần
        dag_configs = cursor.fetchall()
    finally:
        cursor.close()
        connection.close()
    
    # Trả về danh sách cấu hình DAG với tên và lịch biểu
    return [(config[0], config[1]) for config in dag_configs]

# Lấy cấu hình DAG từ cơ sở dữ liệu
dag_configs = get_dynamic_dag_configs()

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
