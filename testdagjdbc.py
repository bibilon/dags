from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime
import cx_Oracle

# Hàm kết nối đến cơ sở dữ liệu Oracle và lấy tên DAG
def get_dynamic_dag_names():
    # Thay thế các thông tin kết nối theo cấu hình của bạn
  dsn_tns = cx_Oracle.makedsn('192.168.121.112', '1521', sid='ORCLCDB')

  # Kết nối đến cơ sở dữ liệu sử dụng SID
  connection = cx_Oracle.connect(user='pbh_app', password='admin@123', dsn=dsn_tns)
    
    cursor = connection.cursor()
    cursor.execute("SELECT dag_name FROM DAG_NAMES")  # Thay đổi câu lệnh truy vấn theo yêu cầu của bạn
    dag_names = cursor.fetchall()
    
    cursor.close()
    connection.close()
    return [name[0] for name in dag_names]  # Trả về danh sách tên DAG

# Lấy tên DAG từ cơ sở dữ liệu
dag_names = get_dynamic_dag_names()

# Tạo DAG cho mỗi tên lấy được
for dag_name in dag_names:
    default_args = {
        'owner': 'airflow',
        'start_date': datetime(2024, 11, 1),
    }

    with DAG(dag_id=dag_name, default_args=default_args, schedule_interval='@daily', catchup=False) as dag:
        task = DummyOperator(task_id='dummy_task')
