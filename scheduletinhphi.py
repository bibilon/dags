from datetime import datetime, timedelta
from croniter import croniter
from dateutil.relativedelta import relativedelta
from airflow.operators.python import PythonOperator,BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow import DAG


class thoigian:
    def __init__(self, day, hour, min):
        self.day = day + hour // 24
        self.hour = hour % 24 + min // 60
        self.min = min % 60

    def format_schedule(self):
        if self.day + 1 < 28:
            return f'{self.min} {self.hour} {self.day + 1} * *'
        elif self.day + 1 == 28:
            return f'{self.min} {self.hour} 1,{self.day + 1} * *'
        elif self.day + 1 == 29:
            return f'{self.min} {self.hour} 1,2,{self.day + 1} * *'
        elif self.day + 1 == 30:
            return f'{self.min} {self.hour} 1,2,3,{self.day + 1} * *'
        elif self.day + 1 == 31:
            return f'{self.min} {self.hour} 1,2,3,4,{self.day + 1} * *'
        else:
            return f'{self.min} {self.hour} * * *'

    def bill_cycle(self, timerun):
        delta = timedelta(days=self.day, hours=self.hour, minutes=self.min)
        now = datetime.strptime(timerun, "%Y-%m-%dT%H:%M:%S")
        result_time = (now - delta) - relativedelta(months=1)
        return result_time.strftime("%Y%m%d")

    def check_run(self, timerun):
        delta = timedelta(days=self.day, hours=self.hour, minutes=self.min)
        now = datetime.strptime(timerun, "%Y-%m-%dT%H:%M:%S")
        result_time = now - delta
        if result_time.day == 1:
            print("true schedule run")
            return 'starttinhphi'
        else :
            print("false schedule run")
            return 'finished'

# Hàm python được gọi trong BranchPythonOperator
def branch_task(tg, **kwargs):
    # Lấy execution_date từ context
    execution_date = kwargs['execution_date']

    # Chuyển execution_date về dạng chuỗi cho dễ xử lý
    timerun = execution_date.strftime("%Y-%m-%dT%H:%M:%S")
    return tg.check_run(timerun)

x = thoigian(30, 2, 30)
with DAG(
    dag_id='branch_dag_example',
    schedule_interval=None,  # hoặc đặt theo cron nếu muốn
    catchup=False
) as dag:

    check = BranchPythonOperator(
        task_id='check_task',
        python_callable=branch_task,
        op_args=[x],
        provide_context=True,
    )

    # Các task downstream
    starttinhphi = PythonOperator(
        task_id='starttinhphi',
        python_callable=lambda: print("Start Tinh Phi"),
        dag=dag,
    )

    finished = PythonOperator(
        task_id='finished',
        python_callable=lambda: print("Finished"),
        dag=dag,
    )

    # Cấu hình đường đi (flow)
    check >> [starttinhphi, finished]
