from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowSkipException
# DAG_day
dag_day = DAG(
    'DAG_test_day-single-dependency',
    start_date=datetime(2023, 8, 6),
    schedule_interval='15 * * * *', # Airflow会把这个cron表达式解释为shedule_interval为1天，所以会在第二天才启动。其实跟设置成@daily的效果一样的。需要当天执行则需要改为小时任务，比如12小时
    default_args={'retries': 3, 'retry_delay': timedelta(minutes=3)},
    tags=['v2_ExternalTaskSensor-single-dependency'],
)

start_task_day = EmptyOperator(
    task_id='start_task_day',
    dag=dag_day,
)

end_task_day = EmptyOperator(
    task_id='end_task_day',
    dag=dag_day,
)

def check_time(ti):
    execution_time = ti.execution_date
    if execution_time.minute != 15 or execution_time.hour != 0:
        raise AirflowSkipException

check_time_task = PythonOperator(
    task_id='check_time',
    python_callable=check_time,
    provide_context=True,
    dag=dag_day,
)


wait_for_task_hour = ExternalTaskSensor(
    task_id=f'wait_for_end_task_hour',
    external_dag_id='DAG_test_hour-single-dependency',
    external_task_id=f'end_task_hour',
    execution_delta=timedelta(hours=1), # execution_delta并不是用来检查特定时间段内所有的任务状态。它仅用来指定一个具体的单一时间点，以便查看在那个时间点运行的特定任务的状态
    check_existence=True, # 是否在任务开始时检查外部 DAG 和任务的存在。
    allowed_states=['success'],
    failed_states=['failed','skipped'],
    mode='reschedule', # 定义当传感器尝试时应该做什么。默认值是 "poke"，表示传感器将继续运行并尝试，"reschedule" 表示传感器会释放 worker 插槽。
    poke_interval=60, # 等待下一次尝试的时间间隔（秒）。
    timeout=3600, # 超时时间（秒），如果传感器在这段时间内没有成功，那么任务就会失败。
    dag=dag_day,
)



