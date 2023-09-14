from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator

def print_time(ti, **kwargs):
    print(f"Current execution date and time: {ti.execution_date}")



# DAG_hour
dag_hour = DAG(
    'DAG_test_hour-single-dependency',
    start_date=datetime(2023, 8, 5),
    schedule_interval='15 * * * *', # This is cron expression, which means every hour at minute 15.
    tags=['v2_ExternalTaskSensor-single-dependency'],
)

start_task_hour = PythonOperator(
    task_id='start_task_hour',
    python_callable=print_time,
    provide_context=True,
    dag=dag_hour,
)

end_task_hour = EmptyOperator(
    task_id='end_task_hour',
    dag=dag_hour,
)

start_task_hour >> end_task_hour