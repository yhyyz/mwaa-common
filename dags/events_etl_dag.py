from airflow.providers.amazon.aws.operators.redshift_sql import RedshiftSQLOperator
from datetime import datetime
from airflow import DAG
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import os
from datetime import timedelta
from airflow.models import Variable
from airflow.utils.dates import days_ago
from airflow.operators.empty import EmptyOperator
import re
import logging
import pendulum
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

DAG_ID = os.path.basename(__file__).replace(".py", "")

DEFAULT_ARGS = {
    "owner": "chaopan",
    "depends_on_past": False,
    "email": ["chaopan@amazon.com"],
    "email_on_failure": False,
    "email_on_retry": False,
}

sql_bucket = Variable.get("s3_sql_bucket")


# get object from s3
def get_object(key, bucket_name):
    hook = S3Hook()
    content_object = hook.read_key(key=key, bucket_name=bucket_name)
    return content_object


# s3 content to sqls list
def get_sql_content(key, bucket_name):
    sqls = get_object(key, bucket_name)
    sql_list = sqls.split(";")
    sql_list_trim = [sql.strip() for sql in sql_list if sql.strip() != ""]
    return list(map(lambda x: x + ";", sql_list_trim))

def print_args(**kwargs):
    for k, v in kwargs.items():
        logging.info(f'{k}({type(v)}): {v}')


def ds_macro_format(execution_date:pendulum.DateTime, days=-1, date_format="%Y%m%d"):
    # days usage: {{ biz_date(data_interval_end) }} {{ biz_date(data_interval_end,-2) }} {{ biz_date(data_interval_end,-2,"%Y-%m-%d") }}
    return execution_date.add(days=days).strftime(date_format)


user_macros = {
    'biz_date': ds_macro_format
}
with DAG(
        dag_id=DAG_ID,
        description="redshift sql etl",
        default_args=DEFAULT_ARGS,
        dagrun_timeout=timedelta(hours=24),
        start_date=days_ago(100),
        catchup=False,
        user_defined_macros=user_macros,
        schedule_interval='@daily',
        # schedule_interval=None,
        tags=['redshift_sql'],
) as dag:
    begin = EmptyOperator(task_id="begin")
    end = EmptyOperator(task_id="end")

    task_seqq = SQLExecuteQueryOperator(
        conn_id="redshift_default",
        task_id='task_seqq',
        sql=get_sql_content("sqls/ods_create_table.sql", sql_bucket)
    )

    # create ods table
    task_ods_create_table = RedshiftSQLOperator(
        redshift_conn_id="redshift_default",
        task_id='task_ods_create_table',
        sql=get_sql_content("sqls/ods_create_table.sql", sql_bucket)
    )

    # insert ods data
    task_ods_insert_data = RedshiftSQLOperator(
        redshift_conn_id="redshift_default",
        task_id='task_ods_insert_data',
        sql=get_sql_content("sqls/ods_insert_data.sql", sql_bucket)
    )

    # ctas filter
    task_ods_ctas_filter = RedshiftSQLOperator(
        redshift_conn_id="redshift_default",
        task_id='task_ods_ctas_filter',
        sql=get_sql_content("sqls/ods_ctas_filter.sql", sql_bucket),
        params={'color': 'Red'},
    )

    # overwrite
    task_ods_overwrite = RedshiftSQLOperator(
        redshift_conn_id="redshift_default",
        task_id='task_ods_overwrite',
        sql=get_sql_content("sqls/ods_overwrite_data.sql", sql_bucket),
        params={'color': 'Red'},
    )

    python_operator_task = PythonOperator(
        task_id='task_python',
        python_callable=print_args,
        provide_context=True
    )

    # ods_date
    task_ods_date = RedshiftSQLOperator(
        redshift_conn_id="redshift_default",
        task_id='task_ods_date',
        sql=get_sql_content("sqls/ods_date.sql", sql_bucket),
    )




    begin >> python_operator_task >> task_ods_date >> task_seqq >> task_ods_create_table >> task_ods_insert_data >> task_ods_ctas_filter >> task_ods_overwrite >> end
