from airflow.providers.amazon.aws.operators.redshift import RedshiftSQLOperator
from datetime import datetime
from airflow import DAG
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import os
from datetime import timedelta
from airflow.models import Variable
from airflow.utils.dates import days_ago
from airflow.operators.dummy import DummyOperator
import re

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


# remove sql comments
def remove_comments(sqls):
    out = re.sub(r'/\*.*?\*/', '', sqls, re.S)
    out = re.sub(r'--.*', '', out)
    return out


# s3 content to sqls list
def get_sql_content(key, bucket_name):
    sqls = get_object(key, bucket_name)
    _sql = remove_comments(sqls)
    sql_list = _sql.replace("\n", "").split(";")
    sql_list_trim = [sql.strip() for sql in sql_list if sql.strip() != ""]
    return list(map(lambda x: x + ";", sql_list_trim))


with DAG(
        dag_id=DAG_ID,
        description="redshift sql etl",
        default_args=DEFAULT_ARGS,
        dagrun_timeout=timedelta(hours=24),
        start_date=days_ago(1),
        catchup=False,
        schedule_interval=None,
        tags=['redshift_sql'],
) as dag:
    begin = DummyOperator(task_id="begin")
    end = DummyOperator(task_id="end")

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

    begin >> task_ods_create_table >> task_ods_insert_data >> task_ods_ctas_filter >> end
