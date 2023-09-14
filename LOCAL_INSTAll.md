#### local airflow
1. init
```sh
export  AIRFLOW_HOME=`pwd`
airflow db init
```
2. create user
```shell
airflow users create \
--username admin \
--password admin \
--firstname chao \
--lastname pan \
--role Admin \
--email admin@example.com

```
3. start
```shell
ps -ef | egrep 'airflow webserver' | grep -v grep | awk '{print $2}' | xargs kill -9
ps -ef | egrep 'airflow-webserver' | grep -v grep | awk '{print $2}' | xargs kill -9
ps -ef | egrep 'airflow scheduler' | grep -v grep| awk '{print $2}' | xargs kill -9
rm -rf *.pid
airflow scheduler -D
airflow webserver --port 8181 -D
```

4. bakcfill
```shell
airflow dags trigger -e '2023-08-01' events_etl_dag
airflow dags backfill \
    --start-date '2023-08-01' \
    --end-date  '2023-08-06' \
    events_etl_dag
```