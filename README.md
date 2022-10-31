#### 本地pycharm python3环境安装依赖
```shell
pip install apache-airflow==2.2.2 apache-airflow-providers-amazon==2.4.0 \
  --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.2.2/constraints-3.7.txt"
```

#### 使用步骤
```markdown
1. mwaa webui 中添加变量key=s3_sql_bucket,value=存放SQL的桶名称
2. mwaa webui 中配置redshift_default Connection,Host=Redshift Endpoint链接地址，Schema=Redshift数据库，Port=5439
Login=用户名，password=密码
```
```shell
# dags目录内容同步到MWAA dags 目录
aws s3 sync ./dags/ s3://mwaa-app-01/dags/
# sql目录内容同步到指定的存储SQL的S3路径中
aws s3 sync ./sqls/ s3://mwaa-app-01/sqls/
```

#### 相关截图
![](https://pcmyp.oss-accelerate.aliyuncs.com/markdown/20221031223837.png)
![](https://pcmyp.oss-accelerate.aliyuncs.com/markdown/20221031223807.png)
![](https://pcmyp.oss-accelerate.aliyuncs.com/markdown/20221031223442.png)