from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

with DAG(
    dag_id="DAG_load_customers",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:

    load = SparkSubmitOperator(
        task_id="load_customers_csv",
        application="/opt/spark/apps/in_customers_ecl_job.py",
        conn_id="spark_conn",
        packages="org.postgresql:postgresql:42.6.0",
        conf={
            "spark.master": "spark://spark-master:7077",
        },

    )
    load