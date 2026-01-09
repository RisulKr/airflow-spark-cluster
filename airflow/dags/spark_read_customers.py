from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

with DAG(
    dag_id="spark_read_customers",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:

    read_csv = SparkSubmitOperator(
        task_id="read_customers_csv",
        application="/opt/spark/apps/read_customers.py",
        conn_id="spark_conn",
        conf={
            "spark.master": "spark://spark-master:7077",
            "spark.pyspark.python": "python3",
            "spark.pyspark.driver.python": "python3",
        },
        verbose=True,
    )
    read_csv