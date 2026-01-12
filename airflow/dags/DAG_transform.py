from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

with DAG(
    dag_id="DAG_transform_customers",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:

    transform_csv = SparkSubmitOperator(
        task_id="transform_customers_csv",
        application="/opt/spark/apps/transform.py",
        conn_id="spark_conn",
        conf={
            "spark.master": "spark://spark-master:7077",
            "spark.pyspark.python": "python3",
            "spark.pyspark.driver.python": "python3",
        },
        verbose=True,
        packages="org.elasticsearch:elasticsearch-spark-30_2.12:7.16.1"
    )
    transform_csv