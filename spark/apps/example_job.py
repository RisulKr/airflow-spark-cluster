from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("AirflowSparkJob").getOrCreate()

df = spark.createDataFrame(
    [(1, "Airflow"), (2, "Spark")],
    ["id", "name"]
)

df.show()

spark.stop()
