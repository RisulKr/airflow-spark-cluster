from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("ReadCustomersCSV")
    .getOrCreate()
)

df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("/data/customers.csv")
)

df.show(truncate=False)
df.printSchema()

spark.stop()
