from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = (
    SparkSession.builder
    .appName("TransformCustomersCSV")
    .getOrCreate()
)

df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("/data/customers.csv")
)



df.write \
  .format("org.elasticsearch.spark.sql") \
  .option("es.nodes", "es") \
  .option("es.port", "9200") \
  .option("es.nodes.wan.only", "true") \
  .option("es.resource", "customers/_doc") \
  .mode("overwrite") \
  .save()

print("Data written to Elasticsearch successfully")

spark.stop()
