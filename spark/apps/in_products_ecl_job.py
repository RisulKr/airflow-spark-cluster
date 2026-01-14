from pyspark.sql import SparkSession
import os




spark = SparkSession.builder \
    .appName("Read Products ECL") \
    .getOrCreate()


jdbc_url = os.getenv("JDBC_URL")
db_user = os.getenv("DB_USER")
db_password = os.getenv("DB_PASSWORD")

db_props = {
    "user": db_user,
    "password": db_password,
    "driver": "org.postgresql.Driver"
}

df_products = spark.read.option("inferSchema","true").option("header", True).csv("/data/dim_products.csv")
df_products.write.mode("overwrite").jdbc(
    url=jdbc_url,
    table="dim_product",
    properties=db_props
)




spark.stop()
