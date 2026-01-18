from pyspark.sql import SparkSession
import os




spark = SparkSession.builder \
    .appName("Read Customers ECL V2") \
    .getOrCreate()


jdbc_url = os.getenv("JDBC_URL")
db_user = os.getenv("DB_USER")
db_password = os.getenv("DB_PASSWORD")
table = "dim_products"

db_props = {
    "user": db_user,
    "password": db_password,
    "driver": "org.postgresql.Driver"
}

df_products = spark.read.option("inferSchema","true").option("header", True).csv("/data/raw/products_*.csv")


df_products.write.mode("append").jdbc(
    url=jdbc_url,
    table=table,
    properties=db_props
)




spark.stop()
