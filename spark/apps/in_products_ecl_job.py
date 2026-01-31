from pyspark.sql import SparkSession
import os
import shutil
import glob

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

raw_dir = "/data/raw"
processed_dir = "/data/processed"

os.makedirs(processed_dir, exist_ok=True)

input_files = glob.glob(f"{raw_dir}/dim_products_*.csv")

if not input_files:
    print("No new product files found.")

for file_path in input_files:
    file_name = os.path.basename(file_path)
    processed_path = f"{processed_dir}/{file_name}"

    if os.path.exists(processed_path):
        print(f"{file_name} already processed. Skipping.")
        continue

    df_products = spark.read \
        .option("inferSchema", "true") \
        .option("header", True) \
        .csv(file_path)

    df_products.write \
        .mode("append") \
        .jdbc(
            url=jdbc_url,
            table="dim_product",
            properties=db_props
        )

    shutil.move(file_path, processed_path)
    print(f"{file_name} processed successfully.")

spark.stop()
