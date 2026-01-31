from pyspark.sql import SparkSession
import os
import shutil
import glob

spark = SparkSession.builder \
    .appName("Read Customers ECL") \
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

# 🔍 Find all customer files
input_files = glob.glob(f"{raw_dir}/dim_customers_*.csv")

if not input_files:
    print("No new customer files found.")

for file_path in input_files:
    file_name = os.path.basename(file_path)
    processed_path = f"{processed_dir}/{file_name}"

    # ✅ Skip already processed files
    if os.path.exists(processed_path):
        print(f"{file_name} already processed. Skipping.")
        continue

    df_customers = spark.read \
        .option("inferSchema", "true") \
        .option("header", True) \
        .csv(file_path)

    df_customers.write \
        .mode("append") \
        .jdbc(
            url=jdbc_url,
            table="dim_customers",
            properties=db_props
        )

    # ✅ Archive only after successful DB write
    shutil.move(file_path, processed_path)
    print(f"{file_name} successfully loaded and archived.")

spark.stop()
