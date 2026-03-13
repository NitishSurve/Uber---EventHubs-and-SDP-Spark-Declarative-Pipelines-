# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

import pandas as pd

files = [
{"file":"bulk_rides"},
{"file":"map_cancellation_reasons"},
{"file": "map_cities"},
{"file":"map_payment_methods"},
{"file":"map_ride_statuses"},
{"file":"map_vehicle_makes"},
{"file":"map_vehicle_types"}
]

for file in files:
    url = f"https://uberdl2397.blob.core.windows.net/raw/ingestion/{file['file']}.json?sp=r&st=2026-03-11T11:54:59Z&se=2026-03-18T20:09:59Z&spr=https&sv=2024-11-04&sr=c&sig=tFqhVJX1EKJQLY24OWuLNkNuTYqatZXgOXOXQEX8Lsk%3D"
    #Writing data to Bronze Layer
    df = pd.read_json(url)
    df_spark = spark.createDataFrame(df)
    df_spark.write.format("delta").mode("overwrite") \
        .option("overwriteSchema","true") \
        .saveAsTable(f"uber.bronze.{file['file']}")


# COMMAND ----------

url = "https://uberdl2397.blob.core.windows.net/raw/ingestion/bulk_rides.json?sp=r&st=2026-03-11T11:54:59Z&se=2026-03-18T20:09:59Z&spr=https&sv=2024-11-04&sr=c&sig=tFqhVJX1EKJQLY24OWuLNkNuTYqatZXgOXOXQEX8Lsk%3D"

df = pd.read_json(url)
df_spark = spark.createDataFrame(df)
if not spark.catalog.tableExists("uber.bronze.bulk_rides"):
    df_spark.write.format("delta")\
            .mode("overwrite")\
            .saveAsTable(f"uber.bronze.bulk_rides")
    print("This will not run more than 1 time")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from uber.bronze.map_cities;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from uber.bronze.rides_raw;

# COMMAND ----------

