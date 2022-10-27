# Databricks notebook source
# MAGIC %run ./Properties

# COMMAND ----------

pip install dlt

# COMMAND ----------

import dlt
from pyspark.sql import *
# File folder, location and type
# File folder, storage account, container, location and type name
file_folder = "/mnt/testdata/Dim"
storage_account = "trfordatabricks"
container = "data"
file_location = f"{file_folder}/Currency.csv"
file_type = "csv"

# COMMAND ----------

if not any(mount.mountPoint == file_folder for mount in dbutils.fs.mounts()):
  try:
    dbutils.fs.mount(
    source = f"wasbs://{container}@{storage_account}.blob.core.windows.net",
    mount_point = file_folder,
    extra_configs = {f"fs.azure.account.key.{storage_account}.blob.core.windows.net":key}
    )
  except Exception as e:
    print("Already mounted. Try to unmount first")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REFRESH STREAMING LIVE TABLE Currency_raw
# MAGIC COMMENT "The csv file ingested from path"
# MAGIC AS SELECT * FROM cloud_files(file_location, file_type, map("infer_schema", "false", "header", "true", "delimiter", ";"))

# COMMAND ----------

df = spark.table("LIVE.customers")

# COMMAND ----------

for col in df.columns:
  if '[' in col or ']' in col:
    col_renamed = col.replace('[','').replace(']','')
    df = df.withColumnRenamed(col,col_renamed)

# COMMAND ----------

#Writing data to Azure Sql
table_name = "Currency"
mode = "overwrite"

finaldf = DataFrameWriter(df)
finaldf.jdbc(url=url, table=table_name, mode=mode, properties=properties)
