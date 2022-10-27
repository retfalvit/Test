# Databricks notebook source
# MAGIC %run ./Properties

# COMMAND ----------

from pyspark.sql import *
# File folder, storage account, container name
file_folder = "/mnt/testdata/Dim"
storage_account = "trfordatabricks"
container = "data"

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

from pyspark.sql import *
# File location and type
file_location = f"{file_folder}/Currency.csv"
file_type = "csv"

# CSV options
infer_schema = "false"
first_row_is_header = "true"
delimiter = ";"

# The applied options are for CSV files. For other file types, these will be ignored.
df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

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
