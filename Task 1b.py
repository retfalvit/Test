# Databricks notebook source
# MAGIC %run ./Properties

# COMMAND ----------

import re, fnmatch
from pyspark.sql.functions import *
file_folder = "/mnt/testdata/Customer"
pattern = "DimCustomer_*.csv"
dfs = []

for item in dbutils.fs.ls(file_folder):
  if fnmatch.fnmatch(item.name,pattern):
    regexp = re.compile(r'DimCustomer_(.+).csv')
    re_match = regexp.match(item.name)
    source = re_match.group(1)
      
    # File location and type
    file_location = f"/mnt/testdata/Customer/DimCustomer_{source}.csv"
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
      
    df = df.withColumn("SourceSystem", lit(source))
    dfs.append(df)
      

# COMMAND ----------

from functools import reduce 
from  pyspark.sql import DataFrameWriter
df = reduce(DataFrame.unionAll, dfs)

#Writing data to Azure Sql
table_name = "Customer_Merged"
mode = "overwrite"

finaldf = DataFrameWriter(df)
finaldf.jdbc(url=url, table=table_name, mode=mode, properties = properties)
