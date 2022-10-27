# Databricks notebook source
# MAGIC %run ./Properties

# COMMAND ----------

from pyspark.sql import *

# COMMAND ----------

cloud_table = (spark.read
  .format("jdbc")
  .option("url", url)
  .option("dbtable", "OnPrem_FactGeneralLedgerTransactions")
  .option("user", properties["user"])
  .option("password", properties["password"])
  .load()
)

# COMMAND ----------

#Writing data to Azure Sql
table_name = "FactGeneralLedgerTransactions"
mode = "overwrite"

finaldf = DataFrameWriter(cloud_table)
finaldf.jdbc(url=url, table=table_name, mode=mode, properties = properties)

