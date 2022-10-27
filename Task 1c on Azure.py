# Databricks notebook source
# MAGIC %md
# MAGIC SQL Database code:

# COMMAND ----------

# MAGIC %sql
# MAGIC -- enabling change tracking
# MAGIC ALTER TABLE [dbo].[OnPrem_FactGeneralLedgerTransactions]
# MAGIC ALTER COLUMN DimSalesLineBK nvarchar(50) not null
# MAGIC ALTER TABLE [dbo].[OnPrem_FactGeneralLedgerTransactions]
# MAGIC ALTER COLUMN  GLAccountNum INT not null
# MAGIC 
# MAGIC ALTER TABLE [dbo].[OnPrem_FactGeneralLedgerTransactions]
# MAGIC ADD PRIMARY KEY (DimSalesLineBK, GLAccountNum)
# MAGIC 
# MAGIC 
# MAGIC ALTER DATABASE TEST
# MAGIC SET CHANGE_TRACKING = ON  
# MAGIC (CHANGE_RETENTION = 2 DAYS, AUTO_CLEANUP = ON)  
# MAGIC 
# MAGIC ALTER TABLE [dbo].[OnPrem_FactGeneralLedgerTransactions]
# MAGIC ENABLE CHANGE_TRACKING  
# MAGIC WITH (TRACK_COLUMNS_UPDATED = ON)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- create new table then insert current version into it.
# MAGIC create table table_store_ChangeTracking_version
# MAGIC (
# MAGIC     TableName varchar(255),
# MAGIC     SYS_CHANGE_VERSION BIGINT,
# MAGIC );
# MAGIC 
# MAGIC DECLARE @ChangeTracking_version BIGINT
# MAGIC SET @ChangeTracking_version = CHANGE_TRACKING_CURRENT_VERSION();  
# MAGIC 
# MAGIC INSERT INTO table_store_ChangeTracking_version
# MAGIC VALUES ('[dbo].[OnPrem_FactGeneralLedgerTransactions]', @ChangeTracking_version)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Creating the procedure for the pipeline / update last value for each pipeline run at the end of that.
# MAGIC CREATE PROCEDURE Update_ChangeTracking_Version @CurrentTrackingVersion BIGINT, @TableName varchar(50)
# MAGIC AS
# MAGIC 
# MAGIC BEGIN
# MAGIC 
# MAGIC UPDATE table_store_ChangeTracking_version
# MAGIC SET [SYS_CHANGE_VERSION] = @CurrentTrackingVersion
# MAGIC WHERE [TableName] = @TableName
# MAGIC 
# MAGIC END

# COMMAND ----------

# MAGIC %md
# MAGIC ADF settings:

# COMMAND ----------

# MAGIC %sql
# MAGIC ---LookupLastChangeTrackingVersionActivity:
# MAGIC ---Using query on table (dataset of dbo.table_store_ChangeTracking_version)

# COMMAND ----------

# MAGIC %sql
# MAGIC --LookupCurrentChangeTrackingVersionActivity
# MAGIC --Using query on dataset of dbo.OnPrem_FactGeneralLedgerTransactions
# MAGIC SELECT CHANGE_TRACKING_CURRENT_VERSION() as CurrentChangeTrackingVersion

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Copy data activity
# MAGIC -- Using query on dataset of dbo.OnPrem_FactGeneralLedgerTransactions
# MAGIC select on_prem.* from [dbo].[OnPrem_FactGeneralLedgerTransactions] on_prem RIGHT OUTER JOIN CHANGETABLE(CHANGES [dbo].[OnPrem_FactGeneralLedgerTransactions], @{activity('LookupLastChangeTrackingVersionActivity').output.firstRow.SYS_CHANGE_VERSION}) as CT on on_prem.DimSalesLineBK = CT.DimSalesLineBK AND on_prem.GLAccountNum = CT.GLAccountNum  where CT.SYS_CHANGE_VERSION <= @{activity('LookupCurrentChangeTrackingVersionActivity').output.firstRow.CurrentChangeTrackingVersion}
# MAGIC 
# MAGIC -- Sink is the other SQL table/ Write behaviour is upsert

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Settings of Stored procedure.
# MAGIC -- Set the name of the previously created stored procedure on Azure SQL
# MAGIC -- Import parameters:
# MAGIC -- CurrentTrackingVersion Value: @{activity('LookupCurrentChangeTrackingVersionActivity').output.firstRow.CurrentChangeTrackingVersion}
# MAGIC -- TableName Value:@{activity('LookupLastChangeTrackingVersionActivity').output.firstRow.TableName}
