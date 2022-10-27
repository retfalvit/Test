# Databricks notebook source
# MAGIC %md
# MAGIC Task 2a

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP VIEW IF EXISTS [dbo].[Customer_Merged_dedup]
# MAGIC GO
# MAGIC 
# MAGIC CREATE VIEW [dbo].[Customer_Merged_dedup] AS
# MAGIC SELECT 
# MAGIC DimCustomerBK,
# MAGIC CustomerNumber,
# MAGIC LegalEntity,
# MAGIC CoutryRegion,
# MAGIC Name,
# MAGIC CustomerGroup,
# MAGIC City,
# MAGIC State,
# MAGIC OnHoldStatus,
# MAGIC PaymentTerms,
# MAGIC PaymentMethod,
# MAGIC BankTransactionType,
# MAGIC CustomerType2,
# MAGIC ClassificationGroupName
# MAGIC FROM
# MAGIC (SELECT 
# MAGIC DimCustomerBK,
# MAGIC CustomerNumber,
# MAGIC LegalEntity,
# MAGIC CoutryRegion,
# MAGIC Name,
# MAGIC CustomerGroup,
# MAGIC City,
# MAGIC State,
# MAGIC OnHoldStatus,
# MAGIC PaymentTerms,
# MAGIC PaymentMethod,
# MAGIC BankTransactionType,
# MAGIC CustomerType2,
# MAGIC ClassificationGroupName,
# MAGIC SourceSystem,
# MAGIC ROW_NUMBER() OVER (PARTITION BY DimCustomerBK ORDER BY SourceSystem ASC) rnk
# MAGIC FROM [dbo].[Customer_Merged]) AS original
# MAGIC WHERE original.rnk = 1

# COMMAND ----------

# MAGIC %md
# MAGIC Task 2b

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP VIEW IF EXISTS [dbo].[Sum_AmountCCY_GreaterThan2000]
# MAGIC GO
# MAGIC 
# MAGIC CREATE VIEW [dbo].[Sum_AmountCCY_GreaterThan2000] AS
# MAGIC SELECT
# MAGIC c.Name,
# MAGIC SUM(f.AmountCCY) as sum 
# MAGIC FROM [dbo].[FactGeneralLedgerTransactions] f
# MAGIC JOIN [dbo].[Customer_Merged_dedup] c ON f.DimCustomerBK = c.DimCustomerBK
# MAGIC GROUP BY c.Name
# MAGIC HAVING SUM(f.AmountCCY) > 2000

# COMMAND ----------

# MAGIC %md
# MAGIC Task 2c

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE [dbo].[FactGeneralLedgerTransactions] 
# MAGIC SET Description = CONCAT_WS('-',Description,COALESCE(d.CoutryRegion,' '),COALESCE(d.City, ' '))
# MAGIC FROM [dbo].[FactGeneralLedgerTransactions] f
# MAGIC JOIN [dbo].[Customer_Merged_dedup] d ON f.DimCustomerBK = d.DimCustomerBK

# COMMAND ----------

# MAGIC %md
# MAGIC Task 3

# COMMAND ----------

# MAGIC %sql
# MAGIC --PK dropping needs to be done before modification of them. Then reset them. Like below:
# MAGIC ALTER TABLE [dbo].[OnPrem_FactGeneralLedgerTransactions]
# MAGIC DROP CONSTRAINT PK_OnPrem_FactGeneralLedgerTransactions
# MAGIC 
# MAGIC ALTER TABLE [dbo].[OnPrem_FactGeneralLedgerTransactions]
# MAGIC ADD CONSTRAINT PK_OnPrem_FactGeneralLedgerTransactions PRIMARY KEY (Col1,Col2..)
# MAGIC 
# MAGIC -- Solution
# MAGIC DECLARE
# MAGIC @Tname varchar(50),
# MAGIC @Tcolumn varchar(50)
# MAGIC DECLARE cursor_Tables CURSOR FOR
# MAGIC SELECT t.table_name, t.column_name
# MAGIC FROM information_schema.columns t
# MAGIC WHERE t.COLUMN_NAME LIKE ‘A_%’
# MAGIC AND t.DATA_TYPE = ‘date’ 
# MAGIC AND t.TABLE_SCHEMA = ‘SAP’
# MAGIC OPEN cursor_Tables
# MAGIC FETCH NEXT FROM cursor_Tables INTO @Tname, @Tcolumn
# MAGIC PRINT 'table' + 'column'
# MAGIC WHILE @@FETCH_STATUS = 0
# MAGIC BEGIN
# MAGIC     EXEC('ALTER TABLE [dbo].['+@tname+'] ALTER COLUMN ['+@tcolumn+'] nvarchar(50)’)
# MAGIC     FETCH NEXT FROM cursor_Tables INTO @Tname, @Tcolumn
# MAGIC END;
# MAGIC CLOSE cursor_Tables;
# MAGIC DEALLOCATE cursor_Tables;
