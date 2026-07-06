# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "5ef15e25-26f6-48ab-a37b-91d89d0a2fa1",
# META       "default_lakehouse_name": "Lakehouse_01",
# META       "default_lakehouse_workspace_id": "f2e2c657-a625-47e2-a93c-04ee7433bccb",
# META       "known_lakehouses": [
# META         {
# META           "id": "5ef15e25-26f6-48ab-a37b-91d89d0a2fa1"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql.functions import *

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

filePath = "abfss://data@datalake2198.dfs.core.windows.net/parquet/Product.parquet"

df = spark.read.format("parquet").load(filePath)

display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

new_df = df.select("SalesOrderID", "TerritoryID").groupBy("TerritoryID").count()
display(new_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

prod_df = df.filter(col('Color').isNull())
display(prod_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Taking the df dataframe and checking for all the columns that may have NULL values and getting the number of rows where the columns have NULL values.**

# CELL ********************

column_nulls=df.select(
    [
    sum(col(cols_in_frame).isNull().cast("int")).alias(cols_in_frame)    
    for cols_in_frame in df.columns
])
display(column_nulls)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Checking for duplicate rows in the dataset.**

# CELL ********************

filePath = "abfss://data@datalake2198.dfs.core.windows.net/parquet/SalesOrderDetail.parquet"

df = spark.read.format("parquet").load(filePath)

display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dupl_df = df.groupBy("SalesOrderID").count().filter("count > 1")
display(dupl_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **To check for duplicate rows in all the columns of the dataframe**

# CELL ********************

dupl_rows_df = df.groupBy(df.columns).count().filter("count > 1")
display(dupl_rows_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Building the Fact Tables**

# CELL ********************

SalesOrderHeader_filePath = "abfss://data@datalake2198.dfs.core.windows.net/parquet/SalesOrderHeader.parquet"
SalesOrderDetail_filePath = "abfss://data@datalake2198.dfs.core.windows.net/parquet/SalesOrderDetail.parquet"

SalesOrderHeader_df = spark.read.format("parquet").load(SalesOrderHeader_filePath).alias("sh")
SalesOrderDetail_df = spark.read.format("parquet").load(SalesOrderDetail_filePath).alias("sd")

display(SalesOrderHeader_df)
display(SalesOrderDetail_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **We can automatically drop the duplicate rows based on the column names in the dataframe using dropDuplicates() function.**

# CELL ********************

dropDupl_df = SalesOrderDetail_df.dropDuplicates(["SalesOrderID"])
display(dropDupl_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

extracted_df = SalesOrderHeader_df.select("SalesOrderID", "OrderDate", year("OrderDate").alias("Year"), month("OrderDate").alias("Month"))
display(extracted_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

SalesOrderHeader_df_changed = SalesOrderHeader_df.withColumn("OrderDate", to_date("OrderDate", "M/d/yyyy H:mm"))
display(SalesOrderHeader_df_changed)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

fact_sales_df_join = SalesOrderHeader_df_changed.join(SalesOrderDetail_df, on = "SalesOrderID", how = "leftouter")
display(fact_sales_df_join)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

fact_sales_df=fact_sales_df_join.select("SalesOrderID",
    "OrderDate","Status", \
  "SalesOrderNumber","CustomerID","SalesPersonID","TerritoryID","SubTotal","TaxAmt","Freight","TotalDue"
)

display(fact_sales_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE TABLE FactSales
# MAGIC ( 
# MAGIC 	SalesOrderID int,
# MAGIC     OrderDate date, 
# MAGIC 	Status int,	
# MAGIC 	SalesOrderNumber varchar(25),	
# MAGIC 	CustomerID int,
# MAGIC 	ProductID int,
# MAGIC 	SalesPersonID int,
# MAGIC 	TerritoryID int,	
# MAGIC 	SubTotal decimal(19,4),
# MAGIC 	TaxAmt decimal(19,4),
# MAGIC 	Freight decimal(19,4),
# MAGIC 	TotalDue decimal(19,4)
# MAGIC );

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

fact_sales_df.write.mode("append").format("delta").saveAsTable("factsales")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT * from FactSales

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
