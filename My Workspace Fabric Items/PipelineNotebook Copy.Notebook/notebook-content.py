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

# MARKDOWN ********************

# **Adding data to the fact table.**

# CELL ********************

from pyspark.sql.functions import to_date, col

SalesOrderHeader_filePath = "abfss://data@datalake2198.dfs.core.windows.net/parquet/SalesOrderHeader.parquet"
SalesOrderDetail_filePath = "abfss://data@datalake2198.dfs.core.windows.net/parquet/SalesOrderDetail.parquet"

SalesOrderHeader_df = spark.read.format("parquet").load(SalesOrderHeader_filePath).alias("sh")
SalesOrderDetail_df = spark.read.format("parquet").load(SalesOrderDetail_filePath).alias("sd")

salesorderheader_changed_df = SalesOrderHeader_df.withColumn(
    "OrderDate", to_date("OrderDate", "M/d/yyyy H:mm")
)

fact_sales_df_join = salesorderheader_changed_df.join(
    SalesOrderDetail_df,
    on="SalesOrderID",
    how="leftouter"
)

fact_sales_df = fact_sales_df_join.select(
    col("sh.SalesOrderID").alias("SalesOrderID"),
    "OrderDate",
    "Status",
    "SalesOrderNumber",
    "CustomerID",
    "SalesPersonID",
    "TerritoryID",
    "SubTotal",
    "TaxAmt",
    "Freight",
    "TotalDue"
)

fact_sales_df.write.mode("append").format("delta").saveAsTable("factsales")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Adding Data to Dimension Product table**

# CELL ********************

Product_filePath = "abfss://data@datalake2198.dfs.core.windows.net/parquet/Product.parquet"
ProductModel_filePath = "abfss://data@datalake2198.dfs.core.windows.net/parquet/ProductModel.parquet"

Product_df = spark.read.format("parquet").load(Product_filePath).alias("pt")
ProductModel_df = spark.read.format("parquet").load(ProductModel_filePath).alias("pm")

NullReplaceProduct_df = Product_df.withColumn("Color", when(col("Color").isNull(), lit("Not Applicable")).otherwise(col("Color")))

dim_product_join_df = NullReplaceProduct_df.join(ProductModel_df, on = "ProductModelID", how = "leftouter")

dim_product_df = dim_product_join_df.select(
    "ProductID",col("pt.Name").alias("ProductName"),"Color","SafetyStockLevel","ProductModelID",col("pm.Name").alias("ProductModelName")
    )

dim_product_df.write.mode("append").format("delta").saveAsTable("dimproduct")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
