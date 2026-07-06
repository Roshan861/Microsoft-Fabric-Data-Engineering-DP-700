# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "2020a881-106f-45f9-a2ce-d382b3b5200d",
# META       "default_lakehouse_name": "Silver_LH",
# META       "default_lakehouse_workspace_id": "a7d080c0-b98d-4748-8ce9-8d2b827c9244",
# META       "known_lakehouses": [
# META         {
# META           "id": "2020a881-106f-45f9-a2ce-d382b3b5200d"
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

# MAGIC %%sql
# MAGIC CREATE TABLE DimCustomer (
# MAGIC     Customer_SK int,
# MAGIC 	CustomerID int,
# MAGIC 	CustomerName varchar(1000),
# MAGIC 	CustomerCategoryName varchar(1000)	
# MAGIC )

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

customers_df=spark.read.table("Bronze_LH.Sales.Customers")
customer_categories_df=spark.read.table("Bronze_LH.Sales.CustomerCategories")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

customers_filtered_df=customers_df.select("CustomerID","CustomerName","CustomerCategoryID")
customer_categories_filtered_df=customer_categories_df.select("CustomerCategoryID","CustomerCategoryName")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

customer_join_df=customers_filtered_df.join(customer_categories_filtered_df,on="CustomerCategoryID",how="leftouter")
customer_join_filtered_df=customer_join_df.select("CustomerID","CustomerName","CustomerCategoryName")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

customer_dim_df=customer_join_filtered_df.withColumn("Customer_SK",monotonically_increasing_id())
customer_dim_final_df=customer_dim_df.select("CustomerID","CustomerName","CustomerCategoryName",col("Customer_SK").cast('int'))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

customer_dim_final_df.write.mode("append").saveAsTable("DimCustomer")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT * from DimCustomer;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE TABLE DimStock (
# MAGIC 	Stock_SK int,
# MAGIC 	StockItemID int,
# MAGIC 	StockItemName varchar(1000),
# MAGIC 	StockGroupName varchar(1000)
# MAGIC )

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

stockitems_df=spark.read.table("Bronze_LH.Sales.StockItems")
stock_groups_df=spark.read.table("Bronze_LH.Sales.StockGroups")
stock_item_stock_groups_df=spark.read.table("Bronze_LH.Sales.StockItemStockGroups")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

stockitems_filtered_df=stockitems_df.select("StockItemID","StockItemName")
stock_groups_filtered_df=stock_groups_df.select("StockGroupID","StockGroupName")
stock_item_stock_groups_filtered_df=stock_item_stock_groups_df.select("StockItemStockGroupID","StockItemID","StockGroupID")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

stock_join_df = stockitems_filtered_df.join(stock_item_stock_groups_filtered_df,on="StockItemID",how="leftouter")
stock_join_filtered_df = stock_join_df.select("StockItemID","StockItemName","StockGroupID")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

stock_group_join_df=stock_join_filtered_df.join(stock_groups_df,on="StockGroupID",how="leftouter")
stock_group_join_filtered_df=stock_group_join_df.select("StockItemID","StockItemName","StockGroupName")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

stock_group_join_dropduplicates_df = stock_group_join_filtered_df.dropDuplicates(['StockItemID'])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

stock_dim_df=stock_group_join_dropduplicates_df.withColumn("Stock_SK",monotonically_increasing_id())
stock_dim_final_df=stock_dim_df.select("StockItemID","StockItemName","StockGroupName",col("Stock_SK").cast('int'))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

stock_dim_final_df.write.mode("append").saveAsTable("DimStock")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT * FROM DimStock;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
