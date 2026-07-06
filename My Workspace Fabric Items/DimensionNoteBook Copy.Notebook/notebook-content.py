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

Customer_filePath = "abfss://data@datalake2198.dfs.core.windows.net/parquet/Customer01.parquet" # Path where the file resides

Customer_df = spark.read.format("parquet").load(Customer_filePath) # Creating a dataframe on the file

display(Customer_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

Customer_Email_df = Customer_df.select("EmailAddress")
display(Customer_Email_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Generating a new column by taking the Customer_Email_df dataframe

EmailSplit_df = Customer_Email_df \
    .withColumn("EmailPrefix", split(col("EmailAddress"), "@").getItem(0)) \
    .withColumn("EmailSuffix", split(col("EmailAddress"), "@").getItem(1))

display(EmailSplit_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Creating Dimension Tables**

# CELL ********************

Product_filePath = "abfss://data@datalake2198.dfs.core.windows.net/parquet/Product.parquet"
ProductModel_filePath = "abfss://data@datalake2198.dfs.core.windows.net/parquet/ProductModel.parquet"

Product_df = spark.read.format("parquet").load(Product_filePath).alias("pt")
ProductModel_df = spark.read.format("parquet").load(ProductModel_filePath).alias("pm")

display(Product_df)
display(ProductModel_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Replacing the NULL values in the color column of the dataframe with "Not Applicable
NullReplaceProduct_df = Product_df.withColumn("Color", when(col("Color").isNull(), lit("Not Applicable")).otherwise(col("Color")))
display(NullReplaceProduct_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE TABLE DimProduct(
# MAGIC 	ProductID int,
# MAGIC 	ProductName varchar(50),
# MAGIC 	Color varchar(15),
# MAGIC 	SafetyStockLevel int,
# MAGIC 	ProductModelID int,
# MAGIC 	ProductModelName varchar(500)
# MAGIC )

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dim_product_join_df = NullReplaceProduct_df.join(ProductModel_df, on = "ProductModelID", how = "leftouter")
display(dim_product_join_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dim_product_df = dim_product_join_df.select(
    "ProductID",col("pt.Name").alias("ProductName"),"Color","SafetyStockLevel","ProductModelID",col("pm.Name").alias("ProductModelName")
    )
display(dim_product_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dim_product_df.write.mode("append").format("delta").saveAsTable("dimproduct")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT * FROM DimProduct

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Building the Customer Dimension Table**

# CELL ********************

Customer01_filePath = "abfss://data@datalake2198.dfs.core.windows.net/parquet/Customer.parquet"
Store_filePath = "abfss://data@datalake2198.dfs.core.windows.net/parquet/Store.parquet"

Customer01_df = spark.read.format("parquet").load(Customer01_filePath).alias("ct")
Store_df = spark.read.format("parquet").load(Store_filePath).alias("st")

display(Customer01_df)
display(Store_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dim_customer_join_df = Customer01_df.join(Store_df,Customer01_df.StoreID==Store_df.BusinessEntityID,how="leftouter")
display(dim_customer_join_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dim_customer_df = dim_customer_join_df.select(
 "CustomerID","StoreID",col("Name").alias("StoreName")
)
 
display(dim_customer_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE TABLE DimCustomer(
# MAGIC 	CustomerID int,
# MAGIC 	StoreID int,
# MAGIC 	StoreName varchar(500)
# MAGIC )

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dim_customer_df.write.mode("append").format("delta").saveAsTable("DimCustomer")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT * FROM DimCustomer

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC delete from FactSales;
# MAGIC 
# MAGIC delete from DimCustomer;
# MAGIC 
# MAGIC delete from DimProduct;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
