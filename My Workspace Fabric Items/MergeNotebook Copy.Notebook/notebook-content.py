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

Product01_filePath = "abfss://product@datalake2198.dfs.core.windows.net/Product01.parquet"
Product02_filePath = "abfss://product@datalake2198.dfs.core.windows.net/Product02.parquet"

Product01_df = spark.read.format("parquet").load(Product01_filePath).alias("p1")

display(Product01_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.table("product").printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### **There is a datatype mismatch between the columns of the dataframe and the existing product table to which the data is being appended. Casting the datatypes of the below dataframe columns to make them compatible with those of the products table and also setting the mergeSchema option to True**

# CELL ********************

Product01_df = Product01_df.withColumn("ProductID", col("ProductID").cast("int"))
Product01_df = Product01_df.withColumn("StandardCost", col("StandardCost").cast("decimal(19,4)"))
Product01_df = Product01_df.withColumn("ListPrice", col("ListPrice").cast("decimal(19,4)"))
Product01_df = Product01_df.withColumn("ProductModelID", col("ProductModelID").cast("int"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Creating a new product table based on the above dataframe
Product01_df.write.mode("append").format("delta").option("mergeSchema", "true").saveAsTable("product")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

productupdates_df=spark.read.load(Product02_filePath,format="parquet")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### **Upserting into a delta table using the merge option to insert new rows of info in the product table iff it dne.**

# CELL ********************

from delta.tables import *
 
deltaTableProduct=DeltaTable.forName(spark,"product")
 
deltaTableProduct.alias('product') \
.merge(productupdates_df.alias('ptupdates'),'product.ProductID = ptupdates.ProductID') \
.whenNotMatchedInsert(values=
{
	"ProductID" : "ptupdates.ProductID",
	"Name" : "ptupdates.Name",
	"ProductNumber" : "ptupdates.ProductNumber",
	"MakeFlag" : "ptupdates.MakeFlag",
	"Color" : "ptupdates.Color",
	"SafetyStockLevel" :"ptupdates.SafetyStockLevel",
	"StandardCost" :"ptupdates.ListPrice",
	"ListPrice" : "ptupdates.ListPrice",
	"ProductSubCategoryID" :"ptupdates.ProductSubCategoryID",
	"ProductModelID": "ptupdates.ProductModelID"
}
).execute()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT * FROM product

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
