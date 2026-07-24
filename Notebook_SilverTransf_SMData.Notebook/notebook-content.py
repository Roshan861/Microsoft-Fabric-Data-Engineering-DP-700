# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "3fdf99ec-242d-4f72-8b33-c35f378d9ca5",
# META       "default_lakehouse_name": "ShoppingMart_Silver_LH",
# META       "default_lakehouse_workspace_id": "f52d8381-48b2-4226-b022-5e6cf063341a",
# META       "known_lakehouses": [
# META         {
# META           "id": "21fbab00-9e78-48bc-aa0e-99472cd018de"
# META         },
# META         {
# META           "id": "3fdf99ec-242d-4f72-8b33-c35f378d9ca5"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# #### **SILVER LAYER NOTEBOOK: DATA CLEANING AND INTEGRATION**

# MARKDOWN ********************

# ##### **LOAD DATA FROM BRONZE LAYER**

# CELL ********************

from pyspark.sql.functions import *
from pyspark.sql.types import *

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_customers = spark.read.format("csv")\
                         .option("header", "true")\
                         .load("Files/ShoppingMart_Bronze_Customers/ShoppingMart_customers.csv")

df_orders = spark.read.format("csv")\
                         .option("header", "true")\
                         .load("Files/ShoppingMart_Bronze_Orders/ShoppingMart_orders.csv")

df_products = spark.read.format("csv")\
                         .option("header", "true")\
                         .load("Files/ShoppingMart_Bronze_Products/ShoppingMart_products.csv")

df_reviews = spark.read.json("Files/unstructdatafiles/ShoppingMart_Bronze_Reviews/ShoppingMart_review.json")

df_social = spark.read.json("Files/unstructdatafiles/ShoppingMart_Bronze_Social_Media/ShoppingMart_social_media.json")

df_weblogs = spark.read.json("Files/unstructdatafiles/ShoppingMart_Bronze_Web_Logs/ShoppingMart_web_logs.json")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### **DATA CLEANING AND ENRICHING**

# CELL ********************

df_orders = df_orders.dropna(subset = ["OrderID", "CustomerID", "ProductID", "OrderDate", "TotalAmount"])
df_orders = df_orders.withColumn("OrderDate", to_date(col("OrderDate")))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### **JOIN ORDERS WITH PRODUCTS & CUSTOMERS**

# CELL ********************

df_orders = df_orders.join(df_customers, on = 'CustomerID', how = "inner")\
                     .join (df_products, on = 'ProductID', how = "inner")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### **WRITE DATA TO SILVER LAYER**

# CELL ********************

df_orders.write.mode("overwrite").parquet("Files/ShoppingMart_Silver_Orders/ShoppingMart_customers_orderdata")
df_reviews.write.mode("overwrite").parquet("Files/ShoppingMart_Silver_Reviews/ShoppingMart_review")
df_social.write.mode("overwrite").parquet("Files/ShoppingMart_Silver_Social_Media/ShoppingMart_social_media")
df_weblogs.write.mode("overwrite").parquet("Files/ShoppingMart_Silver_Web_Logs/ShoppingMart_web_logs")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ###### **Notes**-
# 📌 PySpark Read vs Write Methods (Interview Notes)
# 
# 1. load()
# -----------
# • Used to READ data.
# • Part of DataFrameReader (spark.read).
# • Typically used with .format().
# 
# Syntax:
# df = spark.read.format("parquet").load(path)
# 
# ✅ Remember: load() = Read
# 
# 
# 2. save()
# -----------
# • Used to WRITE data.
# • Part of DataFrameWriter (df.write).
# • Typically used with .format().
# 
# Syntax:
# df.write.format("parquet").save(path)
# 
# ✅ Remember: save() = Write
# 
# 
# 3. parquet()
# --------------
# • Shortcut method for reading and writing Parquet files.
# 
# Read:
# df = spark.read.parquet(path)
# 
# Write:
# df.write.parquet(path)
# 
# ✅ Equivalent to using format("parquet")
# 
# 
# 4. format()
# -------------
# • Specifies the file format.
# • Supports parquet, csv, json, delta, avro, etc.
# 
# Examples:
# spark.read.format("csv")
# spark.read.format("json")
# df.write.format("parquet")
# df.write.format("delta")
# 
# ✅ More flexible than shortcut methods.
# 
# 
# 5. mode()
# -----------
# • Controls behavior when writing data.
# 
# Syntax:
# df.write.mode("overwrite")
# 
# Available Modes:
# 
# append
# → Adds new data to existing data.
# 
# overwrite
# → Replaces existing data.
# 
# ignore
# → Skips writing if data already exists.
# 
# error / errorifexists
# → Throws an error if data already exists.
# 
# 
# 📌 Shortcut vs Generic Syntax
# 
# READ Parquet:
# --------------
# Shortcut:
# df = spark.read.parquet(path)
# 
# Generic:
# df = spark.read.format("parquet").load(path)
# 
# 
# WRITE Parquet:
# ---------------
# Shortcut:
# df.write.mode("overwrite").parquet(path)
# 
# Generic:
# df.write.mode("overwrite") \
#         .format("parquet") \
#         .save(path)
# 
# 
# 📌 Most Asked Interview Question
# 
# Q. What is the difference between load() and save()?
# 
# Answer:
# • load() is used by DataFrameReader to read data from a source.
# • save() is used by DataFrameWriter to write data to a destination.
# 
# Example:
# 
# # Read
# df = spark.read.format("parquet").load(path)
# 
# # Write
# df.write.format("parquet").save(path)
# 
# 
# 📌 Memory Trick
# 
# spark.read  → load() → INPUT
# 
# df.write    → save() → OUTPUT
# 
# READ  = load()
# WRITE = save()

