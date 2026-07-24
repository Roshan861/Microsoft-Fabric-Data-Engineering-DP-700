# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "8ce59a10-4cec-4441-8b41-47b51db82577",
# META       "default_lakehouse_name": "ShoppingMart_Gold_LH",
# META       "default_lakehouse_workspace_id": "f52d8381-48b2-4226-b022-5e6cf063341a",
# META       "known_lakehouses": [
# META         {
# META           "id": "8ce59a10-4cec-4441-8b41-47b51db82577"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# #### **TRANSFORMATIONS AND AGGREGATIONS OF SHOPPING MART DATA**

# CELL ********************

from pyspark.sql.functions import *
from pyspark.sql.types import *

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

orders_df = spark.read.parquet("Files/ShoppingMart_Silver_Orders/ShoppingMart_customers_orderdata")

reviews_df = spark.read.parquet("Files/ShoppingMart_Silver_Reviews/ShoppingMart_review")

social_df = spark.read.parquet("Files/ShoppingMart_Silver_Social_Media/ShoppingMart_social_media")

weblogs_df = spark.read.parquet("Files/ShoppingMart_Silver_Web_Logs/ShoppingMart_web_logs")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### **KPI1 : Aggregates web log data to measure engagement per user on each page and action.**

# CELL ********************

weblogs_df = weblogs_df.groupBy("user_id", "page", "action").count()
weblogs_df.write.mode("overwrite").parquet("Files/ShoppingMart_Gold_Web_Logs/ShoppingMart_web_logs")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### **KPI2 : Aggregates unstructured social media data to track sentiment trends across different platforms.**

# CELL ********************

social_df= social_df.groupBy("platform","sentiment" ).count()
social_df.write.mode("overwrite").parquet("Files/ShoppingMart_Gold_Social_Media/ShoppingMart_social_media")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### **KPI3: Aggregates product reviews to calculate the average rating per product.**

# CELL ********************

reviews_df = reviews_df.groupBy("product_id").agg(round(avg(col("rating")), 2).alias("AvgRating"))
reviews_df.write.mode("overwrite").parquet("Files/ShoppingMart_Gold_Reviews/ShoppingMart_review")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

orders_df.write.mode("overwrite").parquet("Files/ShoppingMart_Gold_Orders/ShoppingMart_customers_orderdata")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
