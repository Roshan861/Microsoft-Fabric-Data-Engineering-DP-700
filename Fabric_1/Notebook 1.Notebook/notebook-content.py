# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "6d8916a2-bd40-4d15-8eb6-da06b0b01e80",
# META       "default_lakehouse_name": "Silver_LH",
# META       "default_lakehouse_workspace_id": "ec178352-dd49-4a0e-9088-26a0e99d0245",
# META       "known_lakehouses": [
# META         {
# META           "id": "6d8916a2-bd40-4d15-8eb6-da06b0b01e80"
# META         },
# META         {
# META           "id": "055b3900-117c-4140-bdfc-9a29c6bdd5fc"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql.functions import *
from pyspark.sql.types import *

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### **Data Reading**

# CELL ********************

df_customers = spark.read.format("parquet")\
                    .load("abfss://DP700_Dev@onelake.dfs.fabric.microsoft.com/Bronze_LH.Lakehouse/Files/Azure_Raw_CSV/olist_customers_dataset.parquet")

# display(df_customers.limit(5))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Select Transformation**

# CELL ********************

df_customers = df_customers.select("customer_id", "customer_unique_id", "customer_city", "customer_zip_code_prefix")
# display(df_customers)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **With Column Rename function**

# CELL ********************

df_customers = df_customers.withColumnRenamed("customer_unique_id", "unique_id")\
                           .withColumnRenamed("customer_zip_code_prefix", "customer_zip_code")
# display(df_customers)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Typecasting using the withColumn() function**

# CELL ********************

df_customers = df_customers.withColumn("customer_zip_code", col("customer_zip_code").cast(IntegerType()))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Print Schema**

# CELL ********************

df_customers.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_orderitems = spark.read.format("parquet")\
                     .load("abfss://DP700_Dev@onelake.dfs.fabric.microsoft.com/Bronze_LH.Lakehouse/Files/Azure_Raw_CSV/olist_order_items_dataset.parquet")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_orderitems.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **TimeStamp**

# CELL ********************

df_orderitems = df_orderitems.withColumn("shipping_limit_date", col("shipping_limit_date").cast(TimestampType()))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_orderitems = df_orderitems.withColumn("price", col("price").cast(FloatType()))\
                             .withColumn("freight_value", col("freight_value").cast(FloatType()))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **inferSchema for the columns of the data**

# CELL ********************

df_orderpayments = spark.read.option("inferSchema", True)\
          .parquet("abfss://DP700_Dev@onelake.dfs.fabric.microsoft.com/Bronze_LH.Lakehouse/Files/Azure_Raw_CSV/olist_order_payments_dataset.parquet")
# df now is a Spark DataFrame containing parquet data from "abfss://DP700_Dev@onelake.dfs.fabric.microsoft.com/Bronze_LH.Lakehouse/Files/Azure_Raw_CSV/olist_order_payments_dataset.parquet".
# display(df_orderpayments)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Replace Values**

# CELL ********************

df_orderpayments = df_orderpayments.withColumn("payment_type" ,regexp_replace(col("payment_type"), "_", " "))\
                                   .withColumn("payment_installments", col("payment_installments").cast(IntegerType()))\
                                   .withColumn("payment_value", col("payment_value").cast(FloatType()))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_orderpayments.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_orderreviews = spark.read.parquet("abfss://DP700_Dev@onelake.dfs.fabric.microsoft.com/Bronze_LH.Lakehouse/Files/Azure_Raw_CSV/olist_order_reviews_dataset.parquet")
# df now is a Spark DataFrame containing parquet data from "abfss://DP700_Dev@onelake.dfs.fabric.microsoft.com/Bronze_LH.Lakehouse/Files/Azure_Raw_CSV/olist_order_reviews_dataset.parquet".
# # display(df_orderreviews)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **String Functions**

# CELL ********************

df_orderreviews = df_orderreviews.withColumn("review_comment_title", upper("review_comment_title"))\
                                 .withColumn("review_comment_message", lower("review_comment_message"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Null Handling**

# CELL ********************

df_orderreviews = df_orderreviews.fillna({"review_comment_message":"Not Available"})

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_products = spark.read.parquet("abfss://DP700_Dev@onelake.dfs.fabric.microsoft.com/Bronze_LH.Lakehouse/Files/Azure_Raw_CSV/olist_products_dataset.parquet")
# df now is a Spark DataFrame containing parquet data from "abfss://DP700_Dev@onelake.dfs.fabric.microsoft.com/Bronze_LH.Lakehouse/Files/Azure_Raw_CSV/olist_products_dataset.parquet".
# display(df_products)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Rename columns**

# CELL ********************

df_products = df_products.withColumnRenamed("product_name_lenght", "product_name_length")\
                         .withColumnRenamed("product_description_lenght", "product_description_length")\
                         .withColumn("product_weight_g", col("product_weight_g").cast(IntegerType()))\
                         .withColumn("product_length_cm", col("product_length_cm").cast(IntegerType()))\
                         .withColumn("product_height_cm", col("product_height_cm").cast(IntegerType()))\
                         .withColumn("product_width_cm", col("product_width_cm").cast(IntegerType()))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df_products.filter(col("product_weight_g")>300).count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Flagging**

# CELL ********************

df_products = df_products.withColumn("300gFlag", when(col("product_weight_g")>300, "Y").otherwise("N"))
# display(df_products)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_orders = spark.read.parquet("abfss://DP700_Dev@onelake.dfs.fabric.microsoft.com/Bronze_LH.Lakehouse/Files/Azure_Raw_CSV/olist_orders_dataset.parquet")
# df now is a Spark DataFrame containing parquet data from "abfss://DP700_Dev@onelake.dfs.fabric.microsoft.com/Bronze_LH.Lakehouse/Files/Azure_Raw_CSV/olist_orders_dataset.parquet".
# display(df_orders)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_orders = df_orders.dropna(subset= ['order_delivered_carrier_date', 'order_delivered_customer_date'])
# display(df_orders)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_orders = df_orders.withColumn('order_purchase_timestamp', col('order_purchase_timestamp').cast(TimestampType()))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Spark SQL**

# CELL ********************

df_orders.createOrReplaceTempView('orders_temp_view')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT *,
# MAGIC        row_number() OVER(ORDER BY order_id) AS rownum
# MAGIC FROM orders_temp_view

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **To create a dataframe on the top of the above SQL query**

# CELL ********************

df_sql = spark.sql("""SELECT *,
       row_number() OVER(ORDER BY order_id) AS rownum
FROM orders_temp_view""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### **Data Writing Scenarios**

# MARKDOWN ********************

# **Writing to Files**

# CELL ********************

df_sql.write.format('delta')\
      .mode('append')\
      .option('path', 'Files/raw_source')\
      .save()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Writing as a Managed Table**

# CELL ********************

df_sql.write.format('delta')\
      .mode('append')\
      .saveAsTable('delta_man_tbl')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Writing as an External Table**

# CELL ********************

df_sql.write.format('delta')\
      .mode('append')\
      .option('header', True)\
      .option('path', 'Files/my_data')\
      .saveAsTable('my_data_tbl')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **You can create a table in one lakehouse and store the data of the table in another lakehouse.**

# CELL ********************

df_sql.write.format('delta')\
      .mode('append')\
      .option('header', True)\
      .option('path', 'abfss://DP700_Dev@onelake.dfs.fabric.microsoft.com/Bronze_LH.Lakehouse/Files/my_data')\
      .saveAsTable('my_data_tbl_ext')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### **Data Writing**

# CELL ********************

df_customers.write.format('delta')\
            .mode('append')\
            .saveAsTable('Silver_LH.enr_customers')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_orderreviews.write.format('delta')\
            .mode('append')\
            .saveAsTable('Silver_LH.enr_reviews')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_orderpayments.write.format('delta')\
            .mode('append')\
            .saveAsTable('Silver_LH.enr_payments')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_orderitems.write.format('delta')\
            .mode('append')\
            .saveAsTable('Silver_LH.enr_orderitems')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_products.write.format('delta')\
            .mode('append')\
            .saveAsTable('Silver_LH.enr_products')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_orders.write.format('delta')\
            .mode('append')\
            .saveAsTable('Silver_LH.enr_orders')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
