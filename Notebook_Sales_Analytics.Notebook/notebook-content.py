# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "76398492-5c38-48d3-8efc-f961b8c187ac",
# META       "default_lakehouse_name": "Lakehouse_Sales",
# META       "default_lakehouse_workspace_id": "9471e225-036a-4440-9343-cafac152320f",
# META       "known_lakehouses": [
# META         {
# META           "id": "76398492-5c38-48d3-8efc-f961b8c187ac"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# #### **Sales Analytics Data Notebook**

# PARAMETERS CELL ********************

table_name = "sales"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import *
from pyspark.sql.types import *

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.read.format("csv")\
               .option("header", "true")\
               .load("Files/Source_Files")

# display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Adding month and year column to the above dataframe**

# CELL ********************

df = df.withColumn("Year", year(col("OrderDate")))\
       .withColumn("Month", date_format(col("OrderDate"), "MMM"))

# display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Loading the data from the above dataframe into a delta table**

# CELL ********************

df.write.format("delta")\
        .mode("append")\
        .saveAsTable(table_name)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
