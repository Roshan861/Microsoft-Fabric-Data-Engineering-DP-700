# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "055b3900-117c-4140-bdfc-9a29c6bdd5fc",
# META       "default_lakehouse_name": "Bronze_LH",
# META       "default_lakehouse_workspace_id": "ec178352-dd49-4a0e-9088-26a0e99d0245",
# META       "known_lakehouses": [
# META         {
# META           "id": "055b3900-117c-4140-bdfc-9a29c6bdd5fc"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# ### **Delta Lake Tables**

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE TABLE Bronze_LH.dbo.Delta_Table
# MAGIC (
# MAGIC     id int,
# MAGIC     name string,
# MAGIC     salary int,
# MAGIC     dept string
# MAGIC )

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC INSERT INTO Bronze_LH.dbo.Delta_Table 
# MAGIC VALUES 
# MAGIC (1, 'abc', 100, 'IT'),
# MAGIC (2, 'def', 200, 'HR'),
# MAGIC (3, 'ghi', 300, 'OP'),
# MAGIC (4, 'jkl', 400, 'SL')

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT * FROM Bronze_LH.dbo.Delta_Table

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **DML Ops**

# CELL ********************

# MAGIC %%sql
# MAGIC UPDATE Bronze_LH.dbo.Delta_Table SET dept = 'HRMS' WHERE id = 2

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC DELETE FROM Bronze_LH.dbo.Delta_Table WHERE id = 1

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Data Versoning**

# CELL ********************

# MAGIC %%sql
# MAGIC DESCRIBE history Bronze_LH.dbo.Delta_Table

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### **Time Travelling on the data**

# CELL ********************

# MAGIC %%sql
# MAGIC RESTORE Bronze_LH.dbo.Delta_Table TO VERSION AS OF 2

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC INSERT INTO Bronze_LH.dbo.Delta_Table 
# MAGIC VALUES 
# MAGIC (5, 'mno', 500, 'IT'),
# MAGIC (6, 'pqr', 600, 'HR'),
# MAGIC (7, 'stu', 700, 'OP'),
# MAGIC (8, 'vwx', 800, 'SL')

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### **Streaming Query**

# CELL ********************

df = spark.readStream.table("Bronze_LH.dbo.Delta_Table")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.writeStream.format('delta')\
              .option('checkpointLocation', 'Files/Streaming_Data')\
              .option('path', 'Files/Streaming_Data')\
              .toTable('Bronze_LH.dbo.Streaming_Table')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT * FROM Bronze_LH.dbo.Streaming_Table

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
