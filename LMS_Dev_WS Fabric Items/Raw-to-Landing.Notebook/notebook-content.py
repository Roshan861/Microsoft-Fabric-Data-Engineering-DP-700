# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# PARAMETERS CELL ********************

today_file = 'File' # Value will be overwritten from the pipeline parameters.
processed_Date = '9999-99-99' # Value will be overwritten from the pipeline parameters.

# This cell has now been converted to a parameter cell, 
# i.e. value for the above two parameters can be overwritten using a pipeline to automatically get new values.

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

account_name = 'datalake2198'
container_name = 'lms-fabric' 
relative_path = 'raw'

adls_path = 'abfss://lms-fabric@datalake2198.dfs.core.windows.net/raw' ## (container_name, account_name, relative_path)

print('Source storage account path is ', adls_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

latest_path = f"{adls_path}/{today_file}"
print(latest_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import *

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

latest_path = f"{adls_path}/{today_file}"
df = spark.read.csv(path= latest_path,header=True,inferSchema=True)

if df.count() > 1:
    print("The file has data")

    df_new = df.withColumn("Processing_date", lit(processed_Date))
    
    df_new.write.format("csv")\
    .option("header", "True")\
    .partitionBy("Processing_date")\
    .mode("append")\
    .save("abfss://lms-fabric@datalake2198.dfs.core.windows.net/landing/")

    print("Data written to the landing zone.")

else:
    print("The file has only header row and no data.")

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
