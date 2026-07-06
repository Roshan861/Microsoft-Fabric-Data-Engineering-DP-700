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

# CELL ********************

from pyspark.sql.functions import *
from pyspark.sql.types import *
import notebookutils

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

notebookutils.help()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

notebookutils.fs.help()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **fs ls**

# CELL ********************

for i in notebookutils.fs.ls('Files/'):
    print(i.path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **cp**

# CELL ********************

notebookutils.fs.cp('Files/Azure_Raw_CSV/olist_customers_dataset.parquet', 'Files/my_data')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **fastcp - optimized to copy large files**

# CELL ********************

notebookutils.fs.fastcp('Files/Azure_Raw_CSV/olist_orders_dataset.parquet', 'Files/Source_Fabric/olist_orders_dataset.parquet')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **mv**

# CELL ********************

notebookutils.fs.mv('Files/Source_Fabric/olist_orders_dataset.parquet', 'Files/my_data/orders_dataset.parquet')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **rm**

# CELL ********************

notebookutils.fs.rm('Files/my_data/orders_dataset.parquet')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **mkdir**

# CELL ********************

notebookutils.fs.mkdirs('Files/new_folder')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **put - helps to create a new file**

# CELL ********************

notebookutils.fs.put('Files/new_folder/newfile.txt', 'This is a new file created using notebookutils function.')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

notebookutils.notebook.help()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **run - you can run a notebook from another notebook**

# CELL ********************

notebookutils.notebook.run('Run_Notebook', 100, {'p_name': 'Selmon'})

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
