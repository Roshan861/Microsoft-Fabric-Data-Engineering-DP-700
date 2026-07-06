# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "ccc64bde-8dc2-4480-b454-c36490304de9",
# META       "default_lakehouse_name": "earthquake_lh",
# META       "default_lakehouse_workspace_id": "8003ee36-646b-446c-b6b0-939193c55d1e",
# META       "known_lakehouses": [
# META         {
# META           "id": "ccc64bde-8dc2-4480-b454-c36490304de9"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

url = "https://earthquake.usgs.gov/fdsnws/event/1/query?format=geojson&starttime=2026-04-01&endtime=2026-05-02"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import *
from pyspark.sql.types import *
import requests

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

response = requests.get(url)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

response.json()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

response.json()['features']

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
