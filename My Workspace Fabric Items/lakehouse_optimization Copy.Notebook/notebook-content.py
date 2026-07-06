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
# META           "id": "5ef15e25-26f6-48ab-a37b-91d89d0a2fa1",
# META           "name": "Lakehouse_01"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Run the below script or schedule it to run regularly to optimize your Lakehouse table 'tollbooth'

from delta.tables import *
deltaTable = DeltaTable.forName(spark, "tollbooth")
deltaTable.optimize().executeCompaction()

# If you only want to optimize a subset of your data, you can specify an optional partition predicate. For example:
#
#     from datetime import datetime, timedelta
#     startDate = (datetime.now() - timedelta(days=3)).strftime('%Y-%m-%d')
#     deltaTable.optimize().where("date > '{}'".format(startDate)).executeCompaction()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
