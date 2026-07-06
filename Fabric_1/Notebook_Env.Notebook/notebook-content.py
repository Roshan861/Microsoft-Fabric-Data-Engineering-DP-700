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
# META     },
# META     "environment": {
# META       "environmentId": "db144b3e-4140-b239-4bfa-942be690c06b",
# META       "workspaceId": "00000000-0000-0000-0000-000000000000"
# META     }
# META   }
# META }

# CELL ********************

# Use the 2 magic commands below to reload the modules if your module has updates during the current session. You only need to run the commands once.
# %load_ext autoreload
# %autoreload 2

import env.secret as secret
# Now use the exported members from this module with identifier: `secret`
dir(secret)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
