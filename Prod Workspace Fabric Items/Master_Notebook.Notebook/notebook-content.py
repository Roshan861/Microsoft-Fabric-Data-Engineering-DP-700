# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "2020a881-106f-45f9-a2ce-d382b3b5200d",
# META       "default_lakehouse_name": "Silver_LH",
# META       "default_lakehouse_workspace_id": "a7d080c0-b98d-4748-8ce9-8d2b827c9244",
# META       "known_lakehouses": [
# META         {
# META           "id": "409a563d-c570-4baf-b3ad-a187c79261c1"
# META         },
# META         {
# META           "id": "2020a881-106f-45f9-a2ce-d382b3b5200d"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

DAG = {
    "activities": [
        {
            "name": "AZ-SQL Bronze_FactTable",
            "path": "AZ-SQL Bronze_FactTable",
            "timeoutPerCellInSeconds": 90,
            "args": {
                "useRootDefaultLakehouse": True
            }
        },
        {
            "name": "Bronze-to-Silver_FactTable",
            "path": "Bronze-to-Silver_FactTable",
            "timeoutPerCellInSeconds": 90,
            "args": {
                "useRootDefaultLakehouse": True
            },
            "dependencies": ["AZ-SQL Bronze_FactTable"]
        }
    ],
    "timeoutInSeconds": 3000,
    "concurrency": 50
}

notebookutils.notebook.runMultiple(
    DAG,
    {"displayDAGViaGraphviz": False}
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
