-- Fabric notebook source

-- METADATA ********************

-- META {
-- META   "kernel_info": {
-- META     "name": "synapse_pyspark"
-- META   },
-- META   "dependencies": {
-- META     "lakehouse": {
-- META       "default_lakehouse": "5ef15e25-26f6-48ab-a37b-91d89d0a2fa1",
-- META       "default_lakehouse_name": "Lakehouse_01",
-- META       "default_lakehouse_workspace_id": "f2e2c657-a625-47e2-a93c-04ee7433bccb",
-- META       "known_lakehouses": [
-- META         {
-- META           "id": "5ef15e25-26f6-48ab-a37b-91d89d0a2fa1"
-- META         }
-- META       ]
-- META     }
-- META   }
-- META }

-- CELL ********************

-- MAGIC %%sql
-- MAGIC CREATE TABLE ActivityLog
-- MAGIC (
-- MAGIC    Correlationid varchar(200),
-- MAGIC    Operationname varchar(300),
-- MAGIC    Status varchar(100),
-- MAGIC    Eventcategory varchar(100),
-- MAGIC    Level varchar(100),
-- MAGIC    Time date,
-- MAGIC    Subscription varchar(200),
-- MAGIC    Eventinitiatedby varchar(1000),
-- MAGIC    Resourcetype varchar(300),
-- MAGIC    Resourcegroup varchar(1000),
-- MAGIC    Resource varchar(2000)
-- MAGIC );

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

SELECT count(*) FROM ActivityLog

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************


-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }
