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

from pyspark.sql.functions import *

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE TABLE FactInvoices(
# MAGIC CustomerID_FK int, 
# MAGIC StockItemID_FK int,
# MAGIC InvoiceDate_FK date,
# MAGIC InvoiceID int,
# MAGIC OrderID int,	
# MAGIC InvoiceLineID int,
# MAGIC Quantity int,	
# MAGIC UnitPrice real,
# MAGIC TaxRate real,
# MAGIC TotalAmount real
# MAGIC );

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

invoices_df=spark.read.table("Bronze_LH.Sales.Invoices")
invoicelines_df=spark.read.table("Bronze_LH.Sales.InvoiceLines")
display(invoices_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

invoices_filtered_df=invoices_df.select("InvoiceID",col("CustomerID").alias("CustomerID_FK"),"OrderID",col("InvoiceDate").alias("InvoiceDate_FK"))
display(invoices_filtered_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

invoiceLines_filtered_df=invoicelines_df.select("InvoiceLineID","InvoiceID",col("StockItemID").alias("StockItemID_FK"),"Quantity",col("UnitPrice").cast('float'),col("TaxRate").cast('float'))
display(invoiceLines_filtered_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

invoices_join_df=invoices_filtered_df.join(invoiceLines_filtered_df,on="InvoiceID",how="leftouter")
display(invoices_join_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

invoices_fact_df=invoices_join_df.withColumn("TotalAmount",col("UnitPrice")*col("Quantity")*(1+col("TaxRate")))
display(invoices_fact_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

invoices_fact_df.write.mode("append").saveAsTable("FactInvoices")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT * from FactInvoices;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
