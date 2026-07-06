# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "409a563d-c570-4baf-b3ad-a187c79261c1",
# META       "default_lakehouse_name": "Bronze_LH",
# META       "default_lakehouse_workspace_id": "a7d080c0-b98d-4748-8ce9-8d2b827c9244",
# META       "known_lakehouses": [
# META         {
# META           "id": "409a563d-c570-4baf-b3ad-a187c79261c1"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql.functions import col
 
invoices_df=spark.read.table("Bronze_LH.Sales.Invoices")
invoiceLines_df=spark.read.table("Bronze_LH.Sales.InvoiceLines")
 
invoices_filtered_df=invoices_df.select("InvoiceID",col("CustomerID").alias("CustomerID_FK"),"OrderID",col("InvoiceDate").alias("InvoiceDate_FK"))
invoiceLines_filtered_df=invoiceLines_df.select("InvoiceLineID","InvoiceID",col("StockItemID").alias("StockItemID_FK"),"Quantity",col("UnitPrice").cast('float'),col("TaxRate").cast('float'))
 
invoices_join_df=invoices_filtered_df.join(invoiceLines_filtered_df,on="InvoiceID",how="leftouter")
 
invoices_fact_df=invoices_join_df.withColumn("TotalAmount",col("UnitPrice")*col("Quantity")*(1+col("TaxRate")))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from delta.tables import *
 
deltaTableFactInvoices=DeltaTable.forName(spark,"Silver_LH.dbo.factinvoices")
 
deltaTableFactInvoices.alias('factinvoices') \
.merge(invoices_fact_df.alias('factinvoiceupdates'),'factinvoices.InvoiceID = factinvoiceupdates.InvoiceID') \
.whenNotMatchedInsert(values=
{
	"InvoiceID" : "factinvoiceupdates.InvoiceID",
	"CustomerID_FK" : "factinvoiceupdates.CustomerID_FK",
	"OrderID" : "factinvoiceupdates.OrderID",
	"InvoiceDate_FK" : "factinvoiceupdates.InvoiceDate_FK",
	"InvoiceLineID" : "factinvoiceupdates.InvoiceLineID",
	"StockItemID_FK" :"factinvoiceupdates.StockItemID_FK",
	"Quantity" :"factinvoiceupdates.Quantity",
	"UnitPrice" : "factinvoiceupdates.UnitPrice",
	"TaxRate" :"factinvoiceupdates.TaxRate",
	"TotalAmount": "factinvoiceupdates.TotalAmount"
 
}
).execute()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
