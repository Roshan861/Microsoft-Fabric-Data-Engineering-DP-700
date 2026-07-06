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

invoicedate=spark.sql('SELECT MAX(InvoiceDate) FROM Bronze_LH.Sales.Invoices').first()[0]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import concat,lit
jdbc_url = "jdbc:sqlserver://dbserver2198.database.windows.net:1433;database=WideWorldImporters;encrypt=true;user=rkrai;password=Adhd@2198;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;" 
sqlquery = f"""(SELECT * FROM Sales.Invoices WHERE InvoiceDate > '{str(invoicedate)}')"""
 
delta_df = spark.read.format("jdbc") \
  .option("url", jdbc_url) \
  .option("query", sqlquery) \
  .load() 
 
display(delta_df) 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from delta.tables import *
 
deltaTableInvoice=DeltaTable.forName(spark,"Bronze_LH.Sales.Invoices")
 
deltaTableInvoice.alias('invoices') \
.merge(delta_df.alias('invoiceupdates'),'invoices.InvoiceID = invoiceupdates.InvoiceID') \
.whenNotMatchedInsert(values=
{
	"InvoiceID" : "invoiceupdates.InvoiceID",
	 "CustomerID" : "invoiceupdates.CustomerID",
	"BillToCustomerID" : "invoiceupdates.BillToCustomerID",
	"OrderID" : "invoiceupdates.OrderID",
	"DeliveryMethodID" : "invoiceupdates.DeliveryMethodID",
	"ContactPersonID" : "invoiceupdates.ContactPersonID",
	"AccountsPersonID" :"invoiceupdates.AccountsPersonID",
	"SalespersonPersonID" :"invoiceupdates.SalespersonPersonID",
	"PackedByPersonID" : "invoiceupdates.PackedByPersonID",
	"InvoiceDate" :"invoiceupdates.InvoiceDate",
	"CustomerPurchaseOrderNumber": "invoiceupdates.CustomerPurchaseOrderNumber",
	"IsCreditNote" :"invoiceupdates.IsCreditNote",
	"CreditNoteReason" :"invoiceupdates.CreditNoteReason",
	"Comments" :"invoiceupdates.Comments",
	"DeliveryInstructions" :"invoiceupdates.DeliveryInstructions",
	"InternalComments" :"invoiceupdates.InternalComments",
	"TotalDryItems" :"invoiceupdates.TotalDryItems",
	"TotalChillerItems" :"invoiceupdates.TotalChillerItems",
	"DeliveryRun" :"invoiceupdates.DeliveryRun",
	"RunPosition" :"invoiceupdates.RunPosition",
	"ReturnedDeliveryData" :"invoiceupdates.ReturnedDeliveryData",
	"ConfirmedDeliveryTime" :"invoiceupdates.ConfirmedDeliveryTime",
	"ConfirmedReceivedBy" :"invoiceupdates.ConfirmedReceivedBy",
	"LastEditedBy" :"invoiceupdates.LastEditedBy",
	"LastEditedWhen" :"invoiceupdates.LastEditedWhen"
 
}
).execute()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
