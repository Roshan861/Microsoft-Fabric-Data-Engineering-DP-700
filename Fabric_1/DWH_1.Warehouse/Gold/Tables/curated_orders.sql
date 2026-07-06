CREATE TABLE [Gold].[curated_orders] (

	[order_id] varchar(8000) NULL, 
	[customer_id] varchar(8000) NULL, 
	[order_status] varchar(8000) NULL, 
	[order_purchase_timestamp] datetime2(6) NULL, 
	[order_approved_at] varchar(8000) NULL, 
	[order_delivered_carrier_date] varchar(8000) NULL, 
	[order_delivered_customer_date] varchar(8000) NULL, 
	[order_estimated_delivery_date] varchar(8000) NULL
);