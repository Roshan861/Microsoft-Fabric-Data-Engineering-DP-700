CREATE TABLE [Gold].[curated_orderitems] (

	[order_id] varchar(8000) NULL, 
	[order_item_id] varchar(8000) NULL, 
	[product_id] varchar(8000) NULL, 
	[seller_id] varchar(8000) NULL, 
	[shipping_limit_date] datetime2(6) NULL, 
	[price] real NULL, 
	[freight_value] real NULL
);