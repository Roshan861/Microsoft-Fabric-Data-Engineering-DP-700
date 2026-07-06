-- Auto Generated (Do not modify) DEAF91C513AC940B5A61F73D695F3F5FE2FABC305BCE92E820EE6FDED0F9F0D3

CREATE VIEW Gold.business_view_1 AS
WITH 
t1 AS
(SELECT o.order_id, oi.product_id FROM DWH_1.Gold.curated_orders o
LEFT JOIN DWH_1.Gold.curated_orderitems oi
ON o.order_id = oi.order_id
WHERE order_status = 'delivered'),
t2 AS 
(SELECT product_id, product_weight_g,
       CASE
       WHEN product_weight_g <= 1000 THEN 'Low'
       WHEN product_weight_g <= 5000 THEN 'Medium'
       ELSE 'High'
       END AS weight_flag
FROM DWH_1.Gold.curated_products),
CTE1 AS (select t1.*, t2.weight_flag from t1 left join t2 on t1.product_id = t2.product_id)
SELECT weight_flag, COUNT(order_id) as total_orders FROM CTE1 GROUP by weight_flag;