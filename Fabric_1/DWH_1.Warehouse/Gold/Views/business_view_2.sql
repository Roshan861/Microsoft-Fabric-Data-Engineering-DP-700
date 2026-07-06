-- Auto Generated (Do not modify) 6A233509F431CA38A08320A64FEAE2F66774C5B45E29DFA1560A1D9E1EC36E3A

CREATE VIEW Gold.business_view_2 AS
SELECT * FROM 
(SELECT *,
       DENSE_RANK() OVER(PARTITION BY payment_type ORDER BY payment_value) AS bucket_rank
FROM DWH_1.Gold.curated_payments) T
WHERE bucket_rank <= 10;