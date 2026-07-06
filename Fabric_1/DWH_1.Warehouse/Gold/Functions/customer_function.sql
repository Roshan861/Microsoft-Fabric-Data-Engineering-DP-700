CREATE FUNCTION Gold.customer_function (@customer_zip INT)
RETURNS TABLE
AS
RETURN
(
 SELECT * FROM DWH_1.Gold.curated_customers
 WHERE customer_zip_code = @customer_zip
);