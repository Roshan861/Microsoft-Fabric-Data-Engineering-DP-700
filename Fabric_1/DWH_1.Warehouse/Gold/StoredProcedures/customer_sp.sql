CREATE PROCEDURE Gold.customer_sp (@customer_zip INT)
AS
BEGIN
 SELECT * FROM DWH_1.Gold.curated_customers WHERE customer_zip_code = @customer_zip;
END;