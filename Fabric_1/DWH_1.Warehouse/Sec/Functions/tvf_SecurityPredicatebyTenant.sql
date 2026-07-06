CREATE FUNCTION Sec.tvf_SecurityPredicatebyTenant
(
    @TenantName NVARCHAR(50)
)
RETURNS TABLE
WITH SCHEMABINDING
AS
RETURN
    SELECT 1 AS result
    WHERE @TenantName = USER_NAME()
       OR USER_NAME() = 'fabric@rkrai7947gmail.onmicrosoft.com';