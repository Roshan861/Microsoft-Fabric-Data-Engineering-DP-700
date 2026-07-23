CREATE PROCEDURE Sales.LoadDataFromStagingLakehouse
    @OrderYear INT
AS
BEGIN

    -- Load Customer Dimension
    INSERT INTO Sales.Dim_Customer
    (
        CustomerID,
        CustomerName,
        FirstName,
        LastName,
        EmailAddress
    )
    SELECT DISTINCT
        CONCAT(FirstName, '_', LastName) AS CustomerID,
        CONCAT(FirstName, ' ', LastName) AS CustomerName,
        FirstName,
        LastName,
        EmailAddress
    FROM Sales.stagingSalesData s
    WHERE YEAR(s.OrderDate) = @OrderYear
      AND NOT EXISTS
      (
          SELECT 1
          FROM Sales.Dim_Customer c
          WHERE c.CustomerName = CONCAT(s.FirstName, ' ', s.LastName)
            AND c.EmailAddress = s.EmailAddress
      );

    -- Load Item Dimension
    INSERT INTO Sales.Dim_Item
    (
        ItemID,
        ItemName
    )
    SELECT DISTINCT
        Item,
        Item
    FROM Sales.stagingSalesData s
    WHERE YEAR(s.OrderDate) = @OrderYear
      AND NOT EXISTS
      (
          SELECT 1
          FROM Sales.Dim_Item i
          WHERE i.ItemName = s.Item
      );

    -- Load Fact Table
    INSERT INTO Sales.Fact_Sales
    (
        CustomerID,
        ItemID,
        SalesOrderNumber,
        SalesOrderLineNumber,
        OrderDate,
        Quantity,
        TaxAmount,
        UnitPrice,
        [Year],
        [Month]
    )
    SELECT
        CONCAT(s.FirstName, '_', s.LastName),
        s.Item,
        s.SalesOrderNumber,
        CAST(s.SalesOrderLineNumber AS INT),
        CAST(s.OrderDate AS DATE),
        CAST(s.Quantity AS INT),
        CAST(s.TaxAmount AS FLOAT),
        CAST(s.UnitPrice AS FLOAT),
        s.[Year],
        s.[Month]
    FROM Sales.stagingSalesData s
    WHERE YEAR(s.OrderDate) = @OrderYear;

END;