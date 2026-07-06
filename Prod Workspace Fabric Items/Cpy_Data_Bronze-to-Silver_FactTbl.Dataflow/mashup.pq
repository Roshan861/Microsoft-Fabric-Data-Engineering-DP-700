[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
shared #"Sales InvoiceLines" = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "a7d080c0-b98d-4748-8ce9-8d2b827c9244"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "409a563d-c570-4baf-b3ad-a187c79261c1"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "Sales.InvoiceLines", ItemKind = "Table"]}[Data],
  #"Removed other columns" = Table.SelectColumns(#"Navigation 2", {"UnitPrice", "TaxRate", "Quantity", "InvoiceLineID", "InvoiceID", "StockItemID"})
in
  #"Removed other columns";
shared #"Sales Invoices" = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "a7d080c0-b98d-4748-8ce9-8d2b827c9244"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "409a563d-c570-4baf-b3ad-a187c79261c1"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "Sales.Invoices", ItemKind = "Table"]}[Data],
  #"Removed other columns" = Table.SelectColumns(#"Navigation 2", {"InvoiceID", "CustomerID", "OrderID", "InvoiceDate"})
in
  #"Removed other columns";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "FactInvoices_DataDestination", IsNewTarget = false], Settings = [Kind = "Manual", AllowCreation = false, ColumnSettings = [Mappings = {[SourceColumnName = "CustomerID", DestinationColumnName = "CustomerID_FK"], [SourceColumnName = "StockItemID", DestinationColumnName = "StockItemID_FK"], [SourceColumnName = "InvoiceDate", DestinationColumnName = "InvoiceDate_FK"], [SourceColumnName = "InvoiceID", DestinationColumnName = "InvoiceID"], [SourceColumnName = "OrderID", DestinationColumnName = "OrderID"], [SourceColumnName = "InvoiceLineID", DestinationColumnName = "InvoiceLineID"], [SourceColumnName = "Quantity", DestinationColumnName = "Quantity"], [SourceColumnName = "UnitPrice", DestinationColumnName = "UnitPrice"], [SourceColumnName = "TaxRate", DestinationColumnName = "TaxRate"], [SourceColumnName = "TotalAmount", DestinationColumnName = "TotalAmount"]}], DynamicSchema = false, UpdateMethod = [Kind = "Append"], TypeSettings = [Kind = "Table"]]]}]
shared FactInvoices = let
  Source = Table.NestedJoin(#"Sales Invoices", {"InvoiceID"}, #"Sales InvoiceLines", {"InvoiceID"}, "Sales InvoiceLines", JoinKind.LeftOuter),
  #"Expanded Sales InvoiceLines" = Table.ExpandTableColumn(Source, "Sales InvoiceLines", {"UnitPrice", "TaxRate", "Quantity", "InvoiceLineID", "StockItemID"}, {"UnitPrice", "TaxRate", "Quantity", "InvoiceLineID", "StockItemID"}),
  #"Added custom" = Table.AddColumn(#"Expanded Sales InvoiceLines", "TotalAmount", each [Quantity]*[UnitPrice]*(1+[TaxRate])),
  #"Changed column type" = Table.TransformColumnTypes(#"Added custom", {{"TotalAmount", type number}})
in
  #"Changed column type";
shared FactInvoices_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false, HierarchicalNavigation = true, EnableVorder = true, OutputMetadataRefresh = true]),
  Navigation_1 = Pattern{[workspaceId = "a7d080c0-b98d-4748-8ce9-8d2b827c9244"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "2020a881-106f-45f9-a2ce-d382b3b5200d"]}[Data],
  Navigation_3 = Navigation_2{[Id = "dbo", ItemKind = "Schema"]}[Data],
  TableNavigation = Navigation_3{[Id = "factinvoices", ItemKind = "Table"]}[Data]
in
  TableNavigation;
