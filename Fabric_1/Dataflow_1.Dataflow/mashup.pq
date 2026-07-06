[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
shared Customers_Dataset = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "ec178352-dd49-4a0e-9088-26a0e99d0245"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "055b3900-117c-4140-bdfc-9a29c6bdd5fc"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "Files", ItemKind = "Folder"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Name = "Azure_Raw_CSV"]}[Content],
  #"Navigation 4" = #"Navigation 3"{[Name = "olist_customers_dataset.parquet"]}[Content],
  #"Imported Parquet" = Parquet.Document(#"Navigation 4"),
  #"Replaced value" = Table.ReplaceValue(#"Imported Parquet", """ """, "-", Replacer.ReplaceText, {"customer_city"}),
  #"Replaced value 1" = Table.ReplaceValue(#"Replaced value", " ", "-", Replacer.ReplaceText, {"customer_city"}),
  #"Capitalized each word" = Table.TransformColumns(#"Replaced value 1", {{"customer_city", each Text.Proper(_), type nullable text}})
in
  #"Capitalized each word";
shared #"olist_orders_dataset parquet" = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "ec178352-dd49-4a0e-9088-26a0e99d0245"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "055b3900-117c-4140-bdfc-9a29c6bdd5fc"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "Files", ItemKind = "Folder"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Name = "Azure_Raw_CSV"]}[Content],
  #"Navigation 4" = #"Navigation 3"{[Name = "olist_orders_dataset.parquet"]}[Content],
  #"Imported Parquet" = Parquet.Document(#"Navigation 4")
in
  #"Imported Parquet";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "Merge_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared Merge = let
  Source = Table.NestedJoin(Customers_Dataset, {"customer_id"}, #"olist_orders_dataset parquet", {"customer_id"}, "olist_orders_dataset parquet", JoinKind.LeftOuter),
  #"Expanded olist_orders_dataset parquet" = Table.ExpandTableColumn(Source, "olist_orders_dataset parquet", {"order_id", "order_status", "order_purchase_timestamp"}, {"order_id", "order_status", "order_purchase_timestamp"}),
  #"Replaced value" = Table.ReplaceValue(#"Expanded olist_orders_dataset parquet", null, "N/A", Replacer.ReplaceValue, {"customer_city"})
in
  #"Replaced value";
shared Merge_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false, HierarchicalNavigation = true, EnableVorder = true, OutputMetadataRefresh = true]),
  Navigation_1 = Pattern{[workspaceId = "ec178352-dd49-4a0e-9088-26a0e99d0245"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "055b3900-117c-4140-bdfc-9a29c6bdd5fc"]}[Data],
  Navigation_3 = Navigation_2{[Id = "dbo", ItemKind = "Schema"]}[Data],
  TableNavigation = Navigation_3{[Id = "Enriched_Table", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
