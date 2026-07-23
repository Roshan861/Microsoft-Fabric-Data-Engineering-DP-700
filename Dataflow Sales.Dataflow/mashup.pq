[DefaultOutputDestinationSettings = [DestinationDefinition = [Kind = "Reference", QueryName = "DefaultDestination", IsNewTarget = true], UpdateMethod = [Kind = "Replace"], DestinationTypeSettings = [Kind = "Table"]], StagingDefinition = [Kind = "FastCopy"]]
section Section1;
shared DefaultDestination = Lakehouse.Contents([EnableFolding = false]){[workspaceId = "9471e225-036a-4440-9343-cafac152320f"]}[Data]{[lakehouseId = "76398492-5c38-48d3-8efc-f961b8c187ac"]}[Data];
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "sales_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared sales = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "9471e225-036a-4440-9343-cafac152320f"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "76398492-5c38-48d3-8efc-f961b8c187ac"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "sales", ItemKind = "Table"]}[Data],
  #"Split column by delimiter" = Table.SplitColumn(#"Navigation 2", "CustomerName", Splitter.SplitTextByDelimiter(" "), {"CustomerName.1", "CustomerName.2"}),
  #"Renamed columns" = Table.RenameColumns(#"Split column by delimiter", {{"CustomerName.1", "FirstName"}, {"CustomerName.2", "LastName"}})
in
  #"Renamed columns";
shared sales_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false, HierarchicalNavigation = true]),
  Navigation_1 = Pattern{[workspaceId = "9471e225-036a-4440-9343-cafac152320f"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "76398492-5c38-48d3-8efc-f961b8c187ac"]}[Data],
  Navigation_3 = Navigation_2{[Id = "dbo", ItemKind = "Schema"]}[Data],
  TableNavigation = Navigation_3{[Id = "modif_sales", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
