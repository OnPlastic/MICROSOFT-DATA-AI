[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "Attendance csv_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared #"Attendance csv" = let
  Quelle = Lakehouse.Contents(null),
  Navigation = Quelle{[workspaceId = "63318995-edf3-4ecd-a82e-fc8e0560ed46"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "14151ac3-e3a1-4625-bde2-d8fbe992146a"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "Files", ItemKind = "Folder"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Name = "internal"]}[Content],
  #"Navigation 4" = #"Navigation 3"{[Name = "Attendance.csv"]}[Content],
  #"Importierte CSV" = Csv.Document(#"Navigation 4", [Delimiter = ";", Columns = 5, QuoteStyle = QuoteStyle.None]),
  #"Höher gestufte Header" = Table.PromoteHeaders(#"Importierte CSV", [PromoteAllScalars = true]),
  #"Geänderter Spaltentyp" = Table.TransformColumnTypes(#"Höher gestufte Header", {{"student_id", type text}, {"course_id", type text}, {"tutor_id", type text}, {"grade", type text}, {"semester", type text}}, "de"),
  #"Hinzugefügte benutzerdefinierte Spalte" = Table.TransformColumnTypes(Table.AddColumn(#"Geänderter Spaltentyp", "load_date", each DateTime.LocalNow()), {{"load_date", type datetime}})
in
  #"Hinzugefügte benutzerdefinierte Spalte";
shared #"Attendance csv_DataDestination" = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "63318995-edf3-4ecd-a82e-fc8e0560ed46"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "b78caba8-7599-4cc3-811c-8c2370edfe5f"]}[Data],
  TableNavigation = Navigation_2{[Id = "attendance", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
