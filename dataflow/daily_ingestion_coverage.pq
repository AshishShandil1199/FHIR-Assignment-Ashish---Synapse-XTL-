let
    Source = Lakehouse.Contents([HierarchicalNavigation = true]),
    Workspace = Source{[workspaceId = "WORKSPACE_ID"]}[Data],
    Lakehouse = Workspace{[lakehouseId = "LAKEHOUSE_ID"]}[Data],
    Bronze = Lakehouse{[Id = "bronze", ItemKind = "Schema"]}[Data],
    Pages = Bronze{[Id = "fhir_page_audit", ItemKind = "Table"]}[Data],
    Attempts = Table.Group(
        Pages, {"resource_type", "source_date", "run_id", "attempt_id"},
        {
            {"pages", each Table.RowCount(_), Int64.Type},
            {"entries_returned", each List.Sum([match_count]), Int64.Type},
            {"extraction_timestamp", each List.Min([extraction_timestamp]), type datetime},
            {"last_raw_saved_timestamp", each List.Max([raw_saved_timestamp]), type datetime}
        }),
    Runs = Bronze{[Id = "fhir_run_audit", ItemKind = "Table"]}[Data],
    Succeeded = Table.SelectRows(Runs, each [status] = "Succeeded"),
    // Match the winning run log to its exact page attempt, excluding partial loads.
    Complete = Table.NestedJoin(
        Attempts, {"resource_type", "source_date", "run_id", "attempt_id"},
        Succeeded, {"resource_type", "source_date", "run_id", "attempt_id"}, "run", JoinKind.Inner),
    Expanded = Table.ExpandTableColumn(Complete, "run", {"finished_timestamp"}, {"finished_timestamp"}),
    Latest = Table.Group(
        Expanded, {"resource_type", "source_date"},
        {{"latest", each Table.First(Table.Sort(_, {{"finished_timestamp", Order.Descending}, {"attempt_id", Order.Descending}})), type record}}),
    Result = Table.ExpandRecordColumn(
        Latest, "latest",
        {"run_id", "attempt_id", "pages", "entries_returned", "extraction_timestamp",
         "last_raw_saved_timestamp", "finished_timestamp"}),
    Typed = Table.TransformColumnTypes(Result, {
        {"resource_type", type text}, {"source_date", type date}, {"run_id", type text}, {"attempt_id", type text},
        {"pages", Int64.Type}, {"entries_returned", Int64.Type}, {"extraction_timestamp", type datetime},
        {"last_raw_saved_timestamp", type datetime}, {"finished_timestamp", type datetime}
    })
in
    Typed
