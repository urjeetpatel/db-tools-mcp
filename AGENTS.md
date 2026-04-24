# AGENTS.md — db-tools-mcp

Guidance for AI agents using this MCP server.

## What this server does

**db-tools-mcp** exposes SQL Server (and Snowflake) schema metadata to agents via MCP tools.
All data is served from a local JSON cache — no live database queries happen at tool-call time.
The cache must be populated first with `refresh_metadata` and is refreshed at most once per 24 hours.

## Setup checklist (one-time)

1. Add a database source: `add_database(...)`
2. Populate the cache: `refresh_metadata(source="<name>")`
3. Verify: `list_sources()` should return your source name

## Tool reference

### Discovery

| Tool | Purpose |
|---|---|
| `list_sources()` | All cached database sources |
| `list_schemas(source)` | Schemas inside a source |
| `list_tables(source, schema)` | Tables inside a schema |
| `get_dialect(source)` | Returns `"mssql"` or `"snowflake"` |

### Tables & columns

| Tool | Purpose |
|---|---|
| `get_table(source, schema, table)` | Columns + inbound/outbound FK relationships for one table |
| `search_tables(source, keyword)` | Find tables by name (case-insensitive substring) |
| `search_columns(source, column_name, schema=None)` | Find columns by name across all tables; returns `{schema, table, column, data_type, nullable}` |
| `list_all_foreign_keys(source, schema)` | Every FK in a schema |
| `find_direct_joins(source, table_a, table_b)` | FK-defined joins between two tables; use `"schema.table"` format |
| `suggest_joins(source, table_a, table_b, max_hops=2)` | Multi-hop join paths ranked by confidence; uses FK graph + heuristic column matching |

### Stored procedures

> Stored procedure tools only return data for `mssql` sources. Snowflake sources have an empty `stored_procedures` dict.

| Tool | Purpose |
|---|---|
| `list_stored_procedures(source, schema)` | All SP names in a schema |
| `get_stored_procedure(source, schema, name)` | Full metadata: parameters (name, ordinal, data_type, max_length, is_output, has_default), create/modify dates, full SQL definition |
| `search_stored_procedures(source, keyword, schema=None)` | Find SPs by **name** (case-insensitive); returns `{schema, procedure}` list |
| `search_stored_procedure_text(source, keyword, schema=None)` | Find SPs whose **body text** contains a keyword; returns `{schema, procedure, match_excerpt}` where `match_excerpt` is the first matching line |
| `get_call_template(source, schema, name, style="sql")` | Ready-to-paste call template. `style="sql"` → EXEC statement; `style="python"` → complete pyodbc script |
| `export_stored_procedure(source, schema, name, output_file)` | Write raw SQL definition to a file; `output_file` must be an absolute path to a writable non-system location |

### Admin

| Tool | Notes |
|---|---|
| `refresh_metadata(source=None, force=False)` | Re-extracts from live databases. Throttled to once per 24 h; use `force=True` to bypass. **Requires DB connectivity.** |
| `add_database(name, db_type, ...)` | Adds a new source to config and tests the connection |

## Recommended workflows

### Explore an unfamiliar schema
```
list_sources()
list_schemas(source)
list_tables(source, schema)
get_table(source, schema, table)          # repeat for tables of interest
suggest_joins(source, "dbo.Orders", "dbo.Customers")
```

### Find where a concept lives
```
search_tables(source, "patient")          # tables with "patient" in the name
search_columns(source, "patient_id")      # columns named like "patient_id"
```

### Work with stored procedures
```
search_stored_procedures(source, "report")        # SPs named like "report"
search_stored_procedure_text(source, "INSERT INTO Audit")  # SPs that write to Audit
get_stored_procedure(source, schema, name)        # inspect parameters + definition
get_call_template(source, schema, name, style="python")    # ready-to-run script
export_stored_procedure(source, schema, name, "C:/work/MyProc.sql")
```

### Onboard a new database
```
add_database(name="prod", db_type="sqlserver", url="mssql+pyodbc:///?odbc_connect=...")
refresh_metadata(source="prod")
list_schemas(source="prod")
```

## Important constraints

- **Cache-first**: tools read from local JSON — they will not reflect schema changes until `refresh_metadata` runs.
- **`find_direct_joins` and `suggest_joins`**: pass table names as `"schema.table"` (e.g. `"dbo.Orders"`), not bare table names.
- **`suggest_joins`** returns at most 10 paths sorted by confidence. A `confidence` of 1.0 means every hop is FK-backed; lower values include heuristic matches.
- **`export_stored_procedure`**: blocked from writing to Windows system dirs, `C:\Program Files`, UNC paths, drive roots, and the db-tools config directory. Always use an absolute path.
- **`get_call_template` style**: only `"sql"` and `"python"` are valid; anything else returns an error string.
- **Snowflake**: schema/table/FK tools work. Stored procedure tools return empty results — Snowflake extraction does not populate procedure definitions.
- **`refresh_metadata`** requires live network access to the target database. Do not call it unless DB connectivity is available.
