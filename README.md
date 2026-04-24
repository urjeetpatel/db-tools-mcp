# db-tools-mcp

MCP server that exposes SQL Server and Snowflake schema metadata to AI coding agents. It caches table/column/FK information locally and provides tools for searching schemas, finding join paths, and managing database connections — all without running live queries on every request.

## Quick start

### 1. Install and run with `uvx`

```bash
uvx db-tools-mcp
```

Or install with `pip`:

```bash
pip install db-tools-mcp
db-tools-mcp
```

Or install locally for development:

```bash
git clone https://github.com/urjeetpatel/db-tools-mcp.git
cd db-tools-mcp
uv sync
uv run db-tools-mcp
```

### 2. Register with your MCP client

**Claude Desktop** (`claude_desktop_config.json`):

```json
{
  "mcpServers": {
    "db-tools": {
      "command": "uvx",
      "args": ["db-tools-mcp"]
    }
  }
}
```

**Claude Code** (`.mcp.json` in your project root):

```json
{
  "mcpServers": {
    "Db_Tools": {
      "command": "uvx",
      "args": ["db-tools-mcp"]
    }
  }
}
```

### 3. Add your first database

Use the `add_database` tool through your MCP client:

```
add_database(
  name="my_db",
  db_type="sqlserver",
  url="mssql+pyodbc:///?odbc_connect=DRIVER=ODBC Driver 17 for SQL Server;Server=myhost;Database=MyDB;Trusted_Connection=Yes;"
)
```

Or copy the example config manually:

```bash
# Linux / macOS
mkdir -p ~/.config/db-tools
cp config.example.yaml ~/.config/db-tools/config.yaml

# Windows (PowerShell)
New-Item -ItemType Directory -Force -Path "$env:USERPROFILE\.config\db-tools"
Copy-Item config.example.yaml "$env:USERPROFILE\.config\db-tools\config.yaml"
```

Then edit `~/.config/db-tools/config.yaml` with your connection details.

### 4. Populate the cache

```
refresh_metadata()          # all sources
refresh_metadata(source="my_db")  # one source
```

Or from the command line:

```bash
uvx db-tools-refresh
db-tools-refresh --source my_db   # if installed locally
```

## Tools

### Read tools (safe, use cached data)

| Tool | Description |
|---|---|
| `list_sources()` | List cached database sources |
| `list_schemas(source)` | List schemas in a source |
| `list_tables(source, schema)` | List tables in a schema |
| `get_table(source, schema, table)` | Get columns + FK relationships |
| `get_dialect(source)` | SQL dialect (`mssql`, `snowflake`) |
| `list_all_foreign_keys(source, schema)` | All FKs in a schema |
| `find_direct_joins(source, table_a, table_b)` | FK joins between two tables |
| `suggest_joins(source, table_a, table_b)` | Multi-hop join path suggestions |
| `search_tables(source, keyword)` | Search table names |
| `search_columns(source, column_name)` | Search column names |

### Stored procedure tools

| Tool | Description |
|---|---|
| `list_stored_procedures(source, schema)` | List stored procedure names in a schema |
| `get_stored_procedure(source, schema, name)` | Get SP metadata: parameters, dates, and definition |
| `search_stored_procedures(source, keyword)` | Search SP names (case-insensitive, optional schema filter) |
| `search_stored_procedure_text(source, keyword)` | Search SP body text for a keyword; returns matching procedures with a one-line excerpt |
| `get_call_template(source, schema, name, style)` | Generate a SQL or Python call template for an SP |
| `export_stored_procedure(source, schema, name, output_file)` | Write SP definition (SQL only) to a file; returns resolved path + line count |

`export_stored_procedure` writes the raw SQL definition only — no JSON wrapper. The `output_file` must be an absolute path to a writable location; writes to system directories, network paths, drive roots, and the db-tools config directory are blocked.

### Admin tools (require confirmation)

| Tool | Description |
|---|---|
| `add_database(name, db_type, ...)` | Add a source to config + test connection |
| `refresh_metadata(source, force)` | Re-scan live databases (throttled to 1/day) |

## Configuration

Config and cache live in `~/.config/db-tools/` (XDG standard):

```
~/.config/db-tools/
  config.yaml          # database connections
  metadata_cache/      # cached JSON per source
  .refresh_state.json  # last-refresh timestamps
  server.log           # MCP server logs
```

Override the location with `DB_TOOLS_CONFIG_DIR` or `XDG_CONFIG_HOME`:

```bash
DB_TOOLS_CONFIG_DIR=/custom/path db-tools-mcp
```

### Supported source types

**SQL Server** (direct ODBC):

```yaml
my_db:
  enabled: true
  url: "mssql+pyodbc:///?odbc_connect=DRIVER=ODBC Driver 17 for SQL Server;Server=host;Database=db;Trusted_Connection=Yes;"
  include_schemas: ["*"]
  exclude_schemas: [INFORMATION_SCHEMA, sys, db_owner, ...]
```

**Snowflake** (via SQL Server linked server / OPENQUERY):

```yaml
my_snowflake:
  enabled: true
  sqlserver_url: "mssql+pyodbc:///?odbc_connect=..."
  linked_server: "SNOWFLAKE"
  database: "MY_SNOWFLAKE_DB"
  include_schemas: ["*"]
  exclude_schemas: [INFORMATION_SCHEMA]
```

## Requirements

- Python >= 3.11
- ODBC Driver 17 for SQL Server (for SQL Server and Snowflake-via-linked-server connections)
- Network access to the target databases

## License

MIT
