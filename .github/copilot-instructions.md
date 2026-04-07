# Project Guidelines

## Code Style
- Target Python 3.11+ and keep type hints when present.
- Keep internal-only modules and helpers private with leading underscores (for example, `_config.py`, `_extractor.py`).
- Prefer small, focused functions that return plain dict/list payloads for MCP tool responses.
- Follow existing logging patterns with `loguru`; do not print from server code paths.

## Architecture
- `src/db_tools/server.py` defines MCP tools and serves read operations from local cache files.
- `src/db_tools/_extractor.py` handles live database extraction, metadata diffs, and CLI refresh behavior.
- `src/db_tools/_config.py` owns app directory resolution, config I/O, logging setup, and refresh state timestamps.
- Keep this separation: server tools should remain cache-backed and avoid ad hoc live DB querying.

## Build And Test
- Install dependencies with `uv sync`.
- Run tests with `pytest`.
- Run the server with `uv run db-tools-mcp`.
- Refresh cache from CLI with `uv run db-tools-refresh` (or `db-tools-refresh` when installed as a script).

## Conventions
- Config and cache are resolved from XDG-style app dir (`DB_TOOLS_CONFIG_DIR` override, then `XDG_CONFIG_HOME/db-tools`, then `~/.config/db-tools`).
- SQL Server sources use `url`; Snowflake sources use `sqlserver_url` + `linked_server` + `database`.
- `refresh_metadata` is throttled by refresh state (24-hour interval) unless explicitly forced.
- For SQL Server extraction, default excluded schemas come from `_config.DEFAULT_MSSQL_EXCLUDE`.
- Preserve cache JSON shape under `metadata_cache/*.json`; tests depend on this structure.

## Key References
- Project usage and client setup: `README.md`
- Config template: `config.example.yaml`
- Packaging and entry points: `pyproject.toml`
- Test fixtures and expected cache patterns: `tests/conftest.py`
