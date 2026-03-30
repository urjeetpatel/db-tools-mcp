"""
Database Metadata MCP Server.

Tools:
  Read  : list_sources, list_schemas, list_tables, get_table, get_dialect,
          list_all_foreign_keys, find_direct_joins, suggest_joins,
          search_tables, search_columns
  Admin : refresh_metadata, add_database

Config : ~/.config/db-tools/config.yaml   (or $DB_TOOLS_CONFIG_DIR)
Cache  : ~/.config/db-tools/metadata_cache/
"""
from __future__ import annotations

import json
from collections import deque
from typing import Any, Dict, List, Optional

from fastmcp import FastMCP
from loguru import logger
from sqlalchemy import create_engine, text

from db_tools._config import (
    APP_DIR,
    CACHE_DIR,
    CONFIG_PATH,
    DEFAULT_MSSQL_EXCLUDE,
    REFRESH_INTERVAL_HOURS,
    hours_since_refresh,
    load_config,
    save_config,
    setup_server_logging,
)
from db_tools._extractor import run_refresh

mcp = FastMCP(name="Db_Tools")


# ---------------------------------------------------------------------------
# Cache loader
# ---------------------------------------------------------------------------
def _load_cache() -> Dict[str, Any]:
    """Load all per-source JSON files from CACHE_DIR."""
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    sources: Dict[str, Any] = {}
    for path in CACHE_DIR.glob("*.json"):
        with path.open("r", encoding="utf-8") as fh:
            sources[path.stem] = json.load(fh)
    return {"sources": sources}


# ---------------------------------------------------------------------------
# Read tools
# ---------------------------------------------------------------------------
@mcp.tool
def list_sources() -> List[str]:
    """List all database sources available in the local metadata cache."""
    return list(_load_cache().get("sources", {}).keys())


@mcp.tool
def list_schemas(source: str) -> List[str]:
    """List all schemas for a given source."""
    return list(_load_cache()["sources"][source]["schemas"].keys())


@mcp.tool
def list_tables(source: str, schema: str) -> List[str]:
    """List all tables in a given schema."""
    return list(
        _load_cache()["sources"][source]["schemas"][schema]["tables"].keys()
    )


@mcp.tool
def get_table(source: str, schema: str, table: str) -> Dict[str, Any]:
    """Get columns and FK relationships (inbound + outbound) for a specific table."""
    data = _load_cache()["sources"][source]["schemas"][schema]
    t = data["tables"].get(table)
    if not t:
        return {
            "error": "Table not found",
            "columns": [],
            "foreign_keys_inbound": [],
            "foreign_keys_outbound": [],
        }
    inbound, outbound = [], []
    for fk in data["foreign_keys"]:
        if fk["parent"]["schema"] == schema and fk["parent"]["table"] == table:
            inbound.append(fk)
        if fk["child"]["schema"] == schema and fk["child"]["table"] == table:
            outbound.append(fk)
    return {
        "columns": t["columns"],
        "foreign_keys_inbound": inbound,
        "foreign_keys_outbound": outbound,
    }


@mcp.tool
def get_dialect(source: str) -> str:
    """Return the SQL dialect for a source (e.g. 'mssql', 'snowflake')."""
    return _load_cache()["sources"][source].get("dialect", "unknown")


@mcp.tool
def list_all_foreign_keys(source: str, schema: str) -> List[Dict[str, Any]]:
    """Return every foreign key defined in a schema."""
    return (
        _load_cache()["sources"][source]["schemas"][schema].get(
            "foreign_keys", []
        )
    )


@mcp.tool
def find_direct_joins(
    source: str, table_a: str, table_b: str
) -> List[Dict[str, Any]]:
    """
    Return FK-defined direct joins between two tables (either direction).
    Provide table names as 'schema.table' (e.g. 'dbo.Orders').
    """
    results = []
    for _sch, sdata in _load_cache()["sources"][source]["schemas"].items():
        for fk in sdata["foreign_keys"]:
            c = f"{fk['child']['schema']}.{fk['child']['table']}"
            p = f"{fk['parent']['schema']}.{fk['parent']['table']}"
            if {c, p} == {table_a, table_b}:
                results.append(fk)
    return results


@mcp.tool
def suggest_joins(
    source: str, table_a: str, table_b: str, max_hops: int = 2
) -> List[Dict[str, Any]]:
    """
    Suggest join paths between two tables using FK graph + heuristic column matching.
    Results ordered by descending confidence. Table names as 'schema.table'.
    """
    data = _load_cache()["sources"][source]["schemas"]
    edges: List[Dict[str, Any]] = []

    for _sch, sdata in data.items():
        for fk in sdata["foreign_keys"]:
            edges.append(
                {
                    "from": f"{fk['child']['schema']}.{fk['child']['table']}",
                    "to": f"{fk['parent']['schema']}.{fk['parent']['table']}",
                    "kind": "fk",
                    "pairs": fk["pairs"],
                    "name": fk["name"],
                    "score": 0.95,
                }
            )
        for (c1, c2, c3), (p1, p2, p3), score, reason in sdata.get(
            "heuristics", []
        ):
            edges.append(
                {
                    "from": f"{c1}.{c2}",
                    "to": f"{p1}.{p2}",
                    "kind": "heuristic",
                    "pairs": [{"child_col": c3, "parent_col": p3}],
                    "name": f"heur_{c2}_{p2}",
                    "score": score,
                    "reason": reason,
                }
            )

    def neighbors(node: str) -> List[Dict[str, Any]]:
        return [e for e in edges if e["from"] == node or e["to"] == node]

    paths: List[Dict[str, Any]] = []
    queue = deque([([table_a], [])])
    seen = {table_a}

    while queue:
        nodes, es = queue.popleft()
        cur = nodes[-1]
        if len(nodes) - 1 > max_hops:
            continue
        if cur == table_b and es:
            confidence = 1.0
            for e in es:
                confidence *= e["score"]
            paths.append(
                {
                    "tables": nodes[:],
                    "edges": es[:],
                    "confidence": round(confidence, 3),
                }
            )
            continue
        for e in neighbors(cur):
            nxt = e["to"] if e["from"] == cur else e["from"]
            if nxt in seen:
                continue
            seen.add(nxt)
            queue.append((nodes + [nxt], es + [e]))

    paths.sort(
        key=lambda x: (
            -x["confidence"],
            -sum(1 for e in x["edges"] if e["kind"] == "fk"),
        )
    )
    return paths[:10]


@mcp.tool
def search_tables(source: str, keyword: str) -> List[Dict[str, str]]:
    """Search for tables whose names contain *keyword* (case-insensitive)."""
    kw = keyword.lower()
    results = [
        {"schema": schema, "table": table}
        for schema, sdata in _load_cache()["sources"][source]["schemas"].items()
        for table in sdata["tables"]
        if kw in table.lower()
    ]
    return sorted(results, key=lambda x: (x["schema"], x["table"]))


@mcp.tool
def search_columns(
    source: str, column_name: str, schema: Optional[str] = None
) -> List[Dict[str, Any]]:
    """
    Search for columns whose names contain *column_name* (case-insensitive).
    Optionally restrict to a single schema.
    """
    kw = column_name.lower()
    all_schemas = _load_cache()["sources"][source]["schemas"]
    scope = {schema: all_schemas[schema]} if schema else all_schemas
    results = []
    for sch, sdata in scope.items():
        for table, tdata in sdata["tables"].items():
            for col in tdata["columns"]:
                if kw in col["name"].lower():
                    results.append(
                        {
                            "schema": sch,
                            "table": table,
                            "column": col["name"],
                            "data_type": col.get("data_type", ""),
                            "nullable": col.get("nullable"),
                        }
                    )
    return sorted(results, key=lambda x: (x["schema"], x["table"], x["column"]))


# ---------------------------------------------------------------------------
# Admin tools
# ---------------------------------------------------------------------------
@mcp.tool
def refresh_metadata(
    source: Optional[str] = None, force: bool = False
) -> Dict[str, Any]:
    """
    Refresh the local schema metadata cache by querying live databases.

    THIS IS AN EXPENSIVE OPERATION — each source may take several minutes.
    Do NOT call automatically before reads. The cache is designed to be long-lived.
    Only call when:
      - No cache exists yet for a source (first-time setup)
      - The user explicitly says a schema has changed

    Each source is throttled to one refresh per 24 hours.
    Set force=True ONLY when the user has confirmed recent schema changes.

    Args:
        source: Specific source to refresh (omit for all enabled sources).
        force:  Bypass the 24-hour throttle.
    """
    try:
        cfg = load_config()
    except FileNotFoundError as exc:
        return {
            "error": str(exc),
            "config_path": str(CONFIG_PATH),
            "hint": "Use the add_database tool to create your first source.",
        }

    if source:
        if source not in cfg:
            available = [k for k in cfg if k != "output"]
            return {
                "error": f"Source '{source}' not in config.",
                "available_sources": available,
            }
        sources_to_run = {source: cfg[source]}
    else:
        sources_to_run = {
            k: v
            for k, v in cfg.items()
            if k != "output" and isinstance(v, dict)
        }

    results: Dict[str, Any] = {}
    for src_name, src_cfg in sources_to_run.items():
        if not src_cfg.get("enabled", True):
            results[src_name] = {"status": "skipped", "reason": "disabled"}
            continue

        hours = hours_since_refresh(src_name)
        if hours is not None and hours < REFRESH_INTERVAL_HOURS and not force:
            results[src_name] = {
                "status": "throttled",
                "hours_since_last": round(hours, 1),
                "threshold_hours": REFRESH_INTERVAL_HOURS,
                "message": (
                    f"Last refreshed {hours:.1f}h ago. "
                    f"Set force=True only if the user confirms schema changes."
                ),
            }
            continue

        try:
            refresh_result = run_refresh(src_name, src_cfg)
            results[src_name] = {
                "status": "ok",
                "detail": refresh_result["detail"],
                "diff": refresh_result["diff"],
            }
            logger.info(f"Refreshed '{src_name}': {refresh_result['detail']}")
        except Exception as exc:
            results[src_name] = {"status": "error", "detail": repr(exc)}
            logger.exception(f"Refresh failed for '{src_name}'")

    return {"results": results}


@mcp.tool
def add_database(
    name: str,
    db_type: str,
    url: Optional[str] = None,
    sqlserver_url: Optional[str] = None,
    linked_server: Optional[str] = None,
    snowflake_database: Optional[str] = None,
    include_schemas: Optional[List[str]] = None,
    exclude_schemas: Optional[List[str]] = None,
    test_connection: bool = True,
) -> Dict[str, Any]:
    """
    Add a new database source to config.yaml and optionally test the connection.

    db_type='sqlserver':
      url (required) -- SQLAlchemy connection URL, e.g.:
        "mssql+pyodbc:///?odbc_connect=DRIVER=ODBC Driver 17 for SQL Server;
         Server=myserver;Database=MyDB;Trusted_Connection=Yes;"

    db_type='snowflake' (via SQL Server linked server + OPENQUERY):
      sqlserver_url      (required) -- SQLAlchemy URL for the gateway SQL Server
      linked_server      (required) -- Linked server name (e.g. "SNOWFLAKE")
      snowflake_database (required) -- Snowflake database (e.g. "MY_DB")

    Common:
      include_schemas  -- ["*"] for all (default), or explicit list
      exclude_schemas  -- defaults to system schemas for the db_type
      test_connection  -- set False to skip the live connection test

    After adding, call refresh_metadata(source='<name>') to populate the cache.
    """
    # --- validate name ---
    if not name or not name.replace("_", "").isalnum():
        return {
            "error": "name must contain only letters, digits, underscores (no spaces/hyphens)."
        }

    # --- validate db_type ---
    if db_type not in ("sqlserver", "snowflake"):
        return {"error": "db_type must be 'sqlserver' or 'snowflake'."}

    # --- validate type-specific params ---
    if db_type == "sqlserver":
        if not url:
            return {"error": "url is required for db_type='sqlserver'."}
    else:
        missing = [
            p
            for p, v in [
                ("sqlserver_url", sqlserver_url),
                ("linked_server", linked_server),
                ("snowflake_database", snowflake_database),
            ]
            if not v
        ]
        if missing:
            return {
                "error": f"Missing required params for db_type='snowflake': {missing}"
            }

    # --- load or init config ---
    try:
        cfg = load_config()
    except FileNotFoundError:
        cfg = {}

    if name in cfg:
        return {
            "error": (
                f"Source '{name}' already exists in {CONFIG_PATH}. "
                f"Remove it first or choose a different name."
            )
        }

    # --- connection test ---
    if test_connection:
        try:
            if db_type == "sqlserver":
                eng = create_engine(url)
                with eng.connect() as conn:
                    conn.execute(text("SELECT 1"))
            else:
                eng = create_engine(sqlserver_url)
                with eng.connect() as conn:
                    conn.execute(
                        text(
                            f"SELECT * FROM OPENQUERY({linked_server}, 'SELECT 1 AS test')"
                        )
                    )
        except Exception as exc:
            return {
                "error": "Connection test failed",
                "detail": str(exc),
                "hint": "Check your connection string and network. "
                "Pass test_connection=False to skip.",
            }

    # --- build entry ---
    entry: Dict[str, Any] = {"enabled": True}
    if db_type == "sqlserver":
        entry["url"] = url
    else:
        entry["sqlserver_url"] = sqlserver_url
        entry["linked_server"] = linked_server
        entry["database"] = snowflake_database

    entry["include_schemas"] = (
        include_schemas if include_schemas is not None else ["*"]
    )
    entry["exclude_schemas"] = (
        exclude_schemas
        if exclude_schemas is not None
        else (
            DEFAULT_MSSQL_EXCLUDE
            if db_type == "sqlserver"
            else ["INFORMATION_SCHEMA"]
        )
    )

    cfg[name] = entry
    save_config(cfg)
    logger.info(f"Added source '{name}' to {CONFIG_PATH}")

    return {
        "status": "ok",
        "message": f"Source '{name}' added to {CONFIG_PATH}",
        "connection_tested": test_connection,
        "next_step": f"Call refresh_metadata(source='{name}') to populate the cache.",
    }


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
def main() -> None:
    setup_server_logging()
    logger.info(
        f"db-tools-mcp starting | config={CONFIG_PATH} | cache={CACHE_DIR}"
    )
    mcp.run()


if __name__ == "__main__":
    main()
