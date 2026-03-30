"""
Database metadata extraction — SQL Server (direct) and Snowflake (via OPENQUERY).

This module is used by:
  - The MCP server (db_tools.server) for the refresh_metadata tool
  - The CLI entry point (db-tools-refresh)
"""
from __future__ import annotations

import argparse
import json
import sys
from collections import defaultdict
from typing import Any, Dict, List, Optional, Tuple

from loguru import logger
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from db_tools._config import (
    CACHE_DIR,
    CONFIG_PATH,
    DEFAULT_MSSQL_EXCLUDE,
    load_config,
    mark_refreshed,
    setup_cli_logging,
)


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------
def _exec(eng: Engine, sql: str, params: Optional[dict] = None) -> List[dict]:
    with eng.connect() as conn:
        result = conn.execute(text(sql), params or {})
        return [dict(r._mapping) for r in result]


def _columns_to_tables(cols: List[dict]) -> Dict[str, Any]:
    tables: Dict[str, Any] = defaultdict(lambda: {"columns": []})
    for c in cols:
        tables[c["TABLE_NAME"]]["columns"].append(
            {
                "name": c["COLUMN_NAME"],
                "data_type": c["DATA_TYPE"],
                "nullable": c["IS_NULLABLE"] == "YES",
            }
        )
    return dict(tables)


def _group_fk_rows(rows: List[dict]) -> List[dict]:
    grouped: Dict[Tuple, Dict[str, Any]] = defaultdict(
        lambda: {"name": None, "child": {}, "parent": {}, "pairs": []}
    )
    for r in rows:
        key = (r["fk_schema"], r["fk_table"], r["fk_name"])
        grouped[key]["name"] = r["fk_name"]
        grouped[key]["child"] = {"schema": r["fk_schema"], "table": r["fk_table"]}
        grouped[key]["parent"] = {"schema": r["pk_schema"], "table": r["pk_table"]}
        grouped[key]["pairs"].append(
            {"child_col": r["fk_column"], "parent_col": r["pk_column"]}
        )
    return list(grouped.values())


def _heuristic_pairs(cols: List[dict]) -> List[Tuple]:
    """Suggest potential join pairs by _id / id column-name patterns within a schema."""
    by_schema: Dict[str, List[dict]] = defaultdict(list)
    for c in cols:
        by_schema[c["TABLE_SCHEMA"]].append(c)

    results: list = []
    for sch, schema_cols in by_schema.items():
        idx: Dict[str, List[dict]] = defaultdict(list)
        for c in schema_cols:
            idx[c["COLUMN_NAME"].lower()].append(c)

        for c in schema_cols:
            t, col = c["TABLE_NAME"], c["COLUMN_NAME"]
            candidates: List[str] = []
            if col.lower().endswith("_id"):
                candidates += [col[:-3], "id"]
            elif col.lower() == "id":
                candidates += [f"{t.lower()}_id"]

            for guess in candidates:
                for c2 in idx.get(guess, []):
                    if c2["TABLE_NAME"] == t:
                        continue
                    same_type = (
                        c["DATA_TYPE"].split("(")[0].lower()
                        == c2["DATA_TYPE"].split("(")[0].lower()
                    )
                    score = 0.7 if same_type else 0.55
                    reason = (
                        f"name match '{col}' <-> '{c2['COLUMN_NAME']}'"
                        f" ({'type ok' if same_type else 'type diff'})"
                    )
                    results.append(
                        (
                            (sch, t, col),
                            (sch, c2["TABLE_NAME"], c2["COLUMN_NAME"]),
                            score,
                            reason,
                        )
                    )
    return results


# ---------------------------------------------------------------------------
# SQL Server extraction
# ---------------------------------------------------------------------------
_FK_SQLSERVER = """\
SELECT
  kcu1.TABLE_SCHEMA   AS fk_schema,
  kcu1.TABLE_NAME     AS fk_table,
  kcu1.COLUMN_NAME    AS fk_column,
  kcu2.TABLE_SCHEMA   AS pk_schema,
  kcu2.TABLE_NAME     AS pk_table,
  kcu2.COLUMN_NAME    AS pk_column,
  rc.CONSTRAINT_NAME  AS fk_name
FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS rc
JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu1
  ON kcu1.CONSTRAINT_CATALOG = rc.CONSTRAINT_CATALOG
 AND kcu1.CONSTRAINT_SCHEMA  = rc.CONSTRAINT_SCHEMA
 AND kcu1.CONSTRAINT_NAME    = rc.CONSTRAINT_NAME
JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu2
  ON kcu2.CONSTRAINT_CATALOG = rc.UNIQUE_CONSTRAINT_CATALOG
 AND kcu2.CONSTRAINT_SCHEMA  = rc.UNIQUE_CONSTRAINT_SCHEMA
 AND kcu2.CONSTRAINT_NAME    = rc.UNIQUE_CONSTRAINT_NAME
 AND kcu2.ORDINAL_POSITION   = kcu1.ORDINAL_POSITION
"""


def extract_sqlserver(url: str, include: List[str], exclude: List[str]) -> dict:
    """Connect to SQL Server, enumerate schemas/tables/FKs, return metadata dict."""
    logger.info("SQL Server extraction started")
    eng = create_engine(url)

    schemas = [r["name"] for r in _exec(eng, "SELECT name FROM sys.schemas")]
    target = [
        s for s in schemas if ("*" in include or s in include) and s not in exclude
    ]
    logger.info(f"Processing {len(target)} schemas")

    all_fks = _exec(eng, _FK_SQLSERVER)
    payload: Dict[str, Any] = {"dialect": "mssql", "schemas": {}}

    for s in target:
        cols = _exec(
            eng,
            "SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, DATA_TYPE, IS_NULLABLE "
            "FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = :schema",
            {"schema": s},
        )
        fks = [r for r in all_fks if r.get("fk_schema") == s]
        tables = _columns_to_tables(cols)
        payload["schemas"][s] = {
            "tables": tables,
            "foreign_keys": _group_fk_rows(fks),
            "heuristics": _heuristic_pairs(cols),
        }
        logger.info(f"  '{s}': {len(tables)} tables, {len(fks)} FKs")

    logger.info("SQL Server extraction complete")
    return payload


# ---------------------------------------------------------------------------
# Snowflake via OPENQUERY (through a SQL Server linked server)
# ---------------------------------------------------------------------------
def _openquery(eng: Engine, linked_server: str, inner_sql: str) -> List[dict]:
    escaped = inner_sql.replace("'", "''")
    return _exec(eng, f"SELECT * FROM OPENQUERY({linked_server}, '{escaped}')")


def extract_snowflake(
    sqlserver_url: str,
    linked_server: str,
    database: str,
    include: List[str],
    exclude: List[str],
) -> dict:
    """Connect via OPENQUERY, enumerate Snowflake schemas/tables/FKs."""
    logger.info("Snowflake extraction via OPENQUERY started")
    eng = create_engine(sqlserver_url)
    payload: Dict[str, Any] = {"dialect": "snowflake", "schemas": {}}

    # Discover schemas
    try:
        rows = _openquery(
            eng,
            linked_server,
            f"SELECT SCHEMA_NAME FROM {database}.INFORMATION_SCHEMA.SCHEMATA "
            f"WHERE SCHEMA_NAME NOT IN ('INFORMATION_SCHEMA')",
        )
        schemas = [r["SCHEMA_NAME"] for r in rows]
    except Exception as exc:
        logger.warning(f"Could not list Snowflake schemas: {exc}")
        schemas = include if "*" not in include else []

    target = [
        s for s in schemas if ("*" in include or s in include) and s not in exclude
    ]
    logger.info(f"Processing {len(target)} schemas")

    # Foreign keys (best-effort — many Snowflake setups have none)
    try:
        fk_rows = _openquery(
            eng,
            linked_server,
            f"SELECT "
            f"  tc.CONSTRAINT_SCHEMA AS schema_name, "
            f"  tc.CONSTRAINT_NAME   AS fk_name, "
            f"  tc.TABLE_NAME        AS table_name, "
            f"  ccu.COLUMN_NAME      AS fk_column_name, "
            f"  NULL                 AS pk_schema_name, "
            f"  NULL                 AS pk_table_name, "
            f"  NULL                 AS pk_column_name, "
            f"  ccu.ORDINAL_POSITION AS key_sequence "
            f"FROM {database}.INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc "
            f"LEFT JOIN {database}.INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE ccu "
            f"  ON ccu.CONSTRAINT_NAME = tc.CONSTRAINT_NAME "
            f" AND ccu.CONSTRAINT_SCHEMA = tc.CONSTRAINT_SCHEMA "
            f"WHERE tc.CONSTRAINT_TYPE = 'FOREIGN KEY'",
        )
    except Exception as exc:
        logger.warning(f"Could not retrieve Snowflake FKs: {exc}")
        fk_rows = []

    fk_index: Dict[str, List[dict]] = defaultdict(list)
    for r in fk_rows:
        fk_index[r.get("schema_name")].append(r)

    for s in target:
        try:
            cols = _openquery(
                eng,
                linked_server,
                f"SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, DATA_TYPE, IS_NULLABLE "
                f"FROM {database}.INFORMATION_SCHEMA.COLUMNS "
                f"WHERE TABLE_SCHEMA = '{s}'",
            )
            fks = [
                {
                    "name": r.get("fk_name"),
                    "child": {
                        "schema": r.get("schema_name"),
                        "table": r.get("table_name"),
                    },
                    "parent": {
                        "schema": r.get("pk_schema_name"),
                        "table": r.get("pk_table_name"),
                    },
                    "pairs": [
                        {
                            "child_col": r.get("fk_column_name"),
                            "parent_col": r.get("pk_column_name"),
                        }
                    ],
                }
                for r in fk_index.get(s, [])
            ]
            tables = _columns_to_tables(cols)
            payload["schemas"][s] = {
                "tables": tables,
                "foreign_keys": fks,
                "heuristics": _heuristic_pairs(cols),
            }
            logger.info(f"  '{s}': {len(tables)} tables, {len(fks)} FKs")
        except Exception as exc:
            logger.error(f"Could not process Snowflake schema '{s}': {exc}")

    logger.info("Snowflake extraction complete")
    return payload


# ---------------------------------------------------------------------------
# Shared refresh entry point (used by MCP tool + CLI)
# ---------------------------------------------------------------------------
def run_refresh(source_name: str, source_cfg: dict) -> str:
    """
    Extract metadata for one source and write JSON to CACHE_DIR.
    Returns a human-readable status string.
    """
    CACHE_DIR.mkdir(parents=True, exist_ok=True)

    if "url" in source_cfg:
        metadata = extract_sqlserver(
            source_cfg["url"],
            source_cfg.get("include_schemas", ["*"]),
            source_cfg.get("exclude_schemas", DEFAULT_MSSQL_EXCLUDE),
        )
    elif "sqlserver_url" in source_cfg and "linked_server" in source_cfg:
        metadata = extract_snowflake(
            source_cfg["sqlserver_url"],
            source_cfg["linked_server"],
            source_cfg["database"],
            source_cfg.get("include_schemas", ["*"]),
            source_cfg.get("exclude_schemas", ["INFORMATION_SCHEMA"]),
        )
    else:
        raise ValueError(
            f"Source '{source_name}' has unrecognized config format. "
            f"Expected 'url' (SQL Server) or 'sqlserver_url'+'linked_server' (Snowflake)."
        )

    out_path = CACHE_DIR / f"{source_name}.json"
    with out_path.open("w", encoding="utf-8") as fh:
        json.dump(metadata, fh, indent=2)

    mark_refreshed(source_name)
    schema_count = len(metadata.get("schemas", {}))
    return f"Wrote {out_path} ({schema_count} schemas)"


# ---------------------------------------------------------------------------
# CLI entry point: db-tools-refresh
# ---------------------------------------------------------------------------
def cli_main() -> None:
    ap = argparse.ArgumentParser(
        prog="db-tools-refresh",
        description="Refresh the local database metadata cache used by db-tools-mcp.",
    )
    ap.add_argument(
        "--source",
        "-s",
        metavar="NAME",
        help="Refresh only this source (default: all enabled sources)",
    )
    ap.add_argument(
        "--verbose", "-v", action="store_true", help="Enable debug logging"
    )
    args = ap.parse_args()

    setup_cli_logging(verbose=args.verbose)
    logger.info(f"Config : {CONFIG_PATH}")
    logger.info(f"Cache  : {CACHE_DIR}")

    try:
        cfg = load_config()
    except FileNotFoundError as exc:
        logger.error(str(exc))
        sys.exit(1)

    if args.source:
        if args.source not in cfg:
            available = [k for k in cfg if k != "output"]
            logger.error(
                f"Source '{args.source}' not found. Available: {available}"
            )
            sys.exit(1)
        sources = {args.source: cfg[args.source]}
    else:
        sources = {
            k: v for k, v in cfg.items() if k != "output" and isinstance(v, dict)
        }

    exit_code = 0
    for src_name, src_cfg in sources.items():
        if not src_cfg.get("enabled", True):
            logger.info(f"Skipping '{src_name}' (disabled)")
            continue
        try:
            msg = run_refresh(src_name, src_cfg)
            logger.info(f"OK  {src_name}: {msg}")
        except Exception as exc:
            logger.error(f"ERR {src_name}: {exc}")
            exit_code = 1

    sys.exit(exit_code)


if __name__ == "__main__":
    cli_main()
