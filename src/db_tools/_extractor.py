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
        col: Dict[str, Any] = {
            "name": c["COLUMN_NAME"],
            "data_type": c["DATA_TYPE"],
            "nullable": c["IS_NULLABLE"] == "YES",
        }
        # Extended fields — present only for SQL Server extractions
        if "CHARACTER_MAXIMUM_LENGTH" in c:
            raw_len = c["CHARACTER_MAXIMUM_LENGTH"]
            # SQL Server returns -1 for varchar(max)/nvarchar(max)/varbinary(max)
            col["max_length"] = "max" if raw_len == -1 else raw_len
        if "COLUMN_DEFAULT" in c:
            col["column_default"] = c["COLUMN_DEFAULT"]  # None when no default defined
        if "IS_IDENTITY" in c:
            col["is_identity"] = bool(c["IS_IDENTITY"])
        if "IS_COMPUTED" in c:
            col["is_computed"] = bool(c["IS_COMPUTED"])
        if "IS_PK" in c:
            col["primary_key"] = bool(c["IS_PK"])
        tables[c["TABLE_NAME"]]["columns"].append(col)
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
_SP_LIST_SQLSERVER = """\
SELECT
    pr.name        AS proc_name,
    pr.create_date,
    pr.modify_date
FROM sys.procedures pr
JOIN sys.schemas s ON pr.schema_id = s.schema_id
WHERE s.name = :schema
ORDER BY pr.name
"""

_SP_PARAMS_SQLSERVER = """\
SELECT
    pr.name              AS proc_name,
    param.parameter_id   AS param_ordinal,
    param.name           AS param_name,
    t.name               AS param_type,
    CASE WHEN param.max_length = -1 THEN 'max'
         ELSE CAST(param.max_length AS NVARCHAR) END AS param_max_length,
    param.is_output      AS is_output,
    param.has_default_value AS has_default
FROM sys.procedures pr
JOIN sys.schemas s ON pr.schema_id = s.schema_id
JOIN sys.parameters param ON pr.object_id = param.object_id
    AND param.parameter_id > 0
JOIN sys.types t ON param.user_type_id = t.user_type_id
WHERE s.name = :schema
ORDER BY pr.name, param.parameter_id
"""

_SP_DEFINITIONS_SQLSERVER = """\
SELECT
    p.name        AS proc_name,
    m.definition
FROM sys.sql_modules m
JOIN sys.procedures p ON m.object_id = p.object_id
JOIN sys.schemas s ON p.schema_id = s.schema_id
WHERE s.name = :schema
"""


def _build_stored_procedures(
    list_rows: List[dict], param_rows: List[dict], def_rows: List[dict]
) -> Dict[str, Any]:
    """Assemble a {proc_name: {...}} dict from three separate query result sets."""
    procs: Dict[str, Any] = {}
    for r in list_rows:
        procs[r["proc_name"]] = {
            "create_date": str(r["create_date"]) if r["create_date"] else None,
            "modify_date": str(r["modify_date"]) if r["modify_date"] else None,
            "parameters": [],
        }
    for r in param_rows:
        if r["proc_name"] in procs:
            procs[r["proc_name"]]["parameters"].append(
                {
                    "name": r["param_name"],
                    "ordinal": r["param_ordinal"],
                    "data_type": r["param_type"],
                    "max_length": r["param_max_length"],
                    "is_output": bool(r["is_output"]),
                    "has_default": bool(r["has_default"]),
                }
            )
    for r in def_rows:
        if r["proc_name"] in procs:
            procs[r["proc_name"]]["definition"] = r["definition"]
    return procs


_PK_SQLSERVER = """\
SELECT kcu.TABLE_SCHEMA, kcu.TABLE_NAME, kcu.COLUMN_NAME
FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
  ON kcu.CONSTRAINT_CATALOG = tc.CONSTRAINT_CATALOG
 AND kcu.CONSTRAINT_SCHEMA  = tc.CONSTRAINT_SCHEMA
 AND kcu.CONSTRAINT_NAME    = tc.CONSTRAINT_NAME
WHERE tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
  AND tc.TABLE_SCHEMA = :schema
"""

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
            """
            SELECT
                TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME,
                DATA_TYPE, CHARACTER_MAXIMUM_LENGTH,
                IS_NULLABLE, COLUMN_DEFAULT,
                COLUMNPROPERTY(OBJECT_ID(TABLE_SCHEMA+'.'+TABLE_NAME), COLUMN_NAME, 'IsIdentity') AS IS_IDENTITY,
                COLUMNPROPERTY(OBJECT_ID(TABLE_SCHEMA+'.'+TABLE_NAME), COLUMN_NAME, 'IsComputed') AS IS_COMPUTED
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = :schema
            ORDER BY ORDINAL_POSITION
            """,
            {"schema": s},
        )
        pk_rows = _exec(eng, _PK_SQLSERVER, {"schema": s})
        pk_set = {(r["TABLE_NAME"], r["COLUMN_NAME"]) for r in pk_rows}
        for c in cols:
            c["IS_PK"] = (c["TABLE_NAME"], c["COLUMN_NAME"]) in pk_set

        fks = [r for r in all_fks if r.get("fk_schema") == s]
        tables = _columns_to_tables(cols)

        sp_list = _exec(eng, _SP_LIST_SQLSERVER, {"schema": s})
        sp_params = _exec(eng, _SP_PARAMS_SQLSERVER, {"schema": s})
        sp_defs = _exec(eng, _SP_DEFINITIONS_SQLSERVER, {"schema": s})
        stored_procedures = _build_stored_procedures(sp_list, sp_params, sp_defs)

        payload["schemas"][s] = {
            "tables": tables,
            "foreign_keys": _group_fk_rows(fks),
            "heuristics": _heuristic_pairs(cols),
            "stored_procedures": stored_procedures,
        }
        logger.info(
            f"  '{s}': {len(tables)} tables, {len(fks)} FKs, "
            f"{len(pk_set)} PK cols, {len(stored_procedures)} stored procedures"
        )

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
# Diff helper
# ---------------------------------------------------------------------------
def _diff_metadata(old: dict, new: dict) -> dict:
    """
    Compare two metadata dicts and return a structured diff suitable for an
    agent to summarise.  Both dicts have the shape produced by extract_*:
      { "dialect": "...", "schemas": { schema_name: { "tables": {...}, ... } } }
    """
    old_schemas = old.get("schemas", {})
    new_schemas = new.get("schemas", {})

    added_schemas = sorted(set(new_schemas) - set(old_schemas))
    removed_schemas = sorted(set(old_schemas) - set(new_schemas))
    changed_schemas: Dict[str, Any] = {}

    for schema in sorted(set(old_schemas) & set(new_schemas)):
        old_tables = old_schemas[schema].get("tables", {})
        new_tables = new_schemas[schema].get("tables", {})

        added_tables = sorted(set(new_tables) - set(old_tables))
        removed_tables = sorted(set(old_tables) - set(new_tables))
        changed_tables: Dict[str, Any] = {}

        for table in sorted(set(old_tables) & set(new_tables)):
            old_cols = {c["name"]: c for c in old_tables[table].get("columns", [])}
            new_cols = {c["name"]: c for c in new_tables[table].get("columns", [])}

            added_cols = sorted(set(new_cols) - set(old_cols))
            removed_cols = sorted(set(old_cols) - set(new_cols))
            modified_cols = [
                {"column": name, "old": old_cols[name], "new": new_cols[name]}
                for name in sorted(set(old_cols) & set(new_cols))
                if old_cols[name] != new_cols[name]
            ]

            if added_cols or removed_cols or modified_cols:
                changed_tables[table] = {
                    "columns_added": added_cols,
                    "columns_removed": removed_cols,
                    "columns_modified": modified_cols,
                }

        old_sps = old_schemas[schema].get("stored_procedures", {})
        new_sps = new_schemas[schema].get("stored_procedures", {})
        added_sps = sorted(set(new_sps) - set(old_sps))
        removed_sps = sorted(set(old_sps) - set(new_sps))
        modified_sps = [
            {"procedure": name, "old_modify_date": old_sps[name].get("modify_date"),
             "new_modify_date": new_sps[name].get("modify_date")}
            for name in sorted(set(old_sps) & set(new_sps))
            if old_sps[name].get("modify_date") != new_sps[name].get("modify_date")
        ]

        if added_tables or removed_tables or changed_tables or added_sps or removed_sps or modified_sps:
            changed_schemas[schema] = {
                "tables_added": added_tables,
                "tables_removed": removed_tables,
                "tables_changed": changed_tables,
                "procedures_added": added_sps,
                "procedures_removed": removed_sps,
                "procedures_modified": modified_sps,
            }

    has_changes = bool(added_schemas or removed_schemas or changed_schemas)
    return {
        "has_changes": has_changes,
        "schemas_added": added_schemas,
        "schemas_removed": removed_schemas,
        "schemas_changed": changed_schemas,
    }


# ---------------------------------------------------------------------------
# Shared refresh entry point (used by MCP tool + CLI)
# ---------------------------------------------------------------------------
def run_refresh(source_name: str, source_cfg: dict) -> dict:
    """
    Extract metadata for one source and write JSON to CACHE_DIR.
    Returns a dict with keys:
      - detail  : human-readable status string
      - diff    : structured diff between old and new metadata
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

    # Load previous cache (if any) so we can diff
    old_metadata: dict = {}
    if out_path.exists():
        try:
            with out_path.open("r", encoding="utf-8") as fh:
                old_metadata = json.load(fh)
        except Exception:
            pass  # treat as first-time refresh

    diff = _diff_metadata(old_metadata, metadata)

    with out_path.open("w", encoding="utf-8") as fh:
        json.dump(metadata, fh, indent=2)

    mark_refreshed(source_name)
    schema_count = len(metadata.get("schemas", {}))
    return {
        "detail": f"Wrote {out_path} ({schema_count} schemas)",
        "diff": diff,
    }


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
    ap.add_argument("--verbose", "-v", action="store_true", help="Enable debug logging")
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
            logger.error(f"Source '{args.source}' not found. Available: {available}")
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
            result = run_refresh(src_name, src_cfg)
            logger.info(f"OK  {src_name}: {result['detail']}")
        except Exception as exc:
            logger.error(f"ERR {src_name}: {exc}")
            exit_code = 1

    sys.exit(exit_code)


if __name__ == "__main__":
    cli_main()
