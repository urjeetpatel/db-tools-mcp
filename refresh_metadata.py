# refresh_metadata.py
import json
import os
import sys
import argparse
from collections import defaultdict
from typing import Dict, List, Tuple, Any, Optional
import yaml
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from loguru import logger


def load_cfg(path: str) -> dict:
    logger.info(f"Loading configuration from {path}")
    with open(path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    logger.info("Configuration loaded successfully")
    return cfg


# ---------- SQL Server ----------
FK_SQLSERVER = """
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
 AND kcu2.ORDINAL_POSITION   = kcu1.ORDINAL_POSITION;
"""


# ---------- Snowflake helpers (via OPENQUERY) ----------
def snowflake_show_imported_keys(
    engine: Engine, linked_server: str, database: str, dry_run: bool = False
) -> List[dict]:
    """Use OPENQUERY to get FK column pairs from Snowflake via SQL Server linked server."""
    logger.info(
        f"Retrieving Snowflake foreign keys via OPENQUERY from linked server '{linked_server}' database '{database}'"
    )
    # Note: SHOW commands through OPENQUERY can be tricky, so we'll try to use INFORMATION_SCHEMA instead
    # If your Snowflake has proper FK metadata in INFORMATION_SCHEMA, use that; otherwise this might return empty
    sql = f"""
    SELECT * FROM OPENQUERY({linked_server}, '
        SELECT
            rc.CONSTRAINT_SCHEMA as schema_name,
            rc.CONSTRAINT_NAME as fk_name,
            kcu1.TABLE_NAME as table_name,
            kcu1.COLUMN_NAME as fk_column_name,
            kcu2.TABLE_SCHEMA as pk_schema_name,
            kcu2.TABLE_NAME as pk_table_name,
            kcu2.COLUMN_NAME as pk_column_name,
            kcu1.ORDINAL_POSITION as key_sequence
        FROM {database}.INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS rc
        JOIN {database}.INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu1
            ON kcu1.CONSTRAINT_NAME = rc.CONSTRAINT_NAME
            AND kcu1.CONSTRAINT_SCHEMA = rc.CONSTRAINT_SCHEMA
        JOIN {database}.INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu2
            ON kcu2.CONSTRAINT_NAME = rc.UNIQUE_CONSTRAINT_NAME
            AND kcu2.CONSTRAINT_SCHEMA = rc.UNIQUE_CONSTRAINT_SCHEMA
            AND kcu2.ORDINAL_POSITION = kcu1.ORDINAL_POSITION
    ')
    """
    if dry_run:
        print("\n=== DRY RUN: Snowflake Foreign Keys Query ===")
        print(sql)
        print("=== End Query ===")
        return []  # Return empty list in dry run

    with engine.connect() as conn:
        result = conn.execute(text(sql))
        rows = [dict(r._mapping) for r in result]
    logger.info(f"Found {len(rows)} foreign key relationships")
    return rows


def snowflake_info_schema_openquery(
    engine: Engine, linked_server: str, inner_sql: str, dry_run: bool = False
) -> List[dict]:
    """Execute Snowflake queries via OPENQUERY."""
    logger.debug(f"Executing OPENQUERY against {linked_server}")
    # Escape single quotes in the inner SQL
    escaped_sql = inner_sql.replace("'", "''")
    openquery_sql = f"SELECT * FROM OPENQUERY({linked_server}, '{escaped_sql}')"

    if dry_run:
        print("\n=== DRY RUN: Snowflake OPENQUERY ===")
        print(openquery_sql)
        print("=== End Query ===")
        return []  # Return empty list in dry run

    with engine.connect() as conn:
        result = [dict(r._mapping) for r in conn.execute(text(openquery_sql))]
    logger.debug(f"OPENQUERY returned {len(result)} rows")
    return result


# ---------- Common ----------
def list_columns(
    engine: Engine,
    dialect: str,
    schema: str,
    linked_server: Optional[str] = None,
    database: Optional[str] = None,
    dry_run: bool = False,
) -> List[dict]:
    logger.info(f"Getting column metadata for schema '{schema}' via {dialect}")
    if dialect == "snowflake":
        # Use OPENQUERY for Snowflake
        if not linked_server or not database:
            raise ValueError(
                "linked_server and database required for Snowflake via OPENQUERY"
            )

        inner_sql = f"""
        SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, DATA_TYPE, IS_NULLABLE
        FROM {database}.INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = '{schema}'
        """
        result = snowflake_info_schema_openquery(
            engine, linked_server, inner_sql, dry_run
        )
    else:  # SQL Server
        sql = """
        SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, DATA_TYPE, IS_NULLABLE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = :schema
        """
        if dry_run:
            print("\n=== DRY RUN: SQL Server Columns Query ===")
            print(f"SQL: {sql}")
            print(f"Parameters: schema='{schema}'")
            print("=== End Query ===")
            return []  # Return empty list in dry run

        with engine.connect() as conn:
            result = [
                dict(r._mapping) for r in conn.execute(text(sql), {"schema": schema})
            ]
    logger.info(f"Found {len(result)} columns in schema '{schema}'")
    return result


def heuristic_pairs(
    columns: List[dict],
) -> List[Tuple[Tuple[str, str, str], Tuple[str, str, str], float, str]]:
    """
    Suggest potential join pairs by name/type patterns across tables within the same schema.
    Returns list of ((schema,table,col),(schema2,table2,col2),score,reason)
    """
    logger.debug(f"Analyzing {len(columns)} columns for heuristic join suggestions")
    by_schema: Dict[str, List[dict]] = defaultdict(list)
    for c in columns:
        by_schema[c["TABLE_SCHEMA"]].append(c)

    suggestions = []

    def norm(s: str) -> str:
        return s.lower()

    for sch, cols in by_schema.items():
        # Build index by column name
        idx = defaultdict(list)
        for c in cols:
            idx[norm(c["COLUMN_NAME"])].append(c)

        for c in cols:
            t, col = c["TABLE_NAME"], c["COLUMN_NAME"]
            # common id patterns
            candidates = []
            if col.lower().endswith("_id"):
                base = col[:-3]
                candidates += [base, "id"]
            elif col.lower() == "id":
                # try table name + _id
                candidates += [f"{t.lower()}_id"]

            for guess in candidates:
                for c2 in idx.get(guess, []):
                    if c2["TABLE_NAME"] == t:
                        continue
                    # simple type compatibility
                    compatible = (
                        c["DATA_TYPE"].split("(")[0].lower()
                        == c2["DATA_TYPE"].split("(")[0].lower()
                    )
                    score = 0.7 if compatible else 0.55
                    reason = f"name match '{col}'↔'{c2['COLUMN_NAME']}' ({'type ok' if compatible else 'type diff'})"
                    suggestions.append(
                        (
                            (sch, t, col),
                            (sch, c2["TABLE_NAME"], c2["COLUMN_NAME"]),
                            score,
                            reason,
                        )
                    )
    return suggestions


def extract_sqlserver(
    url: str, include: List[str], exclude: List[str], dry_run: bool = False
) -> dict:
    logger.info("Starting SQL Server metadata extraction")
    logger.info(
        f"Connecting to SQL Server: {url.split('@')[0] if '@' in url else 'SQL Server'}"
    )
    eng: Optional[Engine] = create_engine(url) if not dry_run else None
    dialect = "mssql"
    payload = {"dialect": dialect, "schemas": {}}

    logger.info("Enumerating available schemas")
    schemas_sql = "SELECT name FROM sys.schemas"
    if dry_run:
        print("\n=== DRY RUN: SQL Server Schemas Query ===")
        print(f"SQL: {schemas_sql}")
        print("=== End Query ===")
        # Use provided include list as fallback
        schemas = include if "*" not in include else ["dbo", "sales", "hr"]
    else:
        if eng is not None:
            with eng.connect() as conn:
                schemas = [r[0] for r in conn.execute(text(schemas_sql))]
        else:
            schemas = include if "*" not in include else ["dbo", "sales", "hr"]
    logger.info(f"Found {len(schemas)} schemas: {', '.join(schemas)}")

    target = [
        s
        for s in schemas
        if (("*" in include) or (s in include)) and (s not in exclude)
    ]
    logger.info(f"Processing {len(target)} target schemas: {', '.join(target)}")

    for s in target:
        logger.info(f"Processing SQL Server schema '{s}'")
        if not dry_run and eng is not None:
            cols = list_columns(eng, dialect, s, dry_run=dry_run)

            logger.debug(f"Getting foreign keys for schema '{s}'")
            fks = [dict(r) for r in conn_execute(eng, FK_SQLSERVER, dry_run)]
        else:
            cols = []
            fks = []
        # Filter to schema s
        fks = [fk for fk in fks if fk.get("fk_schema") == s] if not dry_run else []
        logger.info(f"Found {len(fks)} foreign keys in schema '{s}'")

        tables = _tables_from_columns(cols)
        heuristics = heuristic_pairs(cols)
        logger.info(
            f"Schema '{s}': {len(tables)} tables, {len(fks)} foreign keys, {len(heuristics)} heuristic suggestions"
        )

        payload["schemas"][s] = {
            "tables": tables,
            "foreign_keys": _group_fk_rows(fks),
            "heuristics": heuristics,
        }

    logger.info(
        f"SQL Server metadata extraction complete - processed {len(target)} schemas"
    )
    return payload  # SQL Server FK derivation from INFORMATION_SCHEMA documented.  [5](https://learn.microsoft.com/en-us/sql/relational-databases/system-information-schema-views/key-column-usage-transact-sql?view=sql-server-ver17)[6](https://stackoverflow.com/questions/3907879/sql-server-howto-get-foreign-key-reference-from-information-schema)


def conn_execute(eng: Engine, sql: str, dry_run: bool = False) -> List[dict]:
    if dry_run:
        print("\n=== DRY RUN: SQL Query ===")
        print(f"SQL: {sql}")
        print("=== End Query ===")
        return []  # Return empty list in dry run

    with eng.connect() as conn:
        return [dict(r._mapping) for r in conn.execute(text(sql))]


def _tables_from_columns(cols: List[dict]) -> Dict[str, Any]:
    out: Dict[str, Any] = defaultdict(lambda: {"columns": []})
    for c in cols:
        out[c["TABLE_NAME"]]["columns"].append(
            {
                "name": c["COLUMN_NAME"],
                "data_type": c["DATA_TYPE"],
                "nullable": (c["IS_NULLABLE"] == "YES"),
            }
        )
    return out


def _group_fk_rows(rows: List[dict]) -> List[dict]:
    """Group FK rows (one per column) into FKs with (child_cols, parent_cols)."""
    fks: Dict[Tuple[str, str, str], Dict[str, Any]] = defaultdict(
        lambda: {"name": None, "child": {}, "parent": {}, "pairs": []}
    )
    for r in rows:
        key = (r["fk_schema"], r["fk_table"], r["fk_name"])
        fks[key]["name"] = r["fk_name"]
        fks[key]["child"] = {"schema": r["fk_schema"], "table": r["fk_table"]}
        fks[key]["parent"] = {"schema": r["pk_schema"], "table": r["pk_table"]}
        fks[key]["pairs"].append(
            {"child_col": r["fk_column"], "parent_col": r["pk_column"]}
        )
    return list(fks.values())


def extract_snowflake(
    sqlserver_url: str,
    linked_server: str,
    database: str,
    include: List[str],
    exclude: List[str],
    dry_run: bool = False,
) -> dict:
    """Extract Snowflake metadata using OPENQUERY via SQL Server linked server."""
    logger.info("Starting Snowflake metadata extraction via OPENQUERY")
    logger.info(
        f"Using SQL Server connection for OPENQUERY to linked server '{linked_server}' database '{database}'"
    )

    eng: Optional[Engine] = create_engine(sqlserver_url) if not dry_run else None
    dialect = "snowflake"
    payload = {"dialect": dialect, "schemas": {}}

    # Get schemas via OPENQUERY
    logger.info("Enumerating Snowflake schemas via OPENQUERY")
    schema_sql = f"""
    SELECT SCHEMA_NAME
    FROM {database}.INFORMATION_SCHEMA.SCHEMATA
    WHERE SCHEMA_NAME NOT IN ('INFORMATION_SCHEMA')
    """

    try:
        schema_rows = (
            snowflake_info_schema_openquery(eng, linked_server, schema_sql, dry_run)
            if eng is not None
            else []
        )
        schemas = (
            [r["SCHEMA_NAME"] for r in schema_rows]
            if not dry_run
            else (include if "*" not in include else ["PUBLIC", "CORE"])
        )
        logger.info(f"Found {len(schemas)} Snowflake schemas: {', '.join(schemas)}")
    except Exception as e:
        logger.warning(f"Could not retrieve schemas via OPENQUERY: {e}")
        # Fallback to provided include list
        schemas = include if "*" not in include else []
        logger.info(f"Using fallback schema list: {', '.join(schemas)}")

    # Filter schemas
    target = [
        s
        for s in schemas
        if (("*" in include) or (s in include)) and (s not in exclude)
    ]
    logger.info(
        f"Processing {len(target)} target Snowflake schemas: {', '.join(target)}"
    )

    # FK column pairs via OPENQUERY
    try:
        show_rows = (
            snowflake_show_imported_keys(eng, linked_server, database, dry_run)
            if eng is not None
            else []
        )
    except Exception as e:
        logger.warning(f"Could not retrieve foreign keys via OPENQUERY: {e}")
        show_rows = []

    # Build fast index
    fk_index = defaultdict(list)
    for r in show_rows:
        sch = r.get("schema_name")
        fk_index[sch].append(r)

    for s in target:
        logger.info(f"Processing Snowflake schema '{s}'")
        try:
            if eng is not None:
                cols = list_columns(eng, dialect, s, linked_server, database, dry_run)
            else:
                cols = []
            fks_rows = fk_index.get(s, [])  # may be empty if no FKs defined
            fks = []
            for r in fks_rows:
                fks.append(
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
                        "sequence": r.get("key_sequence"),
                    }
                )

            tables = _tables_from_columns(cols)
            heuristics = heuristic_pairs(cols)
            logger.info(
                f"Snowflake schema '{s}': {len(tables)} tables, {len(fks)} foreign keys, {len(heuristics)} heuristic suggestions"
            )

            payload["schemas"][s] = {
                "tables": tables,
                "foreign_keys": fks,
                "heuristics": heuristics,
            }
        except Exception as e:
            logger.error(f"Could not process Snowflake schema '{s}': {e}")
            continue

    logger.info(
        f"Snowflake metadata extraction complete - processed {len(payload['schemas'])} schemas"
    )
    return payload


def list_available_schemas(cfg: dict, dry_run: bool = False):
    """List all available schemas from enabled data sources."""

    # Process all configured sources dynamically
    for source_name, source_config in cfg.items():
        if source_name == "output":  # Skip output configuration
            continue

        if not source_config.get("enabled", True):  # Skip if explicitly disabled
            logger.info(f"{source_name} schema listing disabled")
            continue

        # Determine source type and list schemas accordingly
        if "url" in source_config:
            # SQL Server source
            logger.info(f"\n=== {source_name} (SQL Server) Schemas ===")

            try:
                if not dry_run:
                    eng = create_engine(source_config["url"])
                    with eng.connect() as conn:
                        schemas = [
                            r[0]
                            for r in conn.execute(text("SELECT name FROM sys.schemas"))
                        ]
                    logger.info(f"Available {source_name} schemas ({len(schemas)}):")
                    for schema in sorted(schemas):
                        print(f"  - {schema}")
                else:
                    print(
                        f"\n=== DRY RUN: {source_name} (SQL Server) Schemas Query ==="
                    )
                    print("SQL: SELECT name FROM sys.schemas")
                    print("=== End Query ===")
                    logger.info(
                        f"DRY RUN - Would connect to {source_name} to list schemas"
                    )

            except Exception as e:
                logger.error(f"Could not connect to {source_name}: {e}")

        elif "sqlserver_url" in source_config and "linked_server" in source_config:
            # Snowflake source (via OPENQUERY)
            logger.info(f"\n=== {source_name} (Snowflake via OPENQUERY) Schemas ===")

            try:
                schema_sql = f"""
                SELECT SCHEMA_NAME
                FROM {source_config["database"]}.INFORMATION_SCHEMA.SCHEMATA
                WHERE SCHEMA_NAME NOT IN ('INFORMATION_SCHEMA')
                ORDER BY SCHEMA_NAME
                """

                if not dry_run:
                    eng = create_engine(source_config["sqlserver_url"])
                    schema_rows = snowflake_info_schema_openquery(
                        eng, source_config["linked_server"], schema_sql, dry_run=False
                    )
                    schemas = [r["SCHEMA_NAME"] for r in schema_rows]
                    logger.info(f"Available {source_name} schemas ({len(schemas)}):")
                    for schema in sorted(schemas):
                        print(f"  - {schema}")
                else:
                    escaped_sql = schema_sql.replace("'", "''")
                    openquery_sql = f"SELECT * FROM OPENQUERY({source_config['linked_server']}, '{escaped_sql}')"
                    print(f"\n=== DRY RUN: {source_name} (Snowflake) Schemas Query ===")
                    print(openquery_sql)
                    print("=== End Query ===")
                    logger.info(
                        f"DRY RUN - Would connect via OPENQUERY to list {source_name} schemas"
                    )

            except Exception as e:
                logger.error(f"Could not connect to {source_name} via OPENQUERY: {e}")
        else:
            logger.warning(f"Skipping {source_name}: unrecognized configuration format")

    print("\n=== Configuration Suggestions ===")
    print("Use these schema names in your config.yaml:")
    print("- Add to 'include_schemas' to process specific schemas")
    print("- Add to 'exclude_schemas' to skip unwanted schemas")
    print('- Use ["*"] in include_schemas to process all schemas')


def main():
    # Setup loguru
    logger.remove()  # Remove default handler
    logger.add(
        sys.stdout,
        format="{time:YYYY-MM-DD HH:mm:ss} - {level} - {message}",
        level="INFO",
    )
    logger.add(
        "refresh_metadata.log",
        format="{time:YYYY-MM-DD HH:mm:ss} - {level} - {message}",
        level="INFO",
    )

    logger.info("=== Database Metadata Refresh Started ===")

    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default="config.yaml")
    ap.add_argument("--verbose", "-v", action="store_true", help="Enable debug logging")
    ap.add_argument(
        "--dry-run", action="store_true", help="Print queries without executing them"
    )
    ap.add_argument(
        "--list-schemas", action="store_true", help="List available schemas and exit"
    )
    ap.add_argument(
        "--source", "-s", type=str, help="Process only the specified source/database"
    )
    args = ap.parse_args()

    if args.verbose:
        logger.remove()
        logger.add(
            sys.stdout,
            format="{time:YYYY-MM-DD HH:mm:ss} - {level} - {message}",
            level="DEBUG",
        )
        logger.add(
            "refresh_metadata.log",
            format="{time:YYYY-MM-DD HH:mm:ss} - {level} - {message}",
            level="DEBUG",
        )
        logger.info("Debug logging enabled")

    if args.dry_run:
        logger.info("DRY RUN MODE - No queries will be executed")

    cfg = load_cfg(args.config)

    if args.list_schemas:
        logger.info("LIST SCHEMAS MODE - Discovering available schemas")
        list_available_schemas(cfg, args.dry_run)
        return

    # Validate source parameter if specified
    if args.source and args.source not in cfg:
        logger.error(f"Source '{args.source}' not found in configuration")
        available_sources = [s for s in cfg.keys() if s != "output"]
        logger.error(f"Available sources: {', '.join(available_sources)}")
        sys.exit(1)

    try:
        # Load existing metadata if we're updating a single source
        existing_metadata = {"sources": {}}
        out_p = cfg["output"]["path"]
        
        if args.source and os.path.exists(out_p):
            logger.info(f"Loading existing metadata from {out_p}")
            try:
                with open(out_p, "r", encoding="utf-8") as f:
                    existing_metadata = json.load(f)
                # Ensure sources key exists
                if "sources" not in existing_metadata:
                    existing_metadata["sources"] = {}
                logger.info("Existing metadata loaded successfully")
            except Exception as e:
                logger.warning(f"Could not load existing metadata: {e}")
                logger.info("Starting with empty metadata")
        
        final = existing_metadata

        # Determine which sources to process
        sources_to_process = {}
        if args.source:
            # Process only the specified source
            logger.info(f"Processing single source: {args.source}")
            sources_to_process[args.source] = cfg[args.source]
        else:
            # Process all sources (original behavior)
            logger.info("Processing all configured sources")
            sources_to_process = {k: v for k, v in cfg.items() if k != "output"}

        # Process the selected sources
        for source_name, source_config in sources_to_process.items():
            if source_name == "output":  # Skip output configuration
                continue

            if not source_config.get("enabled", True):  # Skip if explicitly disabled
                logger.info(f"{source_name} extraction disabled")
                continue

            if args.source:
                logger.info(f"Updating metadata for source '{source_name}' only")

            # Determine source type based on configuration keys
            if "url" in source_config:
                # SQL Server source (has direct URL)
                logger.info(f"{source_name} (SQL Server) extraction enabled")
                final["sources"][source_name] = extract_sqlserver(
                    source_config["url"],
                    source_config.get("include_schemas", ["*"]),
                    source_config.get("exclude_schemas", []),
                    dry_run=args.dry_run,
                )
                logger.info(f"{source_name} (SQL Server) extraction completed")

            elif "sqlserver_url" in source_config and "linked_server" in source_config:
                # Snowflake source (via OPENQUERY/linked server)
                logger.info(
                    f"{source_name} (Snowflake via OPENQUERY) extraction enabled"
                )
                final["sources"][source_name] = extract_snowflake(
                    source_config["sqlserver_url"],
                    source_config["linked_server"],
                    source_config["database"],
                    source_config.get("include_schemas", ["*"]),
                    source_config.get("exclude_schemas", []),
                    dry_run=args.dry_run,
                )
                logger.info(
                    f"{source_name} (Snowflake via OPENQUERY) extraction completed"
                )
            else:
                logger.warning(
                    f"Skipping {source_name}: unrecognized configuration format"
                )

        if not args.dry_run:
            out_p = cfg["output"]["path"]
            logger.info(f"Writing metadata to {out_p}")
            os.makedirs(os.path.dirname(out_p), exist_ok=True)
            with open(out_p, "w", encoding="utf-8") as f:
                json.dump(final, f, indent=2)
            if args.source:
                print(f"Updated metadata for '{args.source}' in {out_p}")
            else:
                print(f"Wrote metadata to {out_p}")
        else:
            logger.info("DRY RUN - Skipping file output")
            print("DRY RUN MODE - No files were written")

        # Log summary
        if args.source:
            processed_schemas = len(final["sources"].get(args.source, {}).get("schemas", {}))
            logger.info("=== Single Source Metadata Refresh Complete ===")
            logger.info(f"Source processed: {args.source}")
            logger.info(f"Schemas processed: {processed_schemas}")
        else:
            total_schemas = sum(
                len(source.get("schemas", {})) for source in final["sources"].values()
            )
            processed_sources = list(sources_to_process.keys())
            logger.info("=== Metadata Refresh Complete ===")
            logger.info(f"Sources processed: {', '.join(processed_sources)}")
            logger.info(f"Total schemas: {total_schemas}")
        
        if not args.dry_run:
            logger.info(f"Output file: {cfg['output']['path']}")

    except Exception as e:
        logger.error(f"Fatal error during metadata refresh: {e}")
        logger.exception("Full traceback:")
        sys.exit(1)


if __name__ == "__main__":
    main()
