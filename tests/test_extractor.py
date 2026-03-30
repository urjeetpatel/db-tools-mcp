"""Tests for db_tools._extractor — helper functions and refresh dispatch."""
from __future__ import annotations

import pytest

from db_tools._extractor import (
    _columns_to_tables,
    _group_fk_rows,
    _heuristic_pairs,
    run_refresh,
)


# ---------------------------------------------------------------------------
# _columns_to_tables
# ---------------------------------------------------------------------------
class TestColumnsToTables:
    def test_basic(self):
        cols = [
            {"TABLE_NAME": "Users", "COLUMN_NAME": "id", "DATA_TYPE": "int", "IS_NULLABLE": "NO"},
            {"TABLE_NAME": "Users", "COLUMN_NAME": "name", "DATA_TYPE": "varchar", "IS_NULLABLE": "YES"},
            {"TABLE_NAME": "Orders", "COLUMN_NAME": "id", "DATA_TYPE": "int", "IS_NULLABLE": "NO"},
        ]
        tables = _columns_to_tables(cols)
        assert set(tables.keys()) == {"Users", "Orders"}
        assert len(tables["Users"]["columns"]) == 2
        assert len(tables["Orders"]["columns"]) == 1

    def test_nullable_mapping(self):
        cols = [
            {"TABLE_NAME": "T", "COLUMN_NAME": "a", "DATA_TYPE": "int", "IS_NULLABLE": "YES"},
            {"TABLE_NAME": "T", "COLUMN_NAME": "b", "DATA_TYPE": "int", "IS_NULLABLE": "NO"},
        ]
        tables = _columns_to_tables(cols)
        col_a = next(c for c in tables["T"]["columns"] if c["name"] == "a")
        col_b = next(c for c in tables["T"]["columns"] if c["name"] == "b")
        assert col_a["nullable"] is True
        assert col_b["nullable"] is False

    def test_empty_input(self):
        assert _columns_to_tables([]) == {}


# ---------------------------------------------------------------------------
# _group_fk_rows
# ---------------------------------------------------------------------------
class TestGroupFkRows:
    def test_single_column_fk(self):
        rows = [
            {
                "fk_schema": "dbo", "fk_table": "Orders", "fk_column": "PatientID",
                "pk_schema": "dbo", "pk_table": "Patients", "pk_column": "PatientID",
                "fk_name": "FK_Orders_Patients",
            }
        ]
        result = _group_fk_rows(rows)
        assert len(result) == 1
        fk = result[0]
        assert fk["name"] == "FK_Orders_Patients"
        assert fk["child"] == {"schema": "dbo", "table": "Orders"}
        assert fk["parent"] == {"schema": "dbo", "table": "Patients"}
        assert len(fk["pairs"]) == 1

    def test_multi_column_fk(self):
        rows = [
            {
                "fk_schema": "dbo", "fk_table": "LineItems", "fk_column": "OrderID",
                "pk_schema": "dbo", "pk_table": "Orders", "pk_column": "OrderID",
                "fk_name": "FK_Composite",
            },
            {
                "fk_schema": "dbo", "fk_table": "LineItems", "fk_column": "LineNum",
                "pk_schema": "dbo", "pk_table": "Orders", "pk_column": "LineNum",
                "fk_name": "FK_Composite",
            },
        ]
        result = _group_fk_rows(rows)
        assert len(result) == 1
        assert len(result[0]["pairs"]) == 2

    def test_multiple_fks(self):
        rows = [
            {
                "fk_schema": "dbo", "fk_table": "A", "fk_column": "bid",
                "pk_schema": "dbo", "pk_table": "B", "pk_column": "id",
                "fk_name": "FK1",
            },
            {
                "fk_schema": "dbo", "fk_table": "A", "fk_column": "cid",
                "pk_schema": "dbo", "pk_table": "C", "pk_column": "id",
                "fk_name": "FK2",
            },
        ]
        result = _group_fk_rows(rows)
        assert len(result) == 2

    def test_empty(self):
        assert _group_fk_rows([]) == []


# ---------------------------------------------------------------------------
# _heuristic_pairs
# ---------------------------------------------------------------------------
class TestHeuristicPairs:
    def test_id_suffix_match(self):
        cols = [
            {"TABLE_SCHEMA": "dbo", "TABLE_NAME": "Orders", "COLUMN_NAME": "patient_id", "DATA_TYPE": "int"},
            {"TABLE_SCHEMA": "dbo", "TABLE_NAME": "Patients", "COLUMN_NAME": "patient", "DATA_TYPE": "int"},
        ]
        pairs = _heuristic_pairs(cols)
        assert len(pairs) >= 1
        # Should match patient_id -> patient
        pair = pairs[0]
        assert pair[2] == 0.7  # same type -> 0.7

    def test_type_mismatch_lower_score(self):
        cols = [
            {"TABLE_SCHEMA": "dbo", "TABLE_NAME": "Orders", "COLUMN_NAME": "patient_id", "DATA_TYPE": "int"},
            {"TABLE_SCHEMA": "dbo", "TABLE_NAME": "Patients", "COLUMN_NAME": "patient", "DATA_TYPE": "varchar"},
        ]
        pairs = _heuristic_pairs(cols)
        assert len(pairs) >= 1
        assert pairs[0][2] == 0.55  # different type

    def test_same_table_excluded(self):
        cols = [
            {"TABLE_SCHEMA": "dbo", "TABLE_NAME": "T", "COLUMN_NAME": "parent_id", "DATA_TYPE": "int"},
            {"TABLE_SCHEMA": "dbo", "TABLE_NAME": "T", "COLUMN_NAME": "parent", "DATA_TYPE": "int"},
        ]
        pairs = _heuristic_pairs(cols)
        assert len(pairs) == 0

    def test_cross_schema_no_match(self):
        cols = [
            {"TABLE_SCHEMA": "dbo", "TABLE_NAME": "A", "COLUMN_NAME": "x_id", "DATA_TYPE": "int"},
            {"TABLE_SCHEMA": "other", "TABLE_NAME": "B", "COLUMN_NAME": "x", "DATA_TYPE": "int"},
        ]
        pairs = _heuristic_pairs(cols)
        # Heuristics only work within same schema
        assert len(pairs) == 0

    def test_empty(self):
        assert _heuristic_pairs([]) == []


# ---------------------------------------------------------------------------
# run_refresh — dispatch validation (no live DB)
# ---------------------------------------------------------------------------
class TestRunRefreshDispatch:
    def test_unknown_config_format_raises(self, app_dir):
        with pytest.raises(ValueError, match="unrecognized config format"):
            run_refresh("bad", {"enabled": True, "bogus_key": "value"})

    def test_sqlserver_config_dispatches(self, app_dir, monkeypatch):
        """Verify run_refresh calls extract_sqlserver for url-based configs."""
        called_with = {}

        def fake_extract(url, include, exclude):
            called_with["url"] = url
            return {"dialect": "mssql", "schemas": {}}

        monkeypatch.setattr("db_tools._extractor.extract_sqlserver", fake_extract)

        cfg = {"url": "mssql+pyodbc:///fake", "include_schemas": ["dbo"], "exclude_schemas": []}
        result = run_refresh("test_src", cfg)
        assert "test_src" in called_with.get("url", "") or called_with["url"] == "mssql+pyodbc:///fake"
        assert "0 schemas" in result

    def test_snowflake_config_dispatches(self, app_dir, monkeypatch):
        """Verify run_refresh calls extract_snowflake for linked-server configs."""
        called = []

        def fake_extract(sqlserver_url, linked_server, database, include, exclude):
            called.append(linked_server)
            return {"dialect": "snowflake", "schemas": {}}

        monkeypatch.setattr("db_tools._extractor.extract_snowflake", fake_extract)

        cfg = {
            "sqlserver_url": "mssql+pyodbc:///fake",
            "linked_server": "SNOWFLAKE",
            "database": "MY_DB",
        }
        run_refresh("sf_test", cfg)
        assert called == ["SNOWFLAKE"]
