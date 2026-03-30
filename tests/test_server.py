"""Tests for db_tools.server — MCP tool functions against fixture cache data."""
from __future__ import annotations

from unittest.mock import patch

import pytest

from tests.conftest import SAMPLE_CACHE

# Import tool functions
from db_tools.server import (
    add_database,
    find_direct_joins,
    get_dialect,
    get_table,
    list_all_foreign_keys,
    list_schemas,
    list_sources,
    list_tables,
    refresh_metadata,
    search_columns,
    search_tables,
    suggest_joins,
)


@pytest.fixture(autouse=True)
def _mock_cache():
    """Patch _load_cache for every test in this module."""
    with patch("db_tools.server._load_cache", return_value=SAMPLE_CACHE):
        yield


# ---------------------------------------------------------------------------
# list_sources
# ---------------------------------------------------------------------------
class TestListSources:
    def test_returns_all_sources(self):
        assert sorted(list_sources()) == ["testdb", "warehouse"]


# ---------------------------------------------------------------------------
# list_schemas
# ---------------------------------------------------------------------------
class TestListSchemas:
    def test_returns_schemas(self):
        assert sorted(list_schemas("testdb")) == ["audit", "dbo"]

    def test_missing_source_raises(self):
        with pytest.raises(KeyError):
            list_schemas("nonexistent")


# ---------------------------------------------------------------------------
# list_tables
# ---------------------------------------------------------------------------
class TestListTables:
    def test_returns_tables(self):
        assert sorted(list_tables("testdb", "dbo")) == ["Drugs", "Orders", "Patients"]

    def test_single_table_schema(self):
        assert list_tables("testdb", "audit") == ["AuditLog"]


# ---------------------------------------------------------------------------
# get_table
# ---------------------------------------------------------------------------
class TestGetTable:
    def test_existing_table(self):
        result = get_table("testdb", "dbo", "Orders")
        assert len(result["columns"]) == 4
        assert any(c["name"] == "PatientID" for c in result["columns"])
        # Orders is child in both FKs -> outbound
        assert len(result["foreign_keys_outbound"]) == 2
        assert len(result["foreign_keys_inbound"]) == 0

    def test_parent_table_has_inbound(self):
        result = get_table("testdb", "dbo", "Patients")
        assert len(result["foreign_keys_inbound"]) == 1
        assert result["foreign_keys_inbound"][0]["name"] == "FK_Orders_Patients"
        assert len(result["foreign_keys_outbound"]) == 0

    def test_missing_table(self):
        result = get_table("testdb", "dbo", "DoesNotExist")
        assert result["error"] == "Table not found"
        assert result["columns"] == []


# ---------------------------------------------------------------------------
# get_dialect
# ---------------------------------------------------------------------------
class TestGetDialect:
    def test_mssql(self):
        assert get_dialect("testdb") == "mssql"

    def test_snowflake(self):
        assert get_dialect("warehouse") == "snowflake"


# ---------------------------------------------------------------------------
# list_all_foreign_keys
# ---------------------------------------------------------------------------
class TestListAllForeignKeys:
    def test_returns_fks(self):
        result = list_all_foreign_keys("testdb", "dbo")
        assert len(result) == 2
        names = {fk["name"] for fk in result}
        assert names == {"FK_Orders_Patients", "FK_Orders_Drugs"}

    def test_schema_with_no_fks(self):
        assert list_all_foreign_keys("testdb", "audit") == []


# ---------------------------------------------------------------------------
# find_direct_joins
# ---------------------------------------------------------------------------
class TestFindDirectJoins:
    def test_finds_join(self):
        result = find_direct_joins("testdb", "dbo.Orders", "dbo.Patients")
        assert len(result) == 1
        assert result[0]["name"] == "FK_Orders_Patients"

    def test_reverse_order_still_works(self):
        result = find_direct_joins("testdb", "dbo.Patients", "dbo.Orders")
        assert len(result) == 1

    def test_no_direct_join(self):
        assert find_direct_joins("testdb", "dbo.Patients", "dbo.Drugs") == []


# ---------------------------------------------------------------------------
# suggest_joins
# ---------------------------------------------------------------------------
class TestSuggestJoins:
    def test_direct_path(self):
        result = suggest_joins("testdb", "dbo.Orders", "dbo.Patients")
        assert len(result) >= 1
        assert result[0]["tables"] == ["dbo.Orders", "dbo.Patients"]
        assert result[0]["confidence"] > 0

    def test_two_hop_path(self):
        # Patients -> Orders -> Drugs (2 hops)
        result = suggest_joins("testdb", "dbo.Patients", "dbo.Drugs", max_hops=2)
        assert len(result) >= 1
        path = result[0]
        assert "dbo.Orders" in path["tables"]
        assert len(path["edges"]) == 2

    def test_no_path_when_hops_too_low(self):
        # Patients -> Drugs requires 2 hops, max_hops=1 should yield nothing
        assert suggest_joins("testdb", "dbo.Patients", "dbo.Drugs", max_hops=1) == []

    def test_confidence_decreases_with_hops(self):
        direct = suggest_joins("testdb", "dbo.Orders", "dbo.Patients")
        two_hop = suggest_joins("testdb", "dbo.Patients", "dbo.Drugs", max_hops=2)
        if direct and two_hop:
            assert direct[0]["confidence"] > two_hop[0]["confidence"]


# ---------------------------------------------------------------------------
# search_tables
# ---------------------------------------------------------------------------
class TestSearchTables:
    def test_keyword_match(self):
        result = search_tables("testdb", "order")
        assert len(result) == 1
        assert result[0]["table"] == "Orders"

    def test_case_insensitive(self):
        result = search_tables("testdb", "PATIENT")
        assert any(r["table"] == "Patients" for r in result)

    def test_no_match(self):
        assert search_tables("testdb", "zzzzz") == []

    def test_cross_schema_search(self):
        result = search_tables("testdb", "log")
        assert len(result) == 1
        assert result[0]["schema"] == "audit"
        assert result[0]["table"] == "AuditLog"


# ---------------------------------------------------------------------------
# search_columns
# ---------------------------------------------------------------------------
class TestSearchColumns:
    def test_finds_column_across_tables(self):
        result = search_columns("testdb", "PatientID")
        # PatientID in Patients and Orders
        assert len(result) == 2
        tables = {r["table"] for r in result}
        assert tables == {"Patients", "Orders"}

    def test_schema_filter(self):
        result = search_columns("testdb", "ID", schema="audit")
        assert all(r["schema"] == "audit" for r in result)
        assert any(r["column"] == "LogID" for r in result)

    def test_includes_data_type_and_nullable(self):
        result = search_columns("testdb", "DOB")
        assert len(result) == 1
        assert result[0]["data_type"] == "date"
        assert result[0]["nullable"] is True

    def test_no_match(self):
        assert search_columns("testdb", "zzzzz") == []


# ---------------------------------------------------------------------------
# add_database (validation only — no live DB)
# ---------------------------------------------------------------------------
class TestAddDatabase:
    """These tests need the app_dir fixture to redirect config I/O to tmp."""

    @pytest.fixture(autouse=True)
    def _use_app_dir(self, app_dir):
        pass

    def test_invalid_name(self):
        result = add_database(name="has spaces", db_type="sqlserver", url="x")
        assert "error" in result

    def test_invalid_db_type(self):
        result = add_database(name="test", db_type="postgres", url="x")
        assert "error" in result

    def test_sqlserver_missing_url(self):
        result = add_database(name="test", db_type="sqlserver")
        assert "error" in result
        assert "url" in result["error"]

    def test_snowflake_missing_params(self):
        result = add_database(name="test", db_type="snowflake", sqlserver_url="x")
        assert "error" in result
        assert "linked_server" in str(result["error"])

    def test_duplicate_name_rejected(self):
        from db_tools._config import save_config

        save_config({"existing": {"enabled": True, "url": "x"}})
        result = add_database(
            name="existing", db_type="sqlserver", url="y", test_connection=False
        )
        assert "error" in result
        assert "already exists" in result["error"]

    def test_success_without_connection_test(self):
        from db_tools._config import load_config

        result = add_database(
            name="new_db",
            db_type="sqlserver",
            url="mssql+pyodbc:///test",
            test_connection=False,
        )
        assert result["status"] == "ok"
        cfg = load_config()
        assert "new_db" in cfg
        assert cfg["new_db"]["enabled"] is True
        assert cfg["new_db"]["url"] == "mssql+pyodbc:///test"
        assert cfg["new_db"]["include_schemas"] == ["*"]

    def test_snowflake_success(self):
        from db_tools._config import load_config

        result = add_database(
            name="sf",
            db_type="snowflake",
            sqlserver_url="mssql+pyodbc:///gw",
            linked_server="SNOW",
            snowflake_database="MY_DB",
            test_connection=False,
        )
        assert result["status"] == "ok"
        cfg = load_config()
        assert cfg["sf"]["linked_server"] == "SNOW"
        assert cfg["sf"]["database"] == "MY_DB"
        assert cfg["sf"]["exclude_schemas"] == ["INFORMATION_SCHEMA"]

    def test_custom_schemas(self):
        from db_tools._config import load_config

        result = add_database(
            name="custom",
            db_type="sqlserver",
            url="mssql+pyodbc:///test",
            include_schemas=["dbo", "sales"],
            exclude_schemas=["sys"],
            test_connection=False,
        )
        assert result["status"] == "ok"
        cfg = load_config()
        assert cfg["custom"]["include_schemas"] == ["dbo", "sales"]
        assert cfg["custom"]["exclude_schemas"] == ["sys"]


# ---------------------------------------------------------------------------
# refresh_metadata (throttle + dispatch logic, no live DB)
# ---------------------------------------------------------------------------
class TestRefreshMetadata:
    @pytest.fixture(autouse=True)
    def _use_app_dir(self, app_dir):
        pass

    def test_missing_config(self):
        result = refresh_metadata()
        assert "error" in result

    def test_unknown_source(self):
        from db_tools._config import save_config

        save_config({"real": {"enabled": True, "url": "x"}})
        result = refresh_metadata(source="fake")
        assert "error" in result
        assert "real" in result["available_sources"]

    def test_disabled_source_skipped(self):
        from db_tools._config import save_config

        save_config({"db1": {"enabled": False, "url": "x"}})
        result = refresh_metadata()
        assert result["results"]["db1"]["status"] == "skipped"

    def test_throttle(self):
        from db_tools._config import mark_refreshed, save_config

        save_config({"db1": {"enabled": True, "url": "x"}})
        mark_refreshed("db1")
        result = refresh_metadata(source="db1")
        assert result["results"]["db1"]["status"] == "throttled"

    def test_force_bypasses_throttle(self, monkeypatch):
        from db_tools._config import mark_refreshed, save_config

        save_config({"db1": {"enabled": True, "url": "mssql+pyodbc:///fake"}})
        mark_refreshed("db1")

        monkeypatch.setattr(
            "db_tools.server.run_refresh", lambda name, cfg: "refreshed 0 schemas"
        )
        result = refresh_metadata(source="db1", force=True)
        assert result["results"]["db1"]["status"] == "ok"
