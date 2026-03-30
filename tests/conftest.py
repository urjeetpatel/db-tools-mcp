"""Shared fixtures for db-tools-mcp tests."""
from __future__ import annotations

import json
from pathlib import Path

import pytest


# ---------------------------------------------------------------------------
# Sample metadata cache — two sources, realistic structure
# ---------------------------------------------------------------------------
SAMPLE_CACHE = {
    "sources": {
        "testdb": {
            "dialect": "mssql",
            "schemas": {
                "dbo": {
                    "tables": {
                        "Patients": {
                            "columns": [
                                {"name": "PatientID", "data_type": "int", "nullable": False},
                                {"name": "FirstName", "data_type": "varchar", "nullable": False},
                                {"name": "LastName", "data_type": "varchar", "nullable": False},
                                {"name": "DOB", "data_type": "date", "nullable": True},
                            ]
                        },
                        "Orders": {
                            "columns": [
                                {"name": "OrderID", "data_type": "int", "nullable": False},
                                {"name": "PatientID", "data_type": "int", "nullable": False},
                                {"name": "OrderDate", "data_type": "datetime", "nullable": False},
                                {"name": "DrugID", "data_type": "int", "nullable": False},
                            ]
                        },
                        "Drugs": {
                            "columns": [
                                {"name": "DrugID", "data_type": "int", "nullable": False},
                                {"name": "DrugName", "data_type": "varchar", "nullable": False},
                                {"name": "NDC", "data_type": "varchar", "nullable": True},
                            ]
                        },
                    },
                    "foreign_keys": [
                        {
                            "name": "FK_Orders_Patients",
                            "child": {"schema": "dbo", "table": "Orders"},
                            "parent": {"schema": "dbo", "table": "Patients"},
                            "pairs": [{"child_col": "PatientID", "parent_col": "PatientID"}],
                        },
                        {
                            "name": "FK_Orders_Drugs",
                            "child": {"schema": "dbo", "table": "Orders"},
                            "parent": {"schema": "dbo", "table": "Drugs"},
                            "pairs": [{"child_col": "DrugID", "parent_col": "DrugID"}],
                        },
                    ],
                    "heuristics": [],
                },
                "audit": {
                    "tables": {
                        "AuditLog": {
                            "columns": [
                                {"name": "LogID", "data_type": "int", "nullable": False},
                                {"name": "Action", "data_type": "varchar", "nullable": False},
                            ]
                        },
                    },
                    "foreign_keys": [],
                    "heuristics": [],
                },
            },
        },
        "warehouse": {
            "dialect": "snowflake",
            "schemas": {
                "PUBLIC": {
                    "tables": {
                        "DIM_PATIENT": {
                            "columns": [
                                {"name": "PATIENT_KEY", "data_type": "NUMBER", "nullable": False},
                                {"name": "PATIENT_NAME", "data_type": "VARCHAR", "nullable": True},
                            ]
                        },
                    },
                    "foreign_keys": [],
                    "heuristics": [],
                },
            },
        },
    }
}


@pytest.fixture
def sample_cache():
    """Return the sample cache dict."""
    return SAMPLE_CACHE


@pytest.fixture
def cache_dir(tmp_path):
    """Write sample cache JSON files to a temp dir and return the path."""
    cache = tmp_path / "metadata_cache"
    cache.mkdir()
    for source_name, source_data in SAMPLE_CACHE["sources"].items():
        (cache / f"{source_name}.json").write_text(
            json.dumps(source_data), encoding="utf-8"
        )
    return cache


@pytest.fixture
def app_dir(tmp_path, cache_dir):
    """
    Return a tmp app directory with cache already populated.
    Patches db_tools._config module-level paths so all code uses the tmp dir.
    """
    import db_tools._config as cfg_mod

    original = {
        "APP_DIR": cfg_mod.APP_DIR,
        "CONFIG_PATH": cfg_mod.CONFIG_PATH,
        "CACHE_DIR": cfg_mod.CACHE_DIR,
        "REFRESH_STATE_PATH": cfg_mod.REFRESH_STATE_PATH,
    }

    cfg_mod.APP_DIR = tmp_path
    cfg_mod.CONFIG_PATH = tmp_path / "config.yaml"
    cfg_mod.CACHE_DIR = cache_dir
    cfg_mod.REFRESH_STATE_PATH = tmp_path / ".refresh_state.json"

    yield tmp_path

    # Restore
    for attr, val in original.items():
        setattr(cfg_mod, attr, val)
