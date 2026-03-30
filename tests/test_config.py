"""Tests for db_tools._config — paths, config I/O, refresh state."""
from __future__ import annotations

import json
from datetime import datetime, timezone, timedelta

import pytest
import yaml


# ---------------------------------------------------------------------------
# Path resolution
# ---------------------------------------------------------------------------
class TestResolveAppDir:
    def test_env_override(self, monkeypatch, tmp_path):
        monkeypatch.setenv("DB_TOOLS_CONFIG_DIR", str(tmp_path / "custom"))
        monkeypatch.delenv("XDG_CONFIG_HOME", raising=False)
        from db_tools._config import _resolve_app_dir

        assert _resolve_app_dir() == tmp_path / "custom"

    def test_xdg_config_home(self, monkeypatch, tmp_path):
        monkeypatch.delenv("DB_TOOLS_CONFIG_DIR", raising=False)
        monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path / "xdg"))
        from db_tools._config import _resolve_app_dir

        assert _resolve_app_dir() == tmp_path / "xdg" / "db-tools"

    def test_default_fallback(self, monkeypatch):
        monkeypatch.delenv("DB_TOOLS_CONFIG_DIR", raising=False)
        monkeypatch.delenv("XDG_CONFIG_HOME", raising=False)
        from pathlib import Path
        from db_tools._config import _resolve_app_dir

        assert _resolve_app_dir() == Path.home() / ".config" / "db-tools"

    def test_env_override_takes_precedence_over_xdg(self, monkeypatch, tmp_path):
        monkeypatch.setenv("DB_TOOLS_CONFIG_DIR", str(tmp_path / "override"))
        monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path / "xdg"))
        from db_tools._config import _resolve_app_dir

        assert _resolve_app_dir() == tmp_path / "override"


# ---------------------------------------------------------------------------
# Config I/O
# ---------------------------------------------------------------------------
class TestConfigIO:
    def test_load_missing_raises(self, app_dir):
        from db_tools._config import load_config

        with pytest.raises(FileNotFoundError, match="Config not found"):
            load_config()

    def test_save_and_load_roundtrip(self, app_dir):
        from db_tools._config import load_config, save_config

        cfg = {
            "my_db": {
                "enabled": True,
                "url": "mssql+pyodbc:///test",
                "include_schemas": ["*"],
                "exclude_schemas": ["sys"],
            }
        }
        save_config(cfg)
        loaded = load_config()
        assert loaded == cfg

    def test_save_creates_parent_dirs(self, tmp_path):
        import db_tools._config as cfg_mod

        nested = tmp_path / "a" / "b" / "c"
        cfg_mod.APP_DIR = nested
        cfg_mod.CONFIG_PATH = nested / "config.yaml"
        try:
            cfg_mod.save_config({"x": 1})
            assert (nested / "config.yaml").exists()
        finally:
            cfg_mod.APP_DIR = cfg_mod._resolve_app_dir()
            cfg_mod.CONFIG_PATH = cfg_mod.APP_DIR / "config.yaml"

    def test_load_empty_yaml_returns_empty_dict(self, app_dir):
        from db_tools._config import CONFIG_PATH, load_config

        CONFIG_PATH.write_text("", encoding="utf-8")
        assert load_config() == {}


# ---------------------------------------------------------------------------
# Refresh state
# ---------------------------------------------------------------------------
class TestRefreshState:
    def test_hours_since_never_refreshed(self, app_dir):
        from db_tools._config import hours_since_refresh

        assert hours_since_refresh("nonexistent") is None

    def test_mark_and_hours(self, app_dir):
        from db_tools._config import hours_since_refresh, mark_refreshed

        mark_refreshed("src1")
        hours = hours_since_refresh("src1")
        assert hours is not None
        assert hours < 0.1  # just marked, should be ~0

    def test_state_persists_to_disk(self, app_dir):
        from db_tools._config import REFRESH_STATE_PATH, mark_refreshed

        mark_refreshed("src1")
        assert REFRESH_STATE_PATH.exists()
        data = json.loads(REFRESH_STATE_PATH.read_text(encoding="utf-8"))
        assert "src1" in data["sources"]
        assert "last_refresh" in data["sources"]["src1"]

    def test_multiple_sources_independent(self, app_dir):
        from db_tools._config import hours_since_refresh, mark_refreshed

        mark_refreshed("a")
        assert hours_since_refresh("b") is None
        mark_refreshed("b")
        assert hours_since_refresh("a") is not None
        assert hours_since_refresh("b") is not None

    def test_hours_with_old_timestamp(self, app_dir):
        from db_tools._config import REFRESH_STATE_PATH, hours_since_refresh

        old_time = (datetime.now(timezone.utc) - timedelta(hours=48)).isoformat()
        state = {"sources": {"old_src": {"last_refresh": old_time}}}
        REFRESH_STATE_PATH.write_text(json.dumps(state), encoding="utf-8")

        hours = hours_since_refresh("old_src")
        assert hours is not None
        assert hours >= 47.9  # roughly 48 hours
