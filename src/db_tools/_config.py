"""
Paths, configuration I/O, refresh-state tracking, and logging setup.

App directory resolution (first match wins):
  1. DB_TOOLS_CONFIG_DIR  env var   (explicit override)
  2. $XDG_CONFIG_HOME/db-tools      (XDG standard)
  3. ~/.config/db-tools              (XDG default fallback — works on all platforms)
"""
from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import yaml
from loguru import logger


# ---------------------------------------------------------------------------
# App directory
# ---------------------------------------------------------------------------
def _resolve_app_dir() -> Path:
    override = os.environ.get("DB_TOOLS_CONFIG_DIR", "").strip()
    if override:
        return Path(override)
    xdg = os.environ.get("XDG_CONFIG_HOME", "").strip()
    if xdg:
        return Path(xdg) / "db-tools"
    return Path.home() / ".config" / "db-tools"


APP_DIR: Path = _resolve_app_dir()
CONFIG_PATH: Path = APP_DIR / "config.yaml"
CACHE_DIR: Path = APP_DIR / "metadata_cache"
REFRESH_STATE_PATH: Path = APP_DIR / ".refresh_state.json"

REFRESH_INTERVAL_HOURS: int = 24

DEFAULT_MSSQL_EXCLUDE: list[str] = [
    "INFORMATION_SCHEMA",
    "sys",
    "db_owner",
    "db_accessadmin",
    "db_securityadmin",
    "db_ddladmin",
    "db_backupoperator",
    "db_datareader",
    "db_datawriter",
    "db_denydatareader",
    "db_denydatawriter",
]


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
def setup_server_logging() -> None:
    """File-only logging — stdout must stay clean for MCP stdio transport."""
    APP_DIR.mkdir(parents=True, exist_ok=True)
    logger.remove()
    logger.add(
        APP_DIR / "server.log",
        rotation="10 MB",
        retention=3,
        format="{time:YYYY-MM-DD HH:mm:ss} | {level:<8} | {message}",
        level="INFO",
    )


def setup_cli_logging(verbose: bool = False) -> None:
    """stdout + file logging for CLI usage."""
    APP_DIR.mkdir(parents=True, exist_ok=True)
    level = "DEBUG" if verbose else "INFO"
    logger.remove()
    logger.add(
        sys.stdout,
        format="{time:YYYY-MM-DD HH:mm:ss} | {level:<8} | {message}",
        level=level,
    )
    logger.add(
        APP_DIR / "refresh.log",
        rotation="10 MB",
        retention=3,
        format="{time:YYYY-MM-DD HH:mm:ss} | {level:<8} | {message}",
        level=level,
    )


# ---------------------------------------------------------------------------
# Config I/O
# ---------------------------------------------------------------------------
def load_config() -> dict:
    """Load config.yaml from the app directory. Raises FileNotFoundError with guidance."""
    if not CONFIG_PATH.exists():
        raise FileNotFoundError(
            f"Config not found at {CONFIG_PATH}\n"
            f"Create it with the add_database tool, or copy config.example.yaml from the repo."
        )
    with CONFIG_PATH.open("r", encoding="utf-8") as fh:
        return yaml.safe_load(fh) or {}


def save_config(cfg: dict) -> None:
    """Write config dict back to config.yaml (overwrites; comments are not preserved)."""
    APP_DIR.mkdir(parents=True, exist_ok=True)
    with CONFIG_PATH.open("w", encoding="utf-8") as fh:
        yaml.safe_dump(
            cfg,
            fh,
            default_flow_style=False,
            sort_keys=False,
            allow_unicode=True,
        )


# ---------------------------------------------------------------------------
# Refresh state
# ---------------------------------------------------------------------------
def _load_state() -> dict:
    if not REFRESH_STATE_PATH.exists():
        return {}
    with REFRESH_STATE_PATH.open("r", encoding="utf-8") as fh:
        return json.load(fh)


def _save_state(state: dict) -> None:
    APP_DIR.mkdir(parents=True, exist_ok=True)
    with REFRESH_STATE_PATH.open("w", encoding="utf-8") as fh:
        json.dump(state, fh, indent=2)


def hours_since_refresh(source: str) -> Optional[float]:
    """Hours elapsed since the last successful refresh, or None if never refreshed."""
    ts = _load_state().get("sources", {}).get(source, {}).get("last_refresh")
    if not ts:
        return None
    last = datetime.fromisoformat(ts)
    if last.tzinfo is None:
        last = last.replace(tzinfo=timezone.utc)
    return (datetime.now(timezone.utc) - last).total_seconds() / 3600


def mark_refreshed(source: str) -> None:
    """Record current UTC time as the last refresh for *source*."""
    state = _load_state()
    state.setdefault("sources", {})[source] = {
        "last_refresh": datetime.now(timezone.utc).isoformat(),
    }
    _save_state(state)
