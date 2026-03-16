"""
DuckDB connection management.
Uses a single persistent connection per process (DuckDB is single-writer).
"""

import threading
from typing import Optional

import duckdb

from app.core.config import get_settings
from app.core.logging import get_logger

log = get_logger(__name__)
_lock = threading.Lock()
_connection: Optional[duckdb.DuckDBPyConnection] = None


def get_db_connection() -> duckdb.DuckDBPyConnection:
    global _connection
    if _connection is None:
        with _lock:
            if _connection is None:
                settings = get_settings()
                log.info("Initializing DuckDB", path=settings.duckdb_path)
                _connection = duckdb.connect(settings.duckdb_path)
                _connection.execute("PRAGMA threads=4")
                _connection.execute("PRAGMA memory_limit='2GB'")
                log.info("DuckDB connected", path=settings.duckdb_path)
    return _connection


def close_db_connection() -> None:
    global _connection
    if _connection is not None:
        _connection.close()
        _connection = None
        log.info("DuckDB connection closed")
