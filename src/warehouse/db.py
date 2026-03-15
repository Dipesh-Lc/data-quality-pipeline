# src/warehouse/db.py

"""
Database connection layer
=========================
Provides a thin SQLAlchemy wrapper around Postgres.

Configuration comes from environment variables (loaded from .env automatically):
  DB_URL                          - full connection string (takes priority)
  DB_HOST, DB_PORT, DB_NAME,      - individual parts (used when DB_URL is not set)
  DB_USER, DB_PASSWORD

Usage
-----
from src.warehouse.db import get_engine, execute_sql

engine = get_engine()
execute_sql("SELECT 1", engine)
"""
# src/warehouse/db.py

"""Database connection and query helpers for Postgres."""
from __future__ import annotations

import os
from contextlib import contextmanager
from functools import lru_cache

try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    pass

try:
    import sqlalchemy as sa
    from sqlalchemy import text
    from sqlalchemy.engine import Engine

    _SA_AVAILABLE = True
except ImportError:
    sa = None  # type: ignore[assignment]
    Engine = None  # type: ignore[assignment,misc]
    _SA_AVAILABLE = False

from src.utils.logger import get_logger

log = get_logger(__name__)


#  Connection factory
# ---------------------------------------------------------------------------------------

def _build_url() -> str:
    """Build the database connection URL from environment variables."""
    if url := os.getenv("DB_URL"):
        if url.startswith("postgres://"):
            url = url.replace("postgres://", "postgresql+psycopg2://", 1)
        elif url.startswith("postgresql://") and "+psycopg2" not in url:
            url = url.replace("postgresql://", "postgresql+psycopg2://", 1)
        return url

    host = os.getenv("DB_HOST", "localhost")
    port = os.getenv("DB_PORT", "5432")
    dbname = os.getenv("DB_NAME", "dqpipeline")
    user = os.getenv("DB_USER", "pipeline_user")
    password = os.getenv("DB_PASSWORD", "pipeline_pass")
    return f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}"


@lru_cache(maxsize=1)
def get_engine(pool_size: int = 5, echo: bool = False):
    """Create and cache the SQLAlchemy engine."""
    if not _SA_AVAILABLE:
        raise ImportError("sqlalchemy is not installed. Run: pip install sqlalchemy psycopg2-binary")

    url = _build_url()
    engine = sa.create_engine(
        url,
        pool_size=pool_size,
        max_overflow=10,
        pool_pre_ping=True,
        echo=echo,
    )
    log.info(
        "Database engine created  url=%s",
        sa.engine.url.make_url(url).render_as_string(hide_password=True),
    )
    return engine


#  SQL helpers
# ---------------------------------------------------------------------------------------

def ping(engine=None) -> bool:
    """Return True if the database is reachable."""
    if not _SA_AVAILABLE:
        return False

    engine = engine or get_engine()
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return True
    except Exception as exc:
        log.error("DB ping failed: %s", exc)
        return False


def execute_sql(sql: str, engine=None, params: dict | None = None) -> None:
    """Execute a SQL statement."""
    if not _SA_AVAILABLE:
        raise ImportError("sqlalchemy not available")

    engine = engine or get_engine()
    with engine.begin() as conn:
        conn.execute(text(sql), params or {})


def execute_sql_file(path: str, engine=None) -> None:
    """Execute SQL statements from a file."""
    if not _SA_AVAILABLE:
        raise ImportError("sqlalchemy not available")

    engine = engine or get_engine()
    with open(path) as fh:
        sql = fh.read()

    statements = [s.strip() for s in sql.split(";") if s.strip()]
    with engine.begin() as conn:
        for stmt in statements:
            conn.execute(text(stmt))
    log.info("Executed SQL file  path=%s  statements=%d", path, len(statements))


def query(sql: str, engine=None, params: dict | None = None) -> list[dict]:
    """Run a query and return rows as dictionaries."""
    if not _SA_AVAILABLE:
        raise ImportError("sqlalchemy not available")

    engine = engine or get_engine()
    with engine.connect() as conn:
        result = conn.execute(text(sql), params or {})
        return [dict(row._mapping) for row in result]


@contextmanager
def transaction(engine=None):
    """Provide a transactional database connection."""
    if not _SA_AVAILABLE:
        raise ImportError("sqlalchemy not available")

    engine = engine or get_engine()
    with engine.begin() as conn:
        yield conn