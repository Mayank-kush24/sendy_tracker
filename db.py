"""
MySQL database connection module using SQLAlchemy.

Credentials are loaded from environment variables (see .env.example).
Supports both local and remote (cPanel) MySQL connections.
"""

import os
import logging
from contextlib import contextmanager
from urllib.parse import quote_plus

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, event, text
from sqlalchemy.exc import OperationalError, SQLAlchemyError
from sqlalchemy.pool import QueuePool

load_dotenv()

logger = logging.getLogger(__name__)

_engine = None


def get_engine():
    """
    Return a module-level singleton SQLAlchemy engine configured from
    environment variables.

    Required env vars:
        DB_HOST       – server hostname or IP  (e.g. your-domain.com)
        DB_PORT       – MySQL port             (default 3306)
        DB_NAME       – database name          (e.g. cpaneluser_sendydb)
        DB_USER       – MySQL user             (e.g. cpaneluser_sendyuser)
        DB_PASSWORD   – MySQL password

    Optional env vars:
        DB_POOL_SIZE        – persistent connections in the pool     (default 5)
        DB_MAX_OVERFLOW     – extra connections above pool size      (default 10)
        DB_POOL_TIMEOUT     – seconds to wait for a free connection  (default 30)
        DB_POOL_RECYCLE     – seconds before recycling a connection  (default 1800)
        DB_CONNECT_TIMEOUT  – TCP connect timeout in seconds         (default 10)
        DB_SSL              – "true" to require SSL, "false" to skip (default false)
        DB_CHARSET          – connection charset                     (default utf8mb4)

    Raises:
        EnvironmentError: if any required env var is missing.
        sqlalchemy.exc.OperationalError: if the database is unreachable.
    """
    global _engine
    if _engine is not None:
        return _engine

    required = ("DB_HOST", "DB_PORT", "DB_NAME", "DB_USER", "DB_PASSWORD")
    missing = [v for v in required if not os.getenv(v)]
    if missing:
        raise EnvironmentError(
            f"Missing required environment variable(s): {', '.join(missing)}"
        )

    host     = os.environ["DB_HOST"]
    port     = os.environ["DB_PORT"]
    name     = os.environ["DB_NAME"]
    user     = os.environ["DB_USER"]
    password = os.environ["DB_PASSWORD"]
    charset  = os.getenv("DB_CHARSET", "utf8mb4")

    pool_size       = int(os.getenv("DB_POOL_SIZE",       "5"))
    max_overflow    = int(os.getenv("DB_MAX_OVERFLOW",    "10"))
    pool_timeout    = int(os.getenv("DB_POOL_TIMEOUT",    "30"))
    pool_recycle    = int(os.getenv("DB_POOL_RECYCLE",    "1800"))
    connect_timeout = int(os.getenv("DB_CONNECT_TIMEOUT", "10"))
    read_timeout    = int(os.getenv("DB_READ_TIMEOUT", "180"))
    write_timeout   = int(os.getenv("DB_WRITE_TIMEOUT", "180"))
    use_ssl         = os.getenv("DB_SSL", "false").lower() == "true"

    # URL-encode password to handle special characters (@, #, !, etc.)
    safe_password = quote_plus(password)
    url = (
        f"mysql+pymysql://{user}:{safe_password}"
        f"@{host}:{port}/{name}?charset={charset}"
    )

    # PyMySQL connect_args for remote connections (read/write timeout avoids
    # indefinite hangs on large queries; increase DB_READ_TIMEOUT if needed).
    connect_args: dict = {
        "connect_timeout": connect_timeout,
        "read_timeout":    read_timeout,
        "write_timeout":   write_timeout,
    }
    if use_ssl:
        # Require SSL but accept the server's self-signed cert (common on cPanel)
        connect_args["ssl"] = {"ssl_disabled": False}
        logger.info("DB: SSL enabled")
    else:
        connect_args["ssl_disabled"] = True

    try:
        engine = create_engine(
            url,
            poolclass=QueuePool,
            pool_size=pool_size,
            max_overflow=max_overflow,
            pool_timeout=pool_timeout,
            pool_recycle=pool_recycle,
            pool_pre_ping=True,        # silently drops stale remote connections
            connect_args=connect_args,
        )

        @event.listens_for(engine, "connect")
        def set_mysql_session_opts(dbapi_conn, _):
            cursor = dbapi_conn.cursor()
            try:
                cursor.execute("SET SESSION max_execution_time = 25000")
                cursor.execute("SET SESSION net_read_timeout = 60")
                cursor.execute("SET SESSION net_write_timeout = 60")
            finally:
                cursor.close()

        # Verify connectivity eagerly so errors surface here, not later.
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))

        logger.info(
            "DB engine ready — host=%s  db=%s  ssl=%s",
            host, name, use_ssl,
        )
        _engine = engine
        return _engine

    except OperationalError as exc:
        raise OperationalError(
            statement=None,
            params=None,
            orig=exc.orig,
        ) from exc


@contextmanager
def _get_connection():
    """Yield a connection and guarantee it is closed afterwards."""
    engine = get_engine()
    conn = engine.connect()
    try:
        yield conn
    finally:
        conn.close()


def fetch_data(query: str, params: dict | None = None) -> pd.DataFrame:
    """
    Execute *query* and return the result as a pandas DataFrame.

    Args:
        query:  Raw SQL string (use ':name' placeholders for parameters).
        params: Optional dict of bind parameters, e.g. ``{"id": 42}``.

    Returns:
        A ``pandas.DataFrame`` containing the query result.

    Raises:
        EnvironmentError:            if env vars are missing (from get_engine).
        sqlalchemy.exc.OperationalError: on connection failure.
        sqlalchemy.exc.SQLAlchemyError: on any other database error.
        ValueError:                  if *query* is empty.

    Example::

        df = fetch_data("SELECT * FROM campaigns WHERE id = :id", {"id": 7})
    """
    if not query or not query.strip():
        raise ValueError("query must be a non-empty string")

    try:
        with _get_connection() as conn:
            df = pd.read_sql(text(query), conn, params=params or {})
        logger.debug("fetch_data returned %d row(s)", len(df))
        return df

    except OperationalError as exc:
        logger.error("Connection error during fetch_data: %s", exc)
        raise
    except SQLAlchemyError as exc:
        logger.error("Database error during fetch_data: %s", exc)
        raise
