"""
trino_client.py
~~~~~~~~~~~~~~~
Единая точка работы с Trino.  
Скрывает детали аутентификации и TLS-настроек.
"""

from __future__ import annotations
import os
from contextlib import contextmanager

from dotenv import load_dotenv
from trino import dbapi
from trino.auth import BasicAuthentication
import urllib3

__all__ = ["get_connection", "query_df"]

import pandas as pd

# ────────────────── init ──────────────────
load_dotenv()
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

_TRINO_HOST = os.getenv("TRINO_HOST", "5.129.208.115")
_TRINO_PORT = int(os.getenv("TRINO_PORT", 8443))
_TRINO_USER = os.getenv("TRINO_USER", "admin")
_TRINO_PASSWORD = os.getenv("TRINO_PASSWORD", "").strip()
_TRINO_CATALOG = os.getenv("TRINO_CATALOG", "iceberg")
_TRINO_SCHEMA = os.getenv("TRINO_SCHEMA", "dbt_model")

if not _TRINO_PASSWORD:
    raise RuntimeError("TRINO_PASSWORD не задан (export или .env)")

# ────────────────── helpers ──────────────────
def _connect() -> dbapi.Connection:
    """Создаёт и возвращает подключение к Trino (без fetch’а)."""
    return dbapi.connect(
        host=_TRINO_HOST,
        port=_TRINO_PORT,
        user=_TRINO_USER,
        catalog=_TRINO_CATALOG,
        schema=_TRINO_SCHEMA,
        http_scheme="https",
        auth=BasicAuthentication(_TRINO_USER, _TRINO_PASSWORD),
        verify=False,  # отключаем проверку сертификата
    )

@contextmanager
def get_connection():
    """Контекст-менеджер для безопасного использования подключения."""
    conn = _connect()
    try:
        yield conn
    finally:
        conn.close()

def query_df(sql: str) -> pd.DataFrame:
    """Выполняет запрос и сразу отдаёт результат в виде DataFrame."""
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        cols = [d[0] for d in cur.description]
    return pd.DataFrame(rows, columns=cols)
