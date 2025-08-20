# bot/data_cache.py
from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

from .templates import METRIC_COLS
from .trino_client import query_df

load_dotenv()
logger = logging.getLogger(__name__)

TRINO_TABLE = os.getenv("RECORDS_TABLE", "iceberg.dbt_model.concat_record")
DATA_DIR = Path(os.getenv("DATA_DIR", "data"))
DATA_DIR.mkdir(parents=True, exist_ok=True)
PARQUET_FILE = DATA_DIR / "concat_record.parquet"
STALE_AFTER = timedelta(hours=int(os.getenv("STALE_HOURS", "6")))

ALL_COLUMNS: list[str] = ["source_nickname"] + METRIC_COLS + [f"{m}_meta" for m in METRIC_COLS]

def fetch_and_cache() -> pd.DataFrame:
    sql = f"SELECT {', '.join(ALL_COLUMNS)} FROM {TRINO_TABLE}"
    logger.info("SQL: %s", sql)
    df = query_df(sql)
    df = df.loc[:, ~df.columns.duplicated()]
    df.to_parquet(PARQUET_FILE, engine="pyarrow", index=False)
    logger.info("Saved %d rows to %s", len(df), PARQUET_FILE)
    return df

def load_data(force: bool = False) -> pd.DataFrame:
    if force or not PARQUET_FILE.exists():
        return fetch_and_cache()
    mtime = datetime.utcfromtimestamp(PARQUET_FILE.stat().st_mtime)
    if datetime.utcnow() - mtime > STALE_AFTER:
        # не блокируем старт бота — обновим кэш в фоне
        try:
            import asyncio
            asyncio.create_task(asyncio.to_thread(fetch_and_cache))
        except Exception:
            pass
    return pd.read_parquet(PARQUET_FILE, engine="pyarrow")
