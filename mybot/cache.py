from __future__ import annotations
import asyncio
from datetime import datetime
import pandas as pd

from .config import PARQUET_FILE, TRINO_TABLE, STALE_AFTER, logger
from .db import fetch_columns
from .templates import METRIC_COLS

ALL_COLUMNS = ["source_nickname"] + METRIC_COLS + [f"{m}_meta" for m in METRIC_COLS]

def fetch_and_cache() -> pd.DataFrame:
    df = fetch_columns(ALL_COLUMNS, TRINO_TABLE)
    df = df.loc[:, ~df.columns.duplicated()]
    df.to_parquet(PARQUET_FILE, engine="pyarrow", index=False)
    logger.info("Saved %d rows", len(df))
    return df

def load_data(force: bool = False) -> pd.DataFrame:
    if force or not PARQUET_FILE.exists():
        return fetch_and_cache()
    mtime = datetime.utcfromtimestamp(PARQUET_FILE.stat().st_mtime)
    if datetime.utcnow() - mtime > STALE_AFTER:
        asyncio.create_task(fetch_and_cache())
    return pd.read_parquet(PARQUET_FILE, engine="pyarrow")