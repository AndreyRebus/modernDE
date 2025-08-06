from trino_client import query_df
from .config import logger

def fetch_columns(columns: list[str], table: str):
    sql = f"SELECT {', '.join(columns)} FROM {table}"
    logger.info("SQL: %s", sql)
    return query_df(sql)