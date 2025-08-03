#!/usr/bin/env python3

import io
import logging
import os
import re
import time
import datetime as dt
import urllib.parse
from typing import Any, Dict, List, Optional

import boto3
import pandas as pd
import requests
import urllib3
from dotenv import load_dotenv
from trino import dbapi
from trino.auth import BasicAuthentication

# ───────────── настройка логирования ─────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
)

# Отключаем InsecureRequestWarning для self‑signed TLS
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ── Загрузка и проверка переменных окружения ──
load_dotenv()

required_env_vars = [
    "RIOT_API_KEY",
    "AWS_ACCESS_KEY_ID",
    "AWS_SECRET_ACCESS_KEY",
    "S3_BUCKET_NAME",
    "TRINO_HOST",
    "TRINO_PORT",
    "TRINO_USER",
    "TRINO_PASSWORD",
    "TRINO_CATALOG",
    "TRINO_SCHEMA",
    "TRINO_TABLE",
]
missing = [var for var in required_env_vars if not os.getenv(var)]
if missing:
    raise EnvironmentError(
        f"Missing required environment variables: {', '.join(missing)}"
    )

# Riot API
RIOT_API_KEY = os.environ["RIOT_API_KEY"]

# AWS S3
AWS_ACCESS_KEY_ID = os.environ["AWS_ACCESS_KEY_ID"]
AWS_SECRET_ACCESS_KEY = os.environ["AWS_SECRET_ACCESS_KEY"]
S3_BUCKET_NAME = os.environ["S3_BUCKET_NAME"]
S3_PREFIX = os.getenv("S3_PREFIX", "stage_load_raw_data")

# Trino
TRINO_HOST = os.environ["TRINO_HOST"]
TRINO_PORT = int(os.environ["TRINO_PORT"])
TRINO_USER = os.environ["TRINO_USER"]
TRINO_PASSWORD = os.environ["TRINO_PASSWORD"]
TRINO_CATALOG = os.environ["TRINO_CATALOG"]
TRINO_SCHEMA = os.environ["TRINO_SCHEMA"]
TRINO_TABLE = os.environ["TRINO_TABLE"]

# Riot routing defaults
PLATFORM_ROUTING = os.getenv("PLATFORM_ROUTING", "ru1")
REGIONAL_ROUTING = os.getenv("REGIONAL_ROUTING", "europe")

META_COLS: List[str] = [
    "metadata.matchId",
    "info.gameCreation",
    "info.gameDuration",
    "info.gameMode",
    "info.queueId",
    "info.gameVersion",
]

# ────────────────── helpers ──────────────────

def safe_get(
    url: str,
    headers: Dict[str, str],
    *,
    max_retries: int = 3,
    backoff: float = 0.5,
) -> Optional[Dict[str, Any]]:
    """GET с JSON‑ответом и автоматическим повтором при 429 / 5xx.
    Возвращает dict либо None после исчерпания попыток."""
    for attempt in range(max_retries):
        try:
            r = requests.get(url, headers=headers, timeout=10)
        except requests.RequestException as exc:
            logging.error("💥 %s — network error: %s", url, exc)
            return None
        if r.status_code == 200:
            try:
                return r.json()
            except ValueError:
                logging.error("💥 %s — invalid JSON: %.120s", url, r.text)
                return None
        if r.status_code == 429 and attempt < max_retries - 1:
            retry_after = int(r.headers.get("Retry-After", 1))
            time.sleep(retry_after + backoff)
            continue
        logging.error("💥 %s — HTTP %s: %.120s", url, r.status_code, r.text)
        return None
    return None


def register_partition(location: str) -> None:
    """Регистрирует партицию Iceberg для заданного S3(location)"""
    sql = f"""
        ALTER TABLE {TRINO_CATALOG}.{TRINO_SCHEMA}.{TRINO_TABLE}
        EXECUTE add_files(
            location => '{location}',
            format   => 'PARQUET'
        )
    """
    try:
        with dbapi.connect(
            host=TRINO_HOST,
            port=TRINO_PORT,
            user=TRINO_USER,
            catalog=TRINO_CATALOG,
            schema=TRINO_SCHEMA,
            http_scheme="https",
            auth=BasicAuthentication(TRINO_USER, TRINO_PASSWORD),
            verify=False,
        ) as conn:
            cur = conn.cursor()
            cur.execute(sql.strip())
            cur.fetchall()
    except Exception as exc:
        msg = str(exc)
        if "File already exists" in msg or "already registered" in msg:
            logging.info("ℹ️  Partition already registered")
        else:
            logging.exception("💥 Failed to register partition at %s: %s", location, exc)
# ────────────────── core ──────────────────

def fetch_matches_once_per_day(
    riot_id: str,
    load_date: dt.date,
    *,
    rate_delay: float = 1.2,
) -> Optional[str]:
    """Возвращает ключ S3 либо None."""

    # S3 session
    session = boto3.session.Session(
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name="ru-central1",
    )
    s3 = session.resource("s3", endpoint_url="https://storage.yandexcloud.net")
    bucket = s3.Bucket(S3_BUCKET_NAME)

    folder_date = load_date.isoformat()
    riot_id_clean = re.sub(r"[\u2066-\u2069]", "", riot_id)
    safe_riot_id = riot_id_clean.replace("#", "_")
    s3_folder = f"{S3_PREFIX}/{folder_date}/{safe_riot_id}/"

    # Если найдены существующие parquet-файлы — регистрируем их и выходим
    existing = [obj.key for obj in bucket.objects.filter(Prefix=s3_folder) if obj.key.endswith('.parquet')]
    if existing:
        logging.info("🔁 %s: found existing parquet files: %s", folder_date, existing)
        location = f"s3://{S3_BUCKET_NAME}/{s3_folder.rstrip('/')}"
        register_partition(location)
        return existing[0]

    headers = {"X-Riot-Token": RIOT_API_KEY}

    # Получаем PUUID
    try:
        game_name, tagline = riot_id_clean.split("#", 1)
    except ValueError:
        raise ValueError("riot_id must be in format GameName#Tagline")
    acct_url = (
        f"https://{REGIONAL_ROUTING}.api.riotgames.com/riot/account/v1/accounts/by-riot-id/"
        f"{urllib.parse.quote(game_name)}/{urllib.parse.quote(tagline)}"
    )
    acct_resp = safe_get(acct_url, headers)
    puuid = acct_resp.get("puuid") if acct_resp else None
    if not puuid:
        logging.error("💥 Failed to get PUUID for %s", riot_id)
        return None

    # Временной диапазон дня
    start_ts = int(dt.datetime.combine(load_date, dt.time()).timestamp())
    end_ts = start_ts + 86400
    ids_url = (
        f"https://{REGIONAL_ROUTING}.api.riotgames.com/lol/match/v5/matches/by-puuid/{puuid}/ids"
        f"?startTime={start_ts}&endTime={end_ts}&count=100"
    )
    match_ids: List[str] = safe_get(ids_url, headers) or []
    if not match_ids:
        logging.info("ℹ️  %s: no matches for %s.", folder_date, riot_id)
        return None

    parts: List[Dict[str, Any]] = []
    for mid in match_ids:
        m = safe_get(
            f"https://{REGIONAL_ROUTING}.api.riotgames.com/lol/match/v5/matches/{mid}",
            headers,
        )
        if not (m and "metadata" in m and "info" in m):
            logging.warning("⚠️ %s: empty/bad match — skip", mid)
            continue
        df_m = pd.json_normalize(m)
        if not all(col in df_m.columns for col in META_COLS):
            logging.warning("⚠️ %s: incomplete schema — skip", mid)
            continue
        base = {c: df_m.at[0, c] for c in META_COLS}
        for p in m["info"]["participants"]:
            parts.append({**base, **{f"participant.{k}": v for k, v in p.items()}})
        time.sleep(rate_delay)
    if not parts:
        logging.info("ℹ️  %s: all matches discarded.", folder_date)
        return None

    # Сохраняем и загружаем новый parquet
    df = pd.DataFrame(parts)
    df["source_nickname"] = riot_id_clean
    buf = io.BytesIO()
    df.to_parquet(buf, index=False, compression="snappy")
    buf.seek(0)
    object_key = f"{s3_folder}{safe_riot_id}_{load_date}_{load_date}.parquet"
    s3.Object(S3_BUCKET_NAME, object_key).upload_fileobj(buf)
    logging.info("✅ %s: uploaded %s rows → %s", folder_date, len(df), object_key)

    # Регистрируем новую партицию
    location = f"s3://{S3_BUCKET_NAME}/{s3_folder.rstrip('/')}"
    register_partition(location)

    return object_key

# ───────────── пример использования ─────────────
if __name__ == "__main__":
    today = dt.date.today()
    start = today - dt.timedelta(weeks=1)
    end = today - dt.timedelta(days=1)
    total_days = (end - start).days + 1
    riot_ids = [
        "Monty Gard#RU1",
        "Breaksthesilence#RU1",
        "2pilka#RU1",
        "Gruntq#RU1",
        "Шaзам#RU1",
        "Prooaknor#RU1",
    ]
    for riot in riot_ids:
        for i in range(total_days):
            day = start + dt.timedelta(days=i)
            try:
                fetch_matches_once_per_day(riot_id=riot, load_date=day)
            except Exception:
                logging.exception("💥 Critical error on %s for %s", day, riot)
            time.sleep(2)
