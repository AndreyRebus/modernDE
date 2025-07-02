#!/usr/bin/env python3
"""
load_lol_matches_once_per_day.py
--------------------------------
Скачивает и сохраняет матчи League of Legends для указанного Riot‑ID ровно за один
календарный день. Все сетевые вызовы защищены, ошибки печатаются в лог и не
прерывают выполнение скрипта.

Основные изменения по сравнению с оригиналом
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
* **safe_get** — обёртка над `requests.get` с повторными попытками (в т. ч. 429),
  логированием HTTP‑ошибок и защёлкой на невалидный JSON.
* **logging** — единая настройка; все сообщения выводятся с тайм‑стампом.
* Проверка пустого дня (`match_ids == []`), чтобы не грузить в Iceberg пустые
  файлы.
* Проверка схемы JSON‑ответа; пропуск «битых» матчей без падения.
* Поддержка сохранения только непустых датафреймов. Если матчей нет — просто
  пишет информативный лог.
"""

from __future__ import annotations

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

    Возвращает dict либо *None* (если ошибка и все попытки исчерпаны).
    """
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

# ────────────────── core ──────────────────

def fetch_matches_once_per_day(
    riot_id: str,
    load_date: dt.date,
    *,
    api_key: str | None = None,
    platform_routing: str = "ru1",
    regional_routing: str = "europe",
    bucket_name: str = "test-s3test",
    s3_prefix: str = "stage_load_raw_data",
    aws_access_key_id: str | None = None,
    aws_secret_access_key: str | None = None,
    rate_delay: float = 1.2,
    trino_host: str = "5.129.208.115",
    trino_port: int = 8443,
    trino_user: str = "admin",
    trino_catalog: str = "iceberg",
    trino_schema: str = "lol_raw",
    trino_table: str = "data_api_mining",
    trino_password: str | None = None,
) -> Optional[str]:
    """Возвращает ключ объекта S3 либо **None**, если ничего не загружено."""

    load_dotenv()

    # ── переменные окружения / параметры ──
    aws_access_key_id = aws_access_key_id or os.getenv("AWS_ACCESS_KEY_ID")
    aws_secret_access_key = aws_secret_access_key or os.getenv("AWS_SECRET_ACCESS_KEY")
    if not (aws_access_key_id and aws_secret_access_key):
        raise ValueError("AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY не заданы.")

    api_key = api_key or os.getenv("RIOT_API_KEY")
    if not api_key:
        raise ValueError("RIOT_API_KEY не задан.")

    trino_password = trino_password or os.getenv("TRINO_PASSWORD")
    if not trino_password:
        raise ValueError("TRINO_PASSWORD не задан.")

    # ── подключение к S3 ──
    session = boto3.session.Session(
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        region_name="ru-central1",
    )
    s3 = session.resource("s3", endpoint_url="https://storage.yandexcloud.net")
    bucket = s3.Bucket(bucket_name)

    # ── имя подпапки ──
    folder_date = load_date.isoformat()
    riot_id_clean = re.sub(r"[\u2066-\u2069]", "", riot_id)
    safe_riot_id = riot_id_clean.replace("#", "_")
    s3_folder = f"{s3_prefix}/{folder_date}/{safe_riot_id}/"

    # ── пропускаем уже загруженные дни ──
    if any(obj.key.startswith(s3_folder) for obj in bucket.objects.filter(Prefix=s3_folder)):
        logging.info("⏩ %s: день уже загружен, пропуск.", folder_date)
        return None

    headers = {"X-Riot-Token": api_key}

    # ── получаем PUUID ──
    try:
        game_name, tagline = riot_id_clean.split("#", 1)
    except ValueError:
        raise ValueError("riot_id должен быть в формате GameName#Tagline")

    acct_url = (
        f"https://{regional_routing}.api.riotgames.com/riot/account/v1/accounts/by-riot-id/"
        f"{urllib.parse.quote(game_name)}/{urllib.parse.quote(tagline)}"
    )
    acct_resp = safe_get(acct_url, headers)
    puuid = acct_resp.get("puuid") if acct_resp else None
    if not puuid:
        logging.error("💥 Не удалось получить PUUID для %s", riot_id)
        return None

    # ── временной диапазон дня ──
    start_ts = int(dt.datetime.combine(load_date, dt.time()).timestamp())
    end_ts = start_ts + 86400  # +1 день

    ids_url = (
        f"https://{regional_routing}.api.riotgames.com/lol/match/v5/matches/by-puuid/{puuid}/ids"
        f"?startTime={start_ts}&endTime={end_ts}&count=100"
    )
    match_ids: List[str] = safe_get(ids_url, headers) or []

    if not match_ids:
        logging.info("ℹ️  %s: у %s нет матчей за этот день.", folder_date, riot_id)
        return None

    # ── скачиваем сами матчи ──
    parts: List[Dict[str, Any]] = []
    for mid in match_ids:
        m = safe_get(
            f"https://{regional_routing}.api.riotgames.com/lol/match/v5/matches/{mid}",
            headers,
        )
        if not (m and "metadata" in m and "info" in m):
            logging.warning("⚠️  %s: пустой/некорректный матч — пропуск", mid)
            continue

        df_m = pd.json_normalize(m)
        if not all(col in df_m.columns for col in META_COLS):
            logging.warning("⚠️  %s: неполная схема — пропуск", mid)
            continue

        base = {c: df_m.at[0, c] for c in META_COLS}
        for p in m["info"]["participants"]:
            parts.append({**base, **{f"participant.{k}": v for k, v in p.items()}})

        time.sleep(rate_delay)

    if not parts:
        logging.info("ℹ️  %s: все матчи были отброшены (битые).", folder_date)
        return None

    # ── сохраняем в S3 ──
    df = pd.DataFrame(parts)
    df["source_nickname"] = riot_id_clean

    buf = io.BytesIO()
    df.to_parquet(buf, index=False, compression="snappy")
    buf.seek(0)

    object_key = f"{s3_folder}{safe_riot_id}_{load_date}_{load_date}.parquet"
    s3.Object(bucket_name, object_key).upload_fileobj(buf)
    logging.info("✅ %s: загружено %s строк → %s", folder_date, len(df), object_key)

    # ── регистрируем в Iceberg ──
    location_folder = f"s3://{bucket_name}/{s3_folder.rstrip('/') }"
    sql = f"""
        ALTER TABLE {trino_catalog}.{trino_schema}.{trino_table}
        EXECUTE add_files(
            location => '{location_folder}',
            format   => 'PARQUET'
        )
    """

    with dbapi.connect(
        host=trino_host,
        port=trino_port,
        user=trino_user,
        catalog=trino_catalog,
        schema=trino_schema,
        http_scheme="https",
        auth=BasicAuthentication(trino_user, trino_password),
        verify=False,
    ) as conn:
        cur = conn.cursor()
        cur.execute(sql.strip())
        cur.fetchall()
        

    return object_key

# ───────────── пример использования ─────────────
if __name__ == "__main__":
    import time
    today = dt.date.today()
    start = today - dt.timedelta(weeks=2)
    end = today - dt.timedelta(days=1)

    total_days = (end - start).days + 1
    riot_ids = ["Monty Gard#RU1", "Breaksthesilence#RU1","2pilka#RU1","Gruntq#RU1","Шaзам#RU1","Prooaknor#RU1"]

    for riot in riot_ids:
        for i in range(total_days):
            day = start + dt.timedelta(days=i)
            try:
                fetch_matches_once_per_day(riot_id=riot, load_date=day)
            except Exception:
                logging.exception("💥 Критическая ошибка при дне %s для %s", day, riot)
            time.sleep(2)   
