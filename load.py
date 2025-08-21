#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import re
import time
import logging
import datetime as dt
import urllib.parse
from typing import Any, Dict, List, Optional, Tuple, Set


import pandas as pd
import requests
import urllib3
from dotenv import load_dotenv

import pyarrow as pa
from pyiceberg.catalog import load_catalog
from pyiceberg.table import Table
from pyiceberg.types import (
    BooleanType, IntegerType, LongType, DoubleType, FloatType, DateType, TimestampType,
    StringType, BinaryType, DecimalType, MapType, ListType, StructType, NestedField
)
from pyiceberg.expressions import And, EqualTo
# ───────────── логирование ─────────────
logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)-8s  %(message)s")
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

load_dotenv()

# ───────────── ENV ─────────────
REQ = [
    "POSTGRES_USER", "POSTGRES_PASSWORD", "POSTGRES_DB",
    "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY",
    "TRINO_SCHEMA", "TRINO_TABLE",
    "RIOT_API_KEY",
]
missing = [v for v in REQ if not os.getenv(v)]
if missing:
    raise EnvironmentError(f"Missing env vars: {', '.join(missing)}")

PG_HOST = os.getenv("POSTGRES_HOST", "postgres")
PG_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
PG_DB   = os.environ["POSTGRES_DB"]
PG_USER = os.environ["POSTGRES_USER"]
PG_PASS = os.environ["POSTGRES_PASSWORD"]

AWS_ACCESS_KEY_ID     = os.environ["AWS_ACCESS_KEY_ID"]
AWS_SECRET_ACCESS_KEY = os.environ["AWS_SECRET_ACCESS_KEY"]
S3_ENDPOINT = os.getenv("S3_ENDPOINT", "https://storage.yandexcloud.net")
S3_REGION   = os.getenv("S3_REGION", "ru-central1")

SCHEMA_NAME = os.environ["TRINO_SCHEMA"]
TABLE_NAME  = os.environ["TRINO_TABLE"]
TABLE_ID    = f"{SCHEMA_NAME}.{TABLE_NAME}"

RIOT_API_KEY = os.environ["RIOT_API_KEY"]
PLATFORM_ROUTING = os.getenv("PLATFORM_ROUTING", "ru1")
REGIONAL_ROUTING = os.getenv("REGIONAL_ROUTING", "europe")

# Верхнеуровневые поля матча
META_COLS = [
    "metadata.matchId",
    "info.gameCreation",
    "info.gameDuration",
    "info.gameMode",
    "info.queueId",
    "info.gameVersion",
]

# ───────────── каталог/таблица ─────────────
def open_catalog() -> Table:
    uri = f"postgresql+psycopg2://{PG_USER}:{PG_PASS}@{PG_HOST}:{PG_PORT}/{PG_DB}"
    # ДОЛЖНО совпадать с iceberg.jdbc-catalog.catalog-name у Trino
    catalog_name = os.getenv("ICEBERG_CATALOG_NAME", os.getenv("TRINO_CATALOG", "iceberg"))
    return load_catalog(
        catalog_name,
        **{
            "type": "sql",
            "uri": uri,
            "s3.endpoint": S3_ENDPOINT,
            "s3.region": S3_REGION,
            "s3.access-key-id": AWS_ACCESS_KEY_ID,
            "s3.secret-access-key": AWS_SECRET_ACCESS_KEY,
        },
    )

def load_table() -> Table:
    cat = open_catalog()
    tbl = cat.load_table(TABLE_ID)
    logging.info("✅ Loaded Iceberg table: %s", TABLE_ID)
    return tbl

# ───────────── Iceberg → Arrow ─────────────
def iceberg_to_arrow_type(t) -> pa.DataType:
    if isinstance(t, BooleanType):   return pa.bool_()
    if isinstance(t, IntegerType):   return pa.int32()
    if isinstance(t, LongType):      return pa.int64()
    if isinstance(t, FloatType):     return pa.float32()
    if isinstance(t, DoubleType):    return pa.float64()
    if isinstance(t, DateType):      return pa.date32()
    if isinstance(t, TimestampType): return pa.timestamp("us")
    if isinstance(t, StringType):    return pa.string()
    if isinstance(t, BinaryType):    return pa.binary()
    if isinstance(t, DecimalType):   return pa.decimal128(t.precision, t.scale)
    if isinstance(t, MapType):       return pa.map_(iceberg_to_arrow_type(t.key_type), iceberg_to_arrow_type(t.value_type))
    if isinstance(t, ListType):      return pa.list_(iceberg_to_arrow_type(t.element_type))
    if isinstance(t, StructType):    return pa.struct([pa.field(f.name, iceberg_to_arrow_type(f.field_type)) for f in t.fields])
    raise TypeError(f"Unsupported Iceberg type: {t}")

def table_arrow_schema(tbl: Table) -> pa.Schema:
    return pa.schema([
        pa.field(f.name, iceberg_to_arrow_type(f.field_type), nullable=f.optional)
        for f in tbl.schema().fields
    ])


# ───────────── утилиты ─────────────
def lower_dotted(name: str) -> str:
    if name in ("source_nickname", "event_date"):
        return name
    if "." in name:
        a, b = name.split(".", 1)
        return f"{a.lower()}.{b.lower()}"
    return name.lower()

def camel_to_flat_lower(s: str) -> str:
    # "timeCCingOthers" -> "timeccingothers"; "summoner1Id" -> "summoner1id"
    return re.sub(r'[^A-Za-z0-9]', '', s).lower()

def participant_api_key_to_col(k: str) -> str:
    return f"participant.{camel_to_flat_lower(k)}"

def cast_map_number_keep_keys(d: Dict[str, Any], *, context: str = "") -> Dict[str, float]:
    """Никакие ключи не дропаем — все приводим к числам по фиксированному правилу."""
    if not isinstance(d, dict):
        raise TypeError(f"{context}: expected dict, got {type(d).__name__}")
    out: Dict[str, float] = {}
    for k, v in d.items():
        try:
            if isinstance(v, bool):
                out[str(k)] = float(int(v))
            elif isinstance(v, (int, float)):
                out[str(k)] = float(v)
            elif isinstance(v, str):
                try:
                    out[str(k)] = float(v)
                except Exception:
                    out[str(k)] = float(len(v))
            elif isinstance(v, (list, tuple, set)):
                out[str(k)] = float(len(v))
            elif isinstance(v, dict):
                out[str(k)] = float(len(v))
            else:
                out[str(k)] = float(len(str(v)))
        except Exception as e:
            logging.warning("%s: failed to cast key '%s' of type %s (%s) → use 0.0",
                            context, k, type(v).__name__, e)
            out[str(k)] = 0.0
    return out

def normalize_perks(perks: Dict[str, Any]) -> Dict[str, Any]:
    stat = perks["statPerks"]
    styles = perks["styles"]
    norm_styles = []
    for s in styles:
        sel = s["selections"]
        norm_sel = []
        for it in sel:
            norm_sel.append({
                "perk": int(it["perk"]),
                "var1": int(it["var1"]),
                "var2": int(it["var2"]),
                "var3": int(it["var3"]),
            })
        norm_styles.append({
            "style": int(s["style"]),
            "description": s.get("description"),
            "selections": norm_sel,
        })
    return {
        "statperks": {"offense": int(stat["offense"]), "flex": int(stat["flex"]), "defense": int(stat["defense"])},
        "styles": norm_styles,
    }

def split_required_optional(tbl: Table) -> Tuple[List[str], List[str]]:
    required, optional = [], []
    for f in tbl.schema().fields:
        (optional if f.optional else required).append(f.name)
    return required, optional

# ───────────── HTTP ─────────────
def safe_get(url: str, headers: Dict[str, str], *, max_retries: int = 3, backoff: float = 0.7) -> Optional[Dict[str, Any]]:
    for attempt in range(max_retries):
        try:
            r = requests.get(url, headers=headers, timeout=15, verify=False)
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

# ───────────── извлечение из Riot → строгие строки ─────────────
def build_rows_for_day(riot_id: str, day: dt.date) -> List[Dict[str, Any]]:
    headers = {"X-Riot-Token": RIOT_API_KEY}
    riot_id_clean = re.sub(r"[\u2066-\u2069]", "", riot_id)

    game_name, tagline = riot_id_clean.split("#", 1)

    acct_url = (
        f"https://{REGIONAL_ROUTING}.api.riotgames.com/riot/account/v1/accounts/by-riot-id/"
        f"{urllib.parse.quote(game_name)}/{urllib.parse.quote(tagline)}"
    )
    acct = safe_get(acct_url, headers) or {}
    puuid = acct["puuid"]  # fail-fast

    start_ts = int(dt.datetime.combine(day, dt.time()).timestamp())
    end_ts   = start_ts + 86400
    ids_url = (
        f"https://{REGIONAL_ROUTING}.api.riotgames.com/lol/match/v5/matches/by-puuid/{puuid}/ids"
        f"?startTime={start_ts}&endTime={end_ts}&count=100"
    )
    match_ids: List[str] = safe_get(ids_url, headers) or []

    logging.info("🎯 [%s] [%s] Riot: %d matches", riot_id_clean, day.isoformat(), len(match_ids))

    rows: List[Dict[str, Any]] = []
    for mid in match_ids:
        m = safe_get(f"https://{REGIONAL_ROUTING}.api.riotgames.com/lol/match/v5/matches/{mid}", headers) or {}

        base_src = pd.json_normalize(m)
        base = {lower_dotted(c): base_src.at[0, c] for c in META_COLS}
        if len(base) != len(META_COLS):
            missing = [lower_dotted(c) for c in META_COLS if c not in base_src.columns]
            raise RuntimeError(f"Missing META columns from API: {missing}")

        for p in m["info"]["participants"]:
            row: Dict[str, Any] = {}
            row.update(base)

            for k, v in p.items():
                col = participant_api_key_to_col(k)
                if col == "participant.challenges":
                    row[col] = cast_map_number_keep_keys(v, context=f"match={mid} participant.challenges")
                elif col == "participant.missions":
                    tmp = cast_map_number_keep_keys(v, context=f"match={mid} participant.missions")
                    row[col] = {kk: int(vv) for kk, vv in tmp.items()}
                elif col == "participant.perks":
                    row[col] = normalize_perks(v)
                else:
                    row[col] = v

            row["source_nickname"] = riot_id_clean
            row["event_date"] = day
            rows.append(row)

    logging.info("🧱 [%s] [%s] Built rows: %d", riot_id_clean, day.isoformat(), len(rows))
    return rows

# ───────────── чтение ключей → антидубликат ─────────────
def fetch_existing_keys(tbl: Table, nick: str, day: dt.date) -> Set[Tuple[str, str]]:
    # Фильтр по (event_date, source_nickname)
    expr = And(EqualTo("event_date", day), EqualTo("source_nickname", nick))

    # ⬇️ правильные аргументы для твоей версии PyIceberg
    scan = tbl.scan(
        row_filter=expr,
        selected_fields=["metadata.matchid", "participant.puuid"],
    )

    existing: Set[Tuple[str, str]] = set()

    try:
        # быстрый путь: одним Arrow-таблицей (если доступно в твоей версии)
        at = scan.to_arrow()
        if at is not None and at.num_rows > 0:
            df = at.to_pandas()
            for mid, puuid in zip(df["metadata.matchid"], df["participant.puuid"]):
                existing.add((mid, puuid))
    except Exception as e:
        logging.warning("⚠️ fetch_existing_keys: scan.to_arrow() не сработал (%s). Переходим на post-plan чтение.", e)

    # fallback: читаем порциями по плану (работает почти во всех версиях)
    if not existing:
        try:
            for task in scan.plan_files():
                try:
                    at = task.to_arrow()
                    if at is not None and at.num_rows > 0:
                        df = at.to_pandas()
                        for mid, puuid in zip(df["metadata.matchid"], df["participant.puuid"]):
                            existing.add((mid, puuid))
                except Exception as e2:
                    logging.warning("⚠️ fetch_existing_keys: task.to_arrow() не удалось: %s", e2)
        except Exception as e3:
            logging.warning("⚠️ fetch_existing_keys: plan_files() не удалось: %s", e3)

    logging.info("🔎 [%s] [%s] Existing keys in table: %d", nick, day.isoformat(), len(existing))
    return existing
    # Соберём пары (nick, day), чтобы минимизировать количество сканов
    pairs: Set[Tuple[str, dt.date]] = set(
        (r["source_nickname"], r["event_date"]) for r in rows
    )

    # Для каждой пары читаем ключи из Iceberg
    existing_map: Dict[Tuple[str, dt.date], Set[Tuple[str, str]]] = {}
    for nick, day in pairs:
        existing_map[(nick, day)] = fetch_existing_keys(tbl, nick, day)

    # Фильтруем дубли
    kept: List[Dict[str, Any]] = []
    dropped = 0
    for r in rows:
        nick = r["source_nickname"]
        day  = r["event_date"]
        key  = (r["metadata.matchid"], r["participant.puuid"])
        if key in existing_map[(nick, day)]:
            dropped += 1
        else:
            kept.append(r)

    if pairs:
        # Для логов красиво отформатируем
        pairs_str = ", ".join([f"{n} {d.isoformat()}" for (n, d) in sorted(pairs)])
        logging.info("🧹 Dedupe for {%s}: kept=%d, dropped=%d", pairs_str, len(kept), dropped)
    else:
        logging.info("🧹 Dedupe: no pairs found")

    return kept

def fetch_existing_keys(tbl: Table, nick: str, day: dt.date) -> Set[Tuple[str, str]]:
    """Читаем существующие ключи (matchId, puuid) для (nick, day) из Iceberg, чтобы отфильтровать дубли."""
    expr = And(EqualTo("event_date", day), EqualTo("source_nickname", nick))
    scan = tbl.scan(
        row_filter=expr,
        selected_fields=["metadata.matchid", "participant.puuid"],
    )

    existing: Set[Tuple[str, str]] = set()

    # Быстрый путь: одним Arrow-таблицей
    try:
        at = scan.to_arrow()
        if at is not None and at.num_rows > 0:
            df = at.to_pandas()
            for mid, puuid in zip(df["metadata.matchid"], df["participant.puuid"]):
                existing.add((mid, puuid))
    except Exception as e:
        logging.warning("⚠️ fetch_existing_keys: scan.to_arrow() не сработал (%s).", e)

    # Fallback: читаем по плану файлами
    if not existing:
        try:
            for task in scan.plan_files():
                try:
                    at = task.to_arrow()
                    if at is not None and at.num_rows > 0:
                        df = at.to_pandas()
                        for mid, puuid in zip(df["metadata.matchid"], df["participant.puuid"]):
                            existing.add((mid, puuid))
                except Exception as e2:
                    logging.warning("⚠️ fetch_existing_keys: task.to_arrow() не удалось: %s", e2)
        except Exception as e3:
            logging.warning("⚠️ fetch_existing_keys: plan_files() не удалось: %s", e3)

    logging.info("🔎 [%s] [%s] Existing keys in table: %d", nick, day.isoformat(), len(existing))
    return existing


def dedupe_rows_by_keys(rows: List[Dict[str, Any]], tbl: Table) -> List[Dict[str, Any]]:
    """Антидубликат: для каждой пары (nick, day) отбрасываем строки,
    чьи (metadata.matchid, participant.puuid) уже есть в таблице."""
    if not rows:
        return rows

    pairs: Set[Tuple[str, dt.date]] = set((r["source_nickname"], r["event_date"]) for r in rows)

    existing_map: Dict[Tuple[str, dt.date], Set[Tuple[str, str]]] = {}
    for nick, day in pairs:
        existing_map[(nick, day)] = fetch_existing_keys(tbl, nick, day)

    kept: List[Dict[str, Any]] = []
    dropped = 0
    for r in rows:
        nick = r["source_nickname"]
        day  = r["event_date"]
        key  = (r["metadata.matchid"], r["participant.puuid"])
        if key in existing_map[(nick, day)]:
            dropped += 1
        else:
            kept.append(r)

    if pairs:
        pairs_str = ", ".join([f"{n} {d.isoformat()}" for (n, d) in sorted(pairs)])
        logging.info("🧹 Dedupe for {%s}: kept=%d, dropped=%d", pairs_str, len(kept), dropped)
    else:
        logging.info("🧹 Dedupe: no pairs found")

    return kept
# ───────────── запись ─────────────
def append_rows_strict(rows: List[Dict[str, Any]]) -> int:
    if not rows:
        return 0

    tbl = load_table()
    # Антидубликат по (metadata.matchid, participant.puuid) в разрезе (event_date, source_nickname)
    rows = dedupe_rows_by_keys(rows, tbl)

    if not rows:
        logging.info("ℹ️ All rows are duplicates — nothing to append.")
        return 0

    required_cols, optional_cols = split_required_optional(tbl)
    all_cols_in_order = [f.name for f in tbl.schema().fields]

    # Проверяем только REQUIRED
    for r in rows:
        missing_req = [c for c in required_cols if c not in r]
        if missing_req:
            raise RuntimeError(f"Missing REQUIRED columns in row: {missing_req[:8]}{'...' if len(missing_req)>8 else ''}")

    df = pd.DataFrame(rows)
    for c in optional_cols:
        if c not in df.columns:
            df[c] = None

    # Порядок столбцов ровно как в схеме
    df = df[all_cols_in_order]

    # event_date → date
    if "event_date" in df.columns:
        df["event_date"] = pd.to_datetime(df["event_date"]).dt.date

    pa_tbl = pa.Table.from_pandas(df, schema=table_arrow_schema(tbl), preserve_index=False)
    tbl.append(pa_tbl)

    # Логи по ник/датам в батче
    by_pair = df.groupby(["source_nickname", "event_date"]).size().reset_index(name="rows")
    for _, row in by_pair.iterrows():
        logging.info("✅ APPEND: [%s] [%s] rows=%d", row["source_nickname"], row["event_date"], int(row["rows"]))

    logging.info("🧾 Appended total %d rows to %s", pa_tbl.num_rows, TABLE_ID)
    return pa_tbl.num_rows

# ───────────── main ─────────────
if __name__ == "__main__":
    today = dt.date.today()
    start = today - dt.timedelta(weeks=1)
    end   = today - dt.timedelta(days=1)

    riot_ids = [
        "Monty Gard#RU1",
        "Breaksthesilence#RU1",
        "2pilka#RU1",
        "Gruntq#RU1",
        "Шaзам#RU1",
        "Prooaknor#RU1",
    ]

    total = 0
    for riot in riot_ids:
        day = start
        while day <= end:
            try:
                logging.info("🚚 START: nick=[%s] day=[%s]", riot, day.isoformat())
                rows = build_rows_for_day(riot_id=riot, day=day)
                total += append_rows_strict(rows)
            except Exception:
                logging.exception("💥 Error on %s for %s", day, riot)
            day += dt.timedelta(days=1)

    logging.info("✅ Done. Total rows written: %d", total)
