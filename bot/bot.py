"""
record_notifier_bot.py
~~~~~~~~~~~~~~~~~~~~~~
Telegram‑бот на **aiogram 3**. Таблица рекордов статична: для каждого игрока (source_nickname)
есть пары колонок `<metric>` и `<metric>_meta`. Бот

1. Загружает таблицу из Trino (только нужные столбцы).
2. Для каждой строки и каждого `<metric>` без постфикса `_meta`:
   • если значение ненулевое — формирует хвалебное сообщение по шаблону;
   • если метрика отсутствует в шаблоне ― логирует `CRITICAL` и выбрасывает
     исключение (бот падает — чтобы заметили проблему).

Доп. команда **/refresh** обновляет локальный Parquet‑кэш, а **/check** читает
кэш и рассылает сообщения.
"""

from __future__ import annotations
import math

import asyncio
import logging
import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import Generator, List

import pandas as pd
from aiogram import Bot, Dispatcher
from aiogram.enums import ParseMode
from aiogram.filters import Command
from aiogram.client.default import DefaultBotProperties
from aiogram_dialog import setup_dialogs
from dotenv import load_dotenv

from templates import TEMPLATES, METRIC_COLS  # все шаблоны сообщений
from trino_client import query_df  # helper для SELECT’а

# ──────────────────────────── logging ────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

# ──────────────────────────── config ─────────────────────────────
load_dotenv()
BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN не задан (export или .env)")

TRINO_TABLE = os.getenv("RECORDS_TABLE", "iceberg.dbt_model.concat_record")
DATA_DIR = Path(os.getenv("DATA_DIR", "data"))
DATA_DIR.mkdir(parents=True, exist_ok=True)
PARQUET_FILE = DATA_DIR / "concat_record.parquet"
STALE_AFTER = timedelta(hours=int(os.getenv("STALE_HOURS", "6")))

# ──────────────────────────── schema ─────────────────────────────
# Статичный список метрик (без _meta и без source_nickname)


# ──────────────────────────── checks ────────────────────────────
missing_templates = [m for m in METRIC_COLS if m not in TEMPLATES]
if missing_templates:
    msg = f"Отсутствуют шаблоны для метрик: {', '.join(missing_templates)}"
    logger.critical(msg)
    raise KeyError(msg)

# ──────────────────────────── data layer ─────────────────────────


ALL_COLUMNS: List[str] = (
    ["source_nickname"] + METRIC_COLS + [f"{m}_meta" for m in METRIC_COLS]
)


def fetch_and_cache() -> pd.DataFrame:
    """Грузит нужные столбцы из Trino и сохраняет локальный Parquet."""
    sql = f"SELECT {', '.join(ALL_COLUMNS)} FROM {TRINO_TABLE}"
    logger.info("Executing SQL: %s", sql)
    df = query_df(sql)
    df = df.loc[:, ~df.columns.duplicated()]
    df.to_parquet(PARQUET_FILE, engine="pyarrow", index=False)
    logger.info("Saved %d rows to %s", len(df), PARQUET_FILE)
    return df


def load_data(force_refresh: bool = False) -> pd.DataFrame:
    if force_refresh or not PARQUET_FILE.exists():
        return fetch_and_cache()

    mtime = datetime.utcfromtimestamp(PARQUET_FILE.stat().st_mtime)
    if datetime.utcnow() - mtime > STALE_AFTER:
        asyncio.create_task(fetch_and_cache())  # обновим в фоне
    return pd.read_parquet(PARQUET_FILE, engine="pyarrow")

# ───────────────────────── message generation ────────────────────
def _split_meta(raw: str) -> tuple[str, str]:
    """RU_531432498-_-Tristana → ('RU_531432498', 'Tristana')."""
    if not raw:
        return "<matchId>", "<champion>"
    if "-_-" in raw:
        match_id, champ = raw.split("-_-", 1)
        return match_id or "<matchId>", champ or "<champion>"
    return raw, "<champion>"

sent_pairs: set[tuple[str, str]] = set()

def row_to_messages(row: pd.Series) -> Generator[str, None, None]:
    nick = row.get("source_nickname")
    if isinstance(nick, pd.Series):
        nick = nick.iloc[0]
    if not isinstance(nick, str) or not nick:
        return

    for metric in METRIC_COLS:
        val = row.get(metric)
        if val in (None, "", "0") or (
            isinstance(val, (int, float)) and (val == 0 or math.isnan(val))
        ):
            continue

        raw_meta = row.get(f"{metric}_meta")
        match_id, champion = _split_meta(raw_meta)

        key = (nick, metric, match_id)
        if key in sent_pairs:
            continue
        sent_pairs.add(key)

        if isinstance(val, float) and val.is_integer():
            val = int(val)

        yield TEMPLATES[metric].format(
            nickname=nick,
            matchId=match_id,
            champion=champion,
            value=val,
        )

# ─────────────────────────── aiogram setup ───────────────────────

bot = Bot(BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher()
setup_dialogs(dp)

# ------------------------ команды -------------------------------

@dp.message(Command("refresh"))
async def cmd_refresh(m):
    await m.answer("🔄 Обновляю данные…")
    try:
        fetch_and_cache()
        await m.answer("✅ Данные обновлены!")
    except Exception as e:
        logger.exception("Ошибка обновления")
        await m.answer(f"❌ Ошибка обновления: {e}")


@dp.message(Command("check"))
async def cmd_check(m):
    await m.answer("⏳ Проверяю рекорды…")
    try:
        df = load_data()
    except Exception as e:
        logger.exception("Ошибка чтения файла")
        await m.answer(f"❌ Ошибка чтения данных: {e}")
        return

    sent = 0
    for _, row in df.iterrows():
        for msg in row_to_messages(row):
            await m.answer(msg)
            sent += 1

    await m.answer("🏁 Рекордов нет." if sent == 0 else f"🏆 Отправлено {sent} рекорд(ов)!")


async def main():
    fetch_and_cache()          # ← убрали create_task
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
