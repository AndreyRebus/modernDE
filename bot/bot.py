
from __future__ import annotations

import asyncio
import logging
import math
import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List

import pandas as pd
from aiogram import Bot, Dispatcher
from aiogram.client.default import DefaultBotProperties
from aiogram_dialog.widgets.media import DynamicMedia
from aiogram_dialog.api.entities import MediaAttachment
import random  
from aiogram.enums import ParseMode
from aiogram.filters import Command

from aiogram_dialog import (
    Dialog,
    DialogManager,
    LaunchMode,
    Window,
    setup_dialogs,
    StartMode
)
from aiogram_dialog.widgets.kbd import Row, Button
from aiogram_dialog.widgets.text import Format, Const
from aiogram.fsm.state import State, StatesGroup
from dotenv import load_dotenv

from templates import TEMPLATES, METRIC_COLS
from trino_client import query_df

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from zoneinfo import ZoneInfo
# ─────────────────────────── logging ────────────────────────────
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# ─────────────────────────── config ─────────────────────────────
load_dotenv()
BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN не задан (export или .env)")

SPLASH_DIR = Path(os.getenv("SPLASH_DIR", "data/splashes"))
TRINO_TABLE = os.getenv("RECORDS_TABLE", "iceberg.dbt_model.concat_record")
DATA_DIR = Path(os.getenv("DATA_DIR", "data"))
DATA_DIR.mkdir(parents=True, exist_ok=True)
PARQUET_FILE = DATA_DIR / "concat_record.parquet"
STALE_AFTER = timedelta(hours=int(os.getenv("STALE_HOURS", "6")))

# ─────────────────────────── data cache ─────────────────────────
ALL_COLUMNS: List[str] = ["source_nickname"] + METRIC_COLS + [f"{m}_meta" for m in METRIC_COLS]

async def getter(dialog_manager: DialogManager, **kwargs):
    data = dialog_manager.dialog_data
    msgs = USER_MESSAGES.get(dialog_manager.event.from_user.id, [])
    idx = data.get("idx", 0)
    total = len(msgs)

    if msgs:
        item = msgs[idx]
        champion = item["champion"]
        # ищем случайный файл вида  Ahri_*.jpg / Ahri_*.png
        files = list(SPLASH_DIR.glob(f"{champion}_*.jpg")) + \
                list(SPLASH_DIR.glob(f"{champion}_*.png"))
        photo = (
            MediaAttachment(
                path=str(random.choice(files)),
                type="photo",                 # ← ОБЯЗАТЕЛЬНО
            )
            if files else None
        )
        current_text = item["text"]
    else:
        photo = None
        current_text = "Рекордов нет."

    return {
        "text": current_text,
        "photo": photo,
        "pos": idx + 1,
        "total": total,
        "disable_left": idx == 0,
        "disable_right": idx >= total - 1,
    }

def fetch_and_cache() -> pd.DataFrame:
    sql = f"SELECT {', '.join(ALL_COLUMNS)} FROM {TRINO_TABLE}"
    logger.info("SQL: %s", sql)
    df = query_df(sql)
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

# ───────────────────────── message building ────────────────────

def _split_meta(raw: str | None):
    if not raw or not isinstance(raw, str):
        return "<match>", "<champion>"
    if "-_-" in raw:
        match_id, champ = raw.split("-_-", 1)
        return match_id or "<match>", champ or "<champion>"
    return raw, "<champion>"

async def push_daily_carousel(chat_id: int):
    df = load_data(force=True)
    USER_MESSAGES[chat_id] = build_messages(df)

    dm = registry.bg(          # берём фонового менеджера из реестра
        bot=bot,
        user_id=chat_id,
        chat_id=chat_id,
    )
    # 3. Запускаем окно и ждём окончания отправки
    await dm.start(
        RecSG.show,
        data={"idx": 0},
        mode=StartMode.RESET_STACK,   # ← новый параметр
    )

def build_messages(df: pd.DataFrame) -> List[Dict]:
    sent_pairs: set[tuple[str, str, str]] = set()
    out: List[Dict] = []

    for _, row in df.iterrows():
        nick = row.get("source_nickname")
        if isinstance(nick, pd.Series):
            nick = nick.iloc[0]
        if not isinstance(nick, str) or not nick:
            continue

        for metric in METRIC_COLS:
            val = row.get(metric)
            if val in (None, "", "0") or (
                isinstance(val, (int, float)) and (val == 0 or math.isnan(val))
            ):
                continue

            match_id, champion = _split_meta(row.get(f"{metric}_meta"))

            # ── Гиперссылка на LeagueOfGraphs ───────────────────────────
            num = match_id.removeprefix("RU_")           # 531424992
            match_link = (
                f'<a href="https://www.leagueofgraphs.com/match/ru/{num}">'
                f'{match_id}</a>'
            )
            # ────────────────────────────────────────────────────────────

            key = (nick, metric, match_id)
            if key in sent_pairs:
                continue
            sent_pairs.add(key)

            if isinstance(val, float) and val.is_integer():
                val = int(val)

            text = TEMPLATES[metric].format(
                nickname=nick, matchId=match_link, champion=champion, value=val
            )
            out.append({"text": text, "champion": champion})
    return out



# ───────────────────────── dialog setup ────────────────────────
class RecSG(StatesGroup):
    show = State()

# in‑memory storage: user_id -> list[str]
USER_MESSAGES: Dict[int, List[str]] = {}


async def on_left(c, button, dialog_manager: DialogManager):
    if dialog_manager.dialog_data.get("idx", 0) > 0:
        dialog_manager.dialog_data["idx"] -= 1


async def on_right(c, button, dialog_manager: DialogManager):
    msgs = USER_MESSAGES.get(dialog_manager.event.from_user.id, [])
    idx = dialog_manager.dialog_data.get("idx", 0)
    if idx < len(msgs) - 1:
        dialog_manager.dialog_data["idx"] = idx + 1


view = Window(
    DynamicMedia("photo"),                     # картинка
    Format("{text}\n\n({pos}/{total})"),       # текст + счётчик
    Row(
        Button(Const("◀"), id="left",
               on_click=on_left,
               when=lambda d, *_: not d["disable_left"]),
        Button(Const("▶"), id="right",
               on_click=on_right,
               when=lambda d, *_: not d["disable_right"]),
    ),
    getter=getter,
    state=RecSG.show,
)

dialog = Dialog(view, launch_mode=LaunchMode.ROOT)

# ───────────────────────── aiogram runtime ─────────────────────

bot = Bot(BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher()

registry = setup_dialogs(dp)   # <-- сохраняем ссылку
dp.include_router(dialog)


# ---------------- commands ----------------

@dp.message(Command("refresh"))
async def cmd_refresh(m):
    await m.answer("🔄 Обновляю данные…")
    try:
        fetch_and_cache()
        await m.answer("✅ Обновление завершено!")
    except Exception as e:
        logger.exception("Ошибка обновления")
        await m.answer(f"❌ Ошибка обновления: {e}")


@dp.message(Command("check"))
async def cmd_check(m, dialog_manager: DialogManager):
    try:
        df = load_data()
        msgs = build_messages(df)
    except Exception as e:
        logger.exception("Ошибка выборки")
        await m.answer(f"❌ Ошибка выборки: {e}")
        return

    USER_MESSAGES[m.from_user.id] = msgs
    await dialog_manager.start(RecSG.show, data={"idx": 0})


async def main():
    fetch_and_cache()        # первичная запись кэша


    loop = asyncio.get_running_loop()
    scheduler = AsyncIOScheduler(
        event_loop=loop,
        timezone=ZoneInfo(os.getenv("TZ", "UTC")),
    )
    scheduler.add_job(
        push_daily_carousel,
        trigger="cron",
        hour=18, minute=27,
        args=[int(os.getenv("TARGET_CHAT_ID"))],
    )
    scheduler.start()

    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
