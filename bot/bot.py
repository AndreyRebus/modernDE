# bot/bot.py
from __future__ import annotations

import asyncio
import logging
import math
import os
from pathlib import Path
import json
import random

import pandas as pd
from aiogram import Bot, Dispatcher
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import Command
from aiogram.fsm.state import State, StatesGroup
from aiogram.types.error_event import ErrorEvent
from aiogram.types import CallbackQuery

from aiogram_dialog import (
    Dialog,
    DialogManager,
    LaunchMode,
    Window,
    setup_dialogs,
    StartMode,
    ShowMode,
    GROUP_STACK_ID,
)
from aiogram_dialog.widgets.kbd import Row, Button, Group
from aiogram_dialog.widgets.media import DynamicMedia
from aiogram_dialog.widgets.text import Format, Const
from aiogram_dialog.api.entities import MediaAttachment
from aiogram_dialog.api.exceptions import OutdatedIntent, UnknownIntent

from dotenv import load_dotenv
from contextlib import suppress

from .templates import TEMPLATES, METRIC_COLS
from .data_cache import fetch_and_cache, load_data

# ─────────────────────────── logging ────────────────────────────
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# ─────────────────────────── config ─────────────────────────────
load_dotenv()
BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN не задан (export или .env)")
SPLASH_DIR = Path(os.getenv("SPLASH_DIR", "data/splashes"))
SPLASH_INDEX_PATH = Path(os.getenv("SPLASH_INDEX", "data/splashes/index.json"))
# ───────────────────────── helpers ──────────────────────────────

def _get_chat_id(dm: DialogManager) -> int | None:
    # 1) сначала из dialog_data (после первого рендера всегда там)
    cid = dm.dialog_data.get("chat_id")
    if cid:
        return cid
    # 2) затем из start_data (есть при первом запуске)
    cid = (dm.start_data or {}).get("chat_id") if hasattr(dm, "start_data") else None
    if cid:
        return cid
    # 3) как крайний случай попробуем event (но при bg-старте его может не быть)
    ev = getattr(dm, "event", None)
    try:
        return getattr(getattr(ev, "chat", None), "id", None)
    except Exception:
        return None

async def getter(dialog_manager: DialogManager, **kwargs):
    chat_id = dialog_manager.dialog_data.get("chat_id") or (dialog_manager.start_data or {}).get("chat_id")
    if not chat_id:
        ev = getattr(dialog_manager, "event", None)
        chat_id = getattr(getattr(ev, "chat", None), "id", None)
    if chat_id:
        dialog_manager.dialog_data["chat_id"] = chat_id
    msgs = USER_MESSAGES.get(chat_id, []) if chat_id is not None else []
    idx = dialog_manager.dialog_data.get("idx", 0)
    total = len(msgs)

    if msgs:
        item = msgs[idx]
        champion = item["champion"]
        photo = pick_splash(champion)
        current_text = item["text"]
    else:
        photo = None
        current_text = "Рекордов нет."

    # держим chat_id в dialog_data, чтобы потом не искать
    if chat_id is not None:
        dialog_manager.dialog_data["chat_id"] = chat_id

    return {
        "text": current_text,
        "photo": photo,
        "pos": idx + 1,
        "total": total,
        "disable_left": idx == 0,
        "disable_right": idx >= total - 1,
    }

def _split_meta(raw: str | None):
    if not raw or not isinstance(raw, str):
        return "<match>", "<champion>"
    if "-_-" in raw:
        match_id, champ = raw.split("-_-", 1)
        return match_id or "<match>", champ or "<champion>"
    return raw, "<champion>"

async def on_riot_click(c, button, dialog_manager: DialogManager):
    with suppress(Exception):
        await c.answer()

def build_messages(df: pd.DataFrame) -> list[dict]:
    sent_pairs: set[tuple[str, str, str]] = set()
    counts: dict[str, int] = {}
    out: list[dict] = []

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

            if counts.get(champion, 0) >= 3:
                continue

            key = (nick, metric, match_id)
            if key in sent_pairs:
                continue
            sent_pairs.add(key)

            if isinstance(val, float) and val.is_integer():
                val = int(val)

            num = match_id.removeprefix("RU_")
            match_link = (
                f'<a href="https://www.leagueofgraphs.com/match/ru/{num}">' +
                f'{match_id}</a>'
            )

            text = TEMPLATES[metric].format(
                nickname=nick, matchId=match_link, champion=champion, value=val
            )
            out.append({"text": text, "champion": champion})

            counts[champion] = counts.get(champion, 0) + 1

    return out

# ───────────────────────── dialog setup ────────────────────────
class RecSG(StatesGroup):
    show = State()

USER_MESSAGES: dict[int, list[dict]] = {}

async def on_startup(bot: Bot):
    target = os.getenv("TARGET_CHAT_ID", "").strip()
    if not target:
        logger.error("TARGET_CHAT_ID не задан")
        return

    chat_id = int(target)

    try:
        # используем ID БОТА как user_id для bg-менеджера
        me = await bot.get_me()
        user_id = me.id
        load_splash_index()
        df = load_data(force=False)
        msgs = build_messages(df)
        USER_MESSAGES[chat_id] = msgs
        logger.info("Prepared %d messages for chat %s", len(msgs), chat_id)

        dm = registry.bg(
            bot=bot,
            user_id=user_id,        # <-- ID бота
            chat_id=chat_id,
            stack_id=GROUP_STACK_ID, # <-- общий стек чата
            load=True,
        )

        await dm.start(
            RecSG.show,
            data={"idx": 0, "chat_id": chat_id},  # <-- чтобы getter знал ключ
            mode=StartMode.RESET_STACK,
            show_mode=ShowMode.SEND,              # <-- отправляем новое сообщение
        )
        logger.info("Initial push sent to chat %s", chat_id)
    except Exception:
        logger.exception("Failed to send initial push")

# champion -> list[str absolute paths]
CHAMP_SPLASHES: dict[str, list[str]] = {}

def load_splash_index() -> None:
    """Разово читаем index.json в память."""
    global CHAMP_SPLASHES
    try:
        with open(SPLASH_INDEX_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        # нормализуем и фильтруем
        CHAMP_SPLASHES = {
            str(k): [str(p) for p in v if isinstance(p, str)]
            for k, v in (data or {}).items()
            if isinstance(v, list)
        }
        logger.info("Splash index: %d champions loaded from %s", len(CHAMP_SPLASHES), SPLASH_INDEX_PATH)
    except FileNotFoundError:
        logger.warning("Splash index file %s not found; fallback to FS scan.", SPLASH_INDEX_PATH)
        CHAMP_SPLASHES = {}
    except Exception:
        logger.exception("Failed to load splash index")
        CHAMP_SPLASHES = {}

def pick_splash(champion: str) -> MediaAttachment | None:
    """Берём случайную картинку из индекса. Если героя нет — мягкий фолбэк на сканирование."""
    if not champion:
        return None
    files = CHAMP_SPLASHES.get(champion)
    if files:
        path = random.choice(files)
        return MediaAttachment(path=path, type="photo")
    # мягкий фолбэк, если индекс не содержит героя
    files2 = list(SPLASH_DIR.glob(f"{champion}_*.jpg")) + list(SPLASH_DIR.glob(f"{champion}_*.png"))
    if files2:
        return MediaAttachment(path=str(random.choice(files2)), type="photo")
    return None


async def on_left(c, button, dialog_manager: DialogManager):
    idx = dialog_manager.dialog_data.get("idx", 0)
    if idx > 0:
        dialog_manager.dialog_data["idx"] = idx - 1

async def on_right(c, button, dialog_manager: DialogManager):
    chat_id = dialog_manager.dialog_data.get("chat_id")
    msgs = USER_MESSAGES.get(chat_id, []) if chat_id is not None else []
    idx = dialog_manager.dialog_data.get("idx", 0)
    if idx < len(msgs) - 1:
        dialog_manager.dialog_data["idx"] = idx + 1

def _parse_riot_ids(raw: str) -> list[str]:
    raw = (raw or "").strip()
    if not raw:
        return []
    try:
        v = json.loads(raw)
        if isinstance(v, list):
            return [str(x).strip().strip('\'"“”„«»') for x in v if str(x).strip()]
    except Exception:
        pass
    parts = [p.strip() for p in raw.split(",")]
    cleaned = [p.strip().strip('\'"“”„«»') for p in parts if p.strip().strip('\'"“”„«»')]
    return cleaned

RIOT_IDS = _parse_riot_ids(os.getenv("RIOT_IDS", ""))

RIOT_BUTTONS = [
    Button(Const(name), id=f"riot_{i}", on_click=on_riot_click)
    for i, name in enumerate(RIOT_IDS)
]

view = Window(
    DynamicMedia("photo"),
    Format("{text}\n\n({pos}/{total})"),
    Row(
        Button(Const("◀"), id="left",
               on_click=on_left,
               when=lambda d, *_: not d["disable_left"]),
        Button(Const("▶"), id="right",
               on_click=on_right,
               when=lambda d, *_: not d["disable_right"]),
    ),
    Group(
        *RIOT_BUTTONS,
        width=2,
        when=lambda d, *_: bool(RIOT_BUTTONS),
    ),
    getter=getter,
    state=RecSG.show,
)

dialog = Dialog(view, launch_mode=LaunchMode.ROOT)

# ───────────────────────── aiogram runtime ─────────────────────
bot = Bot(BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher()

registry = setup_dialogs(dp)
dp.include_router(dialog)
dp.startup.register(on_startup)

# ---------------- команды ----------------
@dp.message(Command("refresh"))
async def cmd_refresh(m):
    await m.answer("🔄 Обновляю данные…")
    try:
        fetch_and_cache()
        await m.answer("✅ Обновление завершено!")
    except Exception as e:
        logger.exception("Ошибка обновления")
        await m.answer(f"❌ Ошибка обновления: {e}")

# ---------------- обработка ошибок ----------------
@dp.errors()
async def on_dialog_errors(event: ErrorEvent):
    exc = event.exception
    if isinstance(exc, (OutdatedIntent, UnknownIntent)):
        cq: CallbackQuery | None = getattr(event.update, "callback_query", None)
        if cq:
            with suppress(Exception):
                await cq.answer("Сообщение устарело. Используйте последнюю карусель.", show_alert=False)
        return True

# ---------------- main ----------------
async def main():
    await bot.delete_webhook(drop_pending_updates=True)
    asyncio.create_task(asyncio.to_thread(fetch_and_cache))
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
