# bot/bot.py
from __future__ import annotations

import asyncio
import logging
import math
import os
from pathlib import Path
import json
import random
import unicodedata
import re

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
IMAGE_DIR = Path(os.getenv("IMAGE_DIR", "bot/data/image"))
MAX_RECORDS_PER_PLAYER = int(os.getenv("MAX_RECORDS_PER_PLAYER", "5"))
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
    # режим
    mode = dialog_manager.dialog_data.get("mode", "carousel")

    # chat_id держим в dialog_data (нам его кладёт on_startup в data)
    chat_id = dialog_manager.dialog_data.get("chat_id") or (dialog_manager.start_data or {}).get("chat_id")
    if chat_id:
        dialog_manager.dialog_data["chat_id"] = chat_id

    msgs = USER_MESSAGES.get(chat_id, []) if chat_id is not None else []
    idx = dialog_manager.dialog_data.get("idx", 0)
    total = len(msgs)

    if mode == "chart":
        # показываем график выбранного игрока
        player = dialog_manager.dialog_data.get("chart_player")
        photo = pick_chart(player)
        # текст оставим прежним (или можно подсветить игрока)
        current_text = f"График: {player}" if player else "График недоступен."
        show_back = True
        disable_left = disable_right = True
    else:
        # обычная карусель
        if msgs:
            item = msgs[idx]
            champion = item["champion"]
            photo = pick_splash(champion) if 'pick_splash' in globals() else None
            # если нет индекса сплэшей — старый фолбэк по файлам
            if photo is None:
                files = list(SPLASH_DIR.glob(f"{champion}_*.jpg")) + list(SPLASH_DIR.glob(f"{champion}_*.png"))
                photo = MediaAttachment(path=str(random.choice(files)), type="photo") if files else None
            current_text = item["text"]
        else:
            photo = None
            current_text = "Рекордов нет."
        show_back = False
        disable_left = idx == 0
        disable_right = idx >= total - 1

    return {
        "text": current_text,
        "photo": photo,
        "pos": idx + 1,
        "total": total,
        "disable_left": disable_left,
        "disable_right": disable_right,
        "show_nav": mode == "carousel",
        "show_back": show_back,
    }


SAFE = re.compile(r"[^\w.\-]", re.UNICODE)

def _safe_name(nick: str) -> str:
    # как в kda_charts.py: сохраняем Unicode, заменяем # и пробелы на _
    name = unicodedata.normalize("NFC", str(nick))
    name = name.replace("#", "_")
    name = re.sub(r"\s+", "_", name, flags=re.UNICODE)
    name = re.sub(r'[\/\\:*?"<>|]', "_", name)
    name = re.sub(r"[^\w.\-]", "_", name, flags=re.UNICODE)
    name = re.sub(r"_+", "_", name).strip("._-")
    return name or "player"

def pick_chart(nickname: str) -> MediaAttachment | None:
    """Берём JPG графика из IMAGE_DIR по нику-кнопке."""
    if not nickname:
        return None
    fname = _safe_name(nickname) + ".jpg"
    path = IMAGE_DIR / fname
    if path.exists():
        return MediaAttachment(path=str(path), type="photo")
    return None

def _split_meta(raw: str | None):
    if not raw or not isinstance(raw, str):
        return "<match>", "<champion>"
    if "-_-" in raw:
        match_id, champ = raw.split("-_-", 1)
        return match_id or "<match>", champ or "<champion>"
    return raw, "<champion>"

async def on_riot_click(c, button, dialog_manager: DialogManager):
    # из id кнопки узнаём ник
    name = RIOT_BTN_TO_NAME.get(button.widget_id)
    dialog_manager.dialog_data.update({
        "mode": "chart",
        "chart_player": name,
    })
    with suppress(Exception):
        await c.answer()

async def on_back(c, button, dialog_manager: DialogManager):
    dialog_manager.dialog_data.update({
        "mode": "carousel",
        "chart_player": None,
    })
    with suppress(Exception):
        await c.answer()

def build_messages(df: pd.DataFrame) -> list[dict]:
    sent_pairs: set[tuple[str, str, str]] = set()
    champ_counts: dict[str, int] = {}     # лимит по чемпиону (как было: не больше 3)
    player_counts: dict[str, int] = {}    # НОВОЕ: лимит сообщений на игрока
    out: list[dict] = []

    for _, row in df.iterrows():
        nick = row.get("source_nickname")
        if isinstance(nick, pd.Series):
            nick = nick.iloc[0]
        if not isinstance(nick, str) or not nick:
            continue

        # если по этому игроку уже набрали лимит — пропускаем все его оставшиеся строки
        if player_counts.get(nick, 0) >= MAX_RECORDS_PER_PLAYER:
            continue

        for metric in METRIC_COLS:
            # проверка лимита на игрока — ещё раз внутри цикла (на случай нескольких метрик)
            if player_counts.get(nick, 0) >= MAX_RECORDS_PER_PLAYER:
                break

            val = row.get(metric)
            if val in (None, "", "0") or (
                isinstance(val, (int, float)) and (val == 0 or math.isnan(val))
            ):
                continue

            match_id, champion = _split_meta(row.get(f"{metric}_meta"))

            # как и раньше: не больше 3 ачивок на одного чемпиона
            if champ_counts.get(champion, 0) >= 3:
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

            # инкременты счётчиков
            champ_counts[champion] = champ_counts.get(champion, 0) + 1
            player_counts[nick] = player_counts.get(nick, 0) + 1

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

RIOT_IDS = _parse_riot_ids(os.getenv("RIOT_IDS", ""))

RIOT_BTN_TO_NAME: dict[str, str] = {}
RIOT_BUTTONS = []
for i, name in enumerate(RIOT_IDS):
    btn_id = f"riot_{i}"
    RIOT_BTN_TO_NAME[btn_id] = name
    RIOT_BUTTONS.append(Button(Const(name), id=btn_id, on_click=on_riot_click))

BACK_BUTTON = Button(Const("↩️ Назад"), id="back", on_click=on_back)

view = Window(
    DynamicMedia("photo"),
    Format("{text}\n\n({pos}/{total})"),
    Row(
        Button(Const("◀"), id="left",
               on_click=on_left,
               when=lambda d, *_: d.get("show_nav") and not d["disable_left"]),
        Button(Const("▶"), id="right",
               on_click=on_right,
               when=lambda d, *_: d.get("show_nav") and not d["disable_right"]),
    ),
    Row(
        BACK_BUTTON,
        when=lambda d, *_: d.get("show_back"),
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
