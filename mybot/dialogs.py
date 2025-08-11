from __future__ import annotations
from typing import Dict, List, Optional
import json
import random
import re
from functools import lru_cache
from pathlib import Path

from aiogram.enums import ContentType
from aiogram.fsm.state import State, StatesGroup
from aiogram_dialog import Window, Dialog, DialogManager, LaunchMode
from aiogram_dialog.api.entities import MediaAttachment
from aiogram_dialog.widgets.media import DynamicMedia
from aiogram_dialog.widgets.kbd import Row, Button, Url
from aiogram_dialog.widgets.text import Format, Const
from urllib.parse import quote

from .config import NICKNAMES, SPLASH_DIR
from .messages import build_messages
from .cache import load_data

# хранение подготовленных сообщений по пользователям
USER_MESSAGES: Dict[int, List[Dict]] = {}

class RecSG(StatesGroup):
    show = State()

# ---------- manifest.json ----------
def _norm(name: str) -> str:
    return re.sub(r"[\s_\-]+", "", name).lower()

@lru_cache(maxsize=1)
def _manifest_map() -> Dict[str, List[str]]:
    path = Path(SPLASH_DIR) / "manifest.json"
    if not path.exists():
        return {}
    with open(path, "r", encoding="utf-8") as f:
        raw = json.load(f)

    idx: Dict[str, List[str]] = {}
    for champion, files in (raw or {}).items():
        if not isinstance(files, list):
            continue
        norm_key = _norm(str(champion))
        abs_files = [str((Path(SPLASH_DIR) / fn).resolve()) for fn in files]
        idx[norm_key] = abs_files
    return idx

def pick_random_splash(champion: str) -> Optional[str]:
    files = _manifest_map().get(_norm(champion))
    if not files:
        return None
    return random.choice(files)

# ---------- кнопки ----------
async def on_left(c, button, dialog_manager: DialogManager):
    if dialog_manager.dialog_data.get("idx", 0) > 0:
        dialog_manager.dialog_data["idx"] -= 1

async def on_right(c, button, dialog_manager: DialogManager):
    msgs = USER_MESSAGES.get(dialog_manager.event.from_user.id, [])
    idx = dialog_manager.dialog_data.get("idx", 0)
    if idx < len(msgs) - 1:
        dialog_manager.dialog_data["idx"] = idx + 1

nick_buttons = [
    Url(
        Const(n.split("#", 1)[0]),
        Const(f"https://www.leagueofgraphs.com/summoner/ru/{quote(n)}"),
    )
    for n in NICKNAMES
]

# ---------- данные окна ----------
async def getter(dialog_manager: DialogManager, **kwargs):
    user_id = dialog_manager.event.from_user.id
    msgs = USER_MESSAGES.get(user_id, [])
    idx = dialog_manager.start_data.get("idx") if "idx" in dialog_manager.start_data else dialog_manager.dialog_data.get("idx", 0)
    idx = max(0, min(idx if isinstance(idx, int) else 0, max(len(msgs) - 1, 0)))
    dialog_manager.dialog_data["idx"] = idx

    text = "Нет данных. Нажмите /check."
    media = None

    if msgs:
        current = msgs[idx]
        text = current.get("text", "")
        champion = current.get("champion")
        img_path = pick_random_splash(champion) if champion else None
        if img_path:
            media = MediaAttachment(ContentType.PHOTO, path=img_path)

    return {
        "text": text,
        "media": media,
        "disable_left": idx <= 0,
        "disable_right": idx >= len(msgs) - 1 if msgs else True,
    }

# ---------- окно и диалог ----------
view = Window(
    Format("{text}"),
    DynamicMedia("media"),
    Row(
        Button(Const("◀"), id="left", on_click=on_left, when=lambda d, *_: not d["disable_left"]),
        Button(Const("▶"), id="right", on_click=on_right, when=lambda d, *_: not d["disable_right"]),
    ),
    Row(*nick_buttons),
    getter=getter,
    state=RecSG.show,
)

dialog = Dialog(view, launch_mode=LaunchMode.ROOT)

# ---------- ежедневный пуш карусели ----------
async def push_daily_carousel(bot, registry, chat_id: int):
    from aiogram_dialog import StartMode

    df = load_data(force=True)
    USER_MESSAGES[chat_id] = build_messages(df)
    dm = registry.bg(bot=bot, user_id=chat_id, chat_id=chat_id)
    await dm.start(RecSG.show, data={"idx": 0}, mode=StartMode.RESET_STACK)
