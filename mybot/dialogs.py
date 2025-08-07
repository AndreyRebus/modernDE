from __future__ import annotations
import random
from typing import Dict, List

from aiogram_dialog import Window, Dialog, DialogManager, LaunchMode
from aiogram_dialog.widgets.media import DynamicMedia
from aiogram_dialog.api.entities import MediaAttachment
from aiogram_dialog.widgets.kbd import Row, Button, Url
from .config import NICKNAMES
from urllib.parse import quote
from aiogram_dialog.widgets.text import Format, Const
from aiogram.fsm.state import State, StatesGroup

from .messages import build_messages
from .config import SPLASH_DIR
from .cache import load_data

USER_MESSAGES: Dict[int, List[Dict]] = {}

class RecSG(StatesGroup):
    show = State()

async def getter(dialog_manager: DialogManager, **kwargs):
    data = dialog_manager.dialog_data
    msgs = USER_MESSAGES.get(dialog_manager.event.from_user.id, [])
    idx = data.get("idx", 0)
    total = len(msgs)

    if msgs:
        item = msgs[idx]
        champion = item["champion"]
        files = list(SPLASH_DIR.glob(f"{champion}_*.jpg")) + list(
            SPLASH_DIR.glob(f"{champion}_*.png")
        )
        photo = (
            MediaAttachment(path=str(random.choice(files)), type="photo") if files else None
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
        Const(n.split("#", 1)[0]),    # текст кнопки
        Const(                        # сам URL тоже Text-объект!
            f"https://www.leagueofgraphs.com/ru/summoner/ru/{quote(n.split('#', 1)[0])}"
        ),
    )
    for n in NICKNAMES
]
nick_row = Row(*nick_buttons)

view = Window(
    DynamicMedia("photo"),
    Format("{text}\n\n({pos}/{total})"),
    nick_row, 
    Row(
        Button(Const("◀"), id="left", on_click=on_left, when=lambda d, *_: not d["disable_left"]),
        Button(Const("▶"), id="right", on_click=on_right, when=lambda d, *_: not d["disable_right"]),
    ),
    getter=getter,
    state=RecSG.show,
)

dialog = Dialog(view, launch_mode=LaunchMode.ROOT)

async def push_daily_carousel(bot, registry, chat_id: int):
    from aiogram_dialog import StartMode

    df = load_data(force=True)
    USER_MESSAGES[chat_id] = build_messages(df)
    dm = registry.bg(bot=bot, user_id=chat_id, chat_id=chat_id)
    await dm.start(RecSG.show, data={"idx": 0}, mode=StartMode.RESET_STACK)