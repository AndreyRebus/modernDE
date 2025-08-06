from aiogram.filters import Command
from aiogram import Router

from .cache import fetch_and_cache, load_data
from .messages import build_messages
from .dialogs import USER_MESSAGES, RecSG
from .config import logger

router = Router()

@router.message(Command("refresh"))
async def cmd_refresh(m):
    await m.answer("🔄 Обновляю данные…")
    try:
        fetch_and_cache()
        await m.answer("✅ Обновление завершено!")
    except Exception as e:
        logger.exception("Ошибка обновления")
        await m.answer(f"❌ Ошибка обновления: {e}")

@router.message(Command("check"))
async def cmd_check(m, dialog_manager):
    try:
        df = load_data()
        msgs = build_messages(df)
    except Exception as e:
        logger.exception("Ошибка выборки")
        await m.answer(f"❌ Ошибка выборки: {e}")
        return

    USER_MESSAGES[m.from_user.id] = msgs
    await dialog_manager.start(RecSG.show, data={"idx": 0})