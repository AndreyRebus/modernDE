import asyncio
import os

from aiogram import Bot, Dispatcher
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram_dialog import setup_dialogs

from .config import BOT_TOKEN
from .dialogs import dialog
from .handlers import router as handlers_router
from .scheduler import setup_scheduler


async def main():
    bot = Bot(BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    dp = Dispatcher()

    registry = setup_dialogs(dp)
    dp.include_router(dialog)
    dp.include_router(handlers_router)

    loop = asyncio.get_running_loop()
    target_chat = int(os.getenv("TARGET_CHAT_ID", "0"))

    setup_scheduler(
        loop=loop,
        timezone=os.getenv("TZ", "UTC"),
        bot=bot,
        registry=registry,
        chat_id=target_chat,
    )

    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
