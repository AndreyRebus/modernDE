from apscheduler.schedulers.asyncio import AsyncIOScheduler
from zoneinfo import ZoneInfo

from .config import logger
from .dialogs import push_daily_carousel


def setup_scheduler(loop, timezone: str, bot, registry, chat_id: int):
    scheduler = AsyncIOScheduler(event_loop=loop, timezone=ZoneInfo(timezone))
    scheduler.add_job(
        push_daily_carousel,
        trigger="cron",
        hour=22,
        minute=30,
        args=[bot, registry, chat_id],
    )
    scheduler.start()

    return scheduler
