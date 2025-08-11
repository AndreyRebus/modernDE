import asyncio
from nats_trigger import setup_nats_trigger_and_bind

from .dialogs import push_daily_carousel


NATS_HANDLE = None


def setup_scheduler(loop, timezone: str, bot, registry, chat_id: int):
    async def _bind():
        global NATS_HANDLE
        NATS_HANDLE = await setup_nats_trigger_and_bind(
            bot=bot,
            registry=registry,
            chat_id=chat_id,
            push_daily_carousel=push_daily_carousel,
        )

    loop.create_task(_bind())
    return None


async def shutdown_scheduler():
    global NATS_HANDLE
    if NATS_HANDLE:
        await NATS_HANDLE.close()
        NATS_HANDLE = None
