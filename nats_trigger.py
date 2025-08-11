import os
import json
import asyncio
import logging
import uuid
from typing import Optional, Callable

import nats
from nats.js.api import (
    StreamConfig,
    RetentionPolicy,
    StorageType,
    ConsumerConfig,
    AckPolicy,
)

log = logging.getLogger("nats-trigger")


def _sec_to_ns(v: Optional[int]) -> Optional[int]:
    if v is None:
        return None
    return int(v * 1_000_000_000)


async def ensure_stream(js, stream: str, subject: str):
    cfg = StreamConfig(
        name=stream,
        subjects=[subject],
        retention=RetentionPolicy.WorkQueue,
        storage=StorageType.File,
        max_age=0,
    )
    try:
        await js.add_stream(cfg)
        log.info("Создан stream %s с subject %s", stream, subject)
    except Exception as e:
        log.debug("ensure_stream: %s", e)


async def ensure_consumer(js, stream: str, durable: str, ack_wait_s: int, max_deliver: int):
    cfg = ConsumerConfig(
        durable_name=durable,
        ack_policy=AckPolicy.Explicit,
        ack_wait=_sec_to_ns(ack_wait_s),
        max_deliver=max_deliver,
    )
    try:
        await js.add_consumer(stream, cfg)
        log.info("Создан consumer %s в stream %s", durable, stream)
    except Exception as e:
        log.debug("ensure_consumer: %s", e)


class NatsTrigger:
    def __init__(
        self,
        nats_url: str,
        stream: str,
        subject: str,
        durable: str,
        queue: str,
        ack_wait_s: int = 300,
        max_deliver: int = 10,
        nak_delay_s: int = 300,
    ):
        self.nats_url = nats_url
        self.stream = stream
        self.subject = subject
        self.durable = durable
        self.queue = queue
        self.ack_wait_s = ack_wait_s
        self.max_deliver = max_deliver
        self.nak_delay_s = nak_delay_s

        self.nc: Optional[nats.NATS] = None
        self.js = None
        self.sub = None

    async def connect(self):
        self.nc = await nats.connect(self.nats_url, name="telegram-bot")
        self.js = self.nc.jetstream()
        await ensure_stream(self.js, self.stream, self.subject)
        await ensure_consumer(self.js, self.stream, self.durable, self.ack_wait_s, self.max_deliver)

    async def subscribe(self, handler: Callable):
        if not self.js:
            await self.connect()

        async def _on_msg(msg):
            try:
                await handler(msg)
            except Exception as e:
                log.exception("Ошибка в handler: %s", e)

        self.sub = await self.js.subscribe(
            self.subject,
            stream=self.stream,
            durable=self.durable,
            queue=self.queue,
            manual_ack=True,
            cb=_on_msg,
        )
        log.info("Подписка оформлена: %s (queue=%s, durable=%s)", self.subject, self.queue, self.durable)

    async def ack(self, msg):
        await msg.ack()

    async def nak(self, msg, delay_s: Optional[int] = None):
        d = delay_s if delay_s is not None else self.nak_delay_s
        await msg.nak(delay=d if d and d > 0 else None)

    async def close(self):
        if self.nc:
            try:
                await self.nc.drain()
            except Exception:
                await self.nc.close()
            finally:
                self.nc = None
                self.js = None
                self.sub = None


async def setup_nats_trigger_and_bind(
    bot,
    registry,
    chat_id: int,
    push_daily_carousel: Callable,
    nats_url: Optional[str] = None,
    stream: Optional[str] = None,
    subject: Optional[str] = None,
    durable: Optional[str] = None,
    queue: Optional[str] = None,
    ack_wait_s: int = 300,
    max_deliver: int = 10,
    nak_delay_s: int = 300,
) -> NatsTrigger:
    nats_url = nats_url or os.getenv("NATS_URL", "nats://localhost:4222")
    stream = stream or os.getenv("NATS_STREAM", "TRIGGERS")
    subject = subject or os.getenv("NATS_SUBJECT", "triggers.daily")
    durable = durable or os.getenv("NATS_DURABLE", "tg-reports")
    queue = queue or os.getenv("NATS_QUEUE", "reports")

    trigger = NatsTrigger(
        nats_url=nats_url,
        stream=stream,
        subject=subject,
        durable=durable,
        queue=queue,
        ack_wait_s=ack_wait_s,
        max_deliver=max_deliver,
        nak_delay_s=nak_delay_s,
    )
    await trigger.connect()

    async def _handle(msg):
        try:
            await push_daily_carousel(bot, registry, chat_id)
            await trigger.ack(msg)
            log.info("Отчёт отправлен. Ack.")
        except Exception as e:
            log.warning("Не удалось отправить отчёт: %s — запросим редоставку", e)
            await trigger.nak(msg)

    await trigger.subscribe(_handle)
    return trigger


async def publish_trigger(
    nats_url: Optional[str] = None,
    stream: Optional[str] = None,
    subject: Optional[str] = None,
    payload: Optional[dict] = None,
):
    nats_url = nats_url or os.getenv("NATS_URL", "nats://localhost:4222")
    stream = stream or os.getenv("NATS_STREAM", "TRIGGERS")
    subject = subject or os.getenv("NATS_SUBJECT", "triggers.daily")

    nc = await nats.connect(nats_url, name="report-publisher")
    js = nc.jetstream()
    await ensure_stream(js, stream, subject)

    headers = {"Nats-Msg-Id": str(uuid.uuid4())}
    data = json.dumps(payload or {}).encode()

    ack = await js.publish(subject, data=data, headers=headers)
    logging.getLogger("nats-trigger").info(
        "Опубликован триггер: stream=%s subject=%s seq=%s", stream, subject, getattr(ack, "seq", None)
    )

    await nc.drain()

