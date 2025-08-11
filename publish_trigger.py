import argparse
import asyncio
import json
import logging
from nats_trigger import publish_trigger

logging.basicConfig(level=logging.INFO)


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--nats", dest="nats_url", default=None)
    p.add_argument("--stream", default=None)
    p.add_argument("--subject", default=None)
    p.add_argument("--json", dest="payload", default=None, help="JSON-пейлоад (опционально)")
    return p.parse_args()


async def main():
    args = parse_args()
    payload = json.loads(args.payload) if args.payload else None
    await publish_trigger(
        nats_url=args.nats_url,
        stream=args.stream,
        subject=args.subject,
        payload=payload,
    )


if __name__ == "__main__":
    asyncio.run(main())

