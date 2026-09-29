"""Creates BOOTH_EVENTS exactly as booth-core does at startup (internal/eventbus/nats.go), with
core's own credential. Test tooling for hack/docker-compose.yml."""

import asyncio
import os

import nats
from nats.js.api import RetentionPolicy, StorageType, StreamConfig


async def main() -> None:
    nc = await nats.connect(os.environ.get("NATS_URL", "nats://nats:4222"), user_credentials=os.environ["NATS_CREDS"])
    js = nc.jetstream()
    cfg = StreamConfig(name="BOOTH_EVENTS", subjects=["booth.>"], retention=RetentionPolicy.LIMITS, max_age=7 * 24 * 3600, storage=StorageType.FILE)
    try:
        await js.add_stream(cfg)
    except Exception:  # already there from an earlier run: update to the same config
        await js.update_stream(cfg)
    await nc.close()
    print("BOOTH_EVENTS ready")


asyncio.run(main())
