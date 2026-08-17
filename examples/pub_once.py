"""Publish payloads on a reused MQTT session. Broker from ``examples/.env``."""

from __future__ import annotations

import asyncio

from envfile import ENV_FILE
from missiva import Message, Missiva


async def main() -> None:
    """Publish a JSON object and a string on the output topic, then disconnect."""
    missiva = Missiva(_env_file=ENV_FILE, client_id="missiva-example-pub")

    async with missiva:
        await missiva.publish(
            Message(topic="missiva/sensors/raw", payload={"celsius": 21.5}),
            qos=1,
            on_published=lambda: print("published"),
        )
        await missiva.publish(
            Message(topic="missiva/sensors/raw", payload="ON"),
            qos=0,
            retain=True,
        )


if __name__ == "__main__":
    asyncio.run(main())
