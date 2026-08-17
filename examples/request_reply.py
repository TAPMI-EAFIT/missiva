"""Publish a command and wait for one ACK. Broker from ``examples/.env``."""

from __future__ import annotations

import asyncio

from envfile import ENV_FILE
from missiva import Message, Missiva


async def main() -> None:
    """Publish a ping on the command topic and wait for one ACK."""
    missiva = Missiva(_env_file=ENV_FILE, client_id="missiva-example-request")
    reply = await missiva.request(
        Message(topic="missiva/device/123/cmd", payload={"ping": True}),
        ack="missiva/device/123/ack",
        idle_timeout=5.0,
        on_message=lambda message: print("ack", message.payload),
        on_timeout=lambda: print("no ack"),
    )
    if reply is None:
        return


if __name__ == "__main__":
    asyncio.run(main())
