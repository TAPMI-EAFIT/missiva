"""Listen and publish on the same session. Broker from ``examples/.env``."""

from __future__ import annotations

import asyncio

from envfile import ENV_FILE
from missiva import Message, Missiva


async def main() -> None:
    """Listen for commands and publish telemetry on the same TCP session."""
    missiva = Missiva(_env_file=ENV_FILE, client_id="missiva-example-duplex")

    async with missiva:
        listen = asyncio.create_task(
            missiva.listen(
                "missiva/device/123/commands",
                on_message=lambda message: print("command", message.payload),
            )
        )
        await missiva.publish(
            Message(topic="missiva/device/123/telemetry", payload={"temp": 21.5})
        )
        await listen


if __name__ == "__main__":
    asyncio.run(main())
