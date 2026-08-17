"""Listen and publish the transformed payload. Broker from ``examples/.env``."""

from __future__ import annotations

import asyncio

from envfile import ENV_FILE
from missiva import Message, Missiva

IN_TOPIC = "missiva/sensors/raw/#"
OUT_TOPIC = "missiva/sensors/normalized"


async def main() -> None:
    """Listen on the raw filter and republish each message to the normalized topic."""
    missiva = Missiva(_env_file=ENV_FILE, client_id="missiva-example-pubsub")

    async def handle(message: Message) -> None:
        """Publish a JSON wrapper of the inbound topic and payload."""
        print("in", message.topic, message.payload)
        await missiva.publish(
            Message(
                topic=OUT_TOPIC,
                payload={"source": message.topic, "payload": message.payload},
            )
        )
        print("out", OUT_TOPIC)

    async with missiva:
        print(f"listening on {IN_TOPIC} → {OUT_TOPIC}")
        await missiva.listen(
            IN_TOPIC,
            idle_timeout=30.0,
            on_message=handle,
            on_timeout=lambda: print("idle"),
        )


if __name__ == "__main__":
    asyncio.run(main())
