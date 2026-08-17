"""Listen until cancelled. Broker from ``examples/.env``."""

from __future__ import annotations

import asyncio

from envfile import ENV_FILE
from missiva import Missiva


async def main() -> None:
    """Listen on the input topic until cancelled; print each payload and idle timeouts."""
    missiva = Missiva(_env_file=ENV_FILE, client_id="missiva-example-sub")
    await missiva.listen(
        "missiva/sensors/#",
        idle_timeout=30.0,
        on_message=lambda message: print(message.topic, message.payload),
        on_timeout=lambda: print("idle"),
    )


if __name__ == "__main__":
    asyncio.run(main())
