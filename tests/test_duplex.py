from __future__ import annotations

import asyncio

from missiva import Message
from tests.conftest import make_missiva
from tests.fakes import FakeBroker


async def test_listen_and_publish_on_same_session(fake_broker: FakeBroker) -> None:
    commands: list[Message] = []
    got_command = asyncio.Event()
    missiva = make_missiva(fake_broker)

    async def on_command(message: Message) -> None:
        commands.append(message)
        got_command.set()

    async with missiva:
        listen = asyncio.create_task(
            missiva.listen("device/123/commands", on_message=on_command)
        )
        await asyncio.sleep(0)
        await missiva.publish(
            Message(topic="device/123/telemetry", payload={"temp": 21.5})
        )
        await fake_broker.push(Message("device/123/commands", {"set": 19}))
        await got_command.wait()
        fake_broker.close_messages()
        await listen

    assert fake_broker.subscribed == ["device/123/commands"]
    assert fake_broker.published == [("device/123/telemetry", {"temp": 21.5}, 1, False)]
    assert [message.payload for message in commands] == [{"set": 19}]
