from __future__ import annotations

from missiva import Message
from tests.conftest import make_missiva
from tests.fakes import FakeBroker


async def test_transform_by_subscribe_then_publish(fake_broker: FakeBroker) -> None:
    missiva = make_missiva(fake_broker)
    await fake_broker.push(Message("in", {"v": 1}))
    async with missiva:
        inbound = await missiva.subscribe("in")
        assert inbound is not None
        await missiva.publish(Message(topic="out", payload={"echo": inbound.payload}))
    assert fake_broker.published == [("out", {"echo": {"v": 1}}, 1, False)]


async def test_listen_handler_publishes(fake_broker: FakeBroker) -> None:
    missiva = make_missiva(fake_broker)

    async def handle(message: Message) -> None:
        await missiva.publish(Message(topic="out", payload="ON"))
        fake_broker.close_messages()

    await fake_broker.push(Message("in", "toggle"))
    async with missiva:
        await missiva.listen("in", on_message=handle)
    assert fake_broker.published == [("out", "ON", 1, False)]


async def test_handler_can_skip_publish(fake_broker: FakeBroker) -> None:
    missiva = make_missiva(fake_broker)

    async def handle(message: Message) -> None:
        fake_broker.close_messages()

    await fake_broker.push(Message("in", {"v": 1}))
    async with missiva:
        await missiva.listen("in", on_message=handle)
    assert fake_broker.published == []
