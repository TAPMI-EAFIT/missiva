from __future__ import annotations

from pydantic import BaseModel

from missiva import Message
from tests.conftest import make_missiva
from tests.fakes import FakeBroker


async def test_request_delivers_ack(fake_broker: FakeBroker) -> None:
    replies: list[Message] = []
    missiva = make_missiva(fake_broker)
    await fake_broker.push(Message("device/123/ack", {"pong": True}))
    ack = await missiva.request(
        Message(topic="device/123/cmd", payload={"ping": True}),
        ack="device/123/ack",
        on_message=lambda message: replies.append(message),
    )
    assert fake_broker.subscribed == ["device/123/ack"]
    assert fake_broker.published == [("device/123/cmd", {"ping": True}, 1, False)]
    assert ack is not None
    assert ack.payload == {"pong": True}
    assert [message.payload for message in replies] == [{"pong": True}]


async def test_request_timeout(fake_broker: FakeBroker) -> None:
    timed_out: list[str] = []
    missiva = make_missiva(fake_broker)
    ack = await missiva.request(
        Message(topic="device/123/cmd", payload={"ping": True}),
        ack="device/123/ack",
        idle_timeout=0.01,
        on_timeout=lambda: timed_out.append("no-ack"),
    )
    assert fake_broker.published == [("device/123/cmd", {"ping": True}, 1, False)]
    assert ack is None
    assert timed_out == ["no-ack"]


async def test_request_qos_and_ack_qos_are_independent(
    fake_broker: FakeBroker,
) -> None:
    missiva = make_missiva(fake_broker)
    await fake_broker.push(Message("device/123/ack", {"pong": True}))
    await missiva.request(
        Message(topic="device/123/cmd", payload={"ping": True}),
        ack="device/123/ack",
        qos=0,
        retain=True,
        ack_qos=2,
    )
    assert fake_broker.subscribe_qos == {"device/123/ack": 2}
    assert fake_broker.published == [("device/123/cmd", {"ping": True}, 0, True)]


async def test_request_models_are_per_call(fake_broker: FakeBroker) -> None:
    class Ping(BaseModel):
        ping: bool

    class Pong(BaseModel):
        pong: bool

    missiva = make_missiva(fake_broker)
    await fake_broker.push(Message("device/123/ack", {"pong": True}))
    ack = await missiva.request(
        Message(topic="device/123/cmd", payload=Ping(ping=True)),
        ack="device/123/ack",
        payload=Pong,
    )
    assert fake_broker.published == [("device/123/cmd", {"ping": True}, 1, False)]
    assert ack is not None
    assert isinstance(ack.payload, Pong)
    assert ack.payload.pong is True
