from __future__ import annotations

import pytest
from pydantic import BaseModel

from missiva import Message
from missiva.errors import BrokerSessionError
from tests.conftest import make_missiva
from tests.fakes import FakeBroker


async def test_publish_one_shot_string_and_bytes(fake_broker: FakeBroker) -> None:
    missiva = make_missiva(fake_broker)
    assert await missiva.publish(Message(topic="switch/power", payload="ON"))
    assert await missiva.publish(Message(topic="blob", payload=b"\xff\xfe"))
    assert fake_broker.published == [
        ("switch/power", "ON", 1, False),
        ("blob", b"\xff\xfe", 1, False),
    ]
    assert fake_broker.connect_calls == 2
    assert fake_broker.disconnect_calls == 2


async def test_publish_reuses_session(fake_broker: FakeBroker) -> None:
    published: list[str] = []
    missiva = make_missiva(fake_broker)
    async with missiva:
        await missiva.publish(
            Message(topic="sensors/temp", payload={"celsius": 21.5}),
            on_published=lambda: published.append("ok"),
        )
        await missiva.publish(Message(topic="sensors/temp", payload="ON"))
    assert fake_broker.published == [
        ("sensors/temp", {"celsius": 21.5}, 1, False),
        ("sensors/temp", "ON", 1, False),
    ]
    assert published == ["ok"]
    assert fake_broker.connect_calls == 1
    assert fake_broker.disconnect_calls == 1


async def test_publish_failure_invokes_callback(fake_broker: FakeBroker) -> None:
    failures: list[BaseException] = []
    fake_broker.publish_error = RuntimeError("broker down")
    missiva = make_missiva(fake_broker)
    ok = await missiva.publish(
        Message(topic="out", payload={"x": 1}),
        on_publish_failed=lambda exc: failures.append(exc),
    )
    assert ok is False
    assert fake_broker.published == []
    assert str(failures[0]) == "broker down"


async def test_publish_session_error_propagates(fake_broker: FakeBroker) -> None:
    fake_broker.publish_error = BrokerSessionError("dropped")
    missiva = make_missiva(fake_broker)
    with pytest.raises(BrokerSessionError):
        await missiva.publish(Message(topic="out", payload={"x": 1}))


async def test_publish_qos_and_retain_are_per_call(fake_broker: FakeBroker) -> None:
    missiva = make_missiva(fake_broker)
    async with missiva:
        await missiva.publish(Message(topic="switch/power", payload="ON"), qos=0)
        await missiva.publish(
            Message(topic="switch/power", payload="OFF"), qos=2, retain=True
        )
    assert fake_broker.published == [
        ("switch/power", "ON", 0, False),
        ("switch/power", "OFF", 2, True),
    ]
    assert fake_broker.connect_calls == 1
    assert fake_broker.disconnect_calls == 1


async def test_publish_dumps_pydantic_payload(fake_broker: FakeBroker) -> None:
    class Reading(BaseModel):
        celsius: float

    class Switch(BaseModel):
        on: bool

    missiva = make_missiva(fake_broker)
    async with missiva:
        await missiva.publish(
            Message(topic="sensors/temp", payload=Reading(celsius=21.5))
        )
        await missiva.publish(Message(topic="switch/power", payload=Switch(on=True)))
    assert fake_broker.published == [
        ("sensors/temp", {"celsius": 21.5}, 1, False),
        ("switch/power", {"on": True}, 1, False),
    ]
