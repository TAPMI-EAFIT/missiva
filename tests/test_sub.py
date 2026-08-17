from __future__ import annotations

from pydantic import BaseModel, Field

from missiva import Message
from tests.conftest import make_missiva
from tests.fakes import FakeBroker


async def test_subscribe_returns_one_message(fake_broker: FakeBroker) -> None:
    received: list[Message] = []
    missiva = make_missiva(fake_broker)
    await fake_broker.push(Message("sensors/1", {"c": 18}))
    message = await missiva.subscribe(
        "sensors/#",
        on_message=lambda msg: received.append(msg),
    )
    assert message is not None
    assert message.payload == {"c": 18}
    assert [item.payload for item in received] == [{"c": 18}]
    assert fake_broker.subscribed == ["sensors/#"]
    assert fake_broker.subscribe_qos == {"sensors/#": 1}


async def test_subscribe_timeout(fake_broker: FakeBroker) -> None:
    timed_out: list[str] = []
    missiva = make_missiva(fake_broker)
    message = await missiva.subscribe(
        "sensors/#",
        idle_timeout=0.01,
        on_timeout=lambda: timed_out.append("idle"),
    )
    assert message is None
    assert timed_out == ["idle"]


async def test_listen_timeout_then_message(fake_broker: FakeBroker) -> None:
    events: list[str] = []

    async def on_timeout() -> None:
        events.append("timeout")
        await fake_broker.push(Message("sensors/1", {"c": 22}))

    async def on_message(message: Message) -> None:
        events.append("message")
        fake_broker.close_messages()

    missiva = make_missiva(fake_broker)
    await missiva.listen(
        "sensors/#",
        idle_timeout=0.01,
        on_timeout=on_timeout,
        on_message=on_message,
    )
    assert events == ["timeout", "message"]


async def test_subscribe_qos_is_per_call(fake_broker: FakeBroker) -> None:
    missiva = make_missiva(fake_broker)
    async with missiva:
        await fake_broker.push(Message("sensors/1", "ON"))
        await missiva.subscribe("sensors/#", qos=2)
        await fake_broker.push(Message("alerts/1", "x"))
        await missiva.subscribe("alerts/#", qos=0)
    assert fake_broker.subscribe_qos == {"sensors/#": 2, "alerts/#": 0}


async def test_subscribe_payload_type_is_per_call(fake_broker: FakeBroker) -> None:
    class Reading(BaseModel):
        celsius: float = Field(gt=-50)

    class Switch(BaseModel):
        on: bool

    missiva = make_missiva(fake_broker)
    async with missiva:
        await fake_broker.push(Message("sensors/1", {"celsius": 18.0}))
        reading = await missiva.subscribe("sensors/#", payload=Reading)
        await fake_broker.push(Message("switch/1", {"on": True}))
        switch = await missiva.subscribe("switch/#", payload=Switch)
        await fake_broker.push(Message("sensors/1", {"celsius": -99}))
        skipped = await missiva.subscribe("sensors/#", payload=Reading)
    assert reading is not None
    assert isinstance(reading.payload, Reading)
    assert reading.payload.celsius == 18.0
    assert switch is not None
    assert isinstance(switch.payload, Switch)
    assert switch.payload.on is True
    assert skipped is None


async def test_subscribe_payload_str_and_dict(fake_broker: FakeBroker) -> None:
    missiva = make_missiva(fake_broker)
    async with missiva:
        await fake_broker.push(Message("switch/1", "ON"))
        text = await missiva.subscribe("switch/#", payload=str)
        await fake_broker.push(Message("sensors/1", {"c": 18}))
        obj = await missiva.subscribe("sensors/#", payload=dict)
        await fake_broker.push(Message("switch/1", {"on": True}))
        skipped = await missiva.subscribe("switch/#", payload=str)
    assert text is not None
    assert text.payload == "ON"
    assert obj is not None
    assert obj.payload == {"c": 18}
    assert skipped is None
