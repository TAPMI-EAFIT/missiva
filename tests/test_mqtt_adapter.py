from __future__ import annotations

import json
import logging
from typing import Any, ClassVar, Self

import aiomqtt
import pytest
from pydantic import SecretStr

from missiva import Missiva
from missiva.adapters.mqtt import AioMqttBroker
from missiva.errors import BrokerReconnectExhausted


def _adapter(**kwargs: Any) -> AioMqttBroker:
    """Build the real MQTT adapter via ``Missiva``, for tests that stub ``aiomqtt.Client``."""
    broker = Missiva(**kwargs)._broker
    assert isinstance(broker, AioMqttBroker)
    return broker


class _FakeClient:
    """Stand-in for ``aiomqtt.Client``: records connect, publish, and subscribe."""

    instances: ClassVar[list[_FakeClient]] = []
    fail_times: ClassVar[int] = 0

    def __init__(self, **kwargs: object) -> None:
        """Store constructor kwargs and register this instance."""
        self.kwargs = kwargs
        self.entered = False
        self.published: list[tuple[str, bytes, int, bool]] = []
        self.subscriptions: list[tuple[str, int]] = []
        type(self).instances.append(self)

    async def __aenter__(self) -> Self:
        """Mark entered, or raise ``MqttError`` while ``fail_times`` remains positive."""
        if type(self).fail_times > 0:
            type(self).fail_times -= 1
            raise aiomqtt.MqttError("refused")
        self.entered = True
        return self

    async def __aexit__(self, *args: object) -> None:
        """Mark the fake session closed."""
        self.entered = False

    async def subscribe(self, topic: str, qos: int = 0) -> None:
        """Record a subscribe call."""
        self.subscriptions.append((topic, qos))

    async def publish(
        self, topic: str, payload: bytes, qos: int = 0, retain: bool = False
    ) -> None:
        """Record a publish call."""
        self.published.append((topic, payload, qos, retain))


@pytest.fixture(autouse=True)
def _reset_fake_client() -> None:
    """Clear recorded clients and connect-fail counts between tests."""
    _FakeClient.instances = []
    _FakeClient.fail_times = 0


async def test_connect_uses_single_session(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("missiva.adapters.mqtt.aiomqtt.Client", _FakeClient)
    broker = _adapter(host="mqtt.local", client_id="missiva-1")
    await broker.connect()
    assert len(_FakeClient.instances) == 1
    assert _FakeClient.instances[0].entered is True
    await broker.disconnect()


async def test_reconnect_retries_then_succeeds(monkeypatch: pytest.MonkeyPatch) -> None:
    _FakeClient.fail_times = 2
    monkeypatch.setattr("missiva.adapters.mqtt.aiomqtt.Client", _FakeClient)
    broker = _adapter(
        host="mqtt.local", reconnect_max_attempts=3, reconnect_delay_seconds=0
    )
    await broker.connect()
    assert len(_FakeClient.instances) == 3
    assert _FakeClient.instances[-1].entered is True
    await broker.disconnect()


async def test_reconnect_exhausted(monkeypatch: pytest.MonkeyPatch) -> None:
    _FakeClient.fail_times = 10
    monkeypatch.setattr("missiva.adapters.mqtt.aiomqtt.Client", _FakeClient)
    broker = _adapter(
        host="mqtt.local", reconnect_max_attempts=2, reconnect_delay_seconds=0
    )
    with pytest.raises(BrokerReconnectExhausted):
        await broker.connect()
    assert len(_FakeClient.instances) == 2


async def test_password_is_not_logged(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    monkeypatch.setattr("missiva.adapters.mqtt.aiomqtt.Client", _FakeClient)
    caplog.set_level(logging.DEBUG, logger="missiva.adapters.mqtt")
    broker = _adapter(
        host="mqtt.local", username="alice", password=SecretStr("super-secret")
    )
    await broker.connect()
    combined = "\n".join(record.getMessage() for record in caplog.records)
    assert "super-secret" not in combined
    assert "mqtt.local" in combined
    client = _FakeClient.instances[0]
    assert client.kwargs["password"] == "super-secret"
    await broker.disconnect()


async def test_publish_encodes_str_dict_and_bytes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("missiva.adapters.mqtt.aiomqtt.Client", _FakeClient)
    broker = _adapter(host="mqtt.local")
    await broker.connect()
    await broker.publish("switch", "ON")
    await broker.publish("sensor", {"n": 1})
    await broker.publish("blob", b"\xff\xfe")
    published = _FakeClient.instances[0].published
    assert published[0] == ("switch", b"ON", 1, False)
    assert published[1][0] == "sensor"
    assert published[1][1] == json.dumps({"n": 1}).encode()
    assert published[2] == ("blob", b"\xff\xfe", 1, False)
    await broker.disconnect()


async def test_publish_qos_and_retain_are_per_call(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("missiva.adapters.mqtt.aiomqtt.Client", _FakeClient)
    broker = _adapter(host="mqtt.local")
    await broker.connect()
    await broker.publish("switch", "ON", qos=0)
    await broker.publish("switch", "OFF", qos=2, retain=True)
    assert _FakeClient.instances[0].published == [
        ("switch", b"ON", 0, False),
        ("switch", b"OFF", 2, True),
    ]
    await broker.disconnect()


async def test_subscribe_qos_is_restored_on_reconnect(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("missiva.adapters.mqtt.aiomqtt.Client", _FakeClient)
    broker = _adapter(host="mqtt.local")
    await broker.connect()
    await broker.subscribe("sensors/#", qos=2)
    await broker.disconnect()
    await broker.connect()
    assert _FakeClient.instances[0].subscriptions == [("sensors/#", 2)]
    assert _FakeClient.instances[1].subscriptions == [("sensors/#", 2)]
    await broker.disconnect()
