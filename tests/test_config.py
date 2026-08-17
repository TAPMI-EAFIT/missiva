from __future__ import annotations

from pathlib import Path

import pytest
from pydantic import SecretStr

from missiva import Missiva, Payload, Topic
from missiva.domain import exceptions as domain_exceptions
from missiva.errors import BrokerReconnectExhausted, BrokerSessionError, MissivaError


def test_topic_and_payload_are_public_domain_aliases() -> None:
    topic: Topic = "sensors/temp"
    payload: Payload = "ON"
    assert topic == "sensors/temp"
    assert payload == "ON"


def test_public_exceptions_are_the_domain_port_errors() -> None:
    assert MissivaError is domain_exceptions.MissivaError
    assert BrokerSessionError is domain_exceptions.BrokerSessionError
    assert BrokerReconnectExhausted is domain_exceptions.BrokerReconnectExhausted
    assert issubclass(BrokerSessionError, MissivaError)
    assert issubclass(BrokerReconnectExhausted, BrokerSessionError)


def test_secret_str_is_not_in_repr() -> None:
    missiva = Missiva(host="mqtt.local", password=SecretStr("s3cret-value"))
    assert isinstance(missiva.password, SecretStr)
    assert missiva.password.get_secret_value() == "s3cret-value"
    assert "s3cret-value" not in repr(missiva)
    assert "s3cret-value" not in str(missiva)


def test_missiva_reads_unset_fields_from_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MISSIVA_BROKER_HOST", "mqtt.env")
    monkeypatch.setenv("MISSIVA_BROKER_PASSWORD", "from-env")
    monkeypatch.setenv("MISSIVA_BROKER_PORT", "8883")
    missiva = Missiva()
    assert missiva.host == "mqtt.env"
    assert missiva.port == 8883
    assert missiva.password is not None
    assert missiva.password.get_secret_value() == "from-env"


def test_missiva_constructor_overrides_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MISSIVA_BROKER_HOST", "from-env")
    monkeypatch.setenv("MISSIVA_BROKER_PASSWORD", "from-env")
    missiva = Missiva(host="from-code")
    assert missiva.host == "from-code"
    assert missiva.password is not None
    assert missiva.password.get_secret_value() == "from-env"


def test_missiva_reads_broker_fields_from_env_file(tmp_path: Path) -> None:
    env = tmp_path / "broker.env"
    env.write_text("MISSIVA_BROKER_HOST=mqtt.file\nMISSIVA_BROKER_PORT=8883\n")
    missiva = Missiva(_env_file=env)
    assert missiva.host == "mqtt.file"
    assert missiva.port == 8883


def test_missiva_does_not_include_qos() -> None:
    assert "qos" not in Missiva.model_fields


def test_missiva_takes_broker_fields_directly() -> None:
    missiva = Missiva(host="mqtt.local", port=8883)
    assert missiva.host == "mqtt.local"
    assert missiva.port == 8883
