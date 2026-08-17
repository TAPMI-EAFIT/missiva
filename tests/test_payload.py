from __future__ import annotations

import json

from pydantic import BaseModel, Field

from missiva.application.payload import (
    decode_mqtt_payload,
    encode_mqtt_payload,
    parse_payload,
    prepare_outgoing,
)
from missiva.domain.models import Message


class _Reading(BaseModel):
    celsius: float = Field(gt=-50)


def test_encode_str_is_raw_utf8_not_json_quoted() -> None:
    assert encode_mqtt_payload("ON") == b"ON"
    assert encode_mqtt_payload("ON") != json.dumps("ON").encode()


def test_encode_dict_is_json_utf8() -> None:
    assert (
        encode_mqtt_payload({"celsius": 21.5}) == json.dumps({"celsius": 21.5}).encode()
    )


def test_encode_bytes_are_unchanged() -> None:
    raw = b"\xff\xfe\x00"
    assert encode_mqtt_payload(raw) is raw


def test_decode_json_object_to_dict() -> None:
    assert decode_mqtt_payload(b'{"celsius": 21.5}') == {"celsius": 21.5}


def test_decode_utf8_text_that_is_not_json_object_stays_str() -> None:
    assert decode_mqtt_payload(b"ON") == "ON"
    assert decode_mqtt_payload(b'"ON"') == '"ON"'
    assert decode_mqtt_payload(b"[1, 2]") == "[1, 2]"
    assert decode_mqtt_payload(b"42") == "42"


def test_decode_non_utf8_stays_bytes() -> None:
    raw = b"\xff\xfe\x00"
    assert decode_mqtt_payload(raw) == raw


def test_prepare_outgoing_accepts_str_bytes_dict_and_model() -> None:
    assert prepare_outgoing("ON") == "ON"
    assert prepare_outgoing(b"\xff") == b"\xff"
    assert prepare_outgoing({"celsius": 21.5}) == {"celsius": 21.5}
    assert prepare_outgoing(_Reading(celsius=21.5)) == {"celsius": 21.5}


def test_parse_payload_without_type_leaves_message() -> None:
    message = Message(topic="t", payload={"celsius": 18.0})
    assert parse_payload(message, None) is message


def test_parse_payload_keeps_str_bytes_and_dict() -> None:
    text = Message(topic="cmd", payload="ON")
    binary = Message(topic="cmd", payload=b"\xff")
    obj = Message(topic="t", payload={"celsius": 18.0})
    assert parse_payload(text, str) is text
    assert parse_payload(binary, bytes) is binary
    assert parse_payload(obj, dict) is obj
    assert parse_payload(text, dict) is None
    assert parse_payload(obj, str) is None
    assert parse_payload(text, bytes) is None


def test_parse_payload_builds_model_from_dict() -> None:
    model = _Reading
    text = Message(topic="cmd", payload="ON")
    binary = Message(topic="cmd", payload=b"\xff")
    assert parse_payload(text, model) is None
    assert parse_payload(binary, model) is None
    adapted = parse_payload(Message(topic="t", payload={"celsius": 18.0}), model)
    assert adapted is not None
    assert isinstance(adapted.payload, _Reading)
    assert adapted.payload.celsius == 18.0
    assert parse_payload(Message(topic="t", payload={"celsius": -99}), model) is None
