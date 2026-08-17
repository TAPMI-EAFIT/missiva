from __future__ import annotations

import inspect
import json
import logging
from collections.abc import Callable
from typing import Any, TypeGuard

from pydantic import BaseModel, ValidationError

from missiva.domain.models import Message, Payload

logger = logging.getLogger(__name__)

type Callback = Callable[..., Any]


async def invoke(callback: Callback | None, *args: Any) -> Any:
    """Run ``callback(*args)`` if set. Awaits the result when it is awaitable.

    Returns:
        The callback result, or None when ``callback`` is None.
    """
    if callback is None:
        return None
    result = callback(*args)
    if inspect.isawaitable(result):
        return await result
    return result


def _is_model_class(payload_type: type[Payload]) -> TypeGuard[type[BaseModel]]:
    """True when ``payload_type`` is a Pydantic model class, not ``str``/``bytes``/``dict``."""
    return isinstance(payload_type, type) and issubclass(payload_type, BaseModel)


def parse_payload(
    message: Message, payload_type: type[Payload] | None
) -> Message | None:
    """Keep or convert ``message`` so its body matches ``payload_type``.

    ``None`` leaves the message unchanged. ``str``, ``bytes``, or ``dict`` keep
    the message only if the body is already that type. A model class parses a
    JSON object (or dumps another model first); invalid or non-object bodies
    are skipped.

    Returns:
        The same or a new ``Message``, or None when the body is rejected.
    """
    if payload_type is None:
        return message
    raw = message.payload
    if _is_model_class(payload_type):
        if isinstance(raw, payload_type):
            return message
        data = raw.model_dump() if isinstance(raw, BaseModel) else raw
        if not isinstance(data, dict):
            logger.warning("Skipping non-object payload on topic %s", message.topic)
            return None
        try:
            validated = payload_type.model_validate(data)
        except ValidationError:
            logger.warning("Skipping invalid payload on topic %s", message.topic)
            return None
        return Message(topic=message.topic, payload=validated)
    if isinstance(raw, payload_type):
        return message
    logger.warning(
        "Skipping payload on topic %s: expected %s",
        message.topic,
        getattr(payload_type, "__name__", payload_type),
    )
    return None


def prepare_outgoing(payload: Payload) -> dict[str, Any] | str | bytes:
    """Turn a publish payload into dict, str, or bytes for the MQTT encoder.

    ``str`` and ``bytes`` pass through. A Pydantic model is dumped. A dict is
    copied with string keys.

    Raises:
        TypeError: any other type.
    """
    if isinstance(payload, (bytes, str)):
        return payload
    if isinstance(payload, BaseModel):
        return payload.model_dump()
    if isinstance(payload, dict):
        return {str(key): value for key, value in payload.items()}
    raise TypeError("outgoing payload must be a dict, str, bytes, or pydantic model")


def encode_mqtt_payload(payload: Payload) -> bytes:
    """Encode a wire payload: dict → JSON UTF-8, str → UTF-8, bytes unchanged.

    Raises:
        TypeError: a Pydantic model or any other type (dump first with
            ``prepare_outgoing``).
    """
    if isinstance(payload, bytes):
        return payload
    if isinstance(payload, str):
        return payload.encode("utf-8")
    if isinstance(payload, dict):
        return json.dumps(payload).encode()
    raise TypeError(f"unsupported payload type: {type(payload)!r}")


def decode_mqtt_payload(raw: bytes) -> Payload:
    """Decode MQTT bytes: JSON object → dict, other UTF-8 → str, else bytes."""
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError:
        return raw
    try:
        decoded = json.loads(text)
    except json.JSONDecodeError:
        return text
    if isinstance(decoded, dict):
        return decoded
    return text
