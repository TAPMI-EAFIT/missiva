"""Domain language: MQTT value types."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal

from pydantic import BaseModel

# Topic to publish or subscribe to, e.g. ``sensors/temp`` or ``device/+/cmd``.
type Topic = str

# Body of a message: JSON object, text, binary, or a Pydantic model.
type Payload = dict[str, Any] | str | bytes | BaseModel

# MQTT Quality of Service for one publish or subscribe.
#
# * ``0`` — at most once: send and forget. The broker may drop the message.
# * ``1`` — at least once: the receiver ACKs. Duplicates are possible.
# * ``2`` — exactly once: handshake so the payload is delivered once. Slowest.
#
# On publish it is the delivery guarantee for that message. On subscribe /
# listen it is the **maximum** QoS this client will accept on that topic;
# the broker may still send a lower QoS.
type QoS = Literal[0, 1, 2]


@dataclass(frozen=True, slots=True)
class Message:
    """One MQTT application message: destination or origin ``topic``, plus ``payload``."""

    topic: Topic
    payload: Payload
