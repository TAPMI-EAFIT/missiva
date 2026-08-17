from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Protocol

from missiva.domain.exceptions import BrokerReconnectExhausted, BrokerSessionError
from missiva.domain.models import Message, Payload, QoS, Topic

__all__ = ["BrokerPort", "BrokerReconnectExhausted", "BrokerSessionError"]


class BrokerPort(Protocol):
    """Broker session used by the application.

    Implementations must not leak driver types (for example aiomqtt). Failures
    are ``BrokerSessionError`` and ``BrokerReconnectExhausted``.
    """

    async def connect(self) -> None:
        """Open a broker session. No-op if already open, unless the adapter retries.

        Raises:
            BrokerReconnectExhausted: consecutive connect attempts reached the limit.
        """

    async def disconnect(self) -> None:
        """Close the session if it is open. Must be safe to call twice."""

    async def publish(
        self, topic: Topic, payload: Payload, *, qos: QoS = 1, retain: bool = False
    ) -> None:
        """Send one payload. Must be safe to call while ``messages()`` is consumed.

        Raises:
            BrokerSessionError: no session, or the send failed.
        """

    async def subscribe(self, topic: Topic, *, qos: QoS = 1) -> None:
        """Subscribe to ``topic`` at ``qos`` and remember it across reconnects.

        Raises:
            BrokerSessionError: no session, or the subscribe failed.
        """

    def messages(self) -> AsyncIterator[Message]:
        """Yield inbound messages for the current subscriptions until the session ends.

        Raises:
            BrokerReconnectExhausted: a dropped session could not be reopened.
        """
