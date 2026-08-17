from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator

from missiva import Message, Payload, QoS, Topic


class FakeBroker:
    """In-memory ``BrokerPort`` for tests. Publish is safe while ``messages()`` runs."""

    def __init__(self) -> None:
        """Start disconnected, with empty publish/subscribe logs and an inbound queue."""
        self.published: list[tuple[Topic, Payload, QoS, bool]] = []
        self.subscribed: list[Topic] = []
        self.subscribe_qos: dict[Topic, QoS] = {}
        self.connected = False
        self.connect_calls = 0
        self.disconnect_calls = 0
        self.publish_error: Exception | None = None
        self._incoming: asyncio.Queue[Message | None] = asyncio.Queue()
        self._lock = asyncio.Lock()
        self._ended = False

    async def connect(self) -> None:
        """Mark connected and allow ``messages()`` to run again after a close."""
        self.connect_calls += 1
        self.connected = True
        self._ended = False

    async def disconnect(self) -> None:
        """Mark disconnected and end the inbound iterator."""
        self.disconnect_calls += 1
        self.connected = False
        self.close_messages()

    async def publish(
        self, topic: Topic, payload: Payload, *, qos: QoS = 1, retain: bool = False
    ) -> None:
        """Append ``(topic, payload, qos, retain)`` or raise ``publish_error`` if set.

        Tests inject ``BrokerSessionError`` here to honor the port contract.
        """
        async with self._lock:
            if self.publish_error is not None:
                raise self.publish_error
            self.published.append((topic, payload, qos, retain))

    async def subscribe(self, topic: Topic, *, qos: QoS = 1) -> None:
        """Record ``topic`` and the QoS used for that subscribe."""
        self.subscribed.append(topic)
        self.subscribe_qos[topic] = qos

    async def messages(self) -> AsyncIterator[Message]:
        """Yield messages from ``push`` until ``close_messages`` sends a sentinel."""
        while True:
            item = await self._incoming.get()
            if item is None:
                return
            yield item

    async def push(self, message: Message) -> None:
        """Enqueue one inbound message for ``messages()`` / ``subscribe`` / ``listen``."""
        await self._incoming.put(message)

    def close_messages(self) -> None:
        """End ``messages()`` once. Further calls do nothing."""
        if self._ended:
            return
        self._ended = True
        self._incoming.put_nowait(None)
