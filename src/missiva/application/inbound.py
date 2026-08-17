from __future__ import annotations

import asyncio
from contextlib import suppress
from typing import Self

from missiva.domain.models import Message
from missiva.domain.ports import BrokerPort

_END = object()


class InboundPump:
    """Background reader of ``broker.messages()`` so idle waits can time out.

    One task fills a queue. ``next`` waits on that queue, so an idle timeout
    cancels only the wait, not the MQTT iterator.
    """

    def __init__(self, broker: BrokerPort) -> None:
        """Bind to ``broker``; the pump task starts on ``async with``."""
        self._broker = broker
        self._queue: asyncio.Queue[object] = asyncio.Queue()
        self._task: asyncio.Task[None] | None = None

    async def __aenter__(self) -> Self:
        """Start the background pump task."""
        self._task = asyncio.create_task(self._pump(), name="missiva-inbound")
        return self

    async def __aexit__(self, *args: object) -> None:
        """Cancel the pump and wait until it stops."""
        if self._task is None:
            return
        self._task.cancel()
        with suppress(asyncio.CancelledError):
            await self._task
        self._task = None

    async def _pump(self) -> None:
        """Copy broker messages onto the queue; always enqueue ``_END`` when done."""
        try:
            async for message in self._broker.messages():
                await self._queue.put(message)
        finally:
            await self._queue.put(_END)

    async def next(self, idle_timeout: float | None) -> Message | None:
        """Return the next message, or None if ``idle_timeout`` seconds elapse.

        Raises:
            StopAsyncIteration: the broker iterator finished (``_END``).
        """
        try:
            if idle_timeout is None:
                item = await self._queue.get()
            else:
                item = await asyncio.wait_for(self._queue.get(), timeout=idle_timeout)
        except TimeoutError:
            return None
        if item is _END:
            raise StopAsyncIteration
        return item  # type: ignore[return-value]
