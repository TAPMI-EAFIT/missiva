from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Self, cast

from pydantic import Field, PrivateAttr, SecretStr, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from missiva.adapters.mqtt import AioMqttBroker
from missiva.application.inbound import InboundPump
from missiva.application.payload import (
    Callback,
    invoke,
    parse_payload,
    prepare_outgoing,
)
from missiva.domain.exceptions import BrokerSessionError
from missiva.domain.models import Message, Payload, QoS, Topic
from missiva.domain.ports import BrokerPort


class Missiva(BaseSettings):
    """MQTT session: connection settings plus publish / subscribe / listen / request.

    Omitted fields are read from ``MISSIVA_BROKER_*`` or ``_env_file``. Constructor
    keywords win over the environment. Extra env keys are ignored.
    """

    model_config = SettingsConfigDict(extra="ignore", env_prefix="MISSIVA_BROKER_")

    host: str = Field(min_length=1)
    port: int = Field(default=1883, ge=1, le=65535)
    username: str | None = None
    password: SecretStr | None = None
    client_id: str | None = None
    keepalive: int | None = Field(default=None, ge=1, le=65535)
    clean_session: bool = True
    reconnect_max_attempts: int = Field(default=10, ge=1)
    reconnect_delay_seconds: float = Field(default=5.0, ge=0)

    _broker: BrokerPort = PrivateAttr()
    _connected: bool = PrivateAttr(default=False)
    _in_context: bool = PrivateAttr(default=False)

    @model_validator(mode="after")
    def persistent_session_needs_a_client_id(self) -> Self:
        """A persistent session is stored on the broker under ``client_id``."""
        if not self.clean_session and not self.client_id:
            raise ValueError("A persistent session requires client_id.")
        return self

    def model_post_init(self, context: object) -> None:
        """Wire the MQTT adapter after settings are loaded. Unused ``context`` is Pydantic's."""
        self._broker = AioMqttBroker(self)

    @classmethod
    def _bind(cls, port: BrokerPort) -> Self:
        """Build a session that uses ``port`` instead of the MQTT adapter (tests)."""
        session = cast(Self, cls.model_construct(host="unused"))
        session._broker = port
        session._connected = False
        session._in_context = False
        return session

    async def __aenter__(self) -> Self:
        """Connect and keep the TCP session open until ``__aexit__``."""
        self._in_context = True
        await self._ensure_connected()
        return self

    async def __aexit__(self, *args: object) -> None:
        """Disconnect and clear the reused-session flag."""
        await self.aclose()

    async def aclose(self) -> None:
        """Disconnect now. Safe to call if already closed."""
        self._connected = False
        self._in_context = False
        await self._broker.disconnect()

    async def _ensure_connected(self) -> None:
        """Connect once; later calls are no-ops until ``aclose``."""
        if not self._connected:
            await self._broker.connect()
            self._connected = True

    @asynccontextmanager
    async def _session(self) -> AsyncIterator[None]:
        """Connect for one operation; disconnect after unless inside ``async with``."""
        owned = not self._connected
        await self._ensure_connected()
        try:
            yield
        finally:
            if owned and not self._in_context:
                await self.aclose()

    async def publish(
        self,
        message: Message,
        *,
        qos: QoS = 1,
        retain: bool = False,
        on_published: Callback | None = None,
        on_publish_failed: Callback | None = None,
    ) -> bool:
        """Publish ``message`` to its topic.

        A Pydantic payload is dumped to a dict, then encoded (dict → JSON,
        str → UTF-8, bytes unchanged). ``qos`` and ``retain`` apply only to
        this call. Reuses ``async with``; otherwise connects for this call.

        ``on_published`` runs after a successful send. Unexpected errors call
        ``on_publish_failed`` and return False. ``BrokerSessionError`` and
        cancellation propagate.

        Returns:
            True if the broker accepted the publish.

        Raises:
            BrokerSessionError: no session, or the broker rejected the send.
            BrokerReconnectExhausted: connect gave up before the send.
        """
        async with self._session():
            try:
                outgoing = prepare_outgoing(message.payload)
                await self._broker.publish(
                    message.topic, outgoing, qos=qos, retain=retain
                )
            except asyncio.CancelledError:
                raise
            except BrokerSessionError:
                raise
            except Exception as exc:  # noqa: BLE001
                await invoke(on_publish_failed, exc)
                return False
            await invoke(on_published)
            return True

    async def subscribe(
        self,
        topic: Topic,
        *,
        qos: QoS = 1,
        idle_timeout: float | None = None,
        payload: type[Payload] | None = None,
        on_message: Callback | None = None,
        on_timeout: Callback | None = None,
    ) -> Message | None:
        """Wait for one inbound message on ``topic``.

        ``qos`` is the maximum QoS this client will accept on that topic.
        ``payload`` keeps only matching bodies (``str``, ``bytes``, ``dict``,
        or a model class parsed from JSON); others are skipped. ``idle_timeout``
        seconds with no message calls ``on_timeout`` and returns None.
        ``on_message`` runs with the accepted message.

        Returns:
            The message, or None on timeout, skip, or a closed inbound stream.

        Raises:
            BrokerSessionError: subscribe failed, or there is no session.
            BrokerReconnectExhausted: connect gave up, or a drop could not be recovered.
        """
        async with self._session():
            await self._broker.subscribe(topic, qos=qos)
            async with InboundPump(self._broker) as inbound:
                try:
                    return await self._deliver(
                        inbound,
                        idle_timeout,
                        on_message,
                        on_timeout,
                        payload,
                    )
                except StopAsyncIteration:
                    return None

    async def listen(
        self,
        topic: Topic,
        *,
        qos: QoS = 1,
        idle_timeout: float | None = None,
        payload: type[Payload] | None = None,
        on_message: Callback | None = None,
        on_timeout: Callback | None = None,
    ) -> None:
        """Deliver inbound messages on ``topic`` until the stream ends or cancel.

        Same ``qos`` / ``payload`` / ``idle_timeout`` rules as ``subscribe``.
        Each idle timeout calls ``on_timeout`` and then waits again. Returns
        when the broker stream ends.

        Raises:
            BrokerSessionError: subscribe failed, or there is no session.
            BrokerReconnectExhausted: connect gave up, or a drop could not be recovered.
        """
        async with self._session():
            await self._broker.subscribe(topic, qos=qos)
            async with InboundPump(self._broker) as inbound:
                while True:
                    try:
                        await self._deliver(
                            inbound,
                            idle_timeout,
                            on_message,
                            on_timeout,
                            payload,
                        )
                    except StopAsyncIteration:
                        return

    async def request(
        self,
        message: Message,
        *,
        ack: Topic,
        qos: QoS = 1,
        retain: bool = False,
        ack_qos: QoS = 1,
        idle_timeout: float | None = None,
        payload: type[Payload] | None = None,
        on_message: Callback | None = None,
        on_timeout: Callback | None = None,
        on_published: Callback | None = None,
        on_publish_failed: Callback | None = None,
    ) -> Message | None:
        """Subscribe to ``ack``, publish ``message``, wait for one reply.

        Correlation is by topic (the ACK filter), not MQTT 5 correlation data.
        ``qos`` / ``retain`` apply to the command; ``ack_qos`` to the subscribe.
        ``payload`` filters or parses the reply the same way as ``subscribe``.

        Returns:
            The reply, or None if publish failed, the reply was skipped, timed
            out, or the inbound stream closed.

        Raises:
            BrokerSessionError: subscribe or publish failed, or there is no session.
            BrokerReconnectExhausted: connect gave up, or a drop could not be recovered.
        """
        async with self._session():
            await self._broker.subscribe(ack, qos=ack_qos)
            async with InboundPump(self._broker) as inbound:
                published = await self.publish(
                    message,
                    qos=qos,
                    retain=retain,
                    on_published=on_published,
                    on_publish_failed=on_publish_failed,
                )
                if not published:
                    return None
                try:
                    return await self._deliver(
                        inbound,
                        idle_timeout,
                        on_message,
                        on_timeout,
                        payload,
                    )
                except StopAsyncIteration:
                    return None

    async def _deliver(
        self,
        inbound: InboundPump,
        idle_timeout: float | None,
        on_message: Callback | None,
        on_timeout: Callback | None,
        payload: type[Payload] | None,
    ) -> Message | None:
        """Take one item from ``inbound``, filter it, and run the callbacks.

        Returns None on idle timeout (after ``on_timeout``) or if ``payload``
        rejects the body. Raises ``StopAsyncIteration`` when the pump ends.
        """
        message = await inbound.next(idle_timeout)
        if message is None:
            await invoke(on_timeout)
            return None
        adapted = parse_payload(message, payload)
        if adapted is None:
            return None
        await invoke(on_message, adapted)
        return adapted
