from __future__ import annotations

import asyncio
import logging
import sys
from collections.abc import AsyncIterator
from typing import TYPE_CHECKING, Any

import aiomqtt

from missiva.application.payload import decode_mqtt_payload, encode_mqtt_payload
from missiva.domain.exceptions import BrokerReconnectExhausted, BrokerSessionError
from missiva.domain.models import Message, Payload, QoS, Topic

if TYPE_CHECKING:
    from missiva.application.missiva import Missiva

logger = logging.getLogger(__name__)

_RECONNECTABLE = (
    aiomqtt.MqttError,
    ConnectionError,
    TimeoutError,
    OSError,
    BrokerSessionError,
)


class AioMqttBroker:
    """MQTT 3.1.1 client behind ``BrokerPort``.

    One ``aiomqtt.Client`` per connect attempt. Publish is serialized with a
    lock so it can run while ``messages()`` is consumed. Dropped sessions
    reconnect and restore remembered subscriptions. Driver errors become
    ``BrokerSessionError`` / ``BrokerReconnectExhausted``.
    """

    def __init__(self, session: Missiva) -> None:
        """Keep host, credentials, and reconnect settings from ``session``."""
        self._session = session
        self._client: aiomqtt.Client | None = None
        self._lock = asyncio.Lock()
        self._subscriptions: dict[Topic, QoS] = {}
        self._closed = False

    def _build_client(self) -> aiomqtt.Client:
        """Build an unconnected MQTT 3.1.1 client from the session settings."""
        kwargs: dict[str, Any] = {
            "hostname": self._session.host,
            "port": self._session.port,
            "protocol": aiomqtt.ProtocolVersion.V311,
            "keepalive": 0
            if self._session.keepalive is None
            else self._session.keepalive,
            "clean_session": self._session.clean_session,
        }
        if self._session.client_id is not None:
            kwargs["identifier"] = self._session.client_id
        if self._session.username is not None:
            kwargs["username"] = self._session.username
        if self._session.password is not None:
            kwargs["password"] = self._session.password.get_secret_value()
        return aiomqtt.Client(**kwargs)

    def _log_connecting(self) -> None:
        """Log host, port, client id, and whether auth is set. Never logs the password."""
        logger.info(
            "Connecting to MQTT broker %s:%s client_id=%s auth=%s",
            self._session.host,
            self._session.port,
            self._session.client_id or "<auto>",
            "yes" if self._session.username or self._session.password else "no",
        )

    async def connect(self) -> None:
        """Open a session, retrying until success or ``reconnect_max_attempts``."""
        self._closed = False
        await self._connect_with_retry()

    async def _connect_with_retry(self) -> None:
        """Connect, waiting ``reconnect_delay_seconds`` between failures.

        Raises:
            BrokerReconnectExhausted: consecutive failures reached the limit.
        """
        consecutive = 0
        max_attempts = self._session.reconnect_max_attempts
        while True:
            self._log_connecting()
            client = self._build_client()
            try:
                await client.__aenter__()
            except asyncio.CancelledError:
                await self._abandon_client(client)
                raise
            except _RECONNECTABLE as exc:
                await self._abandon_client(client)
                consecutive += 1
                logger.warning(
                    "MQTT connection failed (%s/%s) to %s:%s: %s",
                    consecutive,
                    max_attempts,
                    self._session.host,
                    self._session.port,
                    exc,
                )
                if consecutive >= max_attempts:
                    raise BrokerReconnectExhausted(
                        f"Gave up connecting to {self._session.host}:{self._session.port} "
                        f"after {max_attempts} attempt(s)"
                    ) from exc
                await asyncio.sleep(self._session.reconnect_delay_seconds)
                continue
            self._client = client
            await self._restore_subscriptions()
            return

    async def _abandon_client(self, client: aiomqtt.Client) -> None:
        """Close a client that failed to connect. Errors here are ignored."""
        try:
            await client.__aexit__(*sys.exc_info())
        except Exception:
            logger.debug(
                "Ignored error while abandoning a failed MQTT client", exc_info=True
            )

    async def _restore_subscriptions(self) -> None:
        """Re-subscribe every remembered topic at its stored QoS on the current client."""
        if self._client is None:
            return
        for topic, qos in self._subscriptions.items():
            await self._client.subscribe(topic, qos=qos)

    async def disconnect(self) -> None:
        """Stop reconnects and close the current client. Idempotent."""
        self._closed = True
        client = self._client
        self._client = None
        if client is None:
            return
        try:
            await client.__aexit__(None, None, None)
        except Exception:
            logger.debug("Ignored error while disconnecting MQTT client", exc_info=True)

    async def publish(
        self, topic: Topic, payload: Payload, *, qos: QoS = 1, retain: bool = False
    ) -> None:
        """Encode ``payload`` and publish it. Serialized so it can run during ``messages()``.

        Raises:
            BrokerSessionError: no client, or the MQTT publish failed.
        """
        if self._client is None:
            raise BrokerSessionError("Cannot publish without an active MQTT session")
        encoded = encode_mqtt_payload(payload)
        try:
            async with self._lock:
                await self._client.publish(topic, encoded, qos=qos, retain=retain)
        except asyncio.CancelledError:
            raise
        except _RECONNECTABLE as exc:
            raise BrokerSessionError(f"MQTT publish failed on {topic}") from exc

    async def subscribe(self, topic: Topic, *, qos: QoS = 1) -> None:
        """Subscribe now and remember ``topic``/``qos`` for the next reconnect.

        Raises:
            BrokerSessionError: no client, or the MQTT subscribe failed.
        """
        self._subscriptions[topic] = qos
        if self._client is None:
            raise BrokerSessionError("Cannot subscribe without an active MQTT session")
        try:
            await self._client.subscribe(topic, qos=qos)
        except asyncio.CancelledError:
            raise
        except _RECONNECTABLE as exc:
            raise BrokerSessionError(f"MQTT subscribe failed on {topic}") from exc

    async def messages(self) -> AsyncIterator[Message]:
        """Yield decoded inbound messages until disconnect.

        If the TCP session drops, reconnects and continues unless ``disconnect``
        already ran. Bytes are decoded with ``decode_mqtt_payload``.
        """
        while not self._closed:
            if self._client is None:
                await self._connect_with_retry()
            client = self._client
            if client is None:
                return
            try:
                async for raw in client.messages:
                    yield Message(
                        topic=str(raw.topic),
                        payload=decode_mqtt_payload(bytes(raw.payload)),
                    )
                return
            except asyncio.CancelledError:
                raise
            except _RECONNECTABLE as exc:
                if self._closed:
                    return
                logger.warning(
                    "MQTT session dropped (%s:%s): %s",
                    self._session.host,
                    self._session.port,
                    exc,
                )
                await self._close_current_client()
                if self._closed:
                    return
                await self._connect_with_retry()

    async def _close_current_client(self) -> None:
        """Drop the live client after a session error so the next connect is clean."""
        client = self._client
        self._client = None
        if client is None:
            return
        try:
            await client.__aexit__(None, None, None)
        except Exception:
            logger.debug(
                "Ignored error while closing dropped MQTT client", exc_info=True
            )
