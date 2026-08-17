"""Failure modes of ``BrokerPort``. Adapters translate driver errors into these types."""


class MissivaError(Exception):
    """Base class for errors raised by this library."""


class BrokerSessionError(MissivaError):
    """The broker port could not complete a session operation.

    Raised when there is no session, the session dropped, or an in-flight
    publish or subscribe failed. Implementations of ``BrokerPort`` must raise
    this instead of leaking driver exceptions.
    """


class BrokerReconnectExhausted(BrokerSessionError):
    """``connect`` gave up after consecutive failures.

    The limit is ``reconnect_max_attempts`` on the session that owns the port.
    """
