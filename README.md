# missiva

Hexagonal MQTT library for IoT. `Missiva` holds the broker session. Operations are methods: `publish`, `subscribe`, `listen`, `request`. Callbacks are optional arguments on those calls. You own the business logic.

There is no CLI, no YAML, no dashboard, and no storage hook. Broker settings are Pydantic (`SecretStr` for passwords) and may come from the environment.

## Install

Python 3.12+. Until the first PyPI release, install from source:

```bash
uv add git+https://github.com/TAPMI-EAFIT/missiva
```

```bash
pip install git+https://github.com/TAPMI-EAFIT/missiva
```

Runtime dependencies: `pydantic`, `pydantic-settings`, `aiomqtt` (MQTT 3.1.1).

## Public contract

```python
from missiva import Missiva, Message, Payload, Topic, QoS
from missiva.errors import MissivaError, BrokerSessionError, BrokerReconnectExhausted
from pydantic import SecretStr
```

The MQTT adapter is internal and may change without a major version bump.

Broker failures are domain types on ``BrokerPort``: `BrokerSessionError` (no session, or an in-flight publish/subscribe failed) and `BrokerReconnectExhausted` (connect gave up). `MissivaError` is the base. Import them from `missiva.errors`. Driver errors (`aiomqtt`, sockets) are translated by the adapter and do not leak.

### Operations

Without `async with`, each call connects, runs, and disconnects. Inside `async with missiva`, the connection is reused so several operations share the same TCP session.

QoS, retain, and inbound `payload` type are **per call**, not constructor settings. Defaults: `qos=1`, `retain=False`. `0` = at most once, `1` = at least once (duplicates possible), `2` = exactly once. Subscribe QoS is the maximum QoS this client will accept on that topic.

`publish` takes a `Message` whose `payload` is the value (`dict`, `str`, `bytes`, or a Pydantic model instance). `subscribe` / `listen` / `request` take `payload=` as that same **type**: `str`, `bytes`, `dict`, or a model class. Matching messages are kept (models are parsed from JSON); the rest are skipped.

| Method | What it does |
|--------|----------------|
| `publish(message, *, qos=, retain=, on_published=, on_publish_failed=)` | Send one `Message` |
| `subscribe(topic, *, qos=, idle_timeout=, payload=, on_message=, on_timeout=)` | Wait for **one** message (or idle timeout) |
| `listen(topic, *, qos=, idle_timeout=, payload=, on_timeout=, on_message=)` | Deliver messages until cancel / session end |
| `request(message, *, ack, qos=, retain=, ack_qos=, payload=, idle_timeout=, ...)` | Subscribe to `ack`, publish `message`, wait for one reply |

Pub/sub transform and duplex are composition: `listen` + `publish` on the same session.

`Message` is `topic` + `payload`. `Topic` is an MQTT topic or filter (`str`). `Payload` is `dict | str | bytes | BaseModel`.

Wire encoding when publishing:

| Python value | MQTT bytes |
|--------------|------------|
| `dict` | JSON object, UTF-8 |
| `BaseModel` | dumped to dict, then JSON UTF-8 |
| `str` | UTF-8 **raw** — `"ON"` is `ON`, not `"ON"` |
| `bytes` | unchanged |

Inbound: JSON object → `dict`; other UTF-8 → `str`; non-UTF-8 → `bytes`.

## Quick start

### Publish (reused session)

```python
missiva = Missiva(host="mqtt.local")
async with missiva:
    await missiva.publish(
        Message(topic="sensors/temp", payload={"celsius": 21.5}), qos=1
    )
    await missiva.publish(
        Message(topic="sensors/temp", payload="ON"), qos=0, retain=True
    )
```

### Subscribe once

```python
message = await missiva.subscribe("sensors/#", idle_timeout=10.0)
```

### Listen

```python
await missiva.listen(
    "sensors/#",
    idle_timeout=30.0,
    on_message=handle,
    on_timeout=on_idle,
)
```

### Transform (listen + publish)

```python
async def handle(message):
    await missiva.publish(Message(topic="out", payload={"echo": message.payload}))


async with missiva:
    await missiva.listen("in", on_message=handle)
```

### Request-reply

Subscribe to the ACK topic first, publish the command, wait for one inbound message (correlation in v1 is **by topic**).

```python
ack = await missiva.request(
    Message(topic="device/123/cmd", payload={"ping": True}),
    ack="device/123/ack",
    idle_timeout=5.0,
)
```

### Listen and publish together

```python
async with missiva:
    listen = asyncio.create_task(
        missiva.listen("device/123/commands", on_message=on_command)
    )
    await missiva.publish(Message(topic="device/123/telemetry", payload={"temp": 21.5}))
    await listen
```

Inbound messages are processed **serially**. `publish()` may run concurrently with `listen`; the adapter serializes MQTT publish calls.

Reconnect uses a **single** MQTT session per attempt. Protocol is MQTT 3.1.1.

## Configuration

`Missiva` takes broker settings in the constructor. Keyword arguments win; omitted fields come from the environment (`MISSIVA_BROKER_*`) or `_env_file`.

Fields: `host`, `port=1883`, `username`, `password: SecretStr | None`, `client_id`, `reconnect_max_attempts=10`, `reconnect_delay_seconds=5.0`. The password is never logged. QoS and retain are arguments on `publish` / `subscribe` / `listen` / `request`, not on the session. Two live connections must not share `client_id`: MQTT 3.1.1 disconnects the older one. Omit it to generate a unique id.

| Field | Environment variable |
|------|----------------------|
| `host` | `MISSIVA_BROKER_HOST` |
| `port` | `MISSIVA_BROKER_PORT` |
| `username` | `MISSIVA_BROKER_USERNAME` |
| `password` | `MISSIVA_BROKER_PASSWORD` |
| `client_id` | `MISSIVA_BROKER_CLIENT_ID` |
| `reconnect_max_attempts` | `MISSIVA_BROKER_RECONNECT_MAX_ATTEMPTS` |
| `reconnect_delay_seconds` | `MISSIVA_BROKER_RECONNECT_DELAY_SECONDS` |

```python
Missiva()  # host (and the rest) from MISSIVA_BROKER_*
Missiva(host="mqtt.local")  # password still from MISSIVA_BROKER_PASSWORD if set
Missiva(_env_file=".env")
```

Topics are arguments on each call (`str`), not environment settings.

Examples load `examples/.env` for the broker only. `make example-pub` and the other `example-*` targets run them.

## Out of scope (v1)

CLI, YAML, Streamlit, MQTT 5 correlation data, advanced TLS, concurrent inbound handlers, and publishing this package to PyPI (the layout is PyPI-ready).

## License

MIT. Copyright TAPMI - EAFIT.
