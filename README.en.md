# DingTalk Stream SDK

**English** | [简体中文](https://raw.githubusercontent.com/luyifo/dingtalk-open-stream/main/README.md)

## Tech stack

- Python 3.12
- uv
- httpx
- websockets

## Supported features

- Registers a Stream connection over HTTP and opens a persistent WebSocket using a single-use ticket.
- By default, subscribes to all events configured in the Developer Console and bot message callbacks; custom topics can be configured with `Subscription` objects.
- Supports synchronous and asynchronous handlers per topic, with `*` available as a fallback handler; up to 100 business messages are processed concurrently by default, configurable with `max_concurrent_handlers`.
- Creates and sends protocol ACKs automatically; successful event handling returns `SUCCESS`, while failed handling returns `LATER`.
- Responds to system `ping` messages and requests a fresh ticket to reconnect after `disconnect` notices or connection failures; reconnection uses exponential backoff with jitter.
- Supports starting the client in the background with `start()`.
- Keeps system pings responsive independently of business handlers. During graceful shutdown, waits up to `shutdown_timeout` for in-flight messages, then cancels unfinished handlers and releases resources.
- Allows a custom logger instance to be injected.

## Installation

### Manage the project dependency with uv

```sh
uv add dingtalk-open-stream
```

### Install with uv pip

```sh
uv pip install dingtalk-open-stream
```

### Install with pip

```sh
pip install dingtalk-open-stream
```

## Receive bot messages and events

Create a client and register handlers for bot messages and events. See [Lifecycle and graceful shutdown](#lifecycle-and-graceful-shutdown) below for how to start and gracefully stop it.

```python
import asyncio
import logging
import signal

from dingtalk_open_stream import Ack, Message, StreamClient

logging.basicConfig(level=logging.INFO)


async def main() -> None:
    app_logger = logging.getLogger("my-app.dingtalk-stream")
    client = StreamClient(
        client_id="your-app-client-id",
        client_secret="your-app-client-secret",
        logger=app_logger,
    )

    async def on_bot_message(message: Message) -> Ack:
        print(message.data)
        return Ack.ok()

    async def on_event(message: Message) -> Ack:
        print(message.headers.get("eventType"), message.data)
        return Ack.ok()

    client.on("/v1.0/im/bot/messages/get", on_bot_message)
    client.on("*", on_event)

    loop = asyncio.get_running_loop()
    stopping = asyncio.Event()
    try:
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, stopping.set)
            except NotImplementedError:  # Some Windows event loops do not support this API.
                pass
        await client.start()
        await stopping.wait()
    finally:
        await client.close()


asyncio.run(main())
```

## Use Loguru for logging

The SDK accepts logger instances compatible with the Python standard logging interface. To use Loguru, first add it as a project dependency:

```sh
uv add loguru
```

Then pass Loguru's logger to `StreamClient`:

```python
import asyncio

from loguru import logger

from dingtalk_open_stream import StreamClient


async def main() -> None:
    client = StreamClient(
        client_id="your-app-client-id",
        client_secret="your-app-client-secret",
        logger=logger,
    )
    try:
        await client.start()
        # Put application logic here; asyncio.run() cancels this task on Ctrl+C.
        await asyncio.Event().wait()
    finally:
        await client.close()


asyncio.run(main())
```

Connection, reconnection, message handling, and shutdown logs are written through the supplied Loguru logger. Configure log formatting, levels, and sinks using Loguru as usual.

## Subscriptions and message handlers

By default, the client subscribes to all events configured in the DingTalk Developer Console (`EVENT`, topic `*`) and bot message callbacks (`CALLBACK`, topic `/v1.0/im/bot/messages/get`). Pass a list of `Subscription` objects through `subscriptions` to customize the subscriptions.

`Topic` is a public type alias for topic strings. `client.on()` provides type hints for `*`, the bot message callback, and the card callback; `KnownTopic` describes those known options while custom string topics remain accepted.

Register a message handler with `client.on(topic, handler)`. Use `*` as a fallback for unmatched topics. Handlers may be synchronous or asynchronous and can return `Ack.ok(...)` or `Ack.later(...)`. Business handlers run with bounded concurrency, capped at 100 by default; adjust this with `max_concurrent_handlers`. When the limit is reached, the SDK responds with a `LATER` ACK. Because handlers run concurrently, completion order across messages is not guaranteed to match receive order. On successful event handling, the SDK replies with an ACK containing `status: SUCCESS`. If a handler raises an exception, the SDK logs it and replies with `status: LATER` so DingTalk can retry the event.

## Lifecycle and graceful shutdown

### Lifecycle

Call `await client.start()` to launch the connection and reconnection loop in the background. The client requests a fresh ticket for each connection, reconnects with exponential backoff, responds to `ping`, and reconnects after receiving the server's `disconnect` notice. Each client can be started only once.

![Client lifecycle flowchart](https://raw.githubusercontent.com/luyifo/dingtalk-open-stream/main/docs/lifecycle.en.svg)

### Graceful shutdown

When the application detects that it should exit, call `await client.close()` in its cleanup path. The client stops receiving new messages, waits up to `shutdown_timeout` seconds for in-flight handlers, cancels any that remain, closes the WebSocket, and releases the SDK-owned HTTP client. `shutdown_timeout` defaults to 30 seconds and can be changed when creating `StreamClient`. `close()` is safe to call more than once. `stop()` also requests shutdown and waits for cleanup; `close()` is generally the method to use when the application exits.

The two options below differ in how they detect the exit condition; the client's shutdown sequence is the same.

#### Option 1: Listen for process signals

This suits standalone services and command-line programs. When `SIGINT` (usually Ctrl+C) or `SIGTERM` arrives, notify the main task to exit, then close the client in `finally`:

```python
import asyncio
import signal

from dingtalk_open_stream import StreamClient


async def main() -> None:
    client = StreamClient(
        client_id="your-app-client-id",
        client_secret="your-app-client-secret",
    )
    
    loop = asyncio.get_running_loop()
    stopping = asyncio.Event()
    
    try:
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, stopping.set)
            except NotImplementedError:  # Some Windows event loops do not support this API.
                pass
        await client.start()
        await stopping.wait()
    finally:
        await client.close()


asyncio.run(main())
```

Some Windows event loops do not support `loop.add_signal_handler()`. In that case, Ctrl+C is handled by `asyncio.run()`, which cancels the main task and still runs `finally` to close the client. Handling other signals depends on the platform.

#### Option 2: Wrap application logic in `try/finally`

This suits cases where a web framework, worker, or another async task manages the client. The `finally` block closes the client when the surrounding application logic returns, raises an exception, or is cancelled:

```python
import asyncio

from dingtalk_open_stream import StreamClient


async def main() -> None:
    client = StreamClient(
        client_id="your-app-client-id",
        client_secret="your-app-client-secret",
    )
    try:
        await client.start()
        # Run application logic here.
        await asyncio.Event().wait()
    finally:
        await client.close()


asyncio.run(main())
```

Both options perform the same shutdown actions; they differ in what triggers exit. Signal handling responds to signals from the operating system or process manager, while `try/finally` responds to the surrounding application logic returning, raising an exception, or being cancelled. They can also be combined: a signal handler tells the application to exit, and `finally` closes the client.

## Examples

FastAPI example: [Using in FastAPI](https://raw.githubusercontent.com/luyifo/dingtalk-open-stream/main/examples/fastapi_app.py). 
