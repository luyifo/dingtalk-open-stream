# DingTalk Stream SDK

**English** | [简体中文](https://github.com/luyifo/dingtalk-open-stream/blob/main/README.md)

## Tech stack

- Python 3.12
- uv
- httpx
- websockets

## Supported features

- Registers a Stream connection over HTTP and opens a persistent WebSocket using a single-use ticket.
- By default, subscribes to all events configured in the Developer Console and bot message callbacks; custom topics can be configured with `Subscription` objects.
- Supports asynchronous handlers per topic, with `*` available as a fallback handler.
- Responds to system `ping` messages and requests a fresh ticket to reconnect after `disconnect` notices or connection failures; reconnection uses exponential backoff with jitter.
- Supports starting the client in the background with `start()`.
- During graceful shutdown, waits up to `shutdown_timeout` for directly awaited async handlers, then cancels unfinished dispatch tasks and releases resources.
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

Use an `async def` handler to submit work to an application-managed background task and immediately return `Ack.ok()`. The SDK then sends the response while the asynchronous business work runs independently. The example below uses `TaskGroup` to manage background tasks; add asynchronous business logic at the `TODO` placeholder. See [Lifecycle and graceful shutdown](#lifecycle-and-graceful-shutdown) below for how to start and gracefully stop the client.

```python
import asyncio
import logging
import signal

from dingtalk_open_stream import Ack, Message, StreamClient

logging.basicConfig(level=logging.INFO)


async def main() -> None:
    app_logger = logging.getLogger("my-app.dingtalk-stream")

    async with asyncio.TaskGroup() as background:
        client = StreamClient(
            client_id="your-app-client-id",
            client_secret="your-app-client-secret",
            logger=app_logger,
        )

        async def process_message(message: Message) -> None:
            try:
                # TODO: do something with message.data.
                pass
            except Exception:
                app_logger.exception(
                    "Background business processing failed, message_id=%s",
                    message.message_id,
                )

        async def on_message(message: Message) -> Ack:
            background.create_task(process_message(message))
            return Ack.ok()

        client.on("/v1.0/im/bot/messages/get", on_message)
        client.on("*", on_message)

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

Register a message handler with `client.on(topic, handler)`. Use `*` as a fallback for unmatched topics. A handler must return an `Ack`, which the SDK sends immediately. Follow the example above by submitting background work and returning quickly; the application manages asynchronous business work and failure recovery. EVENT messages may be redelivered, so business processing can use `eventId` for idempotency.

## Lifecycle and graceful shutdown

### Lifecycle

Call `await client.start()` to launch the connection and reconnection loop in the background. The client requests a fresh ticket for each connection, reconnects with exponential backoff, responds to `ping`, and reconnects after receiving the server's `disconnect` notice. Each client can be started only once.

In-flight async handlers continue running during connection failures and normal connection rotation. Cancelling a task waiting on `wait_closed()` does not stop the background client; cancelling a task waiting on `stop()` or `close()` does not interrupt the shutdown and cleanup already requested.

![Client lifecycle flowchart](https://raw.githubusercontent.com/luyifo/dingtalk-open-stream/main/docs/lifecycle.en.svg)

### Graceful shutdown

When the application detects that it should exit, call `await client.close()` in its cleanup path. The client stops receiving new messages, waits up to `shutdown_timeout` seconds for directly awaited async handlers, cancels these dispatch tasks if they time out, closes the WebSocket, and releases the SDK-owned HTTP client. This wait does not include background tasks created by the application. In the example above, the client closes before the application exits `TaskGroup`; on normal exit, the group waits for background work. `shutdown_timeout` defaults to 30 seconds and can be changed when creating `StreamClient`. `close()` is safe to call more than once. `stop()` also requests shutdown and waits for cleanup; `close()` is generally the method to use when the application exits.

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

FastAPI example: [Using in FastAPI](https://github.com/luyifo/dingtalk-open-stream/blob/main/examples/fastapi_app.py). Set `DINGTALK_CLIENT_ID` and `DINGTALK_CLIENT_SECRET` before running it.
