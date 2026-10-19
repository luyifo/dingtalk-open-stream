"""钉钉 Stream 异步连接与生命周期管理。

Async DingTalk Stream connection and lifecycle management.
"""

from __future__ import annotations

import asyncio
import json
import logging
import random
from collections.abc import Awaitable, Callable, Iterable
from inspect import isawaitable
from typing import Any, Protocol, overload
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

import httpx
from websockets.asyncio.client import ClientConnection, connect
from websockets.exceptions import ConnectionClosed

from .models import Ack, KnownTopic, Message, Subscription, Topic

logger = logging.getLogger(__name__)
MessageHandler = Callable[[Message], Ack | Awaitable[Ack]]


class LoggerLike(Protocol):
    """提供 SDK 所需日志方法的结构化类型。
    
    Structural type for the logger methods used by the SDK.
    """

    # Positional-only parameters let standard logging.Logger (whose parameter
    # is named ``msg``) satisfy this protocol. The SDK only passes the message.
    def debug(self, message: str, /) -> Any: ...

    def info(self, message: str, /) -> Any: ...

    def warning(self, message: str, /) -> Any: ...

    def error(self, message: str, /) -> Any: ...

    def exception(self, message: str, /) -> Any: ...


class _ReconnectRequested(Exception):
    """关闭当前连接并申请新 ticket 的内部信号。

    Internal signal to close the current socket and register a fresh ticket.
    """


class StreamClient:
    """连接钉钉 Stream Mode 并分发收到的消息。

    ``start`` 在后台运行重连循环。调用 ``stop`` 或 ``close`` 可以停止客户端；
    关闭时会限时等待正在执行的处理器，并释放 WebSocket 和 HTTP 资源。

    ``start`` runs the reconnect loop in the background. Call ``stop`` or
    ``close`` to stop it; shutdown waits for in-flight handlers up to
    ``shutdown_timeout`` and closes both the WebSocket and HTTP resources.
    """

    OPEN_URL = "https://api.dingtalk.com/v1.0/gateway/connections/open"
    DEFAULT_SUBSCRIPTIONS = (
        Subscription("*", "EVENT"),
        Subscription("/v1.0/im/bot/messages/get", "CALLBACK"),
    )

    def __init__(
        self,
        client_id: str,
        client_secret: str,
        *,
        subscriptions: Iterable[Subscription] | None = None,
        logger: LoggerLike | None = None,
        user_agent: str = "dingtalk-open-stream/0.1.0",
        local_ip: str | None = None,
        reconnect_min_delay: float = 1.0,
        reconnect_max_delay: float = 30.0,
        shutdown_timeout: float = 30.0,
    ) -> None:
        """初始化配置和 HTTP 客户端；调用 ``start`` 后才发起连接。

        ``subscriptions=None`` 使用默认订阅，空迭代器则表示不订阅任何主题。
        重连延迟和 ``shutdown_timeout`` 均以秒计；后者仅限制 SDK 分发任务
        的完成等待时间，取消与资源释放还需要额外时间。

        Initialize settings and the HTTP client; connections begin only on ``start``.
        ``subscriptions=None`` selects defaults; an empty iterable subscribes to nothing.
        Reconnect delays and ``shutdown_timeout`` are in seconds. The shutdown timeout
        bounds the handler drain phase, with cancellation and resource cleanup afterward.
        """
        if not client_id or not client_secret:
            raise ValueError("client_id and client_secret are required")
        if reconnect_min_delay <= 0 or reconnect_max_delay < reconnect_min_delay:
            raise ValueError("Invalid reconnect delay range")
        if shutdown_timeout <= 0:
            raise ValueError("shutdown_timeout must be positive")
        self.client_id = client_id
        self.client_secret = client_secret
        self.subscriptions = tuple(
            self.DEFAULT_SUBSCRIPTIONS if subscriptions is None else subscriptions
        )
        self.user_agent = user_agent
        self.local_ip = local_ip
        self.reconnect_min_delay = reconnect_min_delay
        self.reconnect_max_delay = reconnect_max_delay
        self.shutdown_timeout = shutdown_timeout
        self._http = httpx.AsyncClient(timeout=httpx.Timeout(15.0))
        self._logger = logger if logger is not None else logging.getLogger(__name__)
        self._handlers: dict[str, MessageHandler] = {}
        # 跨重连保留分发任务的引用，关闭时统一等待；不包含应用创建的后台任务。
        # Retain dispatch tasks across reconnects for shutdown; user tasks aren't tracked.
        self._handler_tasks: set[asyncio.Task[None]] = set()
        self._send_lock = asyncio.Lock()
        self._stop_event = asyncio.Event()
        self._runner: asyncio.Task[None] | None = None
        self._websocket: ClientConnection | None = None
        self._closed = False

    @overload
    def on(self, topic: KnownTopic, handler: MessageHandler) -> StreamClient: ...

    @overload
    def on(self, topic: Topic, handler: MessageHandler) -> StreamClient: ...

    def on(self, topic: str, handler: MessageHandler) -> StreamClient:
        """注册处理器；同一 topic 重复注册会覆盖，精确匹配优先于 ``*``。

        处理器在客户端事件循环中执行，须返回 Ack 或可等待的 Ack 结果。
        同步处理器也不会自动进入线程池，阻塞业务应由应用自行调度。
        此方法只配置本地分发，不修改向服务端注册的订阅。

        Register a handler, replacing any existing handler for the same topic.
        Exact topics take precedence over ``*``. Handlers run on the client event loop
        and must return Ack or an awaitable resolving to Ack; synchronous handlers
        aren't offloaded to threads. Registration doesn't change server subscriptions.
        """
        self._handlers[topic] = handler
        return self

    async def start(self) -> None:
        """在后台启动连接和重连；此方法只能调用一次。

        返回时不保证注册或握手已完成，不能作为连接就绪信号。

        Start reconnecting in the background; call only once.
        Returning doesn't guarantee registration or the WebSocket handshake is complete.
        """
        if self._closed or self._stop_event.is_set():
            raise RuntimeError("StreamClient is closed")
        if self._runner is not None:
            raise RuntimeError("StreamClient has already been started")
        self._runner = asyncio.create_task(self._run(), name="dingtalk-stream-client")
        self._logger.debug("DingTalk Stream client started")

    async def wait_closed(self) -> None:
        """等待已有运行或清理任务结束；尚未启动时立即返回。

        本方法不请求停止，取消等待也不会取消共享的后台任务。

        Wait for an existing runner or cleanup task; return immediately before start.
        This doesn't request shutdown, and cancelling the waiter leaves shared work running.
        """
        if self._runner is None:
            self._logger.debug("wait_closed called before the Stream client was started")
            return
        self._logger.debug("Waiting for the DingTalk Stream client to stop")
        await asyncio.shield(self._runner)
        self._logger.debug("DingTalk Stream client has stopped")

    async def stop(self) -> None:
        """请求永久停止；通常等待处理器结束和资源释放，可重复调用。

        从 SDK 分发的处理器内部调用时，只发送停止信号并立即返回，
        避免处理器与等待它结束的清理任务互相等待。

        Request permanent shutdown; normally wait for handlers and cleanup. Safe to repeat.
        Calls from a dispatched handler only signal shutdown and return, avoiding a
        deadlock with cleanup waiting for that same handler to finish.
        """
        self._logger.debug("Stop requested for the DingTalk Stream client")
        self._stop_event.set()
        if asyncio.current_task() in self._handler_tasks:
            self._logger.debug(
                "Deferring Stream client shutdown until the current handler finishes"
            )
            return
        runner = self._runner
        if runner is None:
            # Share cleanup even when close() is called before start().
            # 即使尚未启动，也让所有关闭调用共享同一个清理任务。
            runner = self._runner = asyncio.create_task(
                self._cleanup(), name="dingtalk-stream-cleanup"
            )
        if runner is not asyncio.current_task():
            # 取消某个关闭调用的等待，不应中断其他调用共享的清理流程。
            # Cancelling one shutdown waiter must not cancel shared cleanup.
            await asyncio.shield(runner)
        self._logger.debug("DingTalk Stream client stopped")

    async def close(self) -> None:
        """关闭客户端；等同于 ``stop``，并使客户端不可再次启动。

        Alias for ``stop``; also makes this client permanently unusable.
        """
        self._logger.debug("Close requested for the DingTalk Stream client")
        await self.stop()
        self._closed = True
        self._logger.debug("DingTalk Stream client marked closed")

    async def _run(self) -> None:
        """执行注册、连接、重连循环，并在退出时清理资源。

        Run the registration, connection, and reconnection loop, then clean up on exit.
        """
        delay = self.reconnect_min_delay
        try:
            while not self._stop_event.is_set():
                try:
                    registration = await self._register_or_stop()
                    if registration is None:
                        break
                    if self._stop_event.is_set():
                        break
                    endpoint, ticket = registration
                    ws_url = self._ticket_url(endpoint, ticket)
                    self._logger.info("Connecting to DingTalk Stream endpoint")
                    websocket = await self._connect_or_stop(ws_url)
                    if websocket is None:
                        break
                    self._websocket = websocket
                    # 连接成功后重置退避；即使随后立即断线，也从最短延迟重试。
                    # Reset backoff after connecting, even if this socket soon disconnects.
                    delay = self.reconnect_min_delay
                    try:
                        await self._serve_connection(websocket)
                    finally:
                        # Keep the socket available for returned ACKs while shutdown
                        # waits for handlers; cleanup closes it after they finish.
                        # 关闭流程保留连接以回复 ACK，处理器结束后再由清理任务关闭。
                        if not self._stop_event.is_set():
                            try:
                                await websocket.close()
                            except Exception:
                                self._logger.exception(
                                    "Error while closing DingTalk Stream websocket"
                                )
                except asyncio.CancelledError:
                    raise
                except _ReconnectRequested:
                    self._logger.info("DingTalk requested Stream reconnect")
                except (httpx.HTTPError, ConnectionClosed, OSError, asyncio.TimeoutError) as exc:
                    if not self._stop_event.is_set():
                        self._logger.warning(f"DingTalk Stream connection failed: {exc}")
                except Exception:
                    if not self._stop_event.is_set():
                        self._logger.exception("Unexpected DingTalk Stream client error")
                finally:
                    if not self._stop_event.is_set():
                        self._websocket = None
                if self._stop_event.is_set():
                    break
                # 加入随机抖动，避免多个副本同时重连。
                # Add jitter to avoid synchronized reconnects across replicas.
                wait_for = min(
                    self.reconnect_max_delay,
                    min(self.reconnect_max_delay, delay) * random.uniform(0.8, 1.2),
                )
                try:
                    await asyncio.wait_for(self._stop_event.wait(), timeout=wait_for)
                except asyncio.TimeoutError:
                    pass
                delay = min(self.reconnect_max_delay, delay * 2)
        finally:
            await self._cleanup()

    async def _register_or_stop(self) -> tuple[str, str] | None:
        """执行凭证注册；停止请求先到达时取消 HTTP 请求。

        Register connection credentials, cancelling the HTTP request if shutdown comes first.
        """
        register_task = asyncio.create_task(self._register())
        stop_task = asyncio.create_task(self._stop_event.wait())
        try:
            done, _ = await asyncio.wait(
                (register_task, stop_task), return_when=asyncio.FIRST_COMPLETED
            )
            # 同轮完成时优先停止，避免关闭期间继续发起 WebSocket 握手。
            # Prefer shutdown if both finish together, avoiding a new handshake.
            if stop_task in done or self._stop_event.is_set():
                if not register_task.done():
                    register_task.cancel()
                await asyncio.gather(register_task, return_exceptions=True)
                return None
            return register_task.result()
        except BaseException:
            if not register_task.done():
                register_task.cancel()
            await asyncio.gather(register_task, return_exceptions=True)
            raise
        finally:
            if not stop_task.done():
                stop_task.cancel()
            await asyncio.gather(stop_task, return_exceptions=True)

    async def _connect_or_stop(self, ws_url: str) -> ClientConnection | None:
        """建立 WebSocket；停止请求先到达时取消握手。

        Open the WebSocket, cancelling the handshake if shutdown is requested first.
        """
        if self._stop_event.is_set():
            return None

        async def open_websocket() -> ClientConnection:
            return await connect(
                ws_url,
                open_timeout=15,
                close_timeout=5,
                ping_interval=20,
                ping_timeout=20,
                max_size=8 * 1024 * 1024,
            )

        connect_task = asyncio.create_task(open_websocket())
        stop_task = asyncio.create_task(self._stop_event.wait())
        try:
            done, _ = await asyncio.wait(
                (connect_task, stop_task), return_when=asyncio.FIRST_COMPLETED
            )
            if stop_task in done or self._stop_event.is_set():
                if not connect_task.done():
                    connect_task.cancel()
                result = await asyncio.gather(connect_task, return_exceptions=True)
                websocket = result[0]
                # 握手可能恰好完成；即使不再接收消息，也必须释放已建立的连接。
                # A racing handshake may have succeeded; close that unused socket too.
                if isinstance(websocket, ClientConnection):
                    await websocket.close()
                return None
            return connect_task.result()
        except BaseException:
            if not connect_task.done():
                connect_task.cancel()
            result = await asyncio.gather(connect_task, return_exceptions=True)
            websocket = result[0]
            if isinstance(websocket, ClientConnection):
                try:
                    await websocket.close()
                except Exception:
                    self._logger.exception(
                        "Error while closing DingTalk Stream websocket after cancelled handshake"
                    )
            raise
        finally:
            if not stop_task.done():
                stop_task.cancel()
            await asyncio.gather(stop_task, return_exceptions=True)

    async def _register(self) -> tuple[str, str]:
        """向钉钉注册连接并返回 endpoint 与一次性 ticket。

        Register a connection with DingTalk and return its endpoint and single-use ticket.
        """
        body: dict[str, Any] = {
            "clientId": self.client_id,
            "clientSecret": self.client_secret,
            "subscriptions": [sub.as_dict() for sub in self.subscriptions],
            "ua": self.user_agent,
        }
        if self.local_ip:
            body["localIp"] = self.local_ip
        response = await self._http.post(self.OPEN_URL, json=body)
        response.raise_for_status()
        result = response.json()
        try:
            return str(result["endpoint"]), str(result["ticket"])
        except (KeyError, TypeError) as exc:
            raise ValueError("DingTalk registration response lacks endpoint or ticket") from exc

    @staticmethod
    def _ticket_url(endpoint: str, ticket: str) -> str:
        """把 ticket 加入 endpoint 查询参数并规范化为 WebSocket URL。

        Add the ticket to the endpoint query and normalize it as a WebSocket URL.
        """
        parts = urlsplit(endpoint)
        query = dict(parse_qsl(parts.query, keep_blank_values=True))
        query["ticket"] = ticket
        scheme = "wss" if parts.scheme == "https" else parts.scheme
        if scheme not in {"ws", "wss"}:
            raise ValueError(f"Unsupported Stream endpoint scheme: {parts.scheme!r}")
        return urlunsplit((scheme, parts.netloc, parts.path, urlencode(query), parts.fragment))

    async def _serve_connection(self, websocket: ClientConnection) -> None:
        """从连接读取消息并分发，同时监听关闭信号。

        Read and dispatch connection messages while watching for shutdown.
        """
        # Await handlers independently of the reader so async handlers cannot
        # delay SYSTEM messages. Application-created tasks remain user-owned.
        # 独立等待异步处理器，确保系统消息仍被读取；应用自建任务由使用者管理。
        while not self._stop_event.is_set():
            receive_task = asyncio.create_task(websocket.recv())
            stop_task = asyncio.create_task(self._stop_event.wait())
            tasks = (receive_task, stop_task)
            try:
                done, _ = await asyncio.wait(
                    tasks, return_when=asyncio.FIRST_COMPLETED
                )
                if stop_task in done or self._stop_event.is_set():
                    return
                payload = receive_task.result()
            finally:
                for task in tasks:
                    if not task.done():
                        task.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)

            if self._stop_event.is_set():
                return

            try:
                message = Message.from_payload(payload)
            except (UnicodeDecodeError, json.JSONDecodeError, ValueError):
                self._logger.exception("Ignoring malformed DingTalk Stream message")
                continue

            # SYSTEM messages stay on the reader so ping is answered promptly.
            # 系统消息在读取循环中直接处理，确保 ping 能及时响应。
            if message.type == "SYSTEM":
                await self._handle_system_message(websocket, message)
                continue

            task = asyncio.create_task(
                # 绑定消息来源连接，重连后的旧处理器不能向新连接发送 ACK。
                # Bind the source socket so a late ACK never uses a replacement socket.
                self._dispatch_message(websocket, message),
                name=f"dingtalk-stream-handler-{message.topic or 'unknown'}",
            )
            self._handler_tasks.add(task)
            task.add_done_callback(self._handler_done)

    async def _handle_system_message(
        self, websocket: ClientConnection, message: Message
    ) -> None:
        """处理系统推送。 / Automatically handle system messages only."""
        topic = message.topic or ""
        if topic == "disconnect":
            self._logger.info(f"DingTalk requested Stream disconnect: {message.data}")
            raise _ReconnectRequested
        if topic == "ping":
            await self._send_ack(websocket, message, Ack.ok(message.data))
            return
        self._logger.warning(f"Ignoring unknown DingTalk Stream system topic {topic!r}")

    async def _dispatch_message(self, websocket: ClientConnection, message: Message) -> None:
        """调用快速处理器并发送其返回的 ACK；后台业务由使用者管理。

        Invoke a short handler and send its returned ACK; background work is user-owned.
        """
        topic = message.topic or ""
        handler = self._handlers.get(topic) or self._handlers.get("*")
        if handler is None:
            # 未注册处理器或处理器失败时不合成 ACK，确认结果由业务显式返回。
            # Missing or failed handlers don't synthesize ACKs; handlers decide the result.
            self._logger.warning(f"No handler registered for DingTalk topic {topic!r}")
            return
        try:
            result = handler(message)
            if isawaitable(result):
                result = await result
            if not isinstance(result, Ack):
                raise TypeError(
                    f"DingTalk Stream handler must return Ack, got {type(result).__name__}"
                )
        except asyncio.CancelledError:
            raise
        except Exception:
            self._logger.exception(f"DingTalk Stream handler failed for topic {topic}")
            return
        # The handler accepts or rejects the message by returning an ACK.
        # Never wait for application-created background tasks before sending it.
        # 处理器返回 ACK 即可确认接收结果，不等待应用自行创建的后台业务任务。
        await self._send_ack(websocket, message, result)

    async def _send_ack(self, websocket: ClientConnection, message: Message, ack: Ack) -> None:
        """序列化 ACK 并通过指定连接发送，不决定业务确认时机。

        Serialize and send an ACK on its socket without deciding business ACK timing.
        """
        if not message.message_id:
            raise ValueError("Cannot ACK DingTalk message without headers.messageId")
        data = ack.data
        if message.type == "EVENT":
            # EVENT 确认补齐状态字段；同名业务字段以 Ack 的确认结果为准。
            # EVENT ACKs add status fields, overriding conflicting business values.
            data = data if isinstance(data, dict) else {"result": data}
            data = {
                **data,
                "status": "SUCCESS" if ack.success else "LATER",
                "message": ack.message,
            }
        code = 200 if ack.success else 500
        response = {
            "code": code,
            "message": ack.message,
            "headers": {
                "messageId": message.message_id,
                "contentType": "application/json",
            },
            # 外层帧中的 data 是 JSON 字符串，需要先序列化，再编码整个响应帧。
            # The outer frame's data is a JSON string, so encode it before the frame.
            "data": json.dumps(data, ensure_ascii=False, separators=(",", ":")),
        }
        # 系统消息与业务处理器共享发送锁，每个 ACK 作为完整帧依次写入。
        # Serialize complete ACK writes from both system messages and business handlers.
        async with self._send_lock:
            await websocket.send(
                json.dumps(response, ensure_ascii=False, separators=(",", ":"))
            )

    def _handler_done(self, task: asyncio.Task[None]) -> None:
        """移除已完成的分发任务并读取异常，避免后台异常无人处理。

        Release completed dispatch tasks and retrieve their exceptions for logging.
        """
        self._handler_tasks.discard(task)
        if task.cancelled():
            return
        error = task.exception()
        if error is not None:
            self._logger.error(f"DingTalk Stream handler task failed: {error!r}")

    async def _wait_for_handlers(self) -> None:
        """限时等待在途分发任务，再取消超时任务；不等待应用后台业务。

        Drain in-flight dispatch tasks with a deadline, then cancel overdue tasks.
        Application-created background work isn't included.
        """
        tasks = set(self._handler_tasks)
        if not tasks:
            return
        _, pending = await asyncio.wait(tasks, timeout=self.shutdown_timeout)
        if not pending:
            return
        self._logger.warning(
            f"Cancelling {len(pending)} DingTalk Stream handler(s) after shutdown timeout"
        )
        for task in pending:
            task.cancel()
        # Wait briefly for cooperative cancellation without allowing a broken
        # handler to block process shutdown indefinitely.
        # 短暂等待协作式取消，避免异常处理器无限阻塞程序退出。
        done, still_pending = await asyncio.wait(
            pending, timeout=min(1.0, self.shutdown_timeout)
        )
        if done:
            await asyncio.gather(*done, return_exceptions=True)
        if still_pending:
            self._logger.error(
                f"{len(still_pending)} DingTalk Stream handler(s) did not stop after cancellation"
            )

    async def _cancel_handlers(self) -> None:
        """取消当前仍在执行的处理器。 / Cancel all currently running handlers.

        处理器可能属于已经断开的连接或已经开始的关闭流程。
        The handlers may belong to a lost connection or a completed shutdown.
        """
        tasks = set(self._handler_tasks)
        if not tasks:
            return
        for task in tasks:
            task.cancel()
        done, pending = await asyncio.wait(tasks, timeout=min(1.0, self.shutdown_timeout))
        if done:
            await asyncio.gather(*done, return_exceptions=True)
        if pending:
            self._logger.error(
                f"{len(pending)} DingTalk Stream handler(s) remain pending after cancellation"
            )

    async def _cleanup(self) -> None:
        """关闭尚存的 WebSocket，并释放 SDK 自己创建的 HTTP 客户端。

        Close any remaining WebSocket and release the HTTP client owned by the SDK.
        """
        self._logger.debug("Cleaning up DingTalk Stream client resources")
        self._stop_event.set()
        # 先停止接收并等待 ACK，再关闭连接；再次取消仍未退出的分发任务。
        # Stop intake and drain ACKs before closing the socket; retry remaining cancellations.
        await self._wait_for_handlers()
        await self._cancel_handlers()
        if self._websocket is not None:
            try:
                await self._websocket.close(code=1000, reason="client shutdown")
            except Exception:
                self._logger.exception("Error while closing DingTalk Stream websocket")
            self._websocket = None
        if not self._http.is_closed:
            await self._http.aclose()
        self._logger.debug("DingTalk Stream client cleanup completed")
