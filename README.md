# 钉钉 Stream SDK

[English](https://github.com/luyifo/dingtalk-open-stream/blob/main/README.en.md) | **简体中文**

## 技术选型

- Python 3.12
- uv
- httpx
- websockets

## 功能特性

- 通过 HTTP 注册 Stream 连接，并使用一次性 ticket 建立 WebSocket 长连接。
- 默认订阅开发者后台已配置的全部事件，以及机器人消息回调；也支持通过 `Subscription` 自定义订阅 topic。
- 支持为不同 topic 注册异步消息处理器，并使用 `*` 注册兜底处理器。
- 自动响应系统 `ping`，并在收到 `disconnect` 或连接中断后申请新 ticket 重连；重连采用带随机抖动的指数退避。
- 支持通过 `start()` 在后台启动客户端。
- 支持优雅关闭：关闭时最多等待 `shutdown_timeout` 秒让直接执行的异步处理器完成，之后取消未完成任务并释放资源。
- 支持注入自定义日志实例。

## 安装

### 使用 uv 管理项目依赖

```sh
uv add dingtalk-open-stream
```

### 使用 uv pip 安装

```sh
uv pip install dingtalk-open-stream
```

### 使用 pip 安装

```sh
pip install dingtalk-open-stream
```

## 接收机器人消息和事件

推荐让 `async def` 处理器将业务提交到应用管理的后台任务后立即返回 `Ack.ok()`，SDK 随即回复，耗时的异步业务独立执行。下面通过 `TaskGroup` 管理后台任务，在 `TODO` 处填写异步业务逻辑。启动与优雅关闭方式见下方[生命周期与优雅关闭](#生命周期与优雅关闭)。

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
            client_id="你的应用 Client ID",
            client_secret="你的应用 Client Secret",
            logger=app_logger,
        )

        async def process_message(message: Message) -> None:
            try:
                # TODO: do something with message.data.
                pass
            except Exception:
                app_logger.exception(
                    "后台业务处理失败，message_id=%s", message.message_id
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
                except NotImplementedError:  # 部分 Windows 事件循环不支持此接口。
                    pass
            await client.start()
            await stopping.wait()
        finally:
            await client.close()


asyncio.run(main())
```

## 使用 Loguru 日志

SDK 接受兼容 Python 标准日志接口的 logger 实例。使用 Loguru 时，先将它添加为项目依赖：

```sh
uv add loguru
```

然后把 Loguru 的 logger 传给 `StreamClient`：

```python
import asyncio

from loguru import logger

from dingtalk_open_stream import StreamClient


async def main() -> None:
    client = StreamClient(
        client_id="你的应用 Client ID",
        client_secret="你的应用 Client Secret",
        logger=logger,
    )
    try:
        await client.start()
        # 在这里运行应用逻辑；按 Ctrl+C 时 asyncio.run() 会取消此任务。
        await asyncio.Event().wait()
    finally:
        await client.close()


asyncio.run(main())
```

SDK 的连接、重连、消息处理和关闭日志会通过该 Loguru logger 输出。日志格式、级别和输出目标可继续使用 Loguru 的配置方式设置。

## 订阅与消息处理器

默认订阅已在钉钉开发者后台配置的全部事件（`EVENT`，topic 为 `*`）和机器人消息回调（`CALLBACK`，topic 为 `/v1.0/im/bot/messages/get`）。如需调整订阅，可通过 `subscriptions` 参数传入 `Subscription` 列表。

`Topic` 是 topic 字符串的公开类型别名。`client.on()` 会为 `*`、机器人消息回调和卡片回调提供类型提示；`KnownTopic` 描述这些已知选项，同时仍接受自定义字符串 topic。

使用 `client.on(topic, handler)` 注册消息处理器；`*` 可作为未匹配 topic 的兜底处理器。处理器必须返回 `Ack`，SDK 随即发送该确认。推荐按上例只在处理器中提交后台任务并快速返回，将异步业务及失败恢复交给应用管理；EVENT 可能重推，业务可按 `eventId` 实现幂等。

## 生命周期与优雅关闭

### 生命周期

调用 `await client.start()` 会在后台启动连接和重连循环。客户端会为每次连接申请新的 ticket，采用指数退避进行重连，响应 `ping`，并在收到服务端 `disconnect` 通知后重新连接。每个客户端只能启动一次。

连接中断或正常轮换重连时，在途异步处理器会继续执行。取消 `wait_closed()` 的等待者不会停止后台客户端；取消 `stop()` 或 `close()` 的等待者也不会中断已经请求的关闭与清理。

![客户端生命周期流程图](https://raw.githubusercontent.com/luyifo/dingtalk-open-stream/main/docs/lifecycle.zh.svg)

### 优雅关闭

应用检测到退出条件后，在清理路径中调用 `await client.close()`。客户端会停止接收新消息，最多等待 `shutdown_timeout` 秒让直接等待的异步处理器完成；超时后取消这些分发任务，关闭 WebSocket，并释放 SDK 创建的 HTTP 客户端。此等待不包含应用自行创建的后台任务；上例在客户端关闭后退出 `TaskGroup`，正常退出时等待后台业务完成。`shutdown_timeout` 默认是 30 秒，可在创建 `StreamClient` 时调整。`close()` 可以重复调用。`stop()` 也会请求停止并等待清理；通常在应用退出时使用 `close()`。

下面两种方式的区别在于如何检测退出条件；客户端执行的关闭流程相同。

#### 方式一：监听进程信号

适合独立运行的服务或命令行程序。收到 `SIGINT`（通常是 Ctrl+C）或 `SIGTERM` 时，通知主任务退出，并在 `finally` 中关闭客户端：

```python
import asyncio
import signal

from dingtalk_open_stream import StreamClient


async def main() -> None:
    client = StreamClient(
        client_id="你的应用 Client ID",
        client_secret="你的应用 Client Secret",
    )
    
    loop = asyncio.get_running_loop()
    stopping = asyncio.Event()
    
    try:
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, stopping.set)
            except NotImplementedError:  # 部分 Windows 事件循环不支持此接口。
                pass
        await client.start()
        await stopping.wait()
    finally:
        await client.close()


asyncio.run(main())
```

部分 Windows 事件循环不支持 `loop.add_signal_handler()`。此时按 Ctrl+C 会由 `asyncio.run()` 取消主任务，仍会执行 `finally` 并关闭客户端；其他信号的处理能力取决于运行平台。

#### 方式二：用 `try/finally` 包住应用逻辑

适合由 Web 框架、worker 或其他异步任务管理客户端的场景。外围应用逻辑结束、抛出异常或任务被取消时，`finally` 都会关闭客户端：

```python
import asyncio

from dingtalk_open_stream import StreamClient


async def main() -> None:
    client = StreamClient(
        client_id="你的应用 Client ID",
        client_secret="你的应用 Client Secret",
    )
    try:
        await client.start()
        # 在这里运行应用逻辑。
        await asyncio.Event().wait()
    finally:
        await client.close()


asyncio.run(main())
```

两种方式的关闭动作相同，区别在于退出触发源：信号方式响应操作系统或进程管理器发来的信号；`try/finally` 响应外围应用逻辑的结束、异常或取消。两者也可以组合：信号处理器通知应用退出，再由 `finally` 统一关闭客户端。

## 示例

FastAPI 示例：[在 FastAPI 中使用](https://github.com/luyifo/dingtalk-open-stream/blob/main/examples/fastapi_app.py)。运行前设置环境变量 `DINGTALK_CLIENT_ID` 和 `DINGTALK_CLIENT_SECRET`。
