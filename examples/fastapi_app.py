"""在 FastAPI 应用生命周期中运行 DingTalk Stream 客户端的示例。

Example of running a DingTalk Stream client in the FastAPI application lifespan.
"""

from __future__ import annotations

import asyncio
import logging
import os
from contextlib import asynccontextmanager
from typing import AsyncIterator

from fastapi import FastAPI

from dingtalk_open_stream import Ack, Message, StreamClient


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """启动 FastAPI 时启动 Stream，关闭 FastAPI 时停止并清理 Stream。

    Start Stream when FastAPI starts, and stop and clean it up when FastAPI shuts down.
    """
    logging.basicConfig(level=logging.INFO)
    client_id = os.environ["DINGTALK_CLIENT_ID"]
    client_secret = os.environ["DINGTALK_CLIENT_SECRET"]
    # 应用持有后台业务任务；正常退出 TaskGroup 时会等待这些任务完成。
    # The application owns business tasks; normal TaskGroup exit waits for them.
    async with asyncio.TaskGroup() as background:
        client = StreamClient(client_id=client_id, client_secret=client_secret)

        async def process_message(message: Message) -> None:
            """在后台处理业务消息。 / Process business messages in the background."""
            try:
                # TODO: do something with message.data.
                pass
            except Exception:
                # 业务异常在任务内记录，避免传播到 TaskGroup 并取消其他后台任务。
                # Log business failures here so they don't cancel sibling TaskGroup tasks.
                logging.exception("Business processing failed: %s", message.message_id)

        async def on_bot_message(message: Message) -> Ack:
            """提交机器人消息。 / Submit a bot message for background processing."""
            # ACK 表示已提交后台处理；后续业务失败需要应用自行恢复。
            # ACK confirms scheduling; the application handles any later business failure.
            background.create_task(process_message(message))
            return Ack.ok()

        async def on_event(message: Message) -> Ack:
            """提交订阅事件。 / Submit an event for background processing."""
            background.create_task(process_message(message))
            return Ack.ok()

        client.on("/v1.0/im/bot/messages/get", on_bot_message)
        # 精确匹配机器人回调后，其余非系统消息交给兜底处理器。
        # After exact bot-topic matching, route other non-system messages to the fallback.
        client.on("*", on_event)
        try:
            await client.start()
            # start 只启动后台连接循环，此状态引用不表示连接已就绪。
            # start launches the connection loop; this reference isn't a readiness signal.
            app.state.dingtalk_open_stream_client = client
            yield
        finally:
            # 先停止 Stream 接收新消息，再退出 TaskGroup 等待已提交的业务任务。
            # Stop Stream intake before leaving TaskGroup to drain submitted business work.
            await client.close()


app = FastAPI(lifespan=lifespan)


@app.get("/health/live")
async def health() -> dict[str, str]:
    """提供进程存活检查，不代表 Stream 已连接或就绪。

    Liveness check only; this does not indicate that Stream is connected or ready.
    """
    return {"status": "ok"}
