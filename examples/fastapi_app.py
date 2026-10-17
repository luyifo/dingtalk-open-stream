"""在 FastAPI 应用生命周期中运行 DingTalk Stream 客户端的示例。

Example of running a DingTalk Stream client in the FastAPI application lifespan.
"""

from __future__ import annotations

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
    client = StreamClient(
        client_id=os.environ["DINGTALK_CLIENT_ID"],
        client_secret=os.environ["DINGTALK_CLIENT_SECRET"],
    )
    try:
        async def on_bot_message(message: Message) -> Ack:
            """处理机器人消息并确认成功。

            Handle a bot message and acknowledge it as successful.
            """
            logging.info("Bot message: %s", message.data)
            return Ack.ok()

        async def on_event(message: Message) -> Ack:
            """记录收到的事件并确认成功。

            Log the received event and acknowledge it as successful.
            """
            logging.info("Event %s: %s", message.headers.get("eventType"), message.data)
            return Ack.ok()

        client.on("/v1.0/im/bot/messages/get", on_bot_message)
        client.on("*", on_event)
        await client.start()
        app.state.dingtalk_open_stream_client = client
        yield
    finally:
        await client.close()


app = FastAPI(lifespan=lifespan)


@app.get("/health/live")
async def health() -> dict[str, str]:
    """提供进程存活检查，不代表 Stream 已连接或就绪。

    Liveness check only; this does not indicate that Stream is connected or ready.
    """
    return {"status": "ok"}
