"""钉钉 Stream 协议的公开数据模型。

Public data models for the DingTalk Stream protocol.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any, Literal, TypeAlias


Topic: TypeAlias = str
KnownTopic: TypeAlias = Literal[
    "*",
    "/v1.0/im/bot/messages/get",
    "/v1.0/card/instances/callback",
]


@dataclass(frozen=True, slots=True)
class Subscription:
    topic: Topic
    type: Literal["EVENT", "CALLBACK"]

    def as_dict(self) -> dict[str, str]:
        """将订阅转换成钉钉注册接口需要的字典。

        Convert the subscription to the dictionary required by DingTalk's registration API.
        """
        return {"topic": self.topic, "type": self.type}


@dataclass(frozen=True, slots=True)
class Message:
    spec_version: str | None
    type: str
    headers: dict[str, Any]
    raw_data: str

    @property
    def topic(self) -> Topic | None:
        """返回消息 topic；字段缺失时返回 ``None``。

        Return the message topic, or ``None`` if it is missing.
        """
        value = self.headers.get("topic")
        return str(value) if value is not None else None

    @property
    def message_id(self) -> str | None:
        """返回协议消息 ID；字段缺失时返回 ``None``。

        Return the protocol message ID, or ``None`` if it is missing.
        """
        value = self.headers.get("messageId")
        return str(value) if value is not None else None

    @property
    def data(self) -> Any:
        """解析协议中以 JSON 编码的数据字符串。

        Decode the protocol's JSON-encoded data string.
        """
        try:
            return json.loads(self.raw_data)
        except (json.JSONDecodeError, TypeError):
            return self.raw_data

    @classmethod
    def from_payload(cls, payload: str | bytes) -> Message:
        """解析 WebSocket 收到的 JSON 并创建消息对象。

        Parse the JSON received over WebSocket and create a message object.
        """
        if isinstance(payload, bytes):
            payload = payload.decode("utf-8")
        body = json.loads(payload)
        if not isinstance(body, dict):
            raise ValueError("Stream message must be a JSON object")
        headers = body.get("headers") or body.get("header") or {}
        if not isinstance(headers, dict):
            raise ValueError("Stream message headers must be an object")
        raw_data = body.get("data", "")
        if not isinstance(raw_data, str):
            raw_data = json.dumps(raw_data, ensure_ascii=False, separators=(",", ":"))
        return cls(
            spec_version=body.get("specVersion"),
            type=str(body.get("type", "")),
            headers=headers,
            raw_data=raw_data,
        )


@dataclass(frozen=True, slots=True)
class Ack:
    """EVENT 或 CALLBACK 消息的业务处理结果。

    Application result for an EVENT or CALLBACK message.
    """

    success: bool = True
    message: str = "OK"
    data: Any = field(default_factory=lambda: {"response": None})

    @classmethod
    def ok(cls, data: Any = None, message: str = "OK") -> Ack:
        """创建成功 ACK；可选地附带响应数据和说明。

        Create a successful ACK with optional response data and message.
        """
        return cls(True, message, {"response": None} if data is None else data)

    @classmethod
    def later(cls, message: str = "internal error") -> Ack:
        """创建失败 ACK，提示钉钉稍后重试。

        Create a failure ACK asking DingTalk to retry later.
        """
        return cls(False, message, {"status": "LATER", "message": message})
