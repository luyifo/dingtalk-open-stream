"""钉钉 Stream 协议的公开数据模型。

Public data models for the DingTalk Stream protocol.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any, Literal, TypeAlias


Topic: TypeAlias = str
# 已知值用于编辑器提示，不限制调用方使用新的或自定义的 topic 字符串。
# Known values aid completion without restricting new or custom topic strings.
KnownTopic: TypeAlias = Literal[
    "*",
    "/v1.0/im/bot/messages/get",
    "/v1.0/card/instances/callback",
]


@dataclass(frozen=True, slots=True)
class Subscription:
    """注册连接时提交的订阅项；type 区分事件与回调。

    A subscription sent during connection registration; type selects events or callbacks.
    """

    topic: Topic
    type: Literal["EVENT", "CALLBACK"]

    def as_dict(self) -> dict[str, str]:
        """将订阅转换成钉钉注册接口需要的字典。

        Convert the subscription to the dictionary required by DingTalk's registration API.
        """
        return {"topic": self.topic, "type": self.type}


@dataclass(frozen=True, slots=True)
class Message:
    """保留消息头和原始数据字符串的 Stream 消息封装。

    ``data`` 按需解析，``raw_data`` 可用于读取未解析的消息体。
    冻结仅限制字段重新赋值，``headers`` 字典本身仍可修改。

    Stream envelope retaining headers and a raw data string. ``data`` decodes on access;
    ``raw_data`` exposes the unparsed body. Freezing prevents field reassignment,
    but the headers dictionary remains mutable.
    """

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
        """尝试解析 JSON 数据；非 JSON 内容原样返回，每次访问都会重新解析。

        Decode JSON data on each access, returning the original value if decoding fails.
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
        # 同时兼容 headers 和 header；优先使用非空的 headers。
        # Accept both headers and header, preferring a nonempty headers value.
        headers = body.get("headers") or body.get("header") or {}
        if not isinstance(headers, dict):
            raise ValueError("Stream message headers must be an object")
        raw_data = body.get("data", "")
        if not isinstance(raw_data, str):
            # 统一保存为字符串，使 data 属性也能处理直接传入对象的消息。
            # Normalize object payloads to strings for consistent lazy decoding via data.
            raw_data = json.dumps(raw_data, ensure_ascii=False, separators=(",", ":"))
        return cls(
            spec_version=body.get("specVersion"),
            type=str(body.get("type", "")),
            headers=headers,
            raw_data=raw_data,
        )


@dataclass(frozen=True, slots=True)
class Ack:
    """处理器必须返回的 EVENT 或 CALLBACK 消息确认数据。

    ``data`` 必须能被 JSON 序列化；返回后由 SDK 按消息类型封装并发送。
    应用自行提交的后台业务完成或失败不会更改已返回的确认结果。

    Acknowledgement data that an EVENT or CALLBACK handler must return.
    ``data`` must be JSON serializable; the SDK wraps it for the message type and sends it.
    Completion or failure of user-created background work doesn't change a returned ACK.
    """

    success: bool = True
    message: str = "OK"
    data: Any = field(default_factory=lambda: {"response": None})

    @classmethod
    def ok(cls, data: Any = None, message: str = "OK") -> Ack:
        """创建成功 ACK；data 为 None 时使用默认的空响应对象。

        Create a successful ACK, using the default response object when data is None.
        """
        return cls(True, message, {"response": None} if data is None else data)

    @classmethod
    def later(cls, message: str = "internal error") -> Ack:
        """创建失败 ACK；是否重推由消息类型和服务端决定。

        Create a failure ACK; redelivery depends on the message type and server.
        """
        return cls(False, message, {"status": "LATER", "message": message})
