from __future__ import annotations

import asyncio
import json
import logging
import unittest
from typing import Any

from websockets.exceptions import ConnectionClosedError

from dingtalk_open_stream import Ack, Message, StreamClient
from dingtalk_open_stream.client import _ReconnectRequested


BOT_TOPIC = "/v1.0/im/bot/messages/get"


class FakeWebSocket:
    def __init__(self) -> None:
        self.incoming: asyncio.Queue[str | Exception] = asyncio.Queue()
        self.acks: list[dict[str, Any]] = []
        self.ack_received = asyncio.Event()
        self.closed = False

    def feed(
        self,
        message_type: str,
        message_id: str,
        topic: str,
        data: Any = None,
    ) -> None:
        self.incoming.put_nowait(
            json.dumps(
                {
                    "specVersion": "1.0",
                    "type": message_type,
                    "headers": {"messageId": message_id, "topic": topic},
                    "data": json.dumps({} if data is None else data),
                }
            )
        )

    def lose_connection(self) -> None:
        self.closed = True
        self.incoming.put_nowait(ConnectionClosedError(None, None))

    async def recv(self) -> str:
        value = await self.incoming.get()
        if isinstance(value, Exception):
            raise value
        return value

    async def send(self, payload: str) -> None:
        if self.closed:
            raise ConnectionClosedError(None, None)
        self.acks.append(json.loads(payload))
        self.ack_received.set()

    async def close(self, **kwargs: Any) -> None:
        self.closed = True

    async def wait_for_ack(self, message_id: str) -> dict[str, Any]:
        async with asyncio.timeout(2):
            while True:
                for ack in self.acks:
                    if ack["headers"]["messageId"] == message_id:
                        return ack
                self.ack_received.clear()
                await self.ack_received.wait()


class ClientDispatchTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        test_logger = logging.getLogger(self.id())
        test_logger.addHandler(logging.NullHandler())
        test_logger.propagate = False
        self.client = StreamClient(
            "test-client",
            "test-secret",
            shutdown_timeout=0.5,
            logger=test_logger,
        )
        self.readers: list[asyncio.Task[None]] = []
        self.releases: list[asyncio.Event] = []
        self.background_tasks: list[asyncio.Task[None]] = []

    async def asyncTearDown(self) -> None:
        for release in self.releases:
            release.set()
        for reader in self.readers:
            if not reader.done():
                reader.cancel()
        await asyncio.gather(*self.readers, return_exceptions=True)
        await self.client.close()
        await asyncio.gather(*self.background_tasks, return_exceptions=True)

    def serve(self, websocket: FakeWebSocket) -> asyncio.Task[None]:
        reader = asyncio.create_task(self.client._serve_connection(websocket))
        self.readers.append(reader)
        return reader

    def release_event(self) -> asyncio.Event:
        release = asyncio.Event()
        self.releases.append(release)
        return release

    async def test_only_returned_acks_are_sent_for_callback_and_event(self) -> None:
        websocket = FakeWebSocket()
        completed = asyncio.Event()
        handled: list[str | None] = []

        async def handler(message: Message) -> Any:
            handled.append(message.message_id)
            if len(handled) == 10:
                completed.set()
            outcome = message.data["outcome"]
            if outcome == "raises":
                raise RuntimeError("application must handle this failure")
            if outcome == "ok":
                return Ack.ok()
            if outcome == "later":
                return Ack.later("try later")
            if outcome == "invalid":
                return {"success": True}
            return None

        self.client.on(BOT_TOPIC, handler)
        self.serve(websocket)
        expected = []
        with self.assertLogs(self.id(), level="WARNING") as records:
            for message_type in ("CALLBACK", "EVENT"):
                for outcome in ("none", "invalid", "ok", "later", "raises"):
                    message_id = f"{message_type}-{outcome}"
                    expected.append(message_id)
                    websocket.feed(message_type, message_id, BOT_TOPIC, {"outcome": outcome})
            websocket.feed("CALLBACK", "unregistered-callback", "unknown")
            websocket.feed("EVENT", "unregistered-event", "unknown")
            websocket.feed("SYSTEM", "barrier", "ping")

            await asyncio.wait_for(completed.wait(), 2)
            await websocket.wait_for_ack("barrier")
            await asyncio.gather(*self.client._handler_tasks)

        self.assertEqual(handled, expected)
        self.assertCountEqual(
            [ack["headers"]["messageId"] for ack in websocket.acks],
            ["CALLBACK-ok", "CALLBACK-later", "EVENT-ok", "EVENT-later", "barrier"],
        )
        self.assertGreaterEqual(len(records.records), 8)
        for ack in websocket.acks:
            expected_code = 500 if ack["headers"]["messageId"].endswith("later") else 200
            self.assertEqual(ack["code"], expected_code)

    async def test_async_handler_returns_ack_while_background_work_is_pending(self) -> None:
        for message_type in ("CALLBACK", "EVENT"):
            with self.subTest(message_type=message_type):
                websocket = FakeWebSocket()
                release = self.release_event()
                started = asyncio.Event()
                finished = asyncio.Event()

                async def background() -> None:
                    started.set()
                    await release.wait()
                    finished.set()

                async def handler(message: Message) -> Ack:
                    self.background_tasks.append(asyncio.create_task(background()))
                    return Ack.ok()

                self.client.on(BOT_TOPIC, handler)
                reader = self.serve(websocket)
                websocket.feed(message_type, "business-1", BOT_TOPIC)

                ack = await websocket.wait_for_ack("business-1")
                await asyncio.wait_for(started.wait(), 2)
                self.assertEqual(ack["code"], 200)
                self.assertEqual(ack["message"], "OK")
                self.assertEqual(ack["headers"]["contentType"], "application/json")
                expected_data = {"response": None}
                if message_type == "EVENT":
                    expected_data.update(status="SUCCESS", message="OK")
                self.assertEqual(json.loads(ack["data"]), expected_data)
                self.assertFalse(finished.is_set())
                release.set()
                await asyncio.wait_for(finished.wait(), 2)
                await asyncio.gather(*self.client._handler_tasks)
                self.assertEqual(len(websocket.acks), 1)
                reader.cancel()
                await asyncio.gather(reader, return_exceptions=True)

    async def test_background_failure_does_not_replace_or_repeat_returned_ack(self) -> None:
        websocket = FakeWebSocket()
        release = self.release_event()

        async def background() -> None:
            await release.wait()
            raise RuntimeError("business failed after the message was accepted")

        async def handler(message: Message) -> Ack:
            self.background_tasks.append(asyncio.create_task(background()))
            return Ack.ok()

        self.client.on(BOT_TOPIC, handler)
        self.serve(websocket)
        websocket.feed("EVENT", "event-1", BOT_TOPIC)

        ack = await websocket.wait_for_ack("event-1")
        self.assertEqual(ack["code"], 200)
        self.assertFalse(self.background_tasks[0].done())
        release.set()
        result = await asyncio.gather(*self.background_tasks, return_exceptions=True)
        await asyncio.gather(*self.client._handler_tasks)
        self.assertIsInstance(result[0], RuntimeError)
        self.assertEqual(len(websocket.acks), 1)
        self.assertEqual(json.loads(websocket.acks[0]["data"])["status"], "SUCCESS")

    async def test_async_handler_result_is_awaited_and_ping_remains_responsive(self) -> None:
        websocket = FakeWebSocket()
        started = asyncio.Event()
        release = self.release_event()

        async def handler(message: Message) -> Ack:
            started.set()
            await release.wait()
            return Ack.ok({"answer": "完成"}, message="accepted")

        self.client.on(BOT_TOPIC, handler)
        self.serve(websocket)
        websocket.feed("CALLBACK", "bot-1", BOT_TOPIC)
        await asyncio.wait_for(started.wait(), 2)
        websocket.feed("SYSTEM", "ping-1", "ping", {"opaque": "keepalive"})

        ping_ack = await websocket.wait_for_ack("ping-1")
        self.assertEqual(json.loads(ping_ack["data"]), {"opaque": "keepalive"})
        self.assertEqual(len(websocket.acks), 1)
        release.set()
        ack = await websocket.wait_for_ack("bot-1")
        self.assertEqual(ack["code"], 200)
        self.assertEqual(ack["message"], "accepted")
        self.assertEqual(json.loads(ack["data"]), {"answer": "完成"})

    async def test_event_returned_success_and_later_have_protocol_payloads(self) -> None:
        websocket = FakeWebSocket()

        async def handler(message: Message) -> Ack:
            if message.message_id == "success":
                return Ack.ok({"value": 42}, message="accepted")
            if message.message_id == "default":
                return Ack.ok()
            return Ack.later("queue unavailable")

        self.client.on("*", handler)
        self.serve(websocket)
        for message_id in ("success", "default", "later"):
            websocket.feed("EVENT", message_id, "example.event")

        success = await websocket.wait_for_ack("success")
        default = await websocket.wait_for_ack("default")
        later = await websocket.wait_for_ack("later")
        self.assertEqual(success["code"], 200)
        self.assertEqual(json.loads(success["data"]), {"value": 42, "status": "SUCCESS", "message": "accepted"})
        self.assertEqual(json.loads(default["data"]), {"response": None, "status": "SUCCESS", "message": "OK"})
        self.assertEqual(later["code"], 500)
        self.assertEqual(json.loads(later["data"]), {"status": "LATER", "message": "queue unavailable"})
        self.assertEqual(len(websocket.acks), 3)

    async def test_callback_returned_later_is_sent_once(self) -> None:
        websocket = FakeWebSocket()

        async def handler(message: Message) -> Ack:
            return Ack.later("busy")

        self.client.on(BOT_TOPIC, handler)
        self.serve(websocket)
        websocket.feed("CALLBACK", "bot-1", BOT_TOPIC)
        ack = await websocket.wait_for_ack("bot-1")
        await asyncio.gather(*self.client._handler_tasks)
        self.assertEqual(ack["code"], 500)
        self.assertEqual(json.loads(ack["data"]), {"status": "LATER", "message": "busy"})
        self.assertEqual(len(websocket.acks), 1)

    async def test_sdk_does_not_limit_business_concurrency_or_reject_events(self) -> None:
        websocket = FakeWebSocket()
        all_started = asyncio.Event()
        release = self.release_event()
        processed: list[str | None] = []
        count = 105

        async def handler(message: Message) -> Ack:
            processed.append(message.message_id)
            if len(processed) == count:
                all_started.set()
            await release.wait()
            return Ack.ok()

        self.client.on("*", handler)
        self.serve(websocket)
        for index in range(count):
            websocket.feed("EVENT", f"event-{index}", "example.event")
        websocket.feed("SYSTEM", "ping-1", "ping")

        await asyncio.wait_for(all_started.wait(), 2)
        await websocket.wait_for_ack("ping-1")
        self.assertEqual(len(processed), count)
        self.assertEqual([ack["headers"]["messageId"] for ack in websocket.acks], ["ping-1"])
        release.set()
        await websocket.wait_for_ack(f"event-{count - 1}")
        await asyncio.gather(*self.client._handler_tasks)
        self.assertCountEqual(
            [ack["headers"]["messageId"] for ack in websocket.acks],
            ["ping-1", *(f"event-{index}" for index in range(count))],
        )
        self.assertTrue(all(ack["code"] == 200 for ack in websocket.acks))

    async def test_disconnect_does_not_ack_or_cancel_pending_handler(self) -> None:
        websocket = FakeWebSocket()
        started = asyncio.Event()
        release = self.release_event()

        async def handler(message: Message) -> Ack:
            started.set()
            await release.wait()
            return Ack.ok()

        self.client.on(BOT_TOPIC, handler)
        reader = self.serve(websocket)
        websocket.feed("CALLBACK", "bot-1", BOT_TOPIC)
        await asyncio.wait_for(started.wait(), 2)
        websocket.feed("SYSTEM", "disconnect-1", "disconnect", {"reason": "connection is expired"})

        with self.assertRaises(_ReconnectRequested):
            await asyncio.wait_for(asyncio.shield(reader), 2)
        self.assertEqual(len(self.client._handler_tasks), 1)
        self.assertEqual(websocket.acks, [])
        release.set()
        await websocket.wait_for_ack("bot-1")
        self.assertEqual([ack["headers"]["messageId"] for ack in websocket.acks], ["bot-1"])

    async def test_returned_ack_uses_original_socket_after_connection_loss(self) -> None:
        original_socket = FakeWebSocket()
        replacement_socket = FakeWebSocket()
        started = asyncio.Event()
        release = self.release_event()

        async def handler(message: Message) -> Ack:
            started.set()
            await release.wait()
            return Ack.ok()

        self.client.on(BOT_TOPIC, handler)
        reader = self.serve(original_socket)
        original_socket.feed("CALLBACK", "bot-1", BOT_TOPIC)
        await asyncio.wait_for(started.wait(), 2)
        original_socket.lose_connection()
        with self.assertRaises(ConnectionClosedError):
            await asyncio.wait_for(asyncio.shield(reader), 2)
        self.client._websocket = replacement_socket

        with self.assertLogs(self.id(), level="ERROR"):
            release.set()
            results = await asyncio.gather(*self.client._handler_tasks, return_exceptions=True)
        self.assertIsInstance(results[0], ConnectionClosedError)
        self.assertEqual(original_socket.acks, [])
        self.assertEqual(replacement_socket.acks, [])

    async def test_missing_message_id_logs_failure_without_sending_ack(self) -> None:
        websocket = FakeWebSocket()
        completed = asyncio.Event()

        async def handler(message: Message) -> Ack:
            completed.set()
            return Ack.ok()

        self.client.on(BOT_TOPIC, handler)
        self.serve(websocket)
        with self.assertLogs(self.id(), level="ERROR") as records:
            websocket.incoming.put_nowait(
                json.dumps({"type": "CALLBACK", "headers": {"topic": BOT_TOPIC}})
            )
            await asyncio.wait_for(completed.wait(), 2)
            await asyncio.gather(*self.client._handler_tasks, return_exceptions=True)
            await asyncio.sleep(0)  # Let the task's failure-reporting callback run.

        self.assertIn("messageId", "\n".join(records.output))
        self.assertEqual(websocket.acks, [])

    async def test_unknown_system_topic_is_not_forwarded_to_business_handler(self) -> None:
        websocket = FakeWebSocket()
        handled: list[str | None] = []

        async def handler(message: Message) -> Ack:
            handled.append(message.message_id)
            return Ack.ok()

        self.client.on("*", handler)
        self.serve(websocket)
        websocket.feed("SYSTEM", "unknown-system", "future-system-topic")
        websocket.feed("SYSTEM", "ping-1", "ping")

        await websocket.wait_for_ack("ping-1")
        self.assertEqual(handled, [])
        self.assertEqual([ack["headers"]["messageId"] for ack in websocket.acks], ["ping-1"])


if __name__ == "__main__":
    unittest.main()
