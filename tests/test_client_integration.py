"""Exercise reconnection and graceful shutdown over real local WebSockets."""

from __future__ import annotations

import asyncio
import json
import unittest

import httpx
from websockets.asyncio.server import ServerConnection, serve

from dingtalk_open_stream import Ack, Message, StreamClient


class ClientWebSocketTests(unittest.IsolatedAsyncioTestCase):
    async def test_delayed_handler_returns_never_ack_on_replacement_socket(self) -> None:
        started = asyncio.Event()
        release = asyncio.Event()
        ping_received = asyncio.Event()
        processed: list[str | None] = []
        completed: list[str | None] = []
        received_acks: list[dict] = []
        connections = 0

        def message(kind: str, topic: str, message_id: str) -> str:
            return json.dumps(
                {
                    "type": kind,
                    "headers": {"topic": topic, "messageId": message_id},
                    "data": '{"opaque":"keepalive"}',
                }
            )

        async def server_handler(websocket: ServerConnection) -> None:
            nonlocal connections
            connections += 1
            if connections == 1:
                await websocket.send(message("CALLBACK", "business", "bot-1"))
                await started.wait()
                await websocket.send(message("CALLBACK", "business", "bot-2"))
                await websocket.send(message("SYSTEM", "disconnect", "disconnect"))
                await websocket.wait_closed()
            else:
                await websocket.send(message("SYSTEM", "ping", "ping"))
                async for payload in websocket:
                    ack = json.loads(payload)
                    received_acks.append(ack)
                    if ack["headers"]["messageId"] == "ping":
                        ping_received.set()

        async def handler(message: Message) -> Ack:
            processed.append(message.message_id)
            started.set()
            await release.wait()
            completed.append(message.message_id)
            return Ack.ok()

        async with serve(server_handler, "127.0.0.1", 0) as server:
            port = server.sockets[0].getsockname()[1]
            registrations: list[dict] = []

            def register(request: httpx.Request) -> httpx.Response:
                registrations.append(json.loads(request.content))
                return httpx.Response(
                    200,
                    json={"endpoint": f"ws://127.0.0.1:{port}", "ticket": str(len(registrations))},
                )

            client = StreamClient(
                "client", "secret",
                reconnect_min_delay=0.01, reconnect_max_delay=0.01,
                shutdown_timeout=1,
            )
            await client._http.aclose()
            client._http = httpx.AsyncClient(transport=httpx.MockTransport(register))
            client.on("business", handler)
            closing: asyncio.Task[None] | None = None
            try:
                await client.start()
                await asyncio.wait_for(ping_received.wait(), timeout=3)
                self.assertEqual(connections, 2)
                self.assertEqual(len(registrations), 2)
                self.assertEqual(processed, ["bot-1", "bot-2"])

                closing = asyncio.create_task(client.close())
                await client._stop_event.wait()
                await asyncio.sleep(0)
                self.assertFalse(closing.done())
                self.assertIsNotNone(client._websocket)
                release.set()
                await asyncio.wait_for(closing, timeout=3)

                self.assertEqual(processed, ["bot-1", "bot-2"])
                self.assertEqual(completed, ["bot-1", "bot-2"])
                self.assertEqual([ack["headers"]["messageId"] for ack in received_acks], ["ping"])
                self.assertTrue(client._http.is_closed)
                self.assertIsNone(client._websocket)
            finally:
                release.set()
                await client.close()
                if closing is not None:
                    await closing

    async def test_graceful_close_sends_final_ack_on_live_socket(self) -> None:
        started = asyncio.Event()
        release = asyncio.Event()
        received_acks: list[dict] = []

        async def server_handler(websocket: ServerConnection) -> None:
            await websocket.send(json.dumps({
                "type": "CALLBACK", "headers": {"topic": "business", "messageId": "bot"},
                "data": "{}",
            }))
            async for payload in websocket:
                received_acks.append(json.loads(payload))

        async def handler(message: Message) -> Ack:
            started.set()
            await release.wait()
            return Ack.ok()

        async with serve(server_handler, "127.0.0.1", 0) as server:
            port = server.sockets[0].getsockname()[1]
            client = StreamClient("client", "secret", shutdown_timeout=1)
            await client._http.aclose()
            client._http = httpx.AsyncClient(transport=httpx.MockTransport(
                lambda request: httpx.Response(200, json={
                    "endpoint": f"ws://127.0.0.1:{port}", "ticket": "ticket",
                })
            ))
            client.on("business", handler)
            closing: asyncio.Task[None] | None = None
            try:
                await client.start()
                await asyncio.wait_for(started.wait(), timeout=3)
                closing = asyncio.create_task(client.close())
                await client._stop_event.wait()
                release.set()
                await asyncio.wait_for(closing, timeout=3)
                self.assertEqual(len(received_acks), 1)
                self.assertEqual(received_acks[0]["headers"]["messageId"], "bot")
                self.assertEqual(received_acks[0]["code"], 200)
            finally:
                release.set()
                await client.close()
                if closing is not None:
                    await closing

    async def test_returned_ack_arrives_before_user_background_business_finishes(self) -> None:
        for message_type in ("EVENT", "CALLBACK"):
            with self.subTest(message_type=message_type):
                await self.assert_background_business_acknowledged(message_type)

    async def assert_background_business_acknowledged(self, message_type: str) -> None:
        background_started = asyncio.Event()
        background_finished = asyncio.Event()
        release_background = asyncio.Event()
        ack_received = asyncio.Event()
        verify_after_business = asyncio.Event()
        ping_received = asyncio.Event()
        received_acks: list[dict] = []
        background_tasks: list[asyncio.Task[None]] = []

        async def background(message: Message) -> None:
            background_started.set()
            await release_background.wait()
            background_finished.set()

        async def handler(message: Message) -> Ack:
            task = asyncio.create_task(background(message))
            background_tasks.append(task)
            return Ack.ok()

        async def server_handler(websocket: ServerConnection) -> None:
            await websocket.send(json.dumps({
                "type": message_type,
                "headers": {"topic": "business", "messageId": "business-1"},
                "data": "{}",
            }))
            received_acks.append(json.loads(await websocket.recv()))
            ack_received.set()
            await verify_after_business.wait()
            await websocket.send(json.dumps({
                "type": "SYSTEM",
                "headers": {"topic": "ping", "messageId": "after-business"},
                "data": "{}",
            }))
            async for payload in websocket:
                ack = json.loads(payload)
                received_acks.append(ack)
                if ack["headers"]["messageId"] == "after-business":
                    ping_received.set()

        async with serve(server_handler, "127.0.0.1", 0) as server:
            port = server.sockets[0].getsockname()[1]
            client = StreamClient("client", "secret", shutdown_timeout=1)
            await client._http.aclose()
            client._http = httpx.AsyncClient(transport=httpx.MockTransport(
                lambda request: httpx.Response(200, json={
                    "endpoint": f"ws://127.0.0.1:{port}", "ticket": "ticket",
                })
            ))
            client.on("business", handler)
            try:
                await client.start()
                await asyncio.wait_for(ack_received.wait(), timeout=3)
                await asyncio.wait_for(background_started.wait(), timeout=3)

                self.assertFalse(background_finished.is_set())
                self.assertFalse(background_tasks[0].done())
                self.assertEqual(len(received_acks), 1)
                ack = received_acks[0]
                self.assertEqual(ack["headers"]["messageId"], "business-1")
                self.assertEqual(ack["code"], 200)
                data = json.loads(ack["data"])
                if message_type == "EVENT":
                    self.assertEqual(data["status"], "SUCCESS")
                else:
                    self.assertEqual(data, {"response": None})

                release_background.set()
                await asyncio.wait_for(background_tasks[0], timeout=3)
                verify_after_business.set()
                await asyncio.wait_for(ping_received.wait(), timeout=3)

                self.assertTrue(background_finished.is_set())
                self.assertEqual(
                    [ack["headers"]["messageId"] for ack in received_acks],
                    ["business-1", "after-business"],
                )
            finally:
                release_background.set()
                verify_after_business.set()
                await client.close()
                await asyncio.gather(*background_tasks, return_exceptions=True)


if __name__ == "__main__":
    unittest.main()
