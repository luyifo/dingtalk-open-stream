"""Cancellation of one lifecycle waiter must not stop shared client work."""

from __future__ import annotations

import asyncio
import json
import unittest
from collections.abc import Coroutine
from typing import Any
from unittest.mock import AsyncMock

from websockets.exceptions import ConnectionClosedError

from dingtalk_open_stream import Ack, Message, StreamClient


class MemoryWebSocket:
    def __init__(self) -> None:
        self.incoming: asyncio.Queue[str] = asyncio.Queue()
        self.sent: list[dict[str, object]] = []
        self.closed = False
        self.closed_send_attempts = 0

    async def recv(self) -> str:
        return await self.incoming.get()

    async def send(self, payload: str) -> None:
        if self.closed:
            self.closed_send_attempts += 1
            raise ConnectionClosedError(None, None)
        self.sent.append(json.loads(payload))

    async def close(self, **kwargs: object) -> None:
        self.closed = True


class ClientLifecycleTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.clients: list[StreamClient] = []
        self.tasks: list[asyncio.Task[object]] = []
        self.release_events: list[asyncio.Event] = []

    async def asyncTearDown(self) -> None:
        # Also clean up on the unfixed implementation, where an assertion may
        # run after a waiter has already cancelled the shared runner.
        for event in self.release_events:
            event.set()
        for client in self.clients:
            await asyncio.gather(client.close(), return_exceptions=True)
            await client._http.aclose()
        for task in self.tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*self.tasks, return_exceptions=True)

    def make_client(self) -> StreamClient:
        client = StreamClient("client-id", "client-secret", shutdown_timeout=5.0)
        self.clients.append(client)
        return client

    def create_task(
        self, coroutine: Coroutine[Any, Any, object]
    ) -> asyncio.Task[object]:
        task = asyncio.create_task(coroutine)
        self.tasks.append(task)
        return task

    async def start_registering_client(self) -> StreamClient:
        client = self.make_client()
        registering = asyncio.Event()

        async def register() -> tuple[str, str]:
            registering.set()
            await asyncio.Event().wait()
            raise AssertionError("unreachable")

        client._register = AsyncMock(side_effect=register)
        await client.start()
        await asyncio.wait_for(registering.wait(), timeout=2.0)
        return client

    async def test_wait_closed_timeout_leaves_client_running(self) -> None:
        client = await self.start_registering_client()

        with self.assertRaises(TimeoutError):
            await asyncio.wait_for(client.wait_closed(), timeout=0.01)

        self.assertFalse(client._runner.done())
        self.assertFalse(client._stop_event.is_set())
        self.assertFalse(client._http.is_closed)
        await client.close()
        self.assertTrue(client._http.is_closed)

    async def test_cancelled_wait_closed_does_not_cancel_another_waiter(self) -> None:
        client = await self.start_registering_client()
        cancelled_waiter = self.create_task(client.wait_closed())
        other_waiter = self.create_task(client.wait_closed())
        await asyncio.sleep(0)

        cancelled_waiter.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await cancelled_waiter

        self.assertFalse(client._runner.done())
        self.assertFalse(other_waiter.done())
        self.assertFalse(client._stop_event.is_set())
        await client.close()
        await asyncio.wait_for(other_waiter, timeout=2.0)

    async def assert_cancelled_shutdown_preserves_drain(self, method: str) -> None:
        client = self.make_client()
        websocket = MemoryWebSocket()
        client._register = AsyncMock(return_value=("wss://example.test", "ticket"))
        client._connect_or_stop = AsyncMock(return_value=websocket)
        handler_started = asyncio.Event()
        release_handler = asyncio.Event()
        self.release_events.append(release_handler)
        handler_cancelled = asyncio.Event()
        handler_finished = asyncio.Event()

        async def handler(message: Message) -> Ack:
            handler_started.set()
            try:
                await release_handler.wait()
            except asyncio.CancelledError:
                handler_cancelled.set()
                raise
            handler_finished.set()
            return Ack.ok()

        client.on("business", handler)
        websocket.incoming.put_nowait(
            json.dumps(
                {
                    "type": "CALLBACK",
                    "headers": {"topic": "business", "messageId": "message-1"},
                    "data": "{}",
                }
            )
        )
        await client.start()
        await asyncio.wait_for(handler_started.wait(), timeout=2.0)
        cancelled_waiter = self.create_task(getattr(client, method)())
        await asyncio.wait_for(client._stop_event.wait(), timeout=2.0)
        other_waiter = self.create_task(client.close())
        await asyncio.sleep(0)

        cancelled_waiter.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await cancelled_waiter

        self.assertFalse(handler_cancelled.is_set())
        self.assertFalse(handler_finished.is_set())
        self.assertFalse(client._runner.done())
        self.assertFalse(other_waiter.done())
        self.assertFalse(websocket.closed)
        self.assertFalse(client._http.is_closed)
        release_handler.set()
        await asyncio.wait_for(other_waiter, timeout=2.0)

        self.assertTrue(handler_finished.is_set())
        self.assertFalse(handler_cancelled.is_set())
        self.assertEqual(len(websocket.sent), 1)
        self.assertEqual(websocket.sent[0]["code"], 200)
        self.assertTrue(websocket.closed)
        self.assertTrue(client._http.is_closed)

    async def test_cancelled_stop_preserves_other_waiter_and_handler_drain(self) -> None:
        await self.assert_cancelled_shutdown_preserves_drain("stop")

    async def test_cancelled_close_preserves_other_waiter_and_handler_drain(self) -> None:
        await self.assert_cancelled_shutdown_preserves_drain("close")

    async def test_close_during_reregistration_drains_old_in_flight_callbacks(
        self,
    ) -> None:
        client = StreamClient(
            "client-id",
            "client-secret",
            reconnect_min_delay=0.001,
            reconnect_max_delay=0.001,
            shutdown_timeout=5.0,
        )
        self.clients.append(client)
        websocket = MemoryWebSocket()
        reregistering = asyncio.Event()
        registration_calls = 0

        async def register() -> tuple[str, str]:
            nonlocal registration_calls
            registration_calls += 1
            if registration_calls == 1:
                return "wss://example.test", "ticket"
            reregistering.set()
            await asyncio.Event().wait()
            raise AssertionError("unreachable")

        client._register = AsyncMock(side_effect=register)
        client._connect_or_stop = AsyncMock(return_value=websocket)
        first_started = asyncio.Event()
        first_completed = asyncio.Event()
        second_started = asyncio.Event()
        release_first = asyncio.Event()
        release_second = asyncio.Event()
        self.release_events.extend((release_first, release_second))
        completed: list[str] = []
        cancelled: list[str] = []

        async def handler(message: Message) -> Ack:
            message_id = message.message_id
            if message_id == "first":
                first_started.set()
                release = release_first
            else:
                second_started.set()
                release = release_second
            try:
                await release.wait()
            except asyncio.CancelledError:
                cancelled.append(message_id)
                raise
            completed.append(message_id)
            if message_id == "first":
                first_completed.set()
            return Ack.ok()

        def payload(message_id: str) -> str:
            return json.dumps(
                {
                    "type": "CALLBACK",
                    "headers": {"topic": "business", "messageId": message_id},
                    "data": "{}",
                }
            )

        client.on("business", handler)
        websocket.incoming.put_nowait(payload("first"))
        await client.start()
        await asyncio.wait_for(first_started.wait(), timeout=2.0)
        websocket.incoming.put_nowait(payload("second"))
        websocket.incoming.put_nowait(
            json.dumps(
                {
                    "type": "SYSTEM",
                    "headers": {"topic": "disconnect", "messageId": "disconnect"},
                    "data": "{}",
                }
            )
        )
        await asyncio.wait_for(reregistering.wait(), timeout=2.0)
        await asyncio.wait_for(second_started.wait(), timeout=2.0)
        self.assertTrue(websocket.closed)
        self.assertEqual(completed, [])

        closing = self.create_task(client.close())
        await asyncio.wait_for(client._stop_event.wait(), timeout=2.0)
        done, _ = await asyncio.wait((closing,), timeout=0.01)
        self.assertEqual(done, set(), "close returned before old callbacks finished")
        self.assertEqual(cancelled, [])
        self.assertFalse(client._http.is_closed)

        release_first.set()
        # Deliberately delayed handler returns still belong to their old socket.
        await asyncio.wait_for(first_completed.wait(), timeout=2.0)
        self.assertEqual(completed, ["first"])
        self.assertFalse(closing.done())
        release_second.set()
        await asyncio.wait_for(closing, timeout=2.0)

        self.assertEqual(completed, ["first", "second"])
        self.assertEqual(cancelled, [])
        self.assertEqual(websocket.closed_send_attempts, 2)
        self.assertTrue(client._http.is_closed)

    async def test_close_does_not_wait_for_user_created_background_task(self) -> None:
        client = self.make_client()
        websocket = MemoryWebSocket()
        client._register = AsyncMock(return_value=("wss://example.test", "ticket"))
        client._connect_or_stop = AsyncMock(return_value=websocket)
        background_started = asyncio.Event()
        release_background = asyncio.Event()
        self.release_events.append(release_background)
        background_finished = asyncio.Event()
        background_tasks: list[asyncio.Task[object]] = []

        async def background(message: Message) -> None:
            background_started.set()
            await release_background.wait()
            background_finished.set()

        async def handler(message: Message) -> Ack:
            background_tasks.append(self.create_task(background(message)))
            return Ack.ok()

        client.on("business", handler)
        websocket.incoming.put_nowait(json.dumps({
            "type": "CALLBACK",
            "headers": {"topic": "business", "messageId": "background"},
            "data": "{}",
        }))
        await client.start()
        await asyncio.wait_for(background_started.wait(), timeout=2.0)
        await asyncio.wait_for(client.close(), timeout=2.0)

        self.assertFalse(background_tasks[0].done())
        self.assertTrue(websocket.closed)
        self.assertEqual(len(websocket.sent), 1)
        self.assertEqual(websocket.sent[0]["headers"]["messageId"], "background")
        self.assertEqual(websocket.sent[0]["code"], 200)
        self.assertFalse(background_finished.is_set())
        release_background.set()
        await asyncio.wait_for(background_tasks[0], timeout=2.0)
        self.assertTrue(background_finished.is_set())
        self.assertEqual(len(websocket.sent), 1)

    async def test_close_before_start_shares_cleanup_across_waiters(self) -> None:
        client = self.make_client()
        cleanup_started = asyncio.Event()
        release_cleanup = asyncio.Event()
        self.release_events.append(release_cleanup)
        original_aclose = client._http.aclose
        cleanup_cancelled = asyncio.Event()

        async def close_http() -> None:
            cleanup_started.set()
            try:
                await release_cleanup.wait()
            except asyncio.CancelledError:
                cleanup_cancelled.set()
                raise
            await original_aclose()

        client._http.aclose = AsyncMock(side_effect=close_http)
        cancelled_waiter = self.create_task(client.close())
        await asyncio.wait_for(cleanup_started.wait(), timeout=2.0)
        other_waiter = self.create_task(client.close())
        await asyncio.sleep(0)

        cancelled_waiter.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await cancelled_waiter

        self.assertFalse(cleanup_cancelled.is_set())
        self.assertFalse(other_waiter.done())
        release_cleanup.set()
        await asyncio.wait_for(other_waiter, timeout=2.0)
        self.assertEqual(client._http.aclose.await_count, 1)
        self.assertTrue(client._http.is_closed)


if __name__ == "__main__":
    unittest.main()
