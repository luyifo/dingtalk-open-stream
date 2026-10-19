"""Handlers return ACKs independently of their user-managed background work."""

from __future__ import annotations

import asyncio
import threading
import unittest
from collections.abc import Awaitable

from dingtalk_open_stream import Ack, Message, StreamClient

from test_client_dispatch import BOT_TOPIC, FakeWebSocket


class ClientSyncHandlerTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.client = StreamClient("client", "secret", shutdown_timeout=0.05)
        self.websocket = FakeWebSocket()
        self.reader = asyncio.create_task(self.client._serve_connection(self.websocket))
        self.thread_releases: list[threading.Event] = []
        self.releases: list[asyncio.Event] = []
        self.background_tasks: list[asyncio.Task[None]] = []

    async def asyncTearDown(self) -> None:
        for release in self.thread_releases:
            release.set()
        for release in self.releases:
            release.set()
        self.reader.cancel()
        await asyncio.gather(self.reader, return_exceptions=True)
        await self.client.close()
        await asyncio.gather(*self.background_tasks, return_exceptions=True)

    async def test_sync_handler_can_schedule_background_work_and_return_ack(self) -> None:
        loop = asyncio.get_running_loop()
        loop_thread = threading.get_ident()
        handler_loops: list[asyncio.AbstractEventLoop] = []
        handler_threads: list[int] = []
        started = asyncio.Event()
        release = asyncio.Event()
        self.releases.append(release)
        finished = asyncio.Event()

        async def background() -> None:
            started.set()
            await release.wait()
            finished.set()

        def handler(message: Message) -> Ack:
            handler_loops.append(asyncio.get_running_loop())
            handler_threads.append(threading.get_ident())
            self.background_tasks.append(asyncio.create_task(background()))
            return Ack.ok()

        self.client.on(BOT_TOPIC, handler)
        self.websocket.feed("CALLBACK", "bot-1", BOT_TOPIC)
        ack = await self.websocket.wait_for_ack("bot-1")
        await asyncio.wait_for(started.wait(), 2)

        self.assertEqual(ack["code"], 200)
        self.assertEqual(handler_loops, [loop])
        self.assertEqual(handler_threads, [loop_thread])
        self.assertFalse(finished.is_set())
        release.set()
        await asyncio.wait_for(asyncio.gather(*self.background_tasks), 2)
        self.assertTrue(finished.is_set())
        self.assertEqual(len(self.websocket.acks), 1)

    async def test_sync_handler_returning_awaitable_keeps_client_loop(self) -> None:
        loop = asyncio.get_running_loop()
        loop_thread = threading.get_ident()
        handler_threads: list[int] = []
        coroutine_loops: list[asyncio.AbstractEventLoop] = []

        async def result() -> Ack:
            coroutine_loops.append(asyncio.get_running_loop())
            return Ack.ok()

        def handler(message: Message) -> Awaitable[Ack]:
            handler_threads.append(threading.get_ident())
            return result()

        self.client.on(BOT_TOPIC, handler)
        self.websocket.feed("CALLBACK", "bot-1", BOT_TOPIC)
        ack = await self.websocket.wait_for_ack("bot-1")

        self.assertEqual(ack["code"], 200)
        self.assertEqual(coroutine_loops, [loop])
        self.assertEqual(handler_threads, [loop_thread])

    async def test_async_callable_runs_on_client_loop_and_returns_ack(self) -> None:
        loop = asyncio.get_running_loop()
        handler_loops: list[asyncio.AbstractEventLoop] = []

        class AsyncHandler:
            async def __call__(self, message: Message) -> Ack:
                handler_loops.append(asyncio.get_running_loop())
                return Ack.ok()

        self.client.on(BOT_TOPIC, AsyncHandler())
        self.websocket.feed("CALLBACK", "bot-1", BOT_TOPIC)
        ack = await self.websocket.wait_for_ack("bot-1")

        self.assertEqual(ack["code"], 200)
        self.assertEqual(handler_loops, [loop])

    async def test_handler_can_run_thread_work_in_background_before_returning_ack(self) -> None:
        started = threading.Event()
        release = threading.Event()
        self.thread_releases.append(release)
        finished = threading.Event()
        worker_threads: list[int] = []
        loop_thread = threading.get_ident()

        def blocking_work() -> None:
            worker_threads.append(threading.get_ident())
            started.set()
            if not release.wait(2):
                raise TimeoutError("test did not release worker")
            finished.set()

        async def handler(message: Message) -> Ack:
            self.background_tasks.append(asyncio.create_task(asyncio.to_thread(blocking_work)))
            return Ack.ok()

        self.client.on(BOT_TOPIC, handler)
        self.websocket.feed("CALLBACK", "bot-1", BOT_TOPIC)
        ack = await self.websocket.wait_for_ack("bot-1")
        self.assertTrue(await asyncio.to_thread(started.wait, 2))
        self.websocket.feed("SYSTEM", "ping-1", "ping", {"opaque": "still-alive"})

        await self.websocket.wait_for_ack("ping-1")
        self.assertEqual(ack["code"], 200)
        self.assertFalse(finished.is_set())
        self.assertNotEqual(worker_threads, [loop_thread])
        release.set()
        await asyncio.wait_for(asyncio.gather(*self.background_tasks), 2)
        self.assertTrue(finished.is_set())
        self.assertEqual(len(self.websocket.acks), 2)

    async def test_sdk_shutdown_leaves_user_background_task_under_user_control(self) -> None:
        started = asyncio.Event()
        release = asyncio.Event()
        self.releases.append(release)
        finished = asyncio.Event()

        async def background() -> None:
            started.set()
            await release.wait()
            finished.set()

        async def handler(message: Message) -> Ack:
            self.background_tasks.append(asyncio.create_task(background()))
            return Ack.ok()

        self.client.on(BOT_TOPIC, handler)
        self.websocket.feed("CALLBACK", "bot-1", BOT_TOPIC)
        await self.websocket.wait_for_ack("bot-1")
        await asyncio.wait_for(started.wait(), 2)

        await asyncio.wait_for(self.client.close(), timeout=1)
        self.assertTrue(self.client._http.is_closed)
        self.assertFalse(finished.is_set())
        self.assertFalse(self.background_tasks[0].done())
        self.assertEqual([ack["headers"]["messageId"] for ack in self.websocket.acks], ["bot-1"])
        release.set()
        await asyncio.wait_for(asyncio.gather(*self.background_tasks), 2)
        self.assertTrue(finished.is_set())
        self.assertEqual(len(self.websocket.acks), 1)


if __name__ == "__main__":
    unittest.main()
