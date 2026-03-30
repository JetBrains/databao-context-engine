from __future__ import annotations

import asyncio
import concurrent.futures
import queue
import threading
from collections.abc import Coroutine
from typing import Any, Sequence

import asyncpg


class SyncAsyncpgConnection:
    """A synchronous wrapper around asyncpg that works correctly in both sync and async contexts.

    When called from an async context (e.g., MCP server), operations run in a separate thread
    with its own event loop to avoid blocking the calling event loop.

    Note: Uses class-level thread-local storage for event loops in sync contexts. Multiple
    connection instances in the same thread will share the same event loop.
    """

    # Thread-local storage for event loops in sync contexts
    # Shared across all instances to avoid creating multiple loops per thread
    _thread_local = threading.local()

    # Worker loop initialization timeout in seconds
    _WORKER_LOOP_INIT_TIMEOUT = 1.0

    def __init__(self, connect_kwargs: dict[str, Any]):
        self._connect_kwargs = connect_kwargs
        self._conn: asyncpg.Connection | None = None
        self._in_async_context = False
        self._executor: concurrent.futures.ThreadPoolExecutor | None = None
        self._worker_loop: asyncio.AbstractEventLoop | None = None

    async def _async_connect(self) -> None:
        """Establish the async database connection."""
        self._conn = await asyncpg.connect(**self._connect_kwargs)

    async def _async_close(self) -> None:
        """Close the async database connection if it exists."""
        if self._conn is not None:
            await self._conn.close()
            self._conn = None

    async def _async_fetch_rows(self, sql: str, params: Sequence[Any] | None) -> list[dict]:
        """Fetch rows from the database and return as list of dicts."""
        if self._conn is None:
            raise RuntimeError("Connection is not open")
        query_params = [] if params is None else list(params)
        records = await self._conn.fetch(sql, *query_params)
        return [dict(r) for r in records]

    async def _async_fetch_scalar_values(self, sql: str) -> list[Any]:
        """Fetch scalar values (first column) from the database."""
        if self._conn is None:
            raise RuntimeError("Connection is not open")
        records = await self._conn.fetch(sql)
        return [r[0] for r in records]

    def _get_or_create_loop(self) -> asyncio.AbstractEventLoop:
        """Get or create a persistent event loop for this thread."""
        if not hasattr(self._thread_local, "loop") or self._thread_local.loop is None:
            self._thread_local.loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self._thread_local.loop)
        return self._thread_local.loop

    def _setup_worker_loop(self) -> None:
        """Initialize a persistent worker loop for async context.

        Creates a dedicated thread with its own event loop that runs for the
        lifetime of this connection. This ensures all asyncpg operations run
        in the same loop, which is required by asyncpg.
        """
        result_queue: queue.Queue[asyncio.AbstractEventLoop] = queue.Queue()

        def init_loop():
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            result_queue.put(loop)
            loop.run_forever()
            loop.close()

        self._executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)
        self._executor.submit(init_loop)
        self._worker_loop = result_queue.get(timeout=self._WORKER_LOOP_INIT_TIMEOUT)

    def _run_sync(self, coro: Coroutine[Any, Any, Any]) -> Any:
        """Run a coroutine synchronously, handling both sync and async contexts.

        - In sync context: uses a persistent thread-local event loop
        - In async context: uses a persistent worker thread with its own event loop

        Args:
            coro: The coroutine to execute synchronously

        Returns:
            The result of the coroutine execution

        Raises:
            RuntimeError: If worker loop is not initialized in async context
        """
        if self._in_async_context:
            # We're in an async context - use the persistent worker loop
            if self._worker_loop is None:
                raise RuntimeError("Worker loop not initialized")
            future = asyncio.run_coroutine_threadsafe(coro, self._worker_loop)
            return future.result()
        # No async context - use our thread-local loop
        loop = self._get_or_create_loop()
        return loop.run_until_complete(coro)

    def __enter__(self):
        # Check if we're in an async context and remember it
        try:
            # This raises RuntimeError if no event loop is running
            asyncio.get_running_loop()
            self._in_async_context = True
            self._setup_worker_loop()
        except RuntimeError:
            # No event loop running - we're in sync context
            self._in_async_context = False

        self._run_sync(self._async_connect())
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            self._run_sync(self._async_close())
        finally:
            # Always cleanup resources, even if close fails
            if self._in_async_context:
                # Stop the worker loop and shutdown executor
                if self._worker_loop is not None:
                    self._worker_loop.call_soon_threadsafe(self._worker_loop.stop)
                    self._worker_loop = None
                if self._executor is not None:
                    self._executor.shutdown(wait=True)
                    self._executor = None
            else:
                # Only close the loop if we're in sync context
                loop = getattr(self._thread_local, "loop", None)
                if loop and not loop.is_closed():
                    loop.close()
                self._thread_local.loop = None
                asyncio.set_event_loop(None)

    def fetch_rows(self, sql: str, params: Sequence[Any] | None = None) -> list[dict]:
        return self._run_sync(self._async_fetch_rows(sql, params))

    def fetch_scalar_values(self, sql: str) -> list[Any]:
        return self._run_sync(self._async_fetch_scalar_values(sql))
