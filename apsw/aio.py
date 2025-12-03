"Implements async framework controllers"

from __future__ import annotations

import queue
import threading
import contextvars
import contextlib
import sys
import time
import concurrent.futures

import apsw

import types
from typing import TypeVar, Any

T = TypeVar("T")


deadline: contextvars.ContextVar[int | float | None] = contextvars.ContextVar("apsw.aio.deadline", default=None)
"""Set to a value based one :func:`time.monotonic()` for an operation to complete by

This makes a best effort to ensure a database operation including any
callbacks has completed by the time, else :exc:`TimeoutError` will be
raised.  The default (``None``) is no deadline.  It is generally a good
idea to set a deadline because it will unblock deadlocks.

.. code-block:: python

    # 10 seconds from now
    async with apsw.aio.deadline.set(time.monotonic() + 10):
       # do operations
       ...
       # you can use it nested - these operations could take a
       # minute
       async with apsw.aio.deadline.set(time.monotonic() + 60):
          # do other operations
          ...
"""

if sys.version_info >= (3, 14):

    def contextvar_set(var: contextvars.ContextVar[T], value: T) -> contextvars.Token[T]:
        """wrapper for setting a contextvar during a with block

        Python 3.14 lets you do::

            with var.set(value):
                # code here
                pass

        This wrapper provides the same functionality for all
        Python versions::

            with contextvar_set(value):
                # code here
                pass

        """
        return var.set(value)

else:

    def contextvar_set(var: contextvars.ContextVar[T], value: T) -> contextvars.Token[T]:
        @contextlib.contextmanager
        def _contextvar_set_wrapper():
            token = var.set(value)
            try:
                yield
            finally:
                var.reset(token)

        return _contextvar_set_wrapper()


# contextvars have to be top level.  this is used to track the currently
# processing future
_current_future = contextvars.ContextVar("apsw.aio._current_future")


# These were originally members of AsyncIO and we were low single
# digit percent slower than aiosqlite for message round trip.  Making
# them a module function makes us slightly faster!  They are called
# for every message.
def _asyncio_set_future_result(future, result):
    "Update future with result in the event loop"
    # you get an exception if cancelled
    if not future.done():
        future.set_result(result)


def _asyncio_set_future_exception(future, exc):
    "Update future with exception in the event loop"
    if not future.done():
        future.set_exception(exc)


class AsyncIO:
    """Uses :mod:`asyncio` for async concurrency"""

    def configure(self, db: apsw.Connection):
        "Setup database, just after it is created"
        1 / 0  # not implemeted yet
        db.set_progress_handler(self.progress_deadline_checker, 500, id=self)

    def send(self, call):
        "Enqueues call to worker thread"
        try:
            future = self.get_running_loop().create_future()
            self.queue.put((future, call, apsw.async_cursor_prefetch.get(), deadline.get()))
            return future
        except AttributeError:
            if self.queue is None:
                raise apsw.ConnectionClosedError()
            raise

    def close(self):
        "Called from connection destructor, so the worker thread can be stopped"

        # No guarantee of what thread will call this

        # How we tell the worker to exit
        self.queue.put(None)

        # queue.SimpleQueue doesn't have a shutdown method like the more
        # complex ones so we just set it to None which send detects
        self.queue = None

    # The methods above are callbacks from ASyncConnection.  The ones below
    # are our internal workings.

    def progress_deadline_checker(self):
        "Periodic check if the deadline has passed"
        if (this_deadline := deadline.get()) is not None and time.monotonic() > this_deadline:
            raise TimeoutError()
        return False

    def worker_thread_run(self, q):
        "Does the enqueued call processing in the worker thread"

        with contextvar_set(apsw.async_run_coro, self.async_run_coro):
            while (item := q.get()) is not None:
                future, call, this_prefetch, this_deadline = item

                # cancelled?
                if future.done():
                    continue

                # we don't restore these because the queue is not
                # re-entrant, so there is no point
                _current_future.set(future)
                deadline.set(this_deadline)
                apsw.async_cursor_prefetch.set(this_prefetch)

                try:
                    # should we even start?
                    if this_deadline is not None:
                        if time.monotonic() > this_deadline:
                            raise TimeoutError()
                    future.get_loop().call_soon_threadsafe(_asyncio_set_future_result, future, call())

                except BaseException as exc:
                    # BaseException is deliberately used because CancelledError
                    # is a subclass of it
                    future.get_loop().call_soon_threadsafe(_asyncio_set_future_exception, future, exc)
                del future

    def async_run_coro(self, coro):
        "Called in worker thread to run a coroutine in the event loop"
        if (this_deadline := deadline.get()) is not None:
            timeout = this_deadline - time.monotonic()
            if timeout < 0:
                raise TimeoutError()
        else:
            timeout = None

        try:
            return self.run_coroutine_threadsafe(coro, _current_future.get().get_loop()).result(timeout)
        except concurrent.futures.TimeoutError:
            if sys.version_info < (3, 11):
                raise TimeoutError
            raise

    def __init__(self, *, thread_name: str = "asyncio apsw background worker"):
        # we don't top level import because it is expensive, and trio/anyio etc could
        # be in use instead.  we only need two methods
        import asyncio

        self.get_running_loop = asyncio.get_running_loop
        self.run_coroutine_threadsafe = asyncio.run_coroutine_threadsafe

        self.queue = queue.SimpleQueue()

        threading.Thread(name=thread_name, target=self.worker_thread_run, args=(self.queue,)).start()


class Trio:
    """Uses `Trio <https://trio.readthedocs.io/>`__ for async concurrency"""

    # I couldn't see a way of using trio's own thread starting machinery
    # because they prefer a thread pool of workers whereas we need a
    # specific thread for the lifetime of the connection.
    #
    # trio's memory channels are async which would require two
    # levels of await
    #
    # consequently we use the same normal thread and SimpleQueue
    # as AsyncIO

    class _Future:
        # private internal representation of a call providing an
        # awaitable result
        token: Any
        call: Callable
        event: Any
        result: Any
        is_exception: bool
        prefetch: int
        # ::TODO:: clock, timeout, cancellation

        async def result(self):
            await self.event.wait()
            if self.is_exception:
                raise self.result
            return self.result

        def __await__(self):
            return self.result().__await__()

    def configure(self, db: apsw.Connection):
        1 / 0

    def send(self, call):
        future = Trio._Future()
        future.token = self.current_trio_token()
        future.call = call
        future.event = self.event()
        future.prefetch = apsw.async_cursor_prefetch.get()
        future.is_exception = False

        self.queue.put(future)
        return future

    def close(self):
        self.queue.put(None)
        self.queue = None

    async def set_event(self, event):
        event.set()

    def worker_thread_run(self, q):
        with contextvar_set(apsw.async_run_coro, self.async_run_coro):
            while (future := q.get()) is not None:
                with (
                    contextvar_set(_current_future, future),
                    contextvar_set(apsw.async_cursor_prefetch, future.prefetch),
                ):
                    try:
                        future.result = future.call()
                    except BaseException as exc:
                        future.result = exc
                        future.is_exception = True

                    self.from_thread_run(self.set_event, future.event, trio_token=future.token)
                    del future

    async def async_async_run_coro(self, coro):
        return await coro

    def async_run_coro(self, coro):
        return self.from_thread_run(self.async_async_run_coro, coro, trio_token=_current_future.get().token)

    def __init__(self, *, thread_name: str = "trio apsw background worker"):
        import trio

        self.from_thread_run = trio.from_thread.run
        self.current_trio_token = trio.lowlevel.current_trio_token
        self.event = trio.Event

        self.queue = queue.SimpleQueue()
        threading.Thread(name=thread_name, target=self.worker_thread_run, args=(self.queue,)).start()


# some notes about trio
# trio.current_effective_deadline
# trio.lowlevel.current_clock  - meth current_time()
# trio.testing.MockClock
