"Implements async framework controllers"

from __future__ import annotations

import queue
import threading
import contextvars
import contextlib
import sys
import time
import concurrent.futures
import math

import apsw

import types
from typing import TypeVar, Any

T = TypeVar("T")


timeout: contextvars.ContextVar[int | float | None] = contextvars.ContextVar("apsw.aio.timeout", default=None)
"""Timeout in seconds

This makes a best effort to ensure a database operation including any
callbacks has completed within the timeout.  The default (``None``) is
no timeout.

:class:`AsyncIO` and :class:`AnyIO`

    This is the only way to set a timeout.  :exc:`TimeoutError` will be
    raised if the timeout is exceeded.

:class:`Trio`

    If this is set then it is used for the timeout.  `TooSlowError
    <https://trio.readthedocs.io/en/stable/reference-core.html#trio.TooSlowError>`__
    is raised.

    Otherwise the `current effective deadline
    <https://trio.readthedocs.io/en/stable/reference-core.html#trio.current_effective_deadline>`__
    where the call is made is used.

    .. code-block:: python

    # 10 seconds
    async with apsw.aio.timeout.set(10):
       # do operations
       ...
       # you can use it nested - these operations could take a
       # minute
       with apsw.aio.timeout.set(60):
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

# deadline (time.monotonic based) based on the timeout at submission time
_deadline = contextvars.ContextVar("apsw.aio._deadline")


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
            future = asyncio.get_running_loop().create_future()
            this_deadline = timeout.get()
            if this_deadline is not None:
                this_deadline += time.monotonic()
            self.queue.put((future, call, apsw.async_cursor_prefetch.get(), this_deadline))
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

    def progress_deadline_checker(self):
        "Periodic check if the deadline has passed"
        if (this_deadline := _deadline.get()) is not None and time.monotonic() > this_deadline:
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
                _deadline.set(this_deadline)
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

    def async_run_coro(self, coro):
        "Called in worker thread to run a coroutine in the event loop"
        if (this_deadline := _deadline.get()) is not None:
            timeout = this_deadline - time.monotonic()
            if timeout < 0:
                raise TimeoutError()
        else:
            timeout = None

        try:
            # yes we really need the timeout twice.  when the wait_for one fires the
            # exception, isn't propagated to us
            return asyncio.run_coroutine_threadsafe(
                asyncio.wait_for(coro, timeout), _current_future.get().get_loop()
            ).result(timeout)
        except concurrent.futures.TimeoutError:
            if sys.version_info < (3, 11):
                raise TimeoutError
            raise

    def __init__(self, *, thread_name: str = "asyncio apsw background worker"):
        global asyncio
        import asyncio

        self.queue = queue.SimpleQueue()

        threading.Thread(name=thread_name, target=self.worker_thread_run, args=(self.queue,)).start()


class Trio:
    """Uses `Trio <https://trio.readthedocs.io/>`__ for async concurrency"""

    class _Future:
        # Private internal representation of a call providing an
        # awaitable result.  One of these is made for each call.
        __slots__ = (
            # needed to call back into trio
            "token",
            # trio.Event used to signal ready
            "event",
            # result value or exception
            "result",
            # is it an exception?
            "is_exception",
            # cursor prefect value
            "prefetch",
            # call to make
            "call",
            #  timeout handling - always based on time.monotonic
            "deadline",
        )

        async def aresult(self):
            await self.event.wait()
            if self.is_exception:
                raise self.result
            return self.result

        def __await__(self):
            return self.aresult().__await__()

    def configure(self, db: apsw.Connection):
        1 / 0

    def send(self, call):
        future = Trio._Future()
        future.token = trio.lowlevel.current_trio_token()
        future.event = trio.Event()
        future.prefetch = apsw.async_cursor_prefetch.get()
        future.is_exception = False
        future.call = call
        if (this_timeout := timeout.get()) is not None:
            future.deadline = this_timeout + time.monotonic()
        else:
            future.deadline = trio.current_effective_deadline()
            if future.deadline is not math.inf:
                # adjust by whatever clock is being used
                future.deadline += -trio.current_time() + time.monotonic()
        self.queue.put(future)
        return future

    def close(self):
        self.queue.put(None)
        self.queue = None

    def worker_thread_run(self, q):
        with contextvar_set(apsw.async_run_coro, self.async_run_coro):
            while (future := q.get()) is not None:
                _current_future.set(future)
                apsw.async_cursor_prefetch.set(future.prefetch)
                _deadline.set(future.deadline)

                try:
                    if future.deadline < time.monotonic():
                        raise trio.TooSlowError()
                    future.result = future.call()
                except BaseException as exc:
                    future.result = exc
                    future.is_exception = True

                trio.from_thread.run_sync(future.event.set, trio_token=future.token)

    async def async_async_run_coro(self, coro, timeout):
        if timeout is math.inf:
            return await coro
        with trio.fail_after(timeout):
            return await coro

    def async_run_coro(self, coro):
        this_deadline = _deadline.get()
        this_timeout = this_deadline if this_deadline is math.inf else this_deadline - time.monotonic()
        return trio.from_thread.run(
            self.async_async_run_coro, coro, this_timeout, trio_token=_current_future.get().token
        )

    def __init__(self, *, thread_name: str = "trio apsw background worker"):
        global trio
        import trio

        self.queue = queue.SimpleQueue()
        threading.Thread(name=thread_name, target=self.worker_thread_run, args=(self.queue,)).start()


class AnyIO:
    class _Future:
        # Private internal representation of a call providing an
        # awaitable result.  One of these is made for each call.
        __slots__ = (
            # needed to call back into trio
            "token",
            # anyio.Event used to signal ready
            "event",
            # result value or exception
            "result",
            # is it an exception?
            "is_exception",
            # cursor prefect value
            "prefetch",
            # call to make
            "call",
            #  timeout handling - always based on time.monotonic
            "deadline",
        )

        async def aresult(self):
            await self.event.wait()
            if self.is_exception:
                raise self.result
            return self.result

        def __await__(self):
            return self.aresult().__await__()

    def configure(self, db: apsw.Connection):
        1 / 0

    def send(self, call):
        future = AnyIO._Future()
        future.token = anyio.lowlevel.current_token()
        future.event = anyio.Event()
        future.prefetch = apsw.async_cursor_prefetch.get()
        future.is_exception = False
        future.call = call

        future.deadline = timeout.get()
        if future.deadline is not None:
            future.deadline += time.monotonic()

        self.queue.put(future)
        return future

    def close(self):
        self.queue.put(None)
        self.queue = None

    async def async_async_run_coro(self, coro, timeout):
        with anyio.fail_after(timeout):
            return await coro

    def async_run_coro(self, coro):
        if (this_deadline := _deadline.get()) is not None:
            timeout = this_deadline - time.monotonic()
        else:
            timeout = None

        try:
            print(f"async_run_coro start {timeout=}")
            return anyio.from_thread.run(self.async_async_run_coro, coro, timeout, token=_current_future.get().token)
        finally:
            print(f"async_run_coro end {timeout=}")

    def __init__(self, *, thread_name: str = "anyio apsw background worker"):
        global anyio
        import anyio

        self.queue = queue.SimpleQueue()
        threading.Thread(name=thread_name, target=self.worker_thread_run, args=(self.queue,)).start()

    def worker_thread_run(self, q):
        with contextvar_set(apsw.async_run_coro, self.async_run_coro):
            while (future := q.get()) is not None:
                _current_future.set(future)
                apsw.async_cursor_prefetch.set(future.prefetch)
                _deadline.set(future.deadline)

                try:
                    if future.deadline is not None and future.deadline < time.monotonic():
                        raise TimeoutError()
                    future.result = future.call()
                except BaseException as exc:
                    future.result = exc
                    future.is_exception = True

                anyio.from_thread.run_sync(future.event.set, token=future.token)


def Auto() -> Trio | AsyncIO | AnyIO:
    """
    Automatically detects the current async framework and returns the
    appropriate controller.  This is the default for
    :attr:`apsw.async_controller`.

    It uses the same logic as the `sniffio
    <https://sniffio.readthedocs.io>`__ package and only knows about
    the controllers implemented in this module.  :class:`AnyIO` won't
    be returned in practise because it always runs an asyncio or trio
    event loop.

    :exc:`RuntimeError` is raised if the framework can't be detected.

    :rtype: Trio | AsyncIO | AnyIO
    """
    if "trio" in sys.modules:
        try:
            import trio

            trio.lowlevel.current_trio_token()
            return Trio()
        except:
            pass
    if "asyncio" in sys.modules:
        try:
            import asyncio

            asyncio.get_running_loop()
            return AsyncIO()
        except:
            pass
    if "anyio" in sys.modules:
        try:
            import anyio

            anyio.get_current_task()
            return AnyIO()
        except:
            pass
    raise RuntimeError("Unable to determine current Async environment")
