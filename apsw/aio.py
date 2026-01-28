"Implements async framework controllers, configuration, and helpers"

from __future__ import annotations

import contextlib
import contextvars
import math
import queue
import sys
import threading
import time
from collections.abc import Callable, Coroutine
from typing import Any, TypeVar

import apsw

T = TypeVar("T")


deadline: contextvars.ContextVar[int | float | None] = contextvars.ContextVar("apsw.aio.deadline", default=None)
"""Absolute time deadline for a request

This makes a best effort to timeout a database operation including any
sync and async callbacks if the deadline is passed.  The default
(``None``) is no deadline.

The deadline is set at the point an APSW call is made, and changes
after that are not observed.

:class:`AsyncIO`

    This is the only way to set a deadline.  :exc:`TimeoutError` will
    be raised if the deadline is exceeded.  The current time is
    available from  :meth:`asyncio.get_running_loop().time()
    <asyncio.loop.time>`

:class:`Trio`

    If this is set then it is used for the deadline.  :exc:`trio.TooSlowError`
    is raised.

    Otherwise the :func:`trio.current_effective_deadline` where the
    call is made is used.

AnyIO

    If this is set then it is used for the deadline.  :exc:`TimeoutError` is raised.

    Otherwise the :func:`anyio.current_effective_deadline` where the
    call is made is used.

"""

check_progress_steps: contextvars.ContextVar[int] = contextvars.ContextVar(
    "apsw.aio.check_progress_steps", default=50_000
)
"""How many steps between checks to check for cancellation and deadlines

While SQLite queries are executing, periodic checks are made to see if
the request has been cancelled, or the deadline exceeded.  This is
done in the :meth:`progress handler
<apsw.Connection.set_progress_handler>`.

The default should correspond to around 10 checks per second, but will
vary based on the queries.  The smaller the number, the more frequent
the checks, but also more time consumed making the checks.
"""


if sys.version_info >= (3, 14):

    def contextvar_set(var: contextvars.ContextVar[T], value: T) -> contextvars.Token[T]:
        """wrapper for setting a contextvar during a with block

        Python 3.14 lets you do::

            with var.set(value):
                # code here
                ...

        This wrapper provides the same functionality for all
        Python versions::

            with contextvar_set(var, value):
                # code here
                ...

        """
        return var.set(value)

else:

    def contextvar_set(var: contextvars.ContextVar[T], value: T) -> contextvars.Token[T]:
        @contextlib.contextmanager
        def _contextvar_set_wrapper():
            token = var.set(value)
            try:
                yield token
            finally:
                var.reset(token)

        return _contextvar_set_wrapper()


async def make_session(db: apsw.AsyncConnection, schema: str) -> AsyncSession:
    "Helper to create a :class:`~apsw.Session` in async mode for an async database"
    # This mainly exists to give IDEs and type checkers the clues they need
    if not hasattr(apsw, "Session"):
        # misuse is what SQLite uses
        raise apsw.MisuseError("The session extension is not enabled and available")
    return await db.async_run(apsw.Session, db, schema)


class Cancelled(Exception):
    """Result when an operation was cancelled (Trio, AnyIO)

    asyncio uses :class:`asyncio.CancelledError`
    """

    pass


# this is used to track the currently processing call for all controllers
_current_call: contextvars.ContextVar[_CallTracker] = contextvars.ContextVar("apsw.aio._current_call")


class _CallTracker:
    """
    All the details for the lifecycle of a call.
    """

    __slots__ = (
        # Event used to signal ready
        "event",
        # result value or exception
        "result",
        # is it an exception?
        "is_exception",
        # BoxedCall to make
        "call",
        # deadline in event loop clock
        "deadline_loop",
        # deadline in worker thread relative to monotonic clock
        "deadline_monotonic",
        # cancel indication
        "is_cancelled",
        # if a callback is async and run back in the event loop then
        # this can be called to cancel it
        "cancel_async_cb",
    )

    event: asyncio.Event | anyio.Event | trio.Event
    result: Any | BaseException
    is_exception: bool
    call: Callable[[], Any]
    deadline_loop: None | float | int
    deadline_monotonic: None | float | int
    is_cancelled: bool
    cancel_async_cb: Callable[[], Any] | None

    def __init__(self, event: asyncio.Event | anyio.Event | trio.Event, call: Callable[[], Any]) -> None:
        self.is_exception = False
        self.is_cancelled = False
        self.event = event
        self.call = call
        self.deadline_loop = None
        self.deadline_monotonic = None
        self.cancel_async_cb = None

    def set_deadline(self, value: int | float, loop_time: int | float):
        self.deadline_loop = value
        if value is not math.inf:
            self.deadline_monotonic = value - loop_time + time.monotonic()

    def monotonic_exceeded(self) -> bool:
        return self.deadline_monotonic is not None and time.monotonic() > self.deadline_monotonic

    def cancel(self):
        "Cancel the call"

        self.is_cancelled = True
        if self.cancel_async_cb is not None:
            self.cancel_async_cb()


# These are used to directly return values and exceptions without
# sending to the worker thread such as prefetched query rows.
async def _coro_for_value(value):
    return value


if sys.version_info < (3, 12):
    # Python 3.12 unified the exc type, value, and traceback into the single
    # exception object.

    async def _coro_for_exception(exc):
        raise exc[0](exc[1]).with_traceback(exc[2])

else:

    async def _coro_for_exception(exc):
        raise exc


# this is separate to avoid the version issues above
async def _coro_for_stopasynciteration():
    raise StopAsyncIteration


class AsyncIO:
    """Uses :mod:`asyncio` for async concurrency"""

    def configure(self, db: apsw.Connection):
        "Setup database, just after it is created"
        for hook in apsw.connection_hooks:
            hook(db)
        db.set_progress_handler(self.progress_checker, check_progress_steps.get(), id=self)

    async def send(self, call: Callable[[], Any]):
        "Send call to worker"
        tracker = _CallTracker(asyncio.Event(), call)
        if (this_deadline := deadline.get()) is not None:
            tracker.set_deadline(this_deadline, self.loop.time())
        self.queue.put(tracker)
        try:
            await tracker.event.wait()
            if tracker.is_exception:
                raise tracker.result
            return tracker.result
        except (asyncio.CancelledError, TimeoutError):
            tracker.cancel()
            raise

    def close(self):
        "Called on connection close, so the worker thread can be stopped"
        # How we tell the worker thread to exit
        self.queue.put(None)

    def progress_checker(self):
        "Periodic check for cancellation and deadlines"
        if _current_call.get().is_cancelled:
            raise asyncio.CancelledError()
        if _current_call.get().monotonic_exceeded():
            raise TimeoutError()
        return False

    def worker_thread_run(self):
        "Does the enqueued call processing in the worker thread"

        while (tracker := self.queue.get()) is not None:
            try:
                if not tracker.is_cancelled:
                    # adopt caller's contextvars
                    with tracker.call:
                        # we don't restore this because the queue is not
                        # re-entrant, so there is no point
                        _current_call.set(tracker)

                        try:
                            # should we even start?
                            if tracker.monotonic_exceeded():
                                raise TimeoutError()
                            tracker.result = tracker.call()

                        except BaseException as exc:
                            # BaseException is deliberately used because CancelledError
                            # is a subclass of it
                            tracker.result = exc
                            tracker.is_exception = True
            finally:
                self.loop.call_soon_threadsafe(tracker.event.set)

    def async_run_coro(self, coro: Coroutine):
        "Called in worker thread to run a coroutine in the event loop"

        tracker = _current_call.get()

        try:
            if tracker.is_cancelled:
                raise asyncio.CancelledError()

            return asyncio.run_coroutine_threadsafe(
                self.run_coro_in_loop(coro, tracker, contextvars.copy_context()), self.loop
            ).result()

        finally:
            coro.close()

    if sys.version_info < (3, 11):

        async def run_coro_in_loop(self, coro: Coroutine, tracker: _CallTracker, context: contextvars.Context) -> Any:
            "executes the coro in the event loop"

            task = context.run(asyncio.create_task, coro)
            tracker.cancel_async_cb = task.cancel

            return await asyncio.wait_for(task, tracker.deadline_loop - self.loop.time())

    elif sys.version_info < (3, 12):

        async def run_coro_in_loop(self, coro: Coroutine, tracker: _CallTracker, context: contextvars.Context) -> Any:
            "executes the coro in the event loop"

            task = context.run(asyncio.create_task, coro)
            tracker.cancel_async_cb = task.cancel

            async with asyncio.timeout_at(tracker.deadline_loop):
                return await task

    else:

        async def run_coro_in_loop(self, coro: Coroutine, tracker: _CallTracker, context: contextvars.Context) -> Any:
            "executes the coro in the event loop"

            # Note: we don't set cancel_async_cb back to None on exit
            # because cancelling an already completed task is doesn't
            # error or cause problems.

            task = asyncio.create_task(coro, context=context)
            tracker.cancel_async_cb = task.cancel

            async with asyncio.timeout_at(tracker.deadline_loop):
                return await task

    def __init__(self, *, thread_name: str = "asyncio apsw background worker"):
        global asyncio
        import asyncio

        apsw.async_run_coro.set(self.async_run_coro)

        self.queue: queue.SimpleQueue[_CallTracker | None] = queue.SimpleQueue()
        self.loop = asyncio.get_running_loop()
        threading.Thread(name=thread_name, target=self.worker_thread_run).start()


class Trio:
    """Uses |trio| for async concurrency"""

    def configure(self, db: apsw.Connection):
        "Setup database, just after it is created"
        for hook in apsw.connection_hooks:
            hook(db)
        db.set_progress_handler(self.progress_checker, check_progress_steps.get(), id=self)

    async def send(self, call: Callable[[], Any]):
        "Enqueues call to worker thread"
        tracker = _CallTracker(trio.Event(), call)
        if (this_deadline := deadline.get()) is None:
            this_deadline = trio.current_effective_deadline()
        tracker.set_deadline(this_deadline, trio.current_time())

        self.queue.put(tracker)
        try:
            await tracker.event.wait()
            if tracker.is_exception:
                raise tracker.result
            return tracker.result
        except:
            tracker.cancel()
            raise

    def close(self):
        "Called on connection close, so the worker thread can be stopped"
        self.queue.put(None)

    def progress_checker(self):
        "Periodic check for cancellation and deadlines"
        if _current_call.get().is_cancelled:
            raise Cancelled("cancelled in progress handler")
        if _current_call.get().monotonic_exceeded():
            raise trio.TooSlowError("deadline exceeded in progress handler")
        return False

    def worker_thread_run(self):
        "Does the enqueued call processing in the worker thread"
        while (tracker := self.queue.get()) is not None:
            try:
                if not tracker.is_cancelled:
                    # adopt caller's contextvars
                    with tracker.call:
                        # we don't restore this because the queue is not
                        # re-entrant, so there is no point
                        _current_call.set(tracker)

                        try:
                            # should we even start?
                            if tracker.monotonic_exceeded():
                                raise trio.TooSlowError()
                            tracker.result = tracker.call()

                        except BaseException as exc:
                            # BaseException is deliberately used because CancelledError
                            # is a subclass of it
                            tracker.result = exc
                            tracker.is_exception = True
            finally:
                self.token.run_sync_soon(tracker.event.set)

    def async_run_coro(self, coro: Coroutine):
        "Called in worker thread to run a coroutine in the event loop"
        try:
            tracker = _current_call.get()
            if tracker.is_cancelled:
                raise Cancelled("Cancelled in async_run_coro")

            return trio.from_thread.run(self.run_coro_in_loop, coro, tracker, trio_token=self.token)
        finally:
            coro.close()

    async def run_coro_in_loop(self, coro: Coroutine, tracker: _CallTracker):
        "executes the coro in the event loop"
        with trio.CancelScope(deadline=math.inf if tracker.deadline_loop is None else tracker.deadline_loop) as scope:
            tracker.cancel_async_cb = scope.cancel
            return await coro

    def __init__(self, *, thread_name: str = "trio apsw background worker"):
        global trio
        import trio

        apsw.async_run_coro.set(self.async_run_coro)
        self.queue: queue.SimpleQueue[_CallTracker | None] = queue.SimpleQueue()
        self.token = trio.lowlevel.current_trio_token()
        threading.Thread(name=thread_name, target=self.worker_thread_run).start()


class AnyIO:
    """Uses |anyio| for async concurrency"""

    def configure(self, db: apsw.Connection):
        "Setup database, just after it is created"
        for hook in apsw.connection_hooks:
            hook(db)
        db.set_progress_handler(self.progress_checker, check_progress_steps.get(), id=self)

    def send(self, call):
        "Enqueues call to worker thread"
        future = _CallTracker(anyio.Event(), call, self.cancelled_exc_class)
        if (this_deadline := deadline.get()) is None:
            this_deadline = anyio.current_effective_deadline()
        future.set_deadline(this_deadline, anyio.current_time)
        self.queue.put(future)
        return future

    def close(self):
        "Called on connection close, so the worker thread can be stopped"
        self.queue.put(None)

    def progress_checker(self):
        "Periodic check for cancellation and deadlines"
        future = _current_call.get()
        if future._is_cancelled:
            raise Cancelled("cancelled in progress handler")
        if future._monotonic_exceeded():
            raise TimeoutError("deadline exceeded in progress handler")
        return False

    def worker_thread_run(self, q):
        "Does the enqueued call processing in the worker thread"
        while (future := q.get()) is not None:
            if not future._is_cancelled:
                with future._call:
                    _current_call.set(future)

                    try:
                        if future._monotonic_exceeded():
                            raise TimeoutError("Deadline exceeded in queue")

                        future._result = future._call()
                    except BaseException as exc:
                        future._result = exc
                        future._is_exception = True

            # this ensures completion even if cancelled
            anyio.from_thread.run_sync(future._event.set, token=self.token)

    def async_run_coro(self, coro):
        "Called in worker thread to run a coroutine in the event loop"
        try:
            future = _current_call.get()
            if future._is_cancelled:
                raise Cancelled("Cancelled in async_run_coro")
            if future._monotonic_exceeded():
                raise TimeoutError("deadline exceeded in async_run_coro")
            return anyio.from_thread.run(self.loop_run_coro, coro, future._deadline_loop, token=self.token)
        finally:
            coro.close()

    def __init__(self, *, thread_name: str = "anyio apsw background worker"):
        global anyio
        import anyio

        apsw.async_run_coro.set(self.async_run_coro)
        self.queue = queue.SimpleQueue()
        self.token = anyio.lowlevel.current_token()
        self.cancelled_exc_class = anyio.get_cancelled_exc_class()
        threading.Thread(name=thread_name, target=self.worker_thread_run, args=(self.queue,)).start()

    async def loop_run_coro(self, coro, this_deadline):
        with anyio.fail_after(this_deadline - anyio.current_time()):
            return await coro


def Auto() -> Trio | AsyncIO | AnyIO:
    """
    Automatically detects the current async framework running event
    loop and returns the appropriate controller.  This is the default
    for :attr:`apsw.async_controller`.

    **AnyIO note**

        The :class:`AnyIO` controller is only returned if
        :func:`anyio.run` is in the call stack.

        If you are simultaneously using anyio and another framework
        then you should manually configure
        :attr:`apsw.async_controller` to get the one you want.

        This matters especially for timeouts and cancellations where
        each framework is different.

    :exc:`RuntimeError` is raised if the framework can't be detected.

    """
    if "anyio" in sys.modules:
        try:
            import anyio

            # this checks if an anyio supported event loop is running
            # but anyio works with asyncio/trio as the loop ...
            anyio.get_current_task()

            # ... so we need to check if anyio.run is in the call stack
            anyio_run_code = anyio.run.__code__

            frame = sys._getframe()
            while frame:
                if frame.f_code is anyio_run_code:
                    return AnyIO()
                frame = frame.f_back
        except:
            pass
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

    raise RuntimeError("Unable to determine current Async environment")
