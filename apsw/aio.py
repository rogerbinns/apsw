"Implements async framework controllers, configuration, and helpers"

from __future__ import annotations

import contextlib
import contextvars
import logging
import math
import queue
import sys
import threading
import time
from collections.abc import Callable, Coroutine
from typing import Any, TypeVar

import apsw

logger = logging.getLogger(__name__)

T = TypeVar("T")


deadline: contextvars.ContextVar[int | float | None] = contextvars.ContextVar("apsw.aio.deadline", default=None)
"""Absolute time deadline for a request in seconds

This makes a best effort to timeout a database operation including any
sync and async callbacks if the deadline is passed.  The default
(``None``) is no deadline.

The deadline is set at the point an APSW call is made, and changes
after that are not observed.  It is based on the clock used by the
event loop.  Typical usage is:

.. code-block:: python

    # 10 seconds from now.  You'll need to get the time from your
    # framework as documented below.

    with apsw.aio.contextvar_set(apsw.aio.deadline,
            anyio.current_time() + 10):

            async for row in await db.execute("SELECT  time_consuming ..."):
                print(f"{row=}")


:class:`AsyncIO`

    This is the only way to set a deadline.  :exc:`TimeoutError` will
    be raised if the deadline is exceeded.  The current time is
    available from  :meth:`asyncio.get_running_loop().time()
    <asyncio.loop.time>`

:class:`Trio`

    If this is set then it is used for the deadline.  :exc:`trio.TooSlowError`
    is raised.  The current time is available from :func:`trio.current_time`.

    Otherwise the :func:`trio.current_effective_deadline` where the
    call is made is used.

AnyIO

    If this is set then it is used for the deadline.  :exc:`TimeoutError` is raised.
    The current time is available from :func:`anyio.current_time`.

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
vary a lot based on the queries.  The smaller the number, the more
frequent the checks, but also more time consumed making the checks.

This is only used during connection creation.  Typical usage is:

.. code-block:: python

    with apsw.aio.contextvar_set(apsw.aio.check_progress_steps, 500):
        db = await apsw.Connection.as_async(...)
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


async def make_session(db: apsw.AsyncConnection, schema: str) -> apsw.AsyncSession:
    "Helper to create a :class:`~apsw.Session` in async mode for an async database"
    # This mainly exists to give IDEs and type checkers the clues they need
    if not hasattr(apsw, "Session"):
        # misuse is what SQLite uses
        raise apsw.MisuseError("The session extension is not enabled and available")
    return await db.async_run(apsw.Session, db, schema)


class _Cancelled(BaseException):
    """
    Raised in the worker thread on seeing call cancellation.

    The original caller in async will get their framework's
    cancellation exception - this is just to terminate call processing
    back through the call stacks
    """

    pass


# this is used to track the currently processing call for all controllers
# as _tls.current_call
_tls = threading.local()


class _CallTracker:
    """
    All the details for the lifecycle of a call.
    """

    __slots__ = (
        # Used for result ready.  asyncio uses Future which also
        # includes the result/exception, while trio/anyio use the
        # result/is_exception fields
        "completion",
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

    completion: asyncio.Future | anyio.Event | trio.Event
    result: Any | BaseException
    is_exception: bool
    call: Callable[[], Any]
    deadline_loop: None | float | int
    deadline_monotonic: None | float | int
    is_cancelled: bool
    cancel_async_cb: Callable[[], Any] | None

    def __init__(self, completion: asyncio.Event | anyio.Event | trio.Event, call: Callable[[], Any]) -> None:
        self.is_exception = False
        self.is_cancelled = False
        self.completion = completion
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
        apsw.async_run_coro = self.async_run_coro

        for hook in apsw.connection_hooks:
            hook(db)
        db.set_progress_handler(self.progress_checker, check_progress_steps.get(), id=self)

    async def send(self, call: Callable[[], Any]):
        "Send call to worker"
        tracker = _CallTracker(self.loop.create_future(), call)
        if (this_deadline := deadline.get()) is not None:
            tracker.set_deadline(this_deadline, self.loop.time())
        self.queue.put(tracker)
        try:
            await tracker.completion
            return tracker.completion.result()
        except:
            tracker.cancel()
            raise

    def close(self):
        "Called on connection close, so the worker thread can be stopped"
        # How we tell the worker thread to exit
        self.queue.put(None)

    def progress_checker(self):
        "Periodic check for cancellation and deadlines"
        if _tls.current_call.is_cancelled:
            raise _Cancelled("cancelled in progress checked")
        if _tls.current_call.monotonic_exceeded():
            raise TimeoutError()
        return False

    def worker_thread_run(self):
        "Does the enqueued call processing in the worker thread"

        q = self.queue

        while (tracker := q.get()) is not None:
            if not tracker.is_cancelled:
                # we don't restore this because the queue is not
                # re-entrant, so there is no point
                _tls.current_call = tracker

                try:
                    # should we even start?
                    if tracker.monotonic_exceeded():
                        raise TimeoutError()
                    self.loop.call_soon_threadsafe(self.set_future_result, tracker.completion, tracker.call())

                except BaseException as exc:
                    # BaseException is deliberately used because CancelledError
                    # is a subclass of it
                    self.loop.call_soon_threadsafe(self.set_future_exception, tracker.completion, exc)

    def set_future_result(self, future: asyncio.Future, value: Any):
        if not future.done():
            future.set_result(value)

    def set_future_exception(self, future: asyncio.Future, exc: BaseException):
        if not future.done():
            future.set_exception(exc)

    def async_run_coro(self, coro: Coroutine):
        "Called in worker thread to run a coroutine in the event loop"

        tracker = _tls.current_call

        try:
            if tracker.is_cancelled:
                raise _Cancelled("cancelled in async_run_coro")

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
            if tracker.is_cancelled:
                return

            if tracker.deadline_loop is not None:
                return await asyncio.wait_for(task, tracker.deadline_loop - self.loop.time())
            return await task

    elif sys.version_info < (3, 12):

        async def run_coro_in_loop(self, coro: Coroutine, tracker: _CallTracker, context: contextvars.Context) -> Any:
            "executes the coro in the event loop"

            task = context.run(asyncio.create_task, coro)
            tracker.cancel_async_cb = task.cancel
            if tracker.is_cancelled:
                return

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
            if tracker.is_cancelled:
                return

            async with asyncio.timeout_at(tracker.deadline_loop):
                return await task

    def __init__(self, *, thread_name: str = "asyncio apsw background worker"):
        global asyncio
        import asyncio

        self.queue: queue.SimpleQueue[_CallTracker | None] = queue.SimpleQueue()
        self.loop = asyncio.get_running_loop()
        threading.Thread(name=thread_name, target=self.worker_thread_run).start()


class Trio:
    """Uses |trio| for async concurrency"""

    def configure(self, db: apsw.Connection):
        "Setup database, just after it is created"
        apsw.async_run_coro = self.async_run_coro

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
            await tracker.completion.wait()
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
        if _tls.current_call.is_cancelled:
            raise _Cancelled("cancelled in progress handler")
        if _tls.current_call.monotonic_exceeded():
            raise trio.TooSlowError("deadline exceeded in progress handler")
        return False

    def worker_thread_run(self):
        "Does the enqueued call processing in the worker thread"
        q = self.queue

        while (tracker := q.get()) is not None:
            if not tracker.is_cancelled:
                # we don't restore this because the queue is not
                # re-entrant, so there is no point
                _tls.current_call = tracker

                try:
                    # should we even start?
                    if tracker.monotonic_exceeded():
                        raise trio.TooSlowError()
                    tracker.result = tracker.call()

                except BaseException as exc:
                    # BaseException is deliberately used because Cancelled
                    # is a subclass of it
                    tracker.result = exc
                    tracker.is_exception = True

                finally:
                    self.token.run_sync_soon(tracker.completion.set)

    def async_run_coro(self, coro: Coroutine):
        "Called in worker thread to run a coroutine in the event loop"
        try:
            tracker = _tls.current_call
            if tracker.is_cancelled:
                raise _Cancelled("Cancelled in async_run_coro")

            return trio.from_thread.run(self.run_coro_in_loop, coro, tracker, trio_token=self.token)
        finally:
            coro.close()

    async def run_coro_in_loop(self, coro: Coroutine, tracker: _CallTracker):
        "executes the coro in the event loop"
        with trio.fail_at(deadline=math.inf if tracker.deadline_loop is None else tracker.deadline_loop) as scope:
            tracker.cancel_async_cb = scope.cancel
            if tracker.is_cancelled:
                return
            return await coro

    def __init__(self, *, thread_name: str = "trio apsw background worker"):
        global trio
        import trio

        self.queue: queue.SimpleQueue[_CallTracker | None] = queue.SimpleQueue()
        self.token = trio.lowlevel.current_trio_token()
        threading.Thread(name=thread_name, target=self.worker_thread_run).start()


class AnyIO:
    """Uses |anyio| for async concurrency"""

    def configure(self, db: apsw.Connection):
        "Setup database, just after it is created"
        apsw.async_run_coro = self.async_run_coro

        for hook in apsw.connection_hooks:
            hook(db)
        db.set_progress_handler(self.progress_checker, check_progress_steps.get(), id=self)

    async def send(self, call: Callable[[], Any]):
        "Enqueues call to worker thread"

        tracker = _CallTracker(anyio.Event(), call)
        if (this_deadline := deadline.get()) is None:
            this_deadline = anyio.current_effective_deadline()
        tracker.set_deadline(this_deadline, anyio.current_time())

        self.queue.put(tracker)
        try:
            await tracker.completion.wait()
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
        tracker = _tls.current_call
        if tracker.is_cancelled:
            raise _Cancelled("cancelled in progress handler")
        if tracker.monotonic_exceeded():
            raise TimeoutError("deadline exceeded in progress handler")
        return False

    def worker_thread_run(self):
        "Does the enqueued call processing in the worker thread"
        q = self.queue

        while (tracker := q.get()) is not None:
            if not tracker.is_cancelled:
                # we don't restore this because the queue is not
                # re-entrant, so there is no point
                _tls.current_call = tracker

                try:
                    # should we even start?
                    if tracker.monotonic_exceeded():
                        raise TimeoutError("Deadline exceeded in queue")
                    tracker.result = tracker.call()

                except BaseException as exc:
                    # BaseException is deliberately used because CancelledError
                    # is a subclass of it
                    tracker.result = exc
                    tracker.is_exception = True

                finally:
                    anyio.from_thread.run_sync(tracker.completion.set, token=self.token)

    def async_run_coro(self, coro: Coroutine):
        "Called in worker thread to run a coroutine in the event loop"

        try:
            tracker = _tls.current_call
            if tracker.is_cancelled:
                raise _Cancelled("Cancelled in async_run_coro")
            if tracker.monotonic_exceeded():
                raise TimeoutError("deadline exceeded in async_run_coro")
            return anyio.from_thread.run(self.run_coro_in_loop, coro, tracker, token=self.token)
        finally:
            coro.close()

    async def run_coro_in_loop(self, coro: Coroutine, tracker: _CallTracker):
        "executes coro in the event loop"

        with anyio.fail_after(
            math.inf if tracker.deadline_loop is None else tracker.deadline_loop - anyio.current_time()
        ) as scope:
            tracker.cancel_async_cb = scope.cancel
            if tracker.is_cancelled:
                return
            return await coro

    def __init__(self, *, thread_name: str = "anyio apsw background worker"):
        global anyio
        import anyio

        self.queue: queue.SimpleQueue[_CallTracker | None] = queue.SimpleQueue()
        self.token = anyio.lowlevel.current_token()
        threading.Thread(name=thread_name, target=self.worker_thread_run).start()

# True means they can be tried, False means too old etc
_anyio_usable = True
_trio_usable = True

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
    global _anyio_usable, _trio_usable
    # This variable tracks which class to use.  It is instantiated
    # outside of the try/except blocks so exceptions in its
    # initialization will be raised.
    found = None

    if found is None and "anyio" in sys.modules and _anyio_usable:
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
                    found = AnyIO
                    break
                frame = frame.f_back

            if found:
                found = None
                # check its version is ok
                import importlib.metadata
                ver = tuple(map(int, importlib.metadata.version("anyio").split(".")))
                if  ver >= (4, 11, 0):
                    found = AnyIO
                else:
                    logger.error(f"anyio {ver} was found but is too old to be used with the AnyIO controller")
                    _anyio_usable = False

        except:
            pass

    if found is None and "trio" in sys.modules and _trio_usable:
        try:
            import trio

            trio.lowlevel.current_trio_token()

            # check its version is ok
            import importlib.metadata
            ver = tuple(map(int, importlib.metadata.version("trio").split(".")))
            if  ver >= (0, 20, 0):
                found = Trio
            else:
                logger.error(f"trio {ver=} was found but is too old to be used with the Trio controller")
                _trio_usable = False

        except:
            pass

    if found is None and "asyncio" in sys.modules:
        try:
            import asyncio

            asyncio.get_running_loop()
            found = AsyncIO
        except:
            pass

    if not found:
        raise RuntimeError("Unable to determine current Async environment")

    return found()
