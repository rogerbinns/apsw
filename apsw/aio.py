"Implements async framework controllers, configuration, and helpers"

from __future__ import annotations

import queue
import threading
import contextvars
import contextlib
import sys
import concurrent.futures
import math
import time

import apsw

from typing import TypeVar, Protocol

T = TypeVar("T")


deadline: contextvars.ContextVar[int | float | None] = contextvars.ContextVar("apsw.aio.deadline", default=None)
"""Absolute time deadline for a request

This makes a best effort to timeout a database operation including any
sync and async callbacks if the deadline is passed.  The default
(``None``) is no deadline.

The deadline is set at the point an APSW call is made, and changes
after that are not observed.

:class:`AsyncIO`

    This is the only way to set a deadline.  :exc:`TimeoutError` will be
    raised if the deadline is exceeded.  The current time is
    available from  :meth:`asyncio.loop.time`

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


# contextvars should be top level.  this is used to track the currently
# processing future for all controllers
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
        for hook in apsw.connection_hooks:
            hook(db)
        db.set_progress_handler(self.progress_checker, check_progress_steps.get(), id=self)

    def send(self, call):
        "Enqueues call to worker thread"
        future = self.loop.create_future()
        self.queue.put((future, call))
        return future

    def close(self):
        "Called on connection close, so the worker thread can be stopped"
        # How we tell the worker to exit
        self.queue.put(None)

    def progress_checker(self):
        "Periodic check for cancellation and deadlines"
        if (this_deadline := deadline.get()) is not None and self.loop.time() > this_deadline:
            raise TimeoutError()
        if _current_future.get().done():
            raise asyncio.CancelledError()
        return False

    def worker_thread_run(self, q):
        "Does the enqueued call processing in the worker thread"

        while (item := q.get()) is not None:
            future, call = item
            # cancelled?
            if future.done():
                continue

            # adopt caller's contextvars
            with call:
                # we don't restore this because the queue is not
                # re-entrant, so there is no point
                _current_future.set(future)

                try:
                    # should we even start?
                    if (this_deadline := deadline.get()) is not None:
                        if self.loop.time() > this_deadline:
                            raise TimeoutError()
                    self.loop.call_soon_threadsafe(_asyncio_set_future_result, future, call())

                except BaseException as exc:
                    # BaseException is deliberately used because CancelledError
                    # is a subclass of it
                    self.loop.call_soon_threadsafe(_asyncio_set_future_exception, future, exc)

    def async_run_coro(self, coro):
        "Called in worker thread to run a coroutine in the event loop"
        try:
            if _current_future.get().done():
                raise asyncio.CancelledError()

            if (this_timeout := deadline.get()) is not None:
                this_timeout -= self.loop.time()

            # yes we really need the timeout twice.  when the wait_for
            # one fires, the exception isn't propagated to us
            return asyncio.run_coroutine_threadsafe(
                asyncio.wait_for(coro, this_timeout),
                self.loop,
            ).result(this_timeout)
        except concurrent.futures.TimeoutError:
            if sys.version_info < (3, 11):
                raise TimeoutError
            raise
        finally:
            coro.close()

    def __init__(self, *, thread_name: str = "asyncio apsw background worker"):
        global asyncio
        import asyncio

        apsw.async_run_coro.set(self.async_run_coro)

        self.queue = queue.SimpleQueue()
        self.loop = asyncio.get_running_loop()
        threading.Thread(name=thread_name, target=self.worker_thread_run, args=(self.queue,)).start()


class Trio:
    """Uses |trio| for async concurrency"""

    def configure(self, db: apsw.Connection):
        "Setup database, just after it is created"
        for hook in apsw.connection_hooks:
            hook(db)
        db.set_progress_handler(self.progress_checker, check_progress_steps.get(), id=self)

    def send(self, call):
        "Enqueues call to worker thread"
        future = _Future(trio.Event(), call, trio.Cancelled)
        if (this_deadline := deadline.get()) is None:
            this_deadline = trio.current_effective_deadline()
        future._set_deadline(this_deadline, trio.current_time)

        self.queue.put(future)
        return future

    def close(self):
        "Called on connection close, so the worker thread can be stopped"
        self.queue.put(None)

    def progress_checker(self):
        "Periodic check for cancellation and deadlines"
        future = _current_future.get()
        if future._is_cancelled:
            raise Cancelled("cancelled in progress handler")
        if future._monotonic_exceeded():
            raise trio.TooSlowError("deadline exceeded in progress handler")
        return False

    def worker_thread_run(self, q):
        "Does the enqueued call processing in the worker thread"
        while (future := q.get()) is not None:
            if not future._is_cancelled:
                with future._call:
                    _current_future.set(future)

                    try:
                        if future._monotonic_exceeded():
                            raise trio.TooSlowError("Deadline exceeded in queue")

                        future._result = future._call()
                    except BaseException as exc:
                        future._result = exc
                        future._is_exception = True

            # this ensures completion even if cancelled
            self.token.run_sync_soon(future._event.set)

    def async_run_coro(self, coro):
        "Called in worker thread to run a coroutine in the event loop"
        try:
            future = _current_future.get()
            if future._is_cancelled:
                raise Cancelled("Cancelled in async_run_coro")
            if future._monotonic_exceeded():
                raise trio.TooSlowError("deadline exceeded in async_run_coro")
            return trio.from_thread.run(_trio_loop_run_coro, coro, future._deadline_loop, trio_token=self.token)
        finally:
            coro.close()

    def __init__(self, *, thread_name: str = "trio apsw background worker"):
        global trio
        import trio

        apsw.async_run_coro.set(self.async_run_coro)
        self.queue = queue.SimpleQueue()
        self.token = trio.lowlevel.current_trio_token()
        threading.Thread(name=thread_name, target=self.worker_thread_run, args=(self.queue,)).start()


async def _trio_loop_run_coro(coro, this_deadline):
    with trio.fail_at(this_deadline):
        return await coro

class AnyIO:
    """Uses |anyio| for async concurrency"""

    def configure(self, db: apsw.Connection):
        "Setup database, just after it is created"
        for hook in apsw.connection_hooks:
            hook(db)
        db.set_progress_handler(self.progress_checker, check_progress_steps.get(), id=self)

    def send(self, call):
        "Enqueues call to worker thread"
        future = _Future(anyio.Event(), call, self.cancelled_exc_class)
        if (this_deadline := deadline.get()) is None:
            this_deadline = anyio.current_effective_deadline()
        future._set_deadline(this_deadline, anyio.current_time)
        self.queue.put(future)
        return future

    def close(self):
        "Called on connection close, so the worker thread can be stopped"
        self.queue.put(None)

    def progress_checker(self):
        "Periodic check for cancellation and deadlines"
        future = _current_future.get()
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
                    _current_future.set(future)

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
            future = _current_future.get()
            if future._is_cancelled:
                raise Cancelled("Cancelled in async_run_coro")
            if future._monotonic_exceeded():
                raise TimeoutError("deadline exceeded in async_run_coro")
            return anyio.from_thread.run(_anyio_loop_run_coro, coro, future._deadline_loop, token=self.token)
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


async def _anyio_loop_run_coro(coro, this_deadline):
    with anyio.fail_after(this_deadline - anyio.current_time()):
        return await coro


class _Future:
    """Used for most :class:`Trio` and :class:`AnyIO` requests"""

    __slots__ = (
        # Event used to signal ready
        "_event",
        # result value or exception
        "_result",
        # is it an exception?
        "_is_exception",
        # call to make
        "_call",
        # deadline in event loop clock
        "_deadline_loop",
        # deadline in worker thread relative to monotonic clock
        "_deadline_monotonic",
        # cancel handling
        "_is_cancelled",
        # cancelled exception class
        "_cancelled_exc_class"
    )

    def __init__(self, event, call, cancelled_exc_class):
        self._is_exception = False
        self._is_cancelled = False
        self._event = event
        self._call = call
        self._deadline_loop = None
        self._deadline_monotonic = None
        self._cancelled_exc_class = cancelled_exc_class

    def _set_deadline(self, value, loop_time):
        self._deadline_loop = value
        if value is not math.inf:
            self._deadline_monotonic = value - loop_time() + time.monotonic()

    def _monotonic_exceeded(self) -> bool:
        return self._deadline_monotonic is not None and time.monotonic() > self._deadline_monotonic

    async def _aresult(self):
        try:
            await self._event.wait()
        except self._cancelled_exc_class:
            self._is_cancelled = True
            raise
        if self._is_cancelled:
            raise Cancelled()
        if self._is_exception:
            raise self._result
        return self._result

    def __await__(self):
        return self._aresult().__await__()

    def cancel(self):
        "Cancel the call"

        if not self._event.is_set():
            self._is_cancelled = True
        if self._is_cancelled:
            self._event.set()
        return self._is_cancelled

    def cancelled(self):
        "Return ``True`` if call was marked cancelled, else ``False``"
        return self._is_cancelled

    def done(self):
        """Return ``True`` if call has completed, either with a result or cancelled, else ``False``"""
        return self._event.is_set() or self._is_cancelled



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
