"Implements async framework controllers, configuration, and helpers"

from __future__ import annotations

import queue
import threading
import contextvars
import contextlib
import sys
import concurrent.futures
import math

import apsw

from typing import TypeVar, Protocol

T = TypeVar("T")


deadline: contextvars.ContextVar[int | float | None] = contextvars.ContextVar("apsw.aio.deadline", default=None)
"""Deadline in seconds

This makes a best effort to timeout a database operation including any
callbacks if the deadline is passed.  The default (``None``) is
no deadline.

:class:`AsyncIO`

    This is the only way to set a deadline.  :exc:`TimeoutError` will be
    raised if the deadline is exceeded.  The time is measured using
    :meth:`asyncio.loop.time`

:class:`Trio`

    If this is set then it is used for the deadline.  `TooSlowError
    <https://trio.readthedocs.io/en/stable/reference-core.html#trio.TooSlowError>`__
    is raised.

    Otherwise the `current effective deadline
    <https://trio.readthedocs.io/en/stable/reference-core.html#trio.current_effective_deadline>`__
    where the call is made is used.

    Time is measured using `current_clock
    <https://trio.readthedocs.io/en/stable/reference-lowlevel.html#trio.lowlevel.current_clock>`__
    in place when the connection is created.

AnyIO

    You can use `anyio.current_effective_deadline
    <https://anyio.readthedocs.io/en/stable/api.html#anyio.current_effective_deadline>`__
    to set this::

        with anyio.fail_after(15):
            with apsw.aio.deadline.set(anyio.current_effective_deadline):
                # do operations that will pick up the fail after value
                ,,,


.. code-block:: python

    # a minute
    with apsw.aio.timeout.deadline.set(loop.time() + 60):
       # do operations
       ...
       # you can use it nested - these operations are 10
       # seconds
       with apsw.aio.timeout.set(loop.time() + 10):
          # do other operations
          ...

"""

check_progress_steps : contextvars.ContextVar[int] = contextvars.ContextVar("apsw.aio.check_progress_steps", default=50_000)
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

class AsyncResult(Protocol):
    """
    All async results have these methods, no matter which API or
    Controller is in use.  This is a :class:`~typing.Protocol` and
    **not** a real class.  The actual class returned will vary
    even for the same call.

    The methods can only be called in async context - calling in a
    background thread will result in exceptions, wrong answers, or no
    effect.
    """

    def __await__(self) -> Generator[Any, None, Any]:
        "awaitable, giving the call result or exception"
        ...

    def cancel(self) -> bool:
        """Cancel the call

        Attempts to stop the call if already in progress, or not start
        it.  Returns ``True`` if marked for cancellation, or ``False``
        if too late.
        """
        ...

    def cancelled(self) -> bool:
        """Return ``True`` if call was marked cancelled, else ``False``

        Cancellation can only succeed before completion.
        """
        ...

    def done(self) -> bool:
        """Return ``True`` if call has completed, either with a result or cancelled, else ``False`` if
        still waiting for a result"""
        ...


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
        "Called from connection destructor, so the worker thread can be stopped"
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

            # yes we really need the timeout twice.  when the wait_for one fires the
            # exception isn't propagated to us
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
    """Uses `Trio <https://trio.readthedocs.io/>`__ for async concurrency"""

    def configure(self, db: apsw.Connection):
        "Setup database, just after it is created"
        for hook in apsw.connection_hooks:
            hook(db)
        db.set_progress_handler(self.progress_checker, check_progress_steps.get(), id=self)

    def progress_checker(self):
        "Periodic check for cancellation and deadlines"
        future = _current_future.get()
        if future._is_cancelled:
            raise TrioFuture.Cancelled("cancelled in progress handler")
        if future.deadline is not math.inf and future.deadline < self.clock.current_time():
            raise trio.TooSlowError("deadline exceeded in progress handler")
        return False

    def send(self, call):
        future = TrioFuture()
        future.token = trio.lowlevel.current_trio_token()
        future.event = trio.Event()
        future.is_exception = False
        future.call = call
        if (this_deadline := deadline.get()) is None:
            future.deadline = trio.current_effective_deadline()
        else:
            future.deadline = this_deadline
        future._is_cancelled = False
        self.queue.put(future)
        return future

    def close(self):
        self.queue.put(None)

    def worker_thread_run(self, q):
        while (future := q.get()) is not None:
            if not future._is_cancelled:
                with future.call:
                    _current_future.set(future)

                    try:
                        if future.deadline is not math.inf and future.deadline < self.clock.current_time():
                            raise trio.TooSlowError("Deadline exceeded in queue")

                        future.result = future.call()
                    except BaseException as exc:
                        future.result = exc
                        future.is_exception = True

            # this ensures completion even if cancelled
            trio.from_thread.run_sync(future.event.set, trio_token=future.token)

    def async_run_coro(self, coro):
        try:
            future = _current_future.get()
            if future._is_cancelled:
                raise trio.Cancelled("Cancelled in async_run_coro")
            return trio.from_thread.run(_trio_loop_run_coro, coro, future.deadline, trio_token=future.token)
        finally:
            coro.close()

    def __init__(self, *, thread_name: str = "trio apsw background worker"):
        global trio
        import trio

        apsw.async_run_coro.set(self.async_run_coro)
        self.queue = queue.SimpleQueue()
        self.clock = trio.lowlevel.current_clock()
        threading.Thread(name=thread_name, target=self.worker_thread_run, args=(self.queue,)).start()


class TrioFuture:
    """Returned for each :class:`Trio` request

    :meta private:
    """

    __slots__ = (
        # needed to call back into trio
        "token",
        # trio.Event used to signal ready
        "event",
        # result value or exception
        "result",
        # is it an exception?
        "is_exception",
        # call to make
        "call",
        # timeout handling
        "deadline",
        # cancel handling
        "_is_cancelled",
    )

    class Cancelled(Exception):
        "Result when an operation was cancelled"

        pass

    async def aresult(self):
        ":meta private:"
        try:
            await self.event.wait()
        except trio.Cancelled:
            self._is_cancelled = True
            raise
        if self._is_cancelled:
            raise TrioFuture.Cancelled()
        if self.is_exception:
            raise self.result
        return self.result

    def __await__(self):
        return self.aresult().__await__()

    def cancel(self):
        if not self.event.is_set():
            self._is_cancelled = True
        if self._is_cancelled:
            self.event.set()
        return self._is_cancelled

    def cancelled(self):
        return self._is_cancelled

    def done(self):
        return self.event.is_set() or self._is_cancelled


async def _trio_loop_run_coro(coro, this_deadline):
    with trio.fail_at(this_deadline):
        return await coro


def Auto() -> Trio | AsyncIO:
    """
    Automatically detects the current async framework and returns the
    appropriate controller.  This is the default for
    :attr:`apsw.async_controller`.

    It uses the same logic as the `sniffio
    <https://sniffio.readthedocs.io>`__ package and only knows about
    the controllers implemented in this module.  anyio always runs an
    asyncio or trio event loop.

    :exc:`RuntimeError` is raised if the framework can't be detected.

    :rtype: Trio | AsyncIO
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

    raise RuntimeError("Unable to determine current Async environment")
