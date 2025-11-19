"Implements async framework controllers"

from __future__ import annotations

import queue
import threading
import contextvars
import contextlib
import sys
import time

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




# ::TODO:: if sys.version < 3.11 then concurrent.futures.TimeoutError needs to be turned into exceptions.TimeoutError

# contextvars have to be top level.  this is used to track the currently
# processing future
_current_future = contextvars.ContextVar("apsw.aio._current_future")


class AsyncIO:
    """Uses :mod:`asyncio` for async concurrency"""

    def configure(self, db: apsw.Connection):
        "Setup database, just after it is created"
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

    def cancel(self, future):
        # asyncio warns about futures not resolved
        future.cancel()

    async def async_value(self, value):
        return value

    async def async_exception(self, exc: BaseException, tb: types.TracebackType | None):
        if tb is not None:
            exc.with_traceback(tb)
        raise exc


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

                with (
                    contextvar_set(_current_future, future),
                    contextvar_set(deadline, this_deadline),
                    contextvar_set(apsw.async_cursor_prefetch, this_prefetch),
                ):

                    try:
                        # should we even start?
                        if this_deadline is not None:
                            if time.monotonic() > this_deadline:
                                raise TimeoutError()
                        future.get_loop().call_soon_threadsafe(self.set_future_result, future, call())

                    except BaseException as exc:
                        # BaseException is deliberately used because we
                        # want those to be returned as garbage collection
                        # will cause us to be terminated. CancelledError
                        # is a notable example
                        future.get_loop().call_soon_threadsafe(self.set_future_exception, future, exc)

                del future

    def async_run_coro(self, coro):
        "Called in worker thread to run a coroutine in the event loop"
        if (this_deadline := deadline.get()) is not None:
            timeout = this_deadline - time.monotonic()
            if timeout < 0:
                raise TimeoutError()
        else:
            timeout = None

        return self.run_coroutine_threadsafe(coro, _current_future.get().get_loop()).result(timeout)

    def set_future_result(self, future, result):
        "Update future with result in the event loop"
        # you get an exception if cancelled
        if not future.done():
            future.set_result(result)

    def set_future_exception(self, future, exc):
        "Update future with exception in the event loop"
        if not future.done():
            future.set_exception(exc)

    def __init__(self, *, thread_name: str = "asyncio apsw background worker"):
        # we don't top level import because it is expensive, and trio/anyio etc could
        # be in use instead.  we only need two methods
        import asyncio

        self.get_running_loop = asyncio.get_running_loop
        self.run_coroutine_threadsafe = asyncio.run_coroutine_threadsafe

        self.queue = queue.SimpleQueue()

        threading.Thread(name=thread_name, target=self.worker_thread_run, args=(self.queue,)).start()



# some notes about trio
# trio.lowlevel.current_trio_token()
# trio.from_thread.run (coroutine, token=...)  -- have their own deadline system
# have their own thread starting method
# trio.open_memory_channel instead of SimpleQueue maybe, but more convoluted
# there is no Future equivalent.  need to use a dataclass with trio.Event to
# signal completion, and token from above, result and exception
