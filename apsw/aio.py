"Implements async framework controllers"

from __future__ import annotations

import queue
import threading
import contextvars
import time


deadline: contextvars.ContextVar[int | float | None] = contextvars.ContextVar("apsw.aio.deadline", default=None)
"""Set to a value from :func:`time.monotonic()` for an operation to complete by

This makes a best effort to ensure a database operation including any
callbacks has completed by the time, else :exc:`TimeoutError` will be
raised.  The default (``None``) is no deadline.  It is generally a good
idea to set a deadline because it will unblock deadlocks.

.. code-block:: python

    # 10 seconds from now
    with apsw.async.deadline.set(time.monotonic() + 10):
       # do operations
       ...
       # you can use it nested - this operation could take a
       # minute
       with apsw.async.deadline.set(time.monotonic() + 60):
          # do other operations
          ...
"""

# ::TODO:: if sys.version < 3.11 then concurrent.futures.TimeoutError needs to be turned into exceptions.TimeoutError

# contextvars have to be top level.  this is used to track the currently
# processing future
_current_future = contextvars.ContextVar("apsw.aio._current_future")


class AsyncIO:
    """Uses :mod:`asyncio` for async concurrency"""

    def worker_thread_run(self):
        "Does the enqueued work processing in the worker thread"

        try:
            while (item := self.queue.get()) is not None:
                future, meth, args, kwargs, this_deadline = item

                # cancelled?
                if future.done():
                    continue

                with (
                    apsw.async_run_coro.set(self.async_run_coro),
                    _current_future.set(future),
                    deadline.set(this_deadline),
                ):
                    try:
                        # should we even start?
                        if this_deadline is not None and time.monotonic() > this_deadline:
                            raise TimeoutError()

                        future.loop().call_soon_threadsafe(self.set_future_result, future, meth(*args, **kwargs))

                    except Exception as exc:
                        future.loop().call_soon_threadsafe(self.set_future_exception, future, exc)

        # ::TODO:: only in py3.13
        except queue.ShutDown:
            pass

    def async_run_coro(self, coro):
        "Called in worker thread to run a coroutine in the event loop"
        if (this_deadline := deadline.get() is not None) and time.monotonic() > this_deadline:
            raise TimeoutError()
        return self.run_coroutine_threadsafe(coro, _current_future.get().loop()).result(this_deadline)

    def set_future_result(self, future, result):
        "Update future with result"
        # you get an exception if cancelled
        if not future.done():
            future.set_result(result)

    def set_future_exception(self, future, exc):
        "Update future with exception"
        if not future.done():
            future.set_exception(exc)

    def __init__(self):
        # we don't top level import because it is expensive, and trio/anyio etc could
        # be in use instead.  we only need two methods
        import asyncio

        self.get_running_loop = asyncio.get_running_loop
        self.run_coroutine_threadsafe = asyncio.run_coroutine_threadsafe

        self.queue = queue.SimpleQueue()

        threading.Thread(name="asyncio apsw background worker", target=self.worker_thread_run).start()

    def call(self, meth, args, kwargs):
        "enqueues async work to worker thread"
        future = self.get_running_loop().create_future()
        self.queue.put((future, meth, args, kwargs, deadline.get()))
        return future

    def close(self):
        "Called from connection destructor, so the worker thread can be stopped"
        self.queue.put(None)
        # shutdown only in 3.13+
        self.queue.shutdown()
