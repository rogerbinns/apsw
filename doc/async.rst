Async support (AIO)
*******************

.. currentmodule:: apsw

APSW supports using async with SQLite.  This covers both calls into it
such as to connections and cursors, and callbacks back out such as
functions, virtual tables, and VFS.

Callbacks are independent of call-ins, although you would typically use
the same async framework for both.  It is not required.

SQLite is implemented in C and is inherently synchronous.

Callbacks
---------

When a coroutine (async function) is used as a callback, it is
detected and :attr:`apsw.async_run_coro` is called with the coroutine.
It should return the result or raise an exception.  Typically it would
send the coroutine back to the event loop and wait for the result.

SQLite is blocked until it gets an answer.

Async Connection, Cursor, ...
-----------------------------

Calling :meth:`Connection.as_async` will result in an async
:class:`Connection`.  A controller runs the connection in a background
worker thread.  Calls will be sent to the worker thread giving an
awaitable to check for async completion to the caller.

While the type stubs say there is an :class:`AsyncConnection` for
convenience, there is no separate type.  The :class:`Connection`
knows if it is async or not and behaves appropriately.

You can use :attr:`Connection.is_async` to detect async mode.
:attr:`Connection.async_controller` provides the :class:`controller
<AsyncConnectionController>` currently in use, with its ``send``
method letting you run any callable (use :class:`functools.partial`)
in the worker thread.

Deadlocks
---------

It is possible to get deadlocks if you make a callback go to the event
loop which then makes a call back into SQLite and awaits (blocking)
for a result.  Neither side will be able to make any progress.

Configuration is via contextvars
================================

Configuration uses :mod:`contextvars`.  These let you provide thread
local and async context specific values.   It saves having to provide
a parameter to every function in a call chain, instead letting those
that care reach out and find the current value for them.

The recommended way of using them is as a context manager which will
change the value inside, and restore it on exit.

.. code-block:: python

    with var.set(7):
        # now at 7 here
        with var.set(12):
            # now at 12 here
            ...
        # back to 7 here

    # Python 3.14 supports with directly.  For earlier versions
    import apsw.aio.contextvar_set as contextvar_set

    with contextvar_set(var, 7):
        # now at 7 here
        with contextvar_set(var, 12):
            # now at 12 here
            ...
        # back to 7 here

:attr:`apsw.async_run_coro`

    Called with a coroutine and must block until a result/exception is
    returned.

:attr:`apsw.async_controller`

    Used to start a worker thread and run a connection in it.

:attr:`apsw.async_cursor_prefetch`

    Sets how many rows are fetched at once when using a cursor.  This
    improves performance by avoiding a round trip through the worker
    thread for each row.

The :mod:`apsw.aio` controllers also use contextvars.  For example
:attr:`apsw.aio.deadline` is used with :mod:`asyncio` to set a deadline
for a query.

Async Framework Support
=======================

The APSW async implementation is neutral to any framework.
:mod:`apsw.aio` provides implementations for the standard library
:mod:`asyncio` and others.  They can be used as is, or as the basis
for your own customisations.

Functionality like timeouts/deadlines in each framework are honoured
or provided.

* :mod:`asyncio` standard library module
* `trio <https://trio.readthedocs.io>`__
* anyio
* curio https://curio.readthedocs.io/en/latest/reference.html#asynchronous-threads


apsw.aio module
===============

.. automodule:: apsw.aio
    :members:
    :undoc-members: