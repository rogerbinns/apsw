Concurrency & Async
*******************

.. currentmodule:: apsw

How SQLite works
----------------

- each db has mutex
- acquired on call, released after
- can be reacquired re-entrantly but only same thread
- aims to do work as quickly as possible
- pragma threads (eg for sorting)
- cannot get same connection to be concurrent with multiple threads
- inherently synchronous due to C implementation
- get next row only API (network db give back batches of rows)

How Python works
-----------------

- GIL protects all python data structures with one lock, so only
  one thread at a time can be accessing python objects.
  GIL released during I/O operations etc so Python code can
  run in other threads.

- free threaded requires each python object in use to be locked
  individually - ~50% performance hit but code in different
  threads can run at the same time providing they don't
  access the same python objects.

- async has an event loop run by asyncio or trio in one thread.
  multiple coroutines can be "running" at once, which means they are
  waiting for their next thing to happen such as time to pass, network
  I/O activity, work in a background thread to complete.  event loop
  handles time, activity, cancellations etc.

  methods need to be marked ``async`` to say they can be running at
  same time as others, and ``await`` used to get result of waiting on
  something.

How APSW works
--------------

GIL (usual operation)

   GIL protects APSW and Connection Python objects.  When making a SQLite
   call the GIL is released and SQLite acquires its db mutex.  Means Python code
   can run concurrently with SQLite connections.

Free threaded

   Requires expansive code updates to lock Connection / Cursor /Backup
   etc objects, but also input objects like list for executemany so can't
   be modified while in use.  Work ongoing.

   Free threaded build is available on pypi but will re-enable GIL.  Can use
   -Xgil and that won't happen, but you can get crashes if you use same
   connection and objects concurrently across threads.

Async

   :meth:`Connection.as_async` gets async connection.  connection is run in
   dedicated background worker thread.  calls in event loop get forwarded to
   worker thread via controller returning async to event loop that completes
   when worker thread finishes that call.

   can also have async functions for any callback including virtual tables,
   VFS, scalar/aggregate/window functions etc.  Uses async_run_coro to
   get result, with typical (but not required) approach being async controller
   handling that.

   .. warning:: DEADLOCKS

        async callback can't block on re-entrant call.  if need more queries
        eg in vtable, then use sync function to make them


Async usage
-----------

extract from rewrite section below


Async Alternatives
------------------

run in thread
=============

can use regular mode and run expensive operations in thread each time - eg :func:`asyncio.to_thread`
(can't be used with stdlib sqlite3 because everything has to be in same thread each time)

aiosqlite
=========

excellent choice, use if meets your needs

* asyncio only (no trio)
* About equal performance
* aiosqlite exception in cursor iteration
* doesn't directly support async callbacks, but you could wrap each callback to do so
* doesn't support contextvars for sync callbacks



Performance
-----------

explain aiobench, why prefetch size matters, give some output

apsw.aio module
---------------

.. automodule:: apsw.aio
    :members:
    :undoc-members:



OLD DOC TO REWRITE
------------------





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
method letting you run any callable (use :func:`functools.partial`)
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


