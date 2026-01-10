Concurrency & Async
*******************

.. currentmodule:: apsw

How SQLite works
----------------

Each connection has a mutex to protect the SQLite data structure.  It
is acquired on a call into the connection, and released on return of
the call. The mutex can be acquired more times in the same thread,
allowing nested calls, but cannot be acquired outside of the thread
until the top level call completes.

This means you cannot get more concurrency per connection by using
additional threads, although SQLite can do so internally (`pragma
threads <https://www.sqlite.org/pragma.html#pragma_threads>`__`)
such as for sorting.

SQLite is inherently synchronous due to being written in C and using
the C stack.

How Python works
-----------------

GIL (usual operation)

    A single lock (global interpreter lock) protects all Python
    data structures.  It can only be held by one thread at a time.
    It can be released by C code when not using Python data structures
    to allow other threads to run.  This is done during I/O operations
    etc.

Free threaded (Python 3.14+)

    Each Python data structure gets its own lock.  Code has to acquire
    and release the locks on individual Python objects being used,
    which allows code in other threads to run providing they are not
    using the same objects.

    The extra locking can result in around a 50% performance hit
    versus the single global lock in a single thread.

Async

    Traditionally concurrency has been done using the :mod:`concurrent.futures`
    library to spread work over threads and processes,


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

   Requires expansive code updates to lock Connection / Cursor / Backup
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

cancellation & deadlines

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
* manual work to add each API



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


