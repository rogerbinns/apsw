Concurrency & Async
*******************

.. currentmodule:: apsw

How SQLite concurrency works
----------------------------

Each connection has a mutex to protect the SQLite data structure.  It
is acquired on a call into the connection, and released on return of
the call. The mutex can be acquired more times in the same thread,
allowing nested calls, but cannot be acquired outside of the thread
until the top level call in the original thread completes.

This means you cannot get more concurrency per connection by using
additional threads, although SQLite can do so internally (`pragma
threads <https://www.sqlite.org/pragma.html#pragma_threads>`__)
such as for sorting.  You can get concurrency with multiple connections.

SQLite is inherently synchronous due to being written in C and using
the C stack.

How Python concurrency works
----------------------------

GIL (usual operation)

    A single lock (global interpreter lock) protects all Python data
    structures.  It can only be held by one thread at a time.  By
    default it will switch the active thread every 5ms (200 times a
    second), with the operating system scheduler choosing which thread
    runs next.

    The GIL can be released by C code when not using Python data structures
    to allow other threads to run.  This is done during I/O operations
    etc.

Free threaded (Python 3.14+)

    Each Python data structure gets its own lock.  C code has to
    acquire and release the locks on individual Python objects being
    used, which allows code in other threads to run providing they are
    not using the same objects.

    The extra locking can result in around a 50% performance hit
    versus the single global lock in a single thread.

Async

    async is a language level concurrency mechanism, contrasted with
    the traditional library mechanism in :mod:`concurrent.futures`.
    It is done with the ``async`` and ``await`` keywords.

    An event loop does fine grained management of multiple tasks
    as their results become available, timeouts, cancellations,
    task groups etc, with the tasks cooperatively defining the
    points at which they can switch.  There is typically a 50%
    hit to throughput, but latencies and time to complete are
    far more uniform.

How APSW works
--------------

GIL (usual operation)

   The GIL protects APSW Python objects.  When making a SQLite call,
   the GIL is released and SQLite acquires its connection  mutex.
   This means other Python code can run concurrently with SQLite
   connections.  APSW is thread safe in that you can use any thread to
   make any call on any object, due to the GIL protection.

Free threaded

   APSW has not yet been updated.  (:issue:`568`)  It requires
   extensive code changes to lock all APSW objects (connections,
   cursors, backups etc) as well as inputs and outputs such as lists
   for executemany.

   A free threaded build is available on pypi, but loading the module
   will re-enable GIL.  You can use ``-X nogil`` when starting Python
   and that won't happen, but you can get crashes if you use the same
   objects and inputs concurrently across threads, especially if
   making concurrent modifications.

Async

    APSW fully supports async operation.  This is done by running each
    connection in its own dedicated worker thread.  Calls made in the
    event loop get forwarded to the worker thread, and the results can
    then be awaited.

    **All** callbacks such as user defined functions, virtual tables,
    various hooks, VFS etc can be async functions.  The async function
    (coroutine) is forwarded from the worker thread back to the event
    loop with the worker thread blocked until getting a result back.

Async usage
-----------

APSW async usage has been developed and tested with :mod:`asyncio`,
`Trio <https://trio.readthedocs.io/>`__, and `AnyIO
<https://anyio.readthedocs.io/>`__ with asyncio and trio event loops.
This includes cancellations and deadlines/timeouts.  There is a
controller interface (described below) providing implementations
above, or you can write/adapt your own if you have more specialised
needs or a different async framework.

Use :meth:`Connection.as_async` (a class method) to get an async
:class:`Connection`.  Related objects like :class:`Cursor`,
:class:`Blob`, :class:`Backup`, :class:`Session` etc will also be
async.

.. code-block:: python

    db = await apsw.Connection.as_async("database.db")

    # note awaiting the db.execute call to get the cursor, and then
    # using async for to iterate
    async for name, price in await db.execute("SELECT name, price FROM ..."):
        print(f"{name=) {price=}")"

There is no separate ``AsyncConnection`` (or ``AsyncCursor``,
``AsyncBlob`` etc) class.  The existing instances know if they are in
async mode or not, and behave appropriately.  You can use
:attr:`Connection.is_async` to check.

However to make type checkers and IDEs work better, the type stubs
included with APSW have those classes so it is clear when returned
values are direct or need to be awaited.

Awaitable
=========

No matter which async framework is used, all awaitables conform to
:class:`apsw.aio.AsyncResult`.  The underlying  class will vary even
within the same framework.

Contextvars
===========

:mod:`contextvars` let you provide thread local and async context
specific values.   It saves having to provide a parameter to every
function in a call chain, instead letting those that care reach out
and find the current value for their context.

contextvar values at the point of a query in the event loop are
propagated to their processing in the database worker thread, being
available to any callbacks and are also propagated back to the event
loop if any callbacks are async.

Configuration
=============

Configuration uses :mod:`contextvars`.

* :attr:`apsw.async_cursor_prefetch`
* :attr:`apsw.aio.check_progress_steps`
* :attr:`apsw.aio.deadline`
* :attr:`apsw.async_controller`
* :attr:`apsw.async_run_coro`

Deadlines and Cancellation
==========================

The native cancellation of each framework is supported.  This is often
used to cancel all tasks in a group if one fails, or to support
timeouts/deadlines.

You can use * :attr:`apsw.aio.deadline` for  :mod:`asyncio` and anyio
to set a deadline for queries.  Trio's native timeout/deadlines are
supported, with this overriding them.  Because it is a contextvar, the
deadline is propagated back to async callbacks.

Async controllers
-----------------

A controller configured via :attr:`apsw.async_controller` is used to
integrate with the async framework.  :mod:`apsw.aio` contains
implementations for asyncio, trio, and auto-detection (the default).

The controller is responsible for:

* Starting the worker thread
* Configuring teh connection in the worker thread
* Sending calls from the event loop to the worker thread
* Providing the awaitable results
* Checking deadlines and cancellations
* Running coroutines in the event loop, and providing their results
* Stopping the worker thread when told about database close



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


