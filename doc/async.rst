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
|trio|, and |anyio| with asyncio and trio event loops.
This includes cancellations and deadlines/timeouts.  There is a
controller interface (described below) providing event loop
integration, or you can write/adapt your own if you have more
specialised needs, or a different async framework.

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
values are direct, or need to be awaited.

You can use :meth:`Connection.async_run` to run functions in the
async Connection worker thread.

Attributes
==========

Some SQLite functions are provided in APSW as attributes such as
:attr:`Connection.authorizer`.  For an async connection, you will need
to await the result.

.. code-block:: python

    auth = await connection.authorizer

To set them, you will need to use ``setattr`` nin the worker thread.

.. code-block:: python

    await connection.async_run(setattr, connection, "authorizer", my_auth)

The type stubs will make this clear to your IDE and type checker.

API results
===========

Each API has an indicator of its behaviour in sync and async modes.
You can find out if an object is in async mode by checking its
corresponding :attr:`Connection.is_async`.

.. _badge_async_sync:

|badge-async-sync|

Sync only
!!!!!!!!!

Sync object
    A direct result will be provided
Async object
    A :exc:`TypeError` will be raised.  There may be an async
    equivalent such as ``aclose`` instead of ``close``, or you may
    need ``async with`` instead of ``with``, ``async for`` instead of
    ``for``

.. _badge_async_async:

 |badge-async-async|

Async only
!!!!!!!!!!

Sync object
    A :exc:`TypeError` will be raised. Omit the leasing ``async`` and
    use plain ``with`` and ``for``.  There may be a sync equivalent
    without a leasing ``a``.
Async object
    You will need to ``await`` the result when ready

.. _badge_async_dual:

 |badge-async-dual|

Sync / Async
!!!!!!!!!!!!

Sync object
    A direct result will be provided

Async object
    You will need to ``await`` the result when ready.  When objects
    are returned like :class:`Cursor`, :class:`Blob`, :class:`Backup`
    etc, they will also be in async mode.`

.. _badge_async_value:

 |badge-async-value|

Value
!!!!!

Sync object
    A direct result will be provided
Async object
    A direct result will be provided.  Do not ``await``` it.

Callbacks
=========

SQLite has numerous hooks and callbacks such as :meth:`functions
<Connection.create_scalar_function>`, :meth:`hooks
<Connection.set_update_hook>`, :doc:`virtual tables <vtable>`, and
:doc:`VFS <vfs>`.

If you provide sync versions they get called in the connection worker
thread.  You can also provide an async callback/method.  The async
controller will suspend execution in the worker thread, send the
callback back to the event loop, and resume execution on getting a
result (or exception).

.. warning:: DEADLOCK

    If your async callback makes a request back into the connection
    **and** awaits it, then you will get a deadlock.  The connection
    cannot proceed until it gets a result, and the callback is waiting
    on the suspended connection.

Awaitable
=========

No matter which async framework is used, all awaitables conform to
:class:`apsw.aio.AsyncResult`.  The underlying class will vary even
within the same framework.

Contextvars
===========

:mod:`contextvars` let you provide thread local and async context
specific values.   It saves having to provide a parameter to every
function in a call chain, instead letting those that care reach out
and find the current value for their context.

contextvar values at the point of a query in the event loop are
propagated to their processing in the database worker thread, being
available to any callbacks, and are also propagated back to the event
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
used to cancel all tasks in a group if one fails, and to support
timeouts/deadlines.

An example usage of deadlines is if you use a function or virtual
table that makes network requests.  When executing a query you can
ensure reasonable bounds for how long it takes, bounding the internal
functions and virtual tables used to answer the query.

You can set a deadline by which an API request must timeout if not
completed.  This includes sync and async callbacks that are made to
satisfy the request.  The deadline is captured at the point the call
is made, and subsequent changes are not observed.

You can use :attr:`apsw.aio.deadline` to set the deadline - its
documentation provides more details.

Trio and anyio have timeout managers.  If :attr:`apsw.aio.deadline` is
not set, then their ``current_effective_deadline`` used.

* :func:`trio.fail_at`,  :func:`trio.fail_after`, :func:`trio.current_effective_deadline`
* :func:`anyio.fail_after`, :func:`anyio.current_effective_deadline`

Async controllers
=================

A controller configured via :attr:`apsw.async_controller` is used to
integrate with the async framework.  :mod:`apsw.aio` contains
implementations for :mod:`asyncio`, |trio|, |anyio|, and
auto-detection (the default).

The controller is responsible for:

* Starting the worker thread
* Configuring the connection in the worker thread
* Sending calls from the event loop to the worker thread
* Providing the awaitable results
* Checking deadlines and cancellations
* Running coroutines in the event loop, and providing their results
* Stopping the worker thread when told about database close

Although it seems like a lot, they are around 50 lines of code, and
conform to the :class:`AsyncConnectionController` protocol.

Run in thread (alternative)
===========================

Instead of using APSW in async mode, you can request your framework
run expensive operations in a thread.  For example
:func:`asyncio.to_thread`, :func:`trio.to_thread.run_sync` and
:func:`anyio.to_thread.run_sync` can do that for you.

Extensions
==========

Session

    You will need to use :func:`apsw.aio.make_session`

:class:`apsw.fts5.Table`

    Virtually every method and property needs to access the database.
    Therefore you will need to run all of them in the database thread
    using :meth:`Connection.async_run`

    .. code-block:: python

        db = await apsw.Connection.as_async("my.db")

        # creating a table
        table = await db.async_run(
            # table creation method
            apsw.fts5.Table.create,
            # the various parameters
            db, "search", content="recipes", columns=...
        )

        # loading existing table
        table = await db.async_run(
            # load method
            apsw.fts5.Table,
            # parameters
            db, "search"
        )

        # row count (attribute)
        row_count = await db.async_run(
            getattr, table, "row_count"
        )

        # query suggestions (method)
        suggest = await db.async_run(
            table.query_suggest, query
        )

apsw.aio module
---------------

.. automodule:: apsw.aio
    :members:
    :undoc-members:
    :member-order: bysource

Async Performance
-----------------

Performance is dominated by the overhead of sending calls to the
worker thread, and getting the result.  :source:`tools/aio_bench.py`
is a small benchmark that keeps reading rows from a dummy memory
database, and then appending 1,000 to the end of the table, until there
are 300,000 rows in the table.

**Benchmarks aren't real - use your own scenario for real testing!**

Library

    apsw with :mod:`asyncio`, asyncio using |uvloop| as the inner
    loop, |trio|, and |anyio| with asyncio and trio event loops.

    The |aiosqlite| library (asyncio only) is included for comparison
    which also sends calls to a worker thread.

Prefetch

    How many rows are fetched in a batch for queries, controlled by
    :attr:`apsw.async_cursor_prefetch` in APSW and ``iter_chunk_size``
    in |aiosqlite|.  A value of 1 as shown in the first rows ends up
    as 301 thousand messages and responses with the worker thread.
    That is halved with 2 etc.  The default is 64.  The benchmark
    queries return a maximum of 1,000 rows.

Wall

    Wall clock time in seconds for the configuration to run.

CpuTotal / CpuEvtLoop / CpuDbWorker

    The total CPU time used in seconds, with how much of that was in
    the async event loop thread, and how much in the background
    database worker thread.

.. csv-table:: Benchmark Results
    :widths: auto
    :stub-columns: 1
    :header: "Library", "Prefetch", "Wall", "CpuTotal", "CpuEvtLoop", "CpuDbWorker"
    :class: aiobench-table

    "apsw AsyncIO", 1, 6.717, 6.902, 2.596, 4.305
    "apsw AsyncIO uvloop", 1, 4.361, 4.506, 1.149, 3.356
    "apsw Trio", 1, 16.284, 18.690, 9.095, 9.595
    "aiosqlite", 1, 7.495, 7.739, 3.371, 4.368
    "aiosqlite uvloop", 1, 4.847, 5.049, 1.697, 3.352
    "apsw AsyncIO", 2, 3.769, 3.887, 1.457, 2.429
    "apsw AsyncIO uvloop", 2, 2.821, 2.872, 0.732, 2.141
    "apsw Trio", 2, 8.748, 9.964, 4.407, 5.556
    "aiosqlite", 2, 3.968, 4.106, 1.757, 2.348
    "aiosqlite uvloop", 2, 2.631, 2.719, 0.865, 1.854
    "apsw AsyncIO", 16, 1.001, 1.019, 0.224, 0.795
    "apsw AsyncIO uvloop", 16, 0.851, 0.863, 0.131, 0.733
    "apsw Trio", 16, 1.747, 1.900, 0.635, 1.265
    "aiosqlite", 16, 1.011, 1.025, 0.303, 0.722
    "aiosqlite uvloop", 16, 0.834, 0.830, 0.182, 0.649
    "apsw AsyncIO", 64, 0.756, 0.758, 0.107, 0.651
    "apsw AsyncIO uvloop", 64, 0.662, 0.660, 0.062, 0.598
    "apsw Trio", 64, 0.958, 1.002, 0.236, 0.766
    "aiosqlite", 64, 0.660, 0.650, 0.135, 0.515
    "aiosqlite uvloop", 64, 0.569, 0.568, 0.092, 0.476
    "apsw AsyncIO", 512, 0.658, 0.661, 0.076, 0.585
    "apsw AsyncIO uvloop", 512, 0.631, 0.623, 0.052, 0.571
    "apsw Trio", 512, 0.732, 0.749, 0.119, 0.630
    "aiosqlite", 512, 0.533, 0.529, 0.075, 0.454
    "aiosqlite uvloop", 512, 0.517, 0.511, 0.070, 0.441
    "apsw AsyncIO", "8,192", 0.739, 0.719, 0.088, 0.632
    "apsw AsyncIO uvloop", "8,192", 0.625, 0.620, 0.057, 0.564
    "apsw Trio", "8,192", 0.692, 0.707, 0.089, 0.618
    "aiosqlite", "8,192", 0.529, 0.522, 0.080, 0.442
    "aiosqlite uvloop", "8,192", 0.508, 0.506, 0.069, 0.436
    "apsw AsyncIO", "65,536", 0.624, 0.629, 0.063, 0.566
    "apsw AsyncIO uvloop", "65,536", 0.620, 0.611, 0.052, 0.560
    "apsw Trio", "65,536", 0.681, 0.700, 0.090, 0.610
    "aiosqlite", "65,536", 0.543, 0.535, 0.091, 0.445
    "aiosqlite uvloop", "65,536", 0.521, 0.509, 0.066, 0.442

The results show that what is used only matters if you are doing very
large numbers of calls because of very small row batch sizes.  APSW
has to allocate space for results, so increasing the prefetch size
results in more memory consumption and more CPU time to allocate it.
