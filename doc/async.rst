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
    It is done with the :code:`async` and :code:`await` keywords.

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
   will re-enable GIL.  You can use :code:`-X nogil` when starting Python
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

.. _async_usage:

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

There is no separate :code:`AsyncConnection` (or :code:`AsyncCursor`,
:code:`AsyncBlob` etc) class.  The existing instances know if they are in
async mode or not, and behave appropriately.  You can use
:attr:`Connection.is_async` to check.

However to make type checkers and IDEs work better, the type stubs
included with APSW have those classes so it is clear when returned
values are direct, or need to be awaited.

You can use :meth:`Connection.async_run` to run functions in the
async Connection worker thread.

.. _anyio_note:

AnyIO note
==========

Version 4.11.0 (September 2025) or later is required for the APSW
provided :class:`controller <apsw.aio.AnyIO>` due to the mechanisms
for calling back from the worker thread to the event loop.  If you are
using an earlier version of anyio, then APSW will use the corresponding
event loop controller (:class:`~apsw.aio.AsyncIO` or
:class:`~apsw.aio.Trio`).  The main effect is that you may see
:class:`trio.TooSlowError` on timeouts with a trio event loop.

The APSW tests run using version 4 of the anyio API.

.. _trio_note:

Trio note
=========

Version 0.20.0 (February 2022) or later is required for the APSW
provided :class:`controller <apsw.aio.Trio>` due to the mechanisms
for supporting :mod:`contextvars`.  You will need a custom controller
to work with earlier versions.

Attributes
==========

Some SQLite functions are provided in APSW as attributes such as
:attr:`Connection.authorizer`.  For an async connection, you will need
to await the result.

.. code-block:: python

    auth = await connection.authorizer

To set them, you will need to use :code:`setattr` in the worker thread.

.. code-block:: python

    await connection.async_run(setattr, connection, "authorizer", my_auth)

The type stubs will make this clear to your IDE and type checker.

API results
===========

Each API has an indicator of its behaviour in sync and async modes.
You can find out if an object is in async mode by checking its
corresponding :attr:`Connection.is_async`.

The included type stubs will show correct usage for your IDE and type
checker.

.. _badge_async_sync:

|badge-async-sync|

Sync only
!!!!!!!!!

Sync object
    A direct result will be provided
Async object
    A :exc:`TypeError` will be raised.  There may be an async
    equivalent such as :code:`aclose` instead of :code:`close`, or you
    may need :code:`async with` instead of :code:`with`, :code:`async for` instead
    of :code:`for`

.. _badge_async_async:

 |badge-async-async|

Async only
!!!!!!!!!!

Sync object
    A :exc:`TypeError` will be raised. Omit the leasing :code:`async`
    and use plain :code:`with` and :code:`for`.  There may be a sync
    equivalent without a leading :code:`a`.
Async object
    You will need to :code:`await` the result when ready

.. _badge_async_dual:

 |badge-async-dual|

Sync / Async
!!!!!!!!!!!!

Sync object
    A direct result will be provided

Async object
    You will need to :code:`await` the result when ready.  When objects
    are returned like :class:`Cursor`, :class:`Blob`, :class:`Backup`
    etc, they will also be in async mode.`

.. _badge_async_value:

 |badge-async-value|

Value
!!!!!

Sync object
    A direct result will be provided
Async object
    A direct result will be provided.  Do not :code:`await` it.

.. _badge_close:

 |badge-close|

Close
!!!!!

Sync object
    Closes this object, releasing its held resources.  It is safe to
    call close multiple times.  When you call close on a
    :class:`Connection`, then it will close all the corresponding
    objects like :class:`Cursor`, :class:`Blob`, :class:`Session` etc.

Async object
    You should :code:`await` calling :code:`aclose` - the async
    version of close.  It is safe to call multiple times.  It will
    only be effective if the event loop is still running.
    :func:`contextlib.aclosing` is a handy context manager.

    This is important for the :class:`Connection` because the worker
    thread will not exit until the connection is closed.  It will be
    closed if normal garbage collection happens, but it is very easy
    to have a stray reference preventing that.

    It is allowed to call :code:`close` on async objects, which will
    immediately close them and return :exc:`ConnectionClosedError` to
    any subsequent calls made.  This is also the only way to close an
    object after the event loop has finished.  You can use
    :func:`apsw.connections` to get the currently open connections.


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

.. warning:: Not a regular variable

    Each time you :meth:`~contextvars.ContextVar.set` a value, the
    previous value is saved behind the scenes so it can be
    :meth:`restored <contextvars.ContextVar.reset>`.  You will get
    ever increasing memory consumption if you do not
    :meth:`~contextvars.ContextVar.reset`.
    :func:`apsw.aio.contextvar_set` shows a convenient way of doing
    so.

    The context is copied (a trivial internal operation) as it passes
    from async to worker thread and back to async again.  Setting a
    variable is not visible to code earlier in the call chain.  Use a
    dict or similar as the value set by the initial code to provide
    somewhere all the code in the chain can see and make changes.

Configuration
=============

Most configuration uses :mod:`contextvars`.

:attr:`apsw.async_cursor_prefetch`

    How many rows are fetched at once when iterating query results.

:attr:`apsw.aio.check_progress_steps`

    How frequently running SQLite queries check for cancellations and
    timeouts

:attr:`apsw.aio.deadline`

    When SQLite queries or async callbacks should timeout.  (|trio|
    and |anyio|) native timeout is also supported.

:attr:`apsw.async_controller`

    Interface between async framework and worker thread.

:attr:`apsw.async_run_coro`

    How the worker thread runs an async callback back in the event
    loop.

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
not set, then their :code:`current_effective_deadline` used.

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
* Sending calls from the event loop to the worker thread with
  awaitable results
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
database, and then appending 1,000 more rows to the end of the table,
until there are 300,000 rows in the table.

**Benchmarks aren't real - use your own scenario for real testing!**

Library

    apsw with :mod:`asyncio`, asyncio using |uvloop| as the inner
    loop, |trio|, and |anyio| with asyncio and trio event loops.

    The |aiosqlite| library (asyncio only) is included for comparison
    which also sends calls to a worker thread.  Note that it doesn't
    support cancellation, timeouts, or async callbacks.

Prefetch

    How many rows are fetched in a batch for queries, controlled by
    :attr:`apsw.async_cursor_prefetch` in APSW and
    :code:`iter_chunk_size` in |aiosqlite|.  A value of 1 as shown in
    the first rows ends up as 301 thousand messages and responses with
    the worker thread.  That is halved with 2 etc.  The default is 64.
    The benchmark queries return a maximum of 1,000 rows.

Wall

    Wall clock time in seconds for the configuration to run.

CpuTotal / CpuEvtLoop / CpuDbWorker

    The total CPU time used in seconds, with how much of that was in
    the async event loop thread, and how much in the background
    database worker thread.

The results show that what is used only matters if you are doing very
large numbers of calls because of very small row batch sizes.  APSW
has to allocate space for results, so increasing the prefetch size
results in more memory consumption and more CPU time to allocate it.

.. csv-table:: Benchmark Results
    :widths: auto
    :stub-columns: 1
    :header: "Library", "Prefetch", "Wall", "CpuTotal", "CpuEvtLoop", "CpuDbWorker"
    :class: aiobench-table

    apsw AsyncIO,1,7.721,8.010,3.168,4.841
    apsw AsyncIO uvloop,1,5.064,5.256,1.521,3.735
    apsw Trio,1,12.029,13.308,7.805,5.504
    apsw AnyIO asyncio,1,15.202,18.158,7.660,10.499
    apsw AnyIO asyncio uvloop,1,9.652,11.183,3.766,7.416
    apsw AnyIO trio,1,16.670,19.597,10.028,9.570
    aiosqlite,1,8.360,8.755,3.909,4.846
    aiosqlite uvloop,1,5.358,5.635,1.867,3.769
    apsw AsyncIO,2,4.287,4.441,1.684,2.758
    apsw AsyncIO uvloop,2,2.895,2.987,0.797,2.190
    apsw Trio,2,6.403,7.022,3.936,3.086
    apsw AnyIO asyncio,2,8.175,9.702,3.919,5.783
    apsw AnyIO asyncio uvloop,2,4.966,5.704,1.810,3.894
    apsw AnyIO trio,2,8.683,10.140,5.081,5.059
    aiosqlite,2,4.885,5.106,2.180,2.926
    aiosqlite uvloop,2,3.288,3.454,1.129,2.326
    apsw AsyncIO,16,1.231,1.271,0.324,0.947
    apsw AsyncIO uvloop,16,0.977,0.993,0.162,0.831
    apsw Trio,16,1.546,1.667,0.631,1.036
    apsw AnyIO asyncio,16,1.671,1.877,0.570,1.306
    apsw AnyIO asyncio uvloop,16,1.255,1.359,0.286,1.074
    apsw AnyIO trio,16,1.776,1.990,0.738,1.253
    aiosqlite,16,1.152,1.191,0.382,0.809
    aiosqlite uvloop,16,0.868,0.888,0.191,0.697
    apsw AsyncIO,64,0.847,0.868,0.145,0.723
    apsw AsyncIO uvloop,64,0.740,0.744,0.072,0.671
    apsw Trio,64,0.883,0.922,0.207,0.715
    apsw AnyIO asyncio,64,0.984,1.044,0.212,0.832
    apsw AnyIO asyncio uvloop,64,0.835,0.865,0.124,0.741
    apsw AnyIO trio,64,0.983,1.056,0.249,0.807
    aiosqlite,64,0.718,0.729,0.153,0.575
    aiosqlite uvloop,64,0.619,0.623,0.102,0.521
    apsw AsyncIO,512,0.702,0.715,0.071,0.644
    apsw AsyncIO uvloop,512,0.668,0.668,0.053,0.615
    apsw Trio,512,0.706,0.724,0.101,0.623
    apsw AnyIO asyncio,512,0.746,0.767,0.090,0.676
    apsw AnyIO asyncio uvloop,512,0.694,0.703,0.068,0.636
    apsw AnyIO trio,512,0.736,0.762,0.105,0.657
    aiosqlite,512,0.583,0.586,0.098,0.488
    aiosqlite uvloop,512,0.561,0.563,0.084,0.479
    apsw AsyncIO,"8,192",0.670,0.681,0.065,0.616
    apsw AsyncIO uvloop,"8,192",0.652,0.653,0.042,0.610
    apsw Trio,"8,192",0.703,0.720,0.089,0.630
    apsw AnyIO asyncio,"8,192",0.711,0.724,0.078,0.646
    apsw AnyIO asyncio uvloop,"8,192",0.697,0.707,0.064,0.643
    apsw AnyIO trio,"8,192",0.733,0.761,0.098,0.663
    aiosqlite,"8,192",0.564,0.566,0.091,0.475
    aiosqlite uvloop,"8,192",0.542,0.543,0.076,0.467
    apsw AsyncIO,"65.536",0.676,0.686,0.066,0.621
    apsw AsyncIO uvloop,"65.536",0.658,0.659,0.051,0.608
    apsw Trio,"65.536",0.700,0.713,0.093,0.620
    apsw AnyIO asyncio,"65.536",0.724,0.738,0.090,0.648
    apsw AnyIO asyncio uvloop,"65.536",0.698,0.706,0.072,0.634
    apsw AnyIO trio,"65.536",0.743,0.769,0.115,0.654
    aiosqlite,"65.536",0.560,0.562,0.092,0.471
    aiosqlite uvloop,"65.536",0.542,0.543,0.073,0.470
