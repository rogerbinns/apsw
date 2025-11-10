Async support (AIO)
*******************

.. currentmodule:: apsw

APSW supports using async.  This covers both calls into it such as
connections and cursors, and callbacks back out such as functions,
virtual tables, and VFS.

SQLite is implemented in C and is inherently synchronous.  To support
async, :class:`Connection` are run in a background worker thread.
Calls from the event loop get sent to the background thread and are
awaitable.  Calls back out from SQLite in the background thread block
the thread while they are sent to the event loop, until they complete
or a timeout happens.

Connection
==========

Use ``async`` param to connection with controller from
:mod:`apsw.aio` or your own.

Callbacks
=========

use async def instead of def and you are all set.  the async controller
set above will ensure the callbacks execute in your event loop.

.. danger:: DEADLOCKS

    If you make an async call into SQLite, and SQLite has then called
    back to an async function, and that function then is waiting on an
    async call back into SQLite, you will get a deadlock because
    neither side can make any progress.  The SQLite worker thread
    blocks until it gets a result.

    If using a VFS, this affects calling superclass functions and
    then processing their results.


Async Framework Support
=======================

There are multiple frameworks that support asynchronous execution.  They can all
work with APSW.

* :mod:`asyncio` standard library module
* `trio <https://trio.readthedocs.io>`__
* anyio

Calling into SQLite
-------------------

need to implement protocol, see :class:`apsw.aio.AsyncIO`` for done
with :mod:`asyncio`


Calling out from SQLite
-----------------------

In order to control behaviour, the :mod:`apsw` module exposes some
:class:`contextvars.ContextVar`.  They have context local state.
Typical context is the current thread, but you can also use them
nested, and asynchronous frameworks correctly track them.

apsw.aio module
===============

.. automodule:: apsw.aio
    :members:
    :undoc-members: