Async support
*************

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

.. danger:: DEADLOCK

    If SQLite has called out to an async function, and that function
    then waits on an async call back into SQLite, you will get a
    deadlock because neither side can make any progress.  The
    :attr:`async_timeout` sets how long SQLite waits before
    raising a timeout exception.  It is recommended to always
    set it.

Connection
==========

Use ``async`` param to connection, default pointing to
apsw.async.asyncio_controller.  ``async_timeout`` also recommended

Async Framework Support
=======================

There are multiple frameworks that support asynchronous execution.  They can all
work with APSW.

* :mod:`asyncio` standard library module
* `trio <https://trio.readthedocs.io>`__

Calling into SQLite
-------------------

need to implement protocol, see apsw.async.asyncio_controller for done
with asyncio


.. _async_vars:

Calling out from SQLite
-----------------------

In order to control behaviour, the :mod:`apsw` module exposes some
:class:`contextvars.ContextVar`.  They have context local state.
Typical context is the current thread, but you can also use them
nested, and asynchronous frameworks correctly track them.

.. code-block:: python

     cv = contextvars.ContextVar("mode", default = "slow")

     cv.get() # now "slow"

     with cv.set("loud"):
        cv.get() # now "loud"

        with cv.set("fast"):
          cv.get() # now "fast"

        cv.get() # back to "loud"

    cv.get() # now back to "slow"

You need to :meth:`contextvars.ContextVar.set` the value in each
thread or other context as appropriate.

The :attr:`async_run_from_thread` value is called from a background
worker thread to run a coroutine in an event loop, and block until
getting a result with a timeout.  It should return the result, or
raise an exception.

The three parameters are:

#. The :class:`~typing.Coroutine` to execute to completion
#. The loop to run it on, from :attr:`async_loop`
#. The timeout to block waiting, from :attr:`async_timeout`

If not set then it uses :func:`asyncio.run_coroutine_threadsafe`,
to run the coroutine, and :meth:`concurrent.futures.Future.result`
with the timeout to get the result, or exception.

