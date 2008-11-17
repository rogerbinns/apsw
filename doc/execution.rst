*********************
Execution and tracing
*********************

.. currentmodule:: apsw

.. _executionmodel:

Execution model
===============

This section only matters if you give multiple SQL statements in one go to :meth:`Cursor.execute`.
(Statements are seperated by semi-colons.)

SQLite does execution in two steps. First a statement is prepared,
which verifies the syntax, tables and fields and converts the
statement into an internal representation. The prepared statement is
then run. Execution stops when a row is available, there is an error
or the statement is complete.

The :meth:`Cursor.execute` method automatically does the preparing and
starts execution. If none of the statements return rows then execution
will go to the end. If a row is returned then you need to call
:meth:`Cursor.next` to get the row values or use the cursor as an
iterator. Execution will resume as necessary to satisfy
:meth:`~Cursor.next` calls.

However this means that if you don't read the rows returned then the
rest of your statements won't be executed.  APSW will detect
unexecuted previous statements and generate an exception. For
example::

   >>> cursor.execute("select * from foo ; create table bar(x,y,z)")
   >>> cursor.execute("create table bam(x,y,z)")
   Traceback (most recent call last):
     File "<stdin>", line 1, in ?
   apsw.IncompleteExecutionError: Error: there are still remaining sql statements to execute

Because I didn't read the results of ``select * from foo`` then the
following create table command didn't have a chance to get
executed. On the next execute that condition is detected and an
exception raised.

Multi-threading and re-entrancy
===============================

ASPW lets you use SQLite in multi-threaded programs and will let other
threads execute while SQLite is working.  (Technically the `GIL
<http://www.python.org/doc/2.3.4/api/threads.html>`_ is released when
`sqlite3_prepare_v2 <http://sqlite.org/c3ref/prepare.html>`_,
`sqlite3_step <http://sqlite.org/c3ref/step.html>`_ or
`sqlite3_open_v2 <http://sqlite.org/c3ref/open.html>`_ are running, as
well as all other functions that could take more than a trivial amount
of time or use the SQLite mutex. The GIL is re-acquired while user
defined functions, collations and the various hooks/handlers run.)

Note that you cannot use the same cursor object in multiple threads
concurrently to execute statements. APSW will detect this and throw an
exception. It is safe to use the object serially (eg calling
:meth:`Cursor.execute` in one thread and :meth:`Cursor.next` in
another. You also can't do things like try to
:meth:`~Connection.close` a Connection concurrently in two threads.

If you have multiple threads and/or multiple programs accessing the
same database then there may be contention for the file. SQLite will
return SQLITE_BUSY which will be raised as BusyError. You can call
:meth:`Connection.setbusytimeout` to set how long SQLite will retry
for or :meth:`Connection.setbusyhandler` to install your own busy
handler. Note that SQLite won't call the busy handler or timeout if it
believes a deadlock has arisen. SQLite's locking and concurrency is
described `here <http://www.sqlite.org/lockingv3.html>`_.

A cursor object can only be executing one query at a time. You cannot
issue a new query from inside a trace function or from a user defined
function or collation since these are called while executing a
query. You can however make new cursors and use those without
issue. You may want to remember the Connection object when you set
your trace or user defined functions.

.. _tracing:

Tracing
=======

You can install tracers on a cursor as an easy way of seeing exactly
what gets executed and what is returned. The tracers can also abort
execution and cause different values to be returned. This is very
useful for diagnostics and testing without having to modify your main
code.

.. Note::

  You cannot issue new execute statements against the cursor
  your tracer was called from. If you would like to make more queries
  in the tracer then do them from a new cursor object.

.. _executiontracer:

Execution Tracer
----------------


The :meth:`execution tracer <Connection.setexectrace>` is called after
an SQL statement has been prepared. (ie syntax errors will have caused
an exception during preparation so you won't see them with a
tracer). It is called with two arguments. The first is a string which
is the SQL statement about to be executed, and the second is the
bindings used for that statement (and can be None). If the tracer
return value evaluates to False/None then execution is aborted with an
:exc:`ExecTraceAbort` exception.  See the :ref:`example <example-exectrace>`.

.. _rowtracer:

Row Tracer
----------

The :meth:`row tracer <Connection.setrowtrace>` is called before each
row is returned. The arguments are the items about to be
returned. Whatever you return from the tracer is what is actually
returned to the caller of :meth:`~Cursor.execute`. If you return None
then the whole row is skipped. See the :ref:`example
<example-rowtrace>`.

.. _x64bitpy25:

64 bit hosts, Python 2.5+
=========================

Prior to Python 2.5, you were limited to 32 bit quantities for items
in Python such as the length of strings, number of items in a sequence
etc. Python 2.5 and above use 64 bit limits on 64 bit hosts.  APSW
will work correctly with those items in Python 2.5 and above that use
64 bits. Unfortunately SQLite is limited to 32 bit quantities for
strings, blobs, number of columns etc even when compiled for 64
bit. Consequently you will get a TooBig exception from APSW which
checks if strings/buffers longer than 1GB or 2GB (depends on internal
storage) are used. See SQLite ticket `2125
<http://www.sqlite.org/cvstrac/tktview?tn=2125>`_ and `3246
<http://sqlite.org/cvstrac/tktview?tn=3246>`_ for more details.

.. _statementcache:

Statement Cache
===============

Each :class:`Connection` maintains a cache mapping SQL queries to a
`prepared statement <http://www.sqlite.org/c3ref/stmt.html>`_ to avoid
the overhead of `repreparing
<http://www.sqlite.org/c3ref/prepare.html>`_ queries that are executed
multiple times.  This is a classic tradeoff using more memory to
reduce CPU consumption.

By default there are up to 100 entries in the cache.  Once the cache
is full, the least recently used item is discarded to make space for
new items.

You should pick a larger cache size if you have more than 100 unique
queries that you run.  For example if you have 101 different queries
you run in order then the cache will not help.

You can also :class:`specify zero <Connection>` which will disable the
statement cache.

If you are using :meth:`authorizers <Connection.setauthorizer>` then
you should disable the statement cache.  This is because the
authorizer callback is only called while statements are being
prepared.


