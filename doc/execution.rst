*********************
Execution and tracing
*********************

.. currentmodule:: apsw

Quick start
===========

You may find :meth:`apsw.ext.Trace` as a quick convenient overview of
what get executed and its effects.  If you need more detailed
information than that, then keep reading.

.. _executionmodel:

Execution model
===============

SQLite does execution in two steps. First a statement is prepared,
which verifies the syntax, tables and fields and converts the
statement into an internal representation. The prepared statement is
then run. Execution stops when a row is available, there is an error
or the statement is complete.

The :meth:`Cursor.execute` method automatically does the preparing and
starts execution. If none of the statements return rows then execution
will go to the end. If a row is returned then you use the cursor as an
iterator. Execution will resume as necessary to return each result row.

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
threads execute while SQLite is working.  It checks at start that
SQLite was compiled in `threadsafe mode
<https://www.sqlite.org/threadsafe.html>`__ which is the default.  The
`GIL
<https://docs.python.org/3/glossary.html#term-global-interpreter-lock>`_
is released when when SQLite APIs are called, and re-acquired while
running any Python code.

You cannot use the same cursor object in multiple threads concurrently
to execute statements. APSW will detect this and raise an
:exc:`ThreadingViolationError`. It is safe to use the object serially
(eg calling :meth:`Cursor.execute` in one thread and iterator in
another. You also can't do things like try to
:meth:`~Connection.close` a Connection concurrently in two threads.

A cursor object can only be executing one query at a time. You cannot
issue a new query from inside a trace function or from a user defined
function or collation since these are called while executing a
query. You can however make new cursors and use those without
issue. You may want to remember the Connection object when you set
your trace or user defined functions.

64 bit hosts
============

APSW is tested and works correctly on 32 and 64 bit hosts.
SQLite is limited to 32 bit quantities for strings,
blobs, number of columns etc even when compiled for 64 bit.
You will get a :exc:`TooBigError` if trying to use strings
and blobs larger than 1 gigabyte.

.. _statementcache:

Statement Cache
===============

Each :class:`Connection` maintains a cache mapping SQL queries to a
`prepared statement <https://sqlite.org/c3ref/stmt.html>`_ to avoid
the overhead of `repreparing
<https://sqlite.org/c3ref/prepare.html>`_ queries that are executed
multiple times.  This is a classic trade off using more memory to
reduce CPU consumption.

By default there are up to 100 entries in the cache.  Once the cache
is full, the least recently used item is discarded to make space for
new items.

You should pick a larger cache size if you have more than 100 unique
queries that you run.  For example if you have 101 different queries
you run in order then the cache will not help.


If you are using :attr:`authorizers <Connection.authorizer>` then be
aware authorizer callback is only called while statements are being
prepared.  You can :class:`specify zero <Connection>` which will
disable the statement cache completely, use use `can_cache = False`
flag to `execute`/`executemany`.

.. _tracing:

Tracing
=======

You can install tracers on :class:`cursors <Cursor>` or
:class:`connections <Connection>` as an easy way of seeing exactly
what gets executed and what is returned. The tracers can also abort
execution and cause different values to be returned. This is very
useful for diagnostics and testing without having to modify your main
code.

.. Note::

  You cannot issue new execute statements against the cursor
  your tracer was called from. If you would like to make more queries
  in the tracer then do them from a new cursor object.  For example::

    def exec_tracer(cursor, sql, bindings):
      cursor.connection.cursor().execute("insert into log values(?,?)", (sql,str(bindings)))
      return True

.. _executiontracer:

Execution Tracer
----------------

The execution tracer is called after an SQL statement has been
prepared. (ie syntax errors will have caused an exception during
preparation so you won't see them with a tracer). It is called with
three arguments.

  cursor
    The cursor executing the statement
  sql
    The SQL text being executed
  bindings
    The bindings being used.  This may be *None*, a dictionary or
    a tuple.

If the tracer return value is False then execution is
aborted with an :exc:`ExecTraceAbort` exception.  See the
:ref:`example <example_exectrace>`.

Execution tracers can be installed on a specific cursor by setting
:attr:`Cursor.exec_trace` or for all cursors by setting
:attr:`Connection.exec_trace`, with the cursor tracer taking
priority.

.. _rowtracer:

Row Tracer
----------

The row tracer is called before each row is returned. It is called with
two arguments.

  cursor
    The cursor returning the row
  row
    A tuple of the values about to be returned

Whatever you return from the tracer is what is actually returned to
the caller of :meth:`~Cursor.execute`. If you return None then the
whole row is skipped. See the :ref:`example <example_rowtrace>`.
They are useful for logging and testing.

Row tracers can be installed on a specific cursor by setting
:attr:`Cursor.row_trace` or for all cursors by setting
:attr:`Connection.row_trace`, with the cursor tracer taking
priority.

If you want to convert types then see :ref:`typeconversion`.

.. _apswtrace:

APSW Trace
==========

APSW includes a tracer that lets you easily trace SQL execution as
well as providing a summary report without modifying your code.

.. code-block:: console

  $ python3 -m apsw.trace [apswtrace options] yourscript.py [your options]

The following options are available:

.. code-block:: console

  $ python3 -m apsw.trace --help
  Usage: apswtrace.py [options] pythonscript.py [pythonscriptoptions]

  This script runs a Python program that uses APSW and reports on SQL queries
  without modifying the program.  This is done by using connection_hooks and
  registering row and execution tracers.  See APSW documentation for more
  details on the output.

  Options:
    -h, --help            show this help message and exit
    -o OUTPUT, --output=OUTPUT
                          Where to send the output.  Use a filename, a single
                          dash for stdout, or the words stdout and stderr.
                          [stdout]
    -s, --sql             Log SQL statements as they are executed. [False]
    -r, --rows            Log returned rows as they are returned (turns on sql).
                          [False]
    -t, --timestamps      Include timestamps in logging
    -i, --thread          Include thread id in logging
    -l LENGTH, --length=LENGTH
                          Max amount of a string to print [30]
    --no-report           A summary report is normally generated at program
                          exit.  This turns off the report and saves memory.
    --report-items=N      How many items to report in top lists [15]
    --reports=REPORTS     Which reports to show
                          [summary,popular,aggregate,individual]

This is sample output with the following options: **--sql**,
**--rows**, **--timestamps**, **--thread**

.. code-block:: text

  1e0e5a0 0.152 7fccea8456e0 OPEN: ":memory:" unix READWRITE|CREATE
  1f72ac0 0.161 7fccea8456e0 OPEN: "testdb" unix READWRITE|CREATE
  1f6b8d0 0.162 7fccea8456e0 CURSORFROM: 1f72ac0 DB: "testdb"
  1f6b8d0 0.162 7fccea8456e0 SQL: create table foo(x,y,z)
  1f6b8d0 0.239 7fccea8456e0 CURSORFROM: 1f72ac0 DB: "testdb"
  1f6b8d0 0.239 7fccea8456e0 SQL: insert into foo values(?,?,?) BINDINGS: ("kjfhgk", "gkjlfdhgjkhsdfkjg", "gklsdfjgkldfjhnbnvc,mnxb,mnxcv..")
  1f6b8d0 0.242 7fccea8456e0 CURSORFROM: 1f72ac0 DB: "testdb"
  1f6b8d0 0.242 7fccea8456e0 SQL: insert into foo values(?,?,?) BINDINGS: ("gdfklhj", ":gjkhgfdsgfd", "gjkfhgjkhdfkjh")
  1f6b8d0 0.244 7fccea8456e0 CURSORFROM: 1f72ac0 DB: "testdb"
  1f6b8d0 0.245 7fccea8456e0 SQL: insert into foo values(?,?,?) BINDINGS: ("gdfjkhg", "gkjlfd", "")
  1f6b8d0 0.247 7fccea8456e0 CURSORFROM: 1f72ac0 DB: "testdb"
  1f6b8d0 0.247 7fccea8456e0 SQL: insert into foo values(?,?,?) BINDINGS: (1, 2, 30)
  1f6b8d0 0.257 7fccea8456e0 CURSORFROM: 1f72ac0 DB: "testdb"
  1f6b8d0 0.257 7fccea8456e0 SQL: select longest(x,y,z) from foo
  1f6b8d0 0.257 7fccea8456e0 ROW: ("gklsdfjgkldfjhnbnvc,mnxb,mnxcv..")

Each row starts with the following fields:

  id
    This is the :func:`id` of the
    :class:`Cursor` or :class:`Connection`.  You can easily `filter
    <https://en.wikipedia.org/wiki/Grep>`__ the log if you just want to
    find out what happened on a specific cursor or connection.

  timestamp
    This is time since the program started in seconds

  threadid
    The unique :func:`thread identifier <threading.get_ident>`


The remainder of the line has one of the following forms:

  OPEN: "dbname" vfs open_flags
    A :class:`Connection` has been opened.  The *dbname* is the
    filename exactly as given in the call to
    :class:`Connection`. *vfs* is the name of the :ref:`VFS <vfs>`
    used to open the database. *open_flags* is the set of :data:`flags
    <apsw.mapping_open_flags>` supplied with the leading *SQLITE_OPEN*
    prefix omitted.

  CURSORFROM: connectionid DB: "dbname"
    A cursor has been allocated.  The *id* at the beginning of this row
    is of the new cursor.  *connectionid* is the id of the Connection
    it was created from.  The *dbname* is provided for convenience.
    This message is logged the first time a cursor issues a query.

  SQL: query BINDINGS: bindings
    A query was issued on a cursor.

  ROW: row
    A result row was returned by a cursor.

A report is also generated by default.  This is example output from
running the test suite.  When calculating time for queries, your code
execution time is included as well.  For example if your query
returned 10 rows and you slept for 1 second on reading each row then
the time for the query will be recorded as 10 seconds.  Because you
can have multiple queries active at the same time, as well as across
multiple threads, the total processing time can be larger than the
program run time.  The processing time is only recorded for queries
that have no results or where you read all the result rows.
Processing time also includes waiting time on busy connections.

  .. code-block:: text

    APSW TRACE SUMMARY REPORT

    Program run time                    83.073 seconds
    Total connections                   1308
    Total cursors                       3082
    Number of threads used for queries  21
    Total queries                       127973
    Number of distinct queries          578
    Number of rows returned             2369
    Time spent processing queries       120.530 seconds

This shows how many times each query was run.

  .. code-block:: text

    MOST POPULAR QUERIES

     121451 insert into foo values(?)
       1220 insert into abc values(1,2,?)
       1118 select x from foo
        909 select timesten(x) from foo where x=? order by x
        654 select * from foo
        426 update t1 set b=b||a||b
        146 begin
         88 create table foo(x,y)
         79 insert into foo values(1,2)
         76 rollback
         71 pragma locking_mode=exclusive
         71 insert into t1 values(2, 'abcdefghijklmnopqrstuvwxyz')
         71 insert into t1 values(1, 'abcdefghijklmnopqrstuvwxyz')
         71 insert into t1 select 4-a, b from t2
         71 insert into foo values(date('now'), date('now'))

This shows how many times a query was run and the sum of the
processing times in seconds.  The ``begin immediate`` query
illustrates how time spent busy waiting is included.

  .. code-block:: text

    LONGEST RUNNING - AGGREGATE

        413   94.305 select timesten(x) from foo where x=? order by x
     120637   12.941 select * from foo
         12    4.115 begin immediate
     121449    2.179 insert into foo values(?)
       1220    1.509 insert into abc values(1,2,?)
          3    1.380 create index foo_x on foo(x)
        426    0.715 update t1 set b=b||a||b
         38    0.420 insert into foo values(?,?)
         71    0.241 create table t1(a unique, b)
         88    0.206 create table foo(x,y)
         61    0.170 create table abc(a,b,c)
         27    0.165 insert into foo values(?,?,?)
          1    0.158 select row,x,snap(x) from foo
         80    0.150 insert into foo values(1,2)
         71    0.127 insert into foo values(date('now'), date('now'))

This shows the longest running queries with time in seconds.

  .. code-block:: text

    LONGEST RUNNING - INDIVIDUAL

      3.001 begin immediate
      1.377 create index foo_x on foo(x)
      1.102 begin immediate
      0.944 select timesten(x) from foo where x=? order by x
      0.893 select timesten(x) from foo where x=? order by x
      0.817 select timesten(x) from foo where x=? order by x
      0.816 select timesten(x) from foo where x=? order by x
      0.786 select timesten(x) from foo where x=? order by x
      0.783 select timesten(x) from foo where x=? order by x
      0.713 select timesten(x) from foo where x=? order by x
      0.701 select timesten(x) from foo where x=? order by x
      0.651 select timesten(x) from foo where x=? order by x
      0.646 select timesten(x) from foo where x=? order by x
      0.631 select timesten(x) from foo where x=? order by x
      0.620 select timesten(x) from foo where x=? order by x
