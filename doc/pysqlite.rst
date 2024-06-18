.. _pysqlitediffs:

sqlite3 module differences
**************************

.. currentmodule:: apsw

The `sqlite3 <https://docs.python.org/3/library/sqlite3.html>`__
standard module and APSW approached the problem of providing access
to SQLite from Python from fundamentally different directions.

APSW provides access in whatever way is normal for SQLite.  It makes
no effort to hide how SQLite is different from other databases.

sqlite3 tries to provide a DBAPI (:pep:`249`) compliant wrapper for
SQLite and in doing so needs to make it have the same behaviour as
other databases.  Consequently it does hide some of SQLite's nuances.

.. note::

   I suggest using APSW when you want to directly use SQLite and its
   functionality or are using your own code to deal with database
   independence rather than DBAPI.  Use :mod:`sqlite3` and DBAPI if
   your needs are simple, and you don't want to use SQLite specific
   features, and want to easily switch to other databases.


What APSW does better
=====================

APSW has the following enhancements/differences over the sqlite3
module:

* APSW stays up to date with SQLite.  As features are added and
  functionality changed in SQLite, APSW tracks them.

* APSW stays up to date with Python, including releases under
  development as well as older still supported releases.  The current
  APSW release brings the most recent SQLite to Python 3.8 all the way
  through Python 3.13.

* APSW gives all functionality of SQLite including :ref:`virtual
  tables <virtualtables>`, :ref:`VFS`, :ref:`BLOB I/O <blobio>`,
  :ref:`backups <backup>`, :meth:`logging <apsw.ext.log_sqlite>`,  and
  :meth:`file control <Connection.file_control>`.

* APSW includes :mod:`apsw.bestpractice` which configures SQLite
  for best performance, and catches common mistakes.

* You can use the same :class:`Connection` across threads with APSW
  without needing any additional level of locking.  sqlite3 `requires
  <https://docs.python.org/3/library/sqlite3.html?highlight=sqlite#sqlite3.threadsafety>`__
  that the :class:`Connection` and any :class:`cursors <Cursor>` are
  used in the same thread.

* APSW on :ref:`PyPI <pypi>` includes SQLite statically inside which
  does not interfere with any system SQLite, so you have the latest
  SQLite without disruptions.

* If you don't use PyPI, APSW :ref:`build instructions <building>`
  include extensive customisation options for SQLite.

* **Nothing** happens behind your back. By default sqlite3 tries to
  manage transactions (for DBAPI compliance) by processing your SQL for
  you, but you can turn it off. This can result in very unexpected
  behaviour with sqlite3.

* When using a :class:`Connection` as a :meth:`context manager
  <Connection.__enter__>` APSW uses SQLite's ability to have `nested
  transactions <https://sqlite.org/lang_savepoint.html>`__. sqlite3
  has a context manager, but does not implement nesting.

* You can use semi-colons at the end of commands and you can have
  multiple commands in the execute string in APSW. There are no
  restrictions on the type of commands used. For example this will
  work fine in APSW but is not allowed in sqlite3::

    import apsw
    con=apsw.Connection(":memory:")
    cur=con.cursor()
    for row in cur.execute("create table foo(x,y,z);insert into foo values (?,?,?);"
                           "insert into foo values(?,?,?);select * from foo;drop table foo;"
                           "create table bar(x,y);insert into bar values(?,?);"
                           "insert into bar values(?,?);select * from bar;",
                           (1,2,3,4,5,6,7,8,9,10)):
                               print (row)

  And the output as you would expect::

    (1, 2, 3)
    (4, 5, 6)
    (7, 8)
    (9, 10)

* :meth:`Cursor.executemany` also works with statements that return
  data such as selects, and you can have multiple statements.
  sqlite3's *executescript* method doesn't allow any form of
  data being returned (it silently ignores any returned data).

* APSW has better :ref:`execution and row tracing <tracing>`.

* :doc:`ext` includes:

  - Pretty printing query results
  - Forwarding SQLite log messages to the Python logger
  - Helper for implementing the complex xBestIndex method on your own
    virtual tables
  - A wrapper to turn Python functions into virtual tables, including
    taking positional and keyword arguments
  - Converting data types going into and out of SQlite
  - Detailed query information

* sqlite3 swallows exceptions in your callbacks making it far harder
  to debug problems. That also prevents you from raising exceptions in
  your callbacks to be handled in your code that called SQLite.
  sqlite3 does let you turn on `printing of tracebacks
  <https://docs.python.org/3/library/sqlite3.html?highlight=sqlite#sqlite3.enable_callback_tracebacks>`__
  but that is a poor substitute.

  APSW does the right thing as demonstrated by this example.  APSW
  converts Python errors into SQLite errors, so SQLite is aware errors
  happened.

  Source::

    def badfunc(t):
        return 1/0

    # sqlite3
    import sqlite3

    con = sqlite3.connect(":memory:")
    con.create_function("badfunc", 1, badfunc)
    cur = con.cursor()
    cur.execute("select badfunc(3)")

    # apsw
    import apsw
    con = apsw.Connection(":memory:")
    con.create_scalar_function("badfunc", badfunc, 1)
    cur = con.cursor()
    cur.execute("select badfunc(3)")

  Exceptions::

    # sqlite3

    Traceback (most recent call last):
      File "func.py", line 8, in ?
        cur.execute("select badfunc(3)")
    sqlite3.OperationalError: user-defined function raised exception

    # apsw

    Traceback (most recent call last):
      File "t.py", line 8, in ?
        cur.execute("select badfunc(3)")
      File "apsw.c", line 3660, in resetcursor
      File "apsw.c", line 1871, in user-defined-scalar-badfunc
      File "t.py", line 3, in badfunc
        return 1/0

* APSW has significantly enhanced debuggability. More details are
  available than just what is printed out when exceptions happen like
  above. See :ref:`augmented stack traces <augmentedstacktraces>`

* APSW has an :ref:`trace <apswtrace>` utility script that traces
  execution and results in your code without having to modify it in
  any way.  It also outputs summary reports making it easy to see what
  your most time consuming queries are, which are most popular etc.

* APSW has an exception corresponding to each SQLite error code and
  provides the extended error code.  sqlite3 combines several SQLite
  error codes into corresponding DBAPI exceptions.  This is a good
  example of the difference in approach of the two wrappers.

* The APSW test suite is larger and tests more functionality. Virtually every
  failure condition is tested including running out of memory, error
  returns etc. Code coverage by the test suite is 99.6%. sqlite3 is
  good at 81% for C code although there are several places that
  coverage can be improved. I haven't measured code coverage for
  sqlite3's Python code.  The consequences of this are that APSW
  catches issues earlier and gives far better diagnostics.  As an
  example try returning an unsupported type from a registered scalar
  function.

* APSW includes a :doc:`full featured shell <shell>`.  sqlite3 has a
  `simple one
  <https://docs.python.org/3/library/sqlite3.html#command-line-interface>`__
  available since Python 3.12.`

* APSW is faster than sqlite3 in my testing.  Try the :ref:`speedtest`
  benchmark.  All C code in APSW called by Python implements the
  fastcall mechanism, as does most of sqlite3.  All code called from C
  in APSW also uses fastcall, while sqlite3 uses the older mechanism
  that is `significantly slower
  <https://github.com/rogerbinns/python-fastcall>`__.

What sqlite3 does better
========================

* sqlite3 is part of the standard library, and is widely supported by
  libraries that abstract away the database layer.