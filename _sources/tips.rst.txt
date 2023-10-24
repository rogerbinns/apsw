Tips
****

.. currentmodule:: apsw

These tips are based on mailing list postings, issues, and emails.
You are recommended to read all the documentation as well.


SQLite is different
===================

While SQLite provides a SQL database like many others out there, it is
also unique in many ways.  Read about the unique features at the
`SQLite website <https://sqlite.org/different.html>`__ and `quirks
<https://www.sqlite.org/quirks.html>`__.

.. tip::

  :doc:`Best practice <bestpractice>` is recommended.

Transactions
============

Transactions are the changes applied to a database file as a whole.
They either happen completely, or not at all.  SQLite notes all the changes
made during a transaction, and at the end when you do a commit will cause
them to permanently end up in the database.  If you do not commit, or
just exit, then other/new connections will not see the changes and SQLite
handles tidying up the work in progress automatically.

Committing a transaction can be quite time consuming.  SQLite uses a robust
multi-step process that has to handle errors that can occur at any point,
and asks the operating system to ensure that data is on storage and would
survive a power cycle.  This will `limit the rate at which you can do
transactions <https://www.sqlite.org/faq.html#q19>`__.

If you do nothing, then each statement is a single transaction::

   # this will be 3 separate transactions
   db.execute("INSERT ...")
   db.execute("INSERT ...")
   db.execute("INSERT ...")

You can use BEGIN/COMMIT to set the transaction boundary::

   # this will be one transaction
   db.execute("BEGIN")
   db.execute("INSERT ...")
   db.execute("INSERT ...")
   db.execute("INSERT ...")
   db.execute("COMMIT")

However that is extra effort, and also requires error handling.  For example
if the second INSERT failed then you likely want to ROLLBACK the incomplete
transaction, so that additional work on the same connection doesn't see the
partial data.

If you use :meth:`with Connection <Connection.__enter__>` then the transaction
will be automatically started, and committed on success or rolled back if
exceptions occur::

   # this will be one transaction with automatic commit and rollback
   with db:
       db.execute("INSERT ...")
       db.execute("INSERT ...")
       db.execute("INSERT ...")

There are `technical details <https://www.sqlite.org/lang_transaction.html>`__
at the `SQLite site <https://www.sqlite.org/docs.html>`__.

Cursors
=======

SQLite only calculates each result row as you request it.  For example
if your query returns 10 million rows SQLite will not calculate all 10
million up front.  Instead the next row will be calculated as you ask
for it.

Cursors on the same :ref:`Connection <connections>` are not isolated
from each other.  Anything done on one cursor is immediately visible
to all other Cursors on the same connection.  This still applies if
you start transactions.  Connections are isolated from each other.

:meth:`Connection.execute` and :meth:`Connection.executemany`
automatically obtains cursors from  :meth:`Connection.cursor` which
are very cheap.  It is best practise to not re-use them, and instead
get a new one each time.  If you don't, code refactoring and nested
loops can unintentionally use the same cursor object which will not
crash but will cause hard to diagnose behaviour in your program.

Read more about :ref:`Cursors <cursors>`.

Bindings
========

When issuing a query, always use bindings.  `String interpolation
<https://docs.python.org/library/stdtypes.html#printf-style-string-formatting>`_
may seem more convenient but you will encounter difficulties.  You may
feel that you have complete control over all data accessed but if your
code is at all useful then you will find it being used more and more
widely.  The computer will always be better than you at parsing SQL
and the bad guys have years of experience finding and using `SQL
injection attacks <https://en.wikipedia.org/wiki/SQL_injection>`_ in
ways you never even thought possible.

The :ref:`documentation <cursors>` gives many examples of how to use
various forms of bindings.

.. _diagnostics_tips:

Diagnostics
===========

Both SQLite and APSW provide detailed diagnostic information.  Errors
will be signalled via an :doc:`exception <exceptions>`.

APSW ensures you have :ref:`detailed information
<augmentedstacktraces>` both in the stack trace as well as what data
APSW/SQLite was operating on.

SQLite has a `warning/error logging facility
<https://www.sqlite.org/errlog.html>`__.  You can call
:meth:`apsw.ext.log_sqlite` which installs a handler that forwards
SQLite messages to the :mod:`logging module <logging>`.`

To do it yourself::

    def handler(errcode, message):
        errstr=apsw.mapping_result_codes[errcode & 255]
        print (f"SQLITE_LOG: { message } ({ errcode }) { errstr } "
               + apsw.mapping_extended_result_codes.get(errcode, ""))

    apsw.config(apsw.SQLITE_CONFIG_LOG, handler)

This is an example of what gets printed when I use ``/dev/null`` as
the database name in the :class:`Connection` and then tried to create
a table.

.. code-block:: output

    SQLITE_LOG: cannot open file at line 28729 of [7dd4968f23] (14) SQLITE_CANTOPEN
    SQLITE_LOG: os_unix.c:28729: (2) open(/dev/null-journal) - No such file or directory (14) SQLITE_CANTOPEN
    SQLITE_LOG: statement aborts at 38: [create table foo(x,y);] unable to open database file (14) SQLITE_CANTOPEN

Managing and updating your schema
=================================

If your program uses SQLite for `data
<https://sqlite.org/appfileformat.html>`__ then you'll need to manage
and update your schema.  The hard way of doing this is to test for the
existence of tables and their columns, and doing that maintenance
programmatically.  The easy way is to use `pragma user_version
<https://sqlite.org/pragma.html#pragma_user_version>`__ as in this example::

  def ensure_schema(db):
    # a new database starts at user_version 0
    if db.pragma("user_version") == 0:
      with db:
        db.execute("""
          CREATE TABLE IF NOT EXISTS foo(x,y,z);
          CREATE TABLE IF NOT EXISTS bar(x,y,z);
          PRAGMA user_version = 1;""")

    if db.pragma("user_version") == 1:
      with db:
        db.execute("""
        CREATE TABLE IF NOT EXISTS baz(x,y,z);
        CREATE INDEX ....
        PRAGMA user_version = 2;""")

    if db.pragma("user_version") == 2:
      with db:
        db.execute("""
        ALTER TABLE .....
        PRAGMA user_version = 3;""")

This approach will automatically upgrade the schema as you expect.
You can also use `pragma application_id
<https://sqlite.org/pragma.html#pragma_application_id>`__ to mark the
database as made by your application.

Parsing SQL
===========

Sometimes you want to know what a particular SQL statement does.  Use
:func:`apsw.ext.query_info` which will provide as much detail as you
need.

.. _customizing_connection_cursor:

Customizing Connections
=======================

:attr:`apsw.connection_hooks` is a list of callbacks for when
each :class:`Connection` is created.  They are called in turn, with
the new connection as the only parameter.

For example if you wanted to add an `executescript` method to
Connections that is like :meth:`Connection.execute` but ignores all
returned rows::

  def executescript(self, sql, bindings=None):
    for _ in self.execute(sql, bindings):
      pass

  def my_hook(connection):
    connection.executescript = executescript

  apsw.connection_hooks.append(my_hook)

Customizing Cursors
===================

You can customize the behaviour of cursors.  An example would be
wanting a :ref:`rowcount <rowcount>` or batching returned rows.
(These don't make any sense with SQLite but the desire may be to make
the code source compatible with other database drivers).

Set :attr:`Connection.cursor_factory` to any callable, which will be
called with the connection as the only parameter, and return the
object to use as a cursor.

For example instead of returning rows as tuples, we can return them as
dictionaries using a :ref:`row tracer <rowtracer>` with
:meth:`Cursor.get_description`::

  def dict_row(cursor, row):
    return {k[0]: row[i] for i, k in enumerate(cursor.get_description())}

  def my_factory(connection):
    cursor = apsw.Cursor(connection)
    cursor.row_trace = dict_row
    return cursor

  connection.cursor_factory = my_factory


.. _busyhandling:

Busy handling
=============

SQLite uses locks to coordinate access to the database by multiple
connections (within the same process or in a different process).  The
general goal is to have the locks be as lax as possible (allowing
concurrency) and when using more restrictive locks to keep them for as
short a time as possible.  See the `SQLite documentation
<https://sqlite.org/lockingv3.html>`__ for more details.

By default you will get a :exc:`BusyError` if a lock cannot be
acquired.  You can set a :meth:`timeout <Connection.set_busy_timeout>`
which will keep retrying or a :meth:`callback
<Connection.set_busy_handler>` where you decide what to do.

Database schema
===============

When starting a new database, it can be quite difficult to decide what
tables and fields to have and how to link them.  The technique used to
design SQL schemas is called `normalization
<https://en.wikipedia.org/wiki/Database_normalization>`_.  The page
also shows common pitfalls if you don't normalize your schema.

.. _wal:

Write Ahead Logging
===================

SQLite 3.7 introduced `write ahead logging
<https://sqlite.org/wal.html>`__ which has several benefits, but
also some drawbacks as the page documents.  WAL mode is off by
default.  In addition to turning it on manually for each database, you
can also turn it on for all opened databases by using
:attr:`connection_hooks`::

  def setwal(db):
      db.pragma("journal_mode", "wal")
      # custom auto checkpoint interval (use zero to disable)
      db.wal_autocheckpoint(10)

  apsw.connection_hooks.append(setwal)

Note that if wal mode can't be set (eg the database is in memory or
temporary) then the attempt to set wal mode will be ignored.  The
pragma will return the mode in effect.  It is also harmless to call
functions like :meth:`Connection.wal_autocheckpoint` on connections
that are not in wal mode.

If you write your own VFS, then inheriting from an existing VFS that
supports WAL will make your VFS support the extra WAL methods too.
(Your VFS will point directly to the base methods - there is no
indirect call via Python.)

.. _sharedcache:

Shared Cache Mode
=================

SQLite supports a `shared cache mode
<https://sqlite.org/sharedcache.html>`__ where multiple connections to
the same database can share a cache instead of having their own.
SQLite recommend that `you do not use this mode
<https://sqlite.org/sharedcache.html#use_of_shared_cache_is_discouraged>`__.

If you do use it, be aware that :ref:`busy handling <busyhandling>` is
very different, and that you are unlikely to save any memory or I/O
compared to what Python programs usually do.
