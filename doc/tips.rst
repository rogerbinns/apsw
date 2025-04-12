Tips
****

.. currentmodule:: apsw

There are also specific tips in each documentation section, and many of the
classes, functions, and attributes.


SQLite is different
===================

While SQLite provides a SQL database like many others out there, it is
also unique in many ways.  Read about the unique features at the
`SQLite website <https://sqlite.org/different.html>`__ and `quirks
<https://www.sqlite.org/quirks.html>`__.

.. tip::

  Using :doc:`APSW best practice <bestpractice>` is recommended to get
  best performance and avoid common mistakes.

.. _types:

Types
=====

SQLite has `5 storage types
<https://www.sqlite.org/datatype3.html>`__.

.. list-table::
  :header-rows: 1
  :widths: auto

  * - SQLite
    - Python
  * - NULL
    - :data:`None`
  * - Text (limit 1GB when encoded as bytes)
    - :class:`str`
  * - Integer (Signed 64 bit)
    - :class:`int`
  * - Float (`IEEE754 64 bit <https://en.wikipedia.org/wiki/Double-precision_floating-point_format>`__)
    - :class:`float`
  * - `BLOB <https://en.wikipedia.org/wiki/Binary_large_object>`__ (binary data, limit 1GB)
    - :class:`bytes` and similar such as :class:`bytearray` and :class:`array.array`

:index:`Dates and times` do not have a dedicated storage type, but do
have a `variety of functions
<https://www.sqlite.org/lang_datefunc.html>`__ for creating,
manipulating, and storing them. :index:`JSON` does not have a
dedicated storage type, but does have a `variety of functions
<https://www.sqlite.org/json1.html>`__ for creating, manipulating, and
storing JSON.

APSW provides optional :ref:`type conversion <example_type_conversion>`, but
the underlying storage will always be one of the 5 storage types.

If a column declaration gives a type then SQLite
`attempts conversion <https://www.sqlite.org/flextypegood.html>`__.

.. code-block:: python

    connection.execute("""
        create table types1(a, b, c, d, e);
        create table types2(a INTEGER, b REAL, c TEXT, d, e BLOB);
        """)

    data = ("12", 3, 4, 5.5, b"\x03\x72\xf4\x00\x9e")
    connection.execute("insert into types1 values(?,?,?,?,?)", data)
    connection.execute("insert into types2 values(?,?,?,?,?)", data)

    for row in connection.execute("select * from types1"):
        print("types1", repr(row))

    for row in connection.execute("select * from types2"):
        print("types2", repr(row))

.. code-block:: output

  types1 ('12', 3, 4, 5.5, b'\x03r\xf4\x00\x9e')
  types2 (12, 3.0, '4', 5.5, b'\x03r\xf4\x00\x9e')

.. _pyobject:

.. index:: sqlite3_bind_pointer, sqlite3_result_pointer, sqlite3_value_pointer

Runtime Python objects
======================

While SQLite only :ref:`stores 5 types <types>`, it is possible to
pass Python objects into SQLite, operate on them with your
:meth:`functions <Connection.create_scalar_function>` (including
window, aggregates), and to return them in results.

This is done by wrapping the value in :meth:`apsw.pyobject` when
supplying it in a binding or function result.  See the :ref:`example
<example_pyobject>`.

It saves having to convert working objects into SQLite compatible ones
and back again. It is very useful if you work with numpy.  Any attempt
to save the objects to the database or provide them to SQLite provided
functions results in them being seen as ``null``.

Behind the scenes the `pointer passing interface
<https://sqlite.org/bindptr.html>`__ is used.


Transactions
============

Transactions are the changes applied to a database file as a whole.
They either happen completely, or not at all.  SQLite notes all the
changes made during a transaction, and at the end when you commit will
cause them to permanently end up in the database.  If you do not
commit, or just exit, then other/new connections will not see the
changes and SQLite handles tidying up the work in progress
automatically.

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

Queries
=======

SQLite only calculates each result row as you request it.  For example
if your query returns 10 million rows, SQLite will not calculate all 10
million up front.  Instead the next row will be calculated as you ask
for it.  You can use :meth:`Cursor.fetchall` to get all the results.

:class:`Cursors <Cursor>` on the same :ref:`Connection <connections>`
are not isolated from each other.  Anything done on one cursor is
immediately visible to all other cursors on the same connection.  This
still applies if you start transactions.  Connections are isolated
from each other.

:meth:`Connection.execute` and :meth:`Connection.executemany`
automatically obtain cursors from  :meth:`Connection.cursor` which
are very cheap.  It is best practise to not re-use them, and instead
get a new one each time.  If you don't, code refactoring and nested
loops can unintentionally use the same cursor object which will not
crash but will cause hard to diagnose behaviour in your program.

Bindings
========

When issuing a query, always use bindings.  `String interpolation
<https://docs.python.org/3/library/stdtypes.html#printf-style-string-formatting>`_
may seem more convenient but you will encounter difficulties.  You may
feel that you have complete control over all data accessed but if your
code is at all useful then you will find it being used more and more
widely.  The computer will always be better than you at parsing SQL
and the bad guys have years of experience finding and using `SQL
injection attacks <https://en.wikipedia.org/wiki/SQL_injection>`_ in
ways you never even thought possible.

The :ref:`tour <example_why_bindings>` shows why you use bindings, and
the different ways you can supply them.

Query Patterns
==============

These are suggestions on how to structure your Python code for processing
queries.

Zero or more rows expected
--------------------------

Use a for loop.  Note that nothing enforces the Python variables match
the columns inside the SQL.

.. code-block::

  for name, quantity, status in db.execute("SELECT name, quantity, status FROM ..."):
    # do something with the row
    ...

You can use :class:`apsw.ext.DataClassRowFactory` to get the row as
:mod:`dataclasses`.  It is **strongly** recommended that you provide
the SQL level names using ``AS`` since there is no guarantee what the
names will be otherwise.

.. code-block::

  import apsw.ext

  # This affects all queries on db, but not get.  It can be set on
  # a cursor to only affect that cursor.
  db.row_trace = apsw.ext.DataClassRowFactory()

  for row in db.execute("SELECT cat.name AS name, orders.quantity AS quantity FROM ..."):
    # You can access row names
    print(f"{row.name=} {row.quantity=}")
    # You will get an Exception with a wrong name
    print(row.status)

One value expected
------------------

Use :meth:`get <Cursor.get>` which will return the value or
:class:`None` if there was no match.

.. code-block::

    name = db.execute("SELECT name FROM ... WHERE id=?", (item_id,)).get

One row expected
----------------

:meth:`get <Cursor.get>` can be used. There will be an exception if no
row was found because :class:`None` can't be unpacked into the
variables.

.. code-block::

  name, status = db.execute("SELECT name, status FROM ... WHERE id=?", (item_id,)).get

`match
<https://docs.python.org/3/reference/compound_stmts.html#the-match-statement>`__.
can handle :class:`None` when the row was not found.

.. code-block::

  match db.execute("SELECT name, status FROM ... WHERE id=?", (item_id,)).get:
    case None:
      # handle missing
      raise NotFound(...)
    case name, status:
      # do something
      ...

.. _diagnostics_tips:

Diagnostics
===========

Both SQLite and APSW provide detailed diagnostic information.  Errors
will be signalled via an :doc:`exception <exceptions>`.

APSW ensures you have :ref:`detailed information
<augmentedstacktraces>` both in the stack trace as well as what data
APSW/SQLite was operating on.

SQLite has a `warning/error logging facility
<https://www.sqlite.org/errlog.html>`__.  Use :doc:`best practice <bestpractice>` to
forward SQLite log messages to Python's :mod:`logging`.

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
          CREATE TABLE foo(x,y,z);
          CREATE TABLE bar(x,y,z);
          PRAGMA user_version = 1;""")

    if db.pragma("user_version") == 1:
      with db:
        db.execute("""
        CREATE TABLE baz(x,y,z);
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

.. _busyhandling:

Busy handling
=============

SQLite uses locks to coordinate access to the database by multiple
connections (within the same process or in a different process).  The
general goal is to have the locks be as lax as possible (allowing
concurrency) and when using more restrictive locks to keep them for as
short a time as possible.  See the `SQLite documentation
<https://sqlite.org/lockingv3.html>`__ for more details.

By default you will get an immediate :exc:`BusyError` if a lock cannot
be acquired. Use :doc:`best practice <bestpractice>` which sets a
short waiting period, as well as enabling `WAL
<https://www.sqlite.org/wal.html>`__ which reduces contention between
readers and writers.

Database schema
===============

When starting a new database, it can be quite difficult to decide what
tables and column to have and how to link them.  The technique used to
design SQL schemas is called `normalization
<https://en.wikipedia.org/wiki/Database_normalization>`_.  The page
also shows common pitfalls if you do not normalize your schema.

.. _wal:

Write Ahead Logging
===================

SQLite has `write ahead logging
<https://sqlite.org/wal.html>`__ which has several benefits, but
also some drawbacks as the page documents.  WAL mode is off by
default. Use :doc:`best practice <bestpractice>` to automatically
enable it for all connections.

Note that if wal mode can't be set (eg the database is in memory or
temporary) then the attempt to set wal mode will be ignored.  It is
also harmless to call functions like
:meth:`Connection.wal_autocheckpoint` on connections that are not in
wal mode.

If you write your own :doc:`VFS <vfs>`, then inheriting from an
existing VFS that supports WAL will make your VFS support the extra
WAL methods too.

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

.. index::
  single: URI
  single: SQLITE_CONFIG_URI

URI names
=========

SQLite allows `URI filenames <https://www.sqlite.org/uri.html>`__
where you can provide additional parameters at the time of open for a
database.  Opens can include the `SQLITE_OPEN_URI
<https://www.sqlite.org/c3ref/open.html>`__ flag, which will
also apply to ``ATTACH`` on that connection.

You should use :mod:`urllib.parse` to correctly create strings handling
the necessary special characters and quoting.

.. code-block::

  import urllib.parse

  uri_filename = urllib.parse.quote("my db filename.sqlite3")

  uri_parameters = urllib.parse.urlencode(
    {
        "vfs": "memdb",
        "go": "fast",
        "level": 42,
    }
  )

  uri = f"file:{uri_filename}?{uri_parameters}"

.. index::
  single: Memory database
  single:  memdb

.. _memdb:

Memory databases
================

You can get an `in-memory only database
<https://sqlite.org/inmemorydb.html>`__ by using a filename of
``:memory:`` and a temporary disk backed database with a name of an
empty string.  (Note :meth:`shared cache
<apsw.enable_shared_cache>` won't work,)

SQLite has a (currently undocumented) VFS that allows the same
connection to have multiple distinct memory databases, and for
separate connections to share a memory database.

Use the name ``memdb`` as the VFS.  If the filename provided starts
with a ``/`` then it is shared amongst connections, otherwise it is
private to the connection.

.. code-block::

    # normal opens
    connection = apsw.Connection("/shared", vfs="memdb")
    connection = apsw.Connection("not-shared", vfs="memdb")

    # using URI
    connection = apsw.Connection("file:/shared?vfs=memdb",
                    flags=apsw.SQLITE_OPEN_URI | apsw.SQLITE_OPEN_READWRITE)
    connection = apsw.Connection("file:not-shared?vfs=memdb",
                    flags=apsw.SQLITE_OPEN_URI | apsw.SQLITE_OPEN_READWRITE)