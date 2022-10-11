.. currentmodule:: apsw.ext

Various interesting and useful bits of functionality
====================================================

You need to import `apsw.ext` to use this module. :mod:`dataclasses`
are used, and only Python 3.7+ is supported.

You can use these as is, or as the basis for your own needs.

Accessing result rows by column name
------------------------------------

By default :class:`cursors <apsw.Cursor>` return a :class:`tuple` of the
values for a row::

    con.execute("create table books(id, title, author, year")

    # tuple
    for row in con.execute("SELECT * from books"):
        # if the column order changes or the query is more complex
        # these can easily get out of sync
        title = row[1]
        author = row[2]


It can be more convenient to access them by name.  To achieve this you
can use :class:`apsw.ext.DataClassRowFactory` like this::

    con.setrowtrace(apsw.ext.DataClassRowFactory())

    for row in con.execute("SELECT * from books"):
        print(row.title, row.author)

See the :class:`API reference <apsw.ext.DataClassRowFactory>` for more
details on usage and configuration.

Converting types into and out of SQLite
---------------------------------------

SQLite only stores and returns 5 types:

* None
* int
* float
* str
* bytes

Sometimes it is handy to pretend it stores more types and have them
automagically converted.  Use :class:`TypesConverterCursorFactory` to
do this::

    t = apsw.ext.TypesConverterCursorFactory()
    connection.cursor_factory = t

To adapt Python types to SQLite types, you can inherit from :class:`SQLiteTypeAdapter`
and define `to_sqlite_value`::

    class Point(apsw.ext.SQLiteTypeAdapter):
        def __init__(self, x, y):
            self.x = x
            self.y = y

        def to_sqlite_value(self):
            # this is called to do the conversion
            return f"{ self.x };{ self.y }"

You can also register an adapter::

    def complex_to_sqlite_value(c):
        return f"{ c.real };{ c.imag }"

    t.register_adapter(complex, complex_to_sqlite_value)

To convert SQLite types back to Python types, you need to set the type
in SQLite when creating the table.

.. code-block:: SQL

    CREATE TABLE example(number INT, other COMPLEX);

Then register an adapter, giving the type from your SQL schema.  It must be an
exact match including case (`COMPLEX` in this example)::

    def sqlite_to_complex(v):
        return complex(*(float(part) for part in v.split(";")))

    t.register_converter("COMPLEX", sqlite_to_complex)

Detailed Query Information
--------------------------

SQLite can provide lots of information about queries.  The
:meth:`query_info <apsw.ext.query_info>` function can gather them up
for you.  This includes:

* **readonly** if the query makes no direct changes
* **first_query** if multiple queries are provided
* **actions** which databases, tables, columns, functions, views etc
  are referenced - see `actions
  <https://sqlite.org/c3ref/c_alter_table.html>`__
* **query_plan** which indices, tables scans etc are used to find the
  query results - see `query plans <https://sqlite.org/eqp.html>`__
* **explain** for the low level steps taken inside SQLite - see
  `SQLite bytecode <https://sqlite.org/opcode.html>`__


API Reference
-------------

.. automodule:: apsw.ext
    :members:
    :undoc-members:
    :member-order: bysource
    :special-members: __call__