.. currentmodule:: apsw

Various interesting and useful bits of functionality
====================================================

You need to import `apsw.ext` to use this module. :mod:`dataclasses`
are used, available in the standard library (Python 3.7+) or via `PyPi
<https://pypi.org/project/dataclasses/>`__ for Python 3.6.

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
    :exclude-members: iskeyword