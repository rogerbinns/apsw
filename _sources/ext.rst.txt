.. currentmodule:: apsw.ext

Various interesting and useful bits of functionality
====================================================

You need to import `apsw.ext` to use this module.

Pretty printing
---------------

:meth:`format_query_table` makes nicely formatted query output - see
the :ref:`the example <example_format_query>`.

Logging and tracebacks
----------------------

You can use :meth:`log_sqlite` to forward SQLite log messages
to the :mod:`logging` module.

:meth:`print_augmented_traceback` prints an exception the usual way
but also includes local variables, which :ref:`APSW includes
<augmentedstacktraces>` to make debugging quicker and easier.

Virtual Tables
--------------

Use :meth:`index_info_to_dict` to get :class:`apsw.IndexInfo`
in an easier to print and work with format.

Use :meth:`make_virtual_module` to easily turn a Python function
into a virtual table source.

:meth:`generate_series` and :meth:`generate_series_sqlite` provide
`generate_series <https://sqlite.org/series.html>`__.

Database storage usage
----------------------

Use :func:`analyze_pages` to find out how much storage is in use, and
how fragmented it is.  See `example output
<_static/samples/analyze_pages.txt>`__.  It is also available
graphically with :func:`page_usage_to_svg` - `example output
<_static/samples/chinook.svg>`__.

Accessing result rows by column name
------------------------------------

See :ref:`the example <example_colnames>`.

Use :class:`apsw.ext.DataClassRowFactory` as a
:attr:`apsw.Connection.row_trace` for an entire connection, or
:attr:`apsw.Cursor.row_trace` for a specific cursor.

.. _typeconversion:

Converting types into and out of SQLite
---------------------------------------

SQLite only stores and returns 5 types:

* None
* int
* float
* str
* bytes

Use :class:`TypesConverterCursorFactory` as
:attr:`apsw.Connection.cursor_factory` to adapt values going into
SQLite, and convert them coming out.  See :ref:`the example
<example_type_conversion>`.

To convert values going into SQLite, do either of:

* Inherit from :class:`apsw.ext.SQLiteTypeAdapter` and define a
  *to_sqlite_value* method on the class

* Call :meth:`TypesConverterCursorFactory.register_adapter` with the
  type and a adapter function

To adapt values coming out of SQLite:

* Call :meth:`TypesConverterCursorFactory.register_converter` with the
  exact type string in the table and a converter function

Detailed Query Information
--------------------------

SQLite can provide lots of information about queries.  The
:meth:`~apsw.ext.query_info` function can gather them up
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

See :ref:`the example <example_query_details>`.

API Reference
-------------

.. automodule:: apsw.ext
    :synopsis: Various interesting and useful bits of functionality
    :members:
    :undoc-members:
    :member-order: bysource
    :special-members: __call__