APSW documentation
==================

.. centered:: APSW |version| released |today|

Use with SQLite 3.39 or later, CPython 3.6 and later:

  Version `3.37.0-r1 <https://github.com/rogerbinns/apsw/releases/tag/3.37.0-r1>`__
  from January 2022 supports all CPython versions back to 2.3.  The
  `tips <https://rogerbinns.github.io/apsw/tips.html>`__ include more
  information about APSW and SQLite versions.

APSW provides an SQLite 3 wrapper that provides the thinnest layer
over the `SQLite <https://sqlite.org>`_ database library possible.
Everything you can do from the `SQLite C API
<https://sqlite.org/c3ref/intro.html>`_, you can do from Python.
Although APSW looks vaguely similar to the :pep:`249` (DBAPI), it is
:ref:`not compliant <dbapinotes>` with that API because instead it
works the way SQLite 3 does. (Read more about the :ref:`differences
<pysqlitediffs>`).

In general you should use Python's `builtin sqlite3 module
<https://docs.python.org/3/library/sqlite3.html>`__.  Use APSW when
you are intentionally using SQLite, want to use its APIs, want to
control what :ref:`versions <version_stuff>` are used, or want to
control SQLite's configuration (primarily done at `compile time
<https://www.sqlite.org/compile.html>`__) or extensions (like `JSON
<https://www.sqlite.org/json1.html>`__ or `FTS
<https://www.sqlite.org/fts5.html>`__)

APSW is hosted at https://github.com/rogerbinns/apsw and can be
:doc:`downloaded <download>` from PyPI

Contents:

.. toctree::
   :maxdepth: 2

   tips
   example
   download
   build
   extensions

   apsw
   connection
   cursor
   blob
   backup
   vtable
   vfs
   shell

   exceptions
   types
   execution
   dbapi
   pysqlite
   benchmarking
   copyright
   changes

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
