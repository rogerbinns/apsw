APSW documentation
==================

.. centered:: APSW |version| released |today|

Use with SQLite 3.37 or later, CPython (2.3 and later, 3.1 and later).

This is the last release supporting Python 2 and Python 3 before 3.7.
If you still use those Python versions then you should pin to this
APSW version.  (`More information
<https://www.rogerbinns.com/blog/apsw-ending-python2early3.html>`__).

APSW provides an SQLite 3 wrapper that provides the thinnest layer
over the `SQLite <https://sqlite.org>`_ database library possible.
Everything you can do from the `SQLite C API
<https://sqlite.org/c3ref/intro.html>`_, you can do from Python.
Although APSW looks vaguely similar to the :pep:`249` (DBAPI), it is
:ref:`not compliant <dbapinotes>` with that API because instead it
works the way SQLite 3 does. (`pysqlite
<https://github.com/ghaering/pysqlite>`_ which became the builtin
sqlite3 module is DBAPI compliant - see the :ref:`differences
<pysqlitediffs>`).

In general you should use Python's builtin sqlite3 module.  Use APSW
when you are intentionally using SQLite, want to use its APIs, want to
control what :ref:`versions <version_stuff>` are used, or want to
control SQLite's configuration (primarily done at `compile time
<https://www.sqlite.org/compile.html>`__) or extensions (like `JSON
<https://www.sqlite.org/json1.html>`__ or `FTS
<https://www.sqlite.org/fts5.html>`__)

APSW is hosted at https://github.com/rogerbinns/apsw

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
