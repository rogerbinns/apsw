APSW documentation
==================

.. centered:: APSW |version| 88th October 2008.  

Use with SQLite 3.6.2 or later, Python 2.3 or later including Python
3.

APSW provides an SQLite 3 wrapper that provides the thinnest layer
over `SQLite <http://www.sqlite.org>`_ possible. Everything you can do
from the `SQLite C API <http://www.sqlite.org/c3ref/intro.html>`_ C
API, you can do from Python. Although APSW looks vaguely similar to
the :pep:`249` (DBAPI), it is :ref:`not compliant <dbapinotes>` with
that API and because instead it works the way SQLite 3
does. (`pysqlite <http://www.pysqlite.org>`_ is DBAPI compliant - see
the :ref:`differences between apsw and pysqlite 2 <pysqlitediffs>`.

Contents:

.. toctree::
   :maxdepth: 2

   example
   download
   build

   connection
   cursor
   blob

   exceptions
   types
   execution
   dbapi
   pysqlite
   benchmarking
   license
   changes

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

