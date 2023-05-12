About
=====

**APSW** |version| **released** |today|

Version Compatibility
---------------------

Use with `SQLite <https://sqlite.org/>`__ 3.41 or later, `Python
<https://www.python.org/downloads/>`__ 3.6 and later.

Dependencies
------------

APSW has no dependencies other than Python itself.

What it does
------------

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

Hosting
-------

APSW is hosted at https://github.com/rogerbinns/apsw  with `source
releases at Github <https://github.com/rogerbinns/apsw/releases>`__.
See the :doc:`installation instructions <install>`.

Mailing lists/contacts
----------------------

* `Python SQLite discussion group <http://groups.google.com/group/python-sqlite>`__
  (preferred)
* `Github discussions <https://github.com/rogerbinns/apsw/discussions>`__
* You can also email the author at `rogerb@rogerbinns.com
  <mailto:rogerb@rogerbinns.com>`__

Issue tracking
--------------

You can find existing and fixed bugs by clicking on `Issues
<https://github.com/rogerbinns/apsw/issues>`__ and using "New Issue"
to report previously unknown issues.
