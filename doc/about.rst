About
=====

**APSW** |version| **released** |today|

Use with `SQLite <https://sqlite.org/>`__ 3.47 or later, `Python
<https://www.python.org/downloads/>`__ 3.9 and later.


What APSW does
--------------

APSW lets you get the most out of the `SQLite <https://sqlite.org/>`__
embedded relational database engine from Python, and the most out of
Python from SQLite.  APSW glues together the complete `SQLite C API
<https://sqlite.org/c3ref/intro.html>`__ and `Python's C API
<https://docs.python.org/3/c-api/index.html>`__, staying up to date
with both SQLite and Python.

It is recommended to use the builtin :mod:`sqlite3 <sqlite3>` module
if you want SQLite to appear interchangeable with the other database
drivers.

Use APSW when you want to use SQLite fully, and have an improved
developer experience.  See more about the :doc:`differences between
APSW and sqlite3 <pysqlite>`.

Dependencies
------------

APSW has no dependencies other than Python itself, and SQLite which
you can provide or have APSW fetch and include statically in the
extension.

Hosting
-------

APSW is hosted at https://github.com/rogerbinns/apsw  with `source
releases at Github <https://github.com/rogerbinns/apsw/releases>`__
and :code:`pip install apsw` at `PyPI
<https://pypi.org/project/apsw/>`__.  See the :doc:`installation
details <install>`.

Mailing lists/contacts
----------------------

* `Python SQLite discussion group <https://groups.google.com/group/python-sqlite>`__
  (preferred)
* `Github discussions <https://github.com/rogerbinns/apsw/discussions>`__
* You can also email the author at `rogerb@rogerbinns.com
  <mailto:rogerb@rogerbinns.com>`__

Issue tracking
--------------

You can find existing and fixed bugs by clicking on `Issues
<https://github.com/rogerbinns/apsw/issues>`__ and using "New Issue"
to report previously unknown issues.

.. _backcompat:

Backwards compatibility
-----------------------

Great effort is expended to ensure your code using APSW continues to
work without maintenance as Python, SQLite, and APSW update over time.
The very rare breaks are documented in the :doc:`change log
<changes>`.  20 year old code still works unchanged!

APSW and SQLite versions
------------------------

SQLite has approximately quarterly releases.  These include tweaks,
bug fixes, and new functionality based on the billions of SQLite
databases in use, and the many programs that use SQLite (eg almost
every browser, mail client, photo library, mobile and desktop OS).
Despite these changes, SQLite retains backwards and forwards
compatibility with the `file format
<https://www.sqlite.org/onefile.html>`__ and APIs.

APSW wraps the `SQLite C API
<https://www.sqlite.org/c3ref/intro.html>`__.  That means when SQLite
adds new constant or API, then so does APSW.  You can think of APSW as
the Python expression of SQLite's C API.  You can `lookup
<genindex.html#S>`__ SQLite APIs to find which APSW functions and
attributes call them.

Consequently the APSW version mirrors the SQLite version, with an
additional final number on the end to reflect APSW iterations.  For
example the SQLite :code:`3.42.1` release would have APSW
:code:`3.42.1.0` as the corresponding release with the final
:code:`.0` incrementing if there are more APSW releases for the same
SQLite version.

You can use APSW with the corresponding version of SQLite, or any
newer version of SQLite.  You could use the original 2004 release of
APSW with today's SQLite just fine, although it wouldn't know about
the new APIs and constants.

Python versions
---------------

APSW supports `all supported Python versions
<https://devguide.python.org/versions/>`__, including versions under
development.  Once a Python release goes end of life, there will be
one final APSW release supporting that version of Python.

.. list-table:: Last APSW release
  :header-rows: 1
  :widths: auto

  * - Python version
    - APSW release
  * - 3.8
    - `3.46.1.0 <https://github.com/rogerbinns/apsw/releases/tag/3.46.1.0>`__
  * - 3.6, 3.7
    - `3.43.0.0 <https://github.com/rogerbinns/apsw/releases/tag/3.43.0.0>`__
  * - 2.3 - 3.5
    - `3.37.0-r1 <https://github.com/rogerbinns/apsw/releases/tag/3.37.0-r1>`__