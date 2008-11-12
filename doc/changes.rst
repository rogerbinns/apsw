Change History
**************
.. currentmodule:: apsw

3.6.5-r1
========

The distribution now includes a :ref:`speedtest` script.  You can use
this to see how APSW performs relative to pysqlite, or to track
performance differences between SQLite versions.  The underlying
queries are derived from `SQLite's speed test
<http://www.sqlite.org/cvstrac/fileview?f=sqlite/tool/mkspeedsql.tcl>`_

The statement cache was completely rewritten.  It uses less memory and
scales significantly better.

It was possible to get a deadlock between the Python GIL and the
SQLite database mutex when using the same :class:`Connection` across
multiple threads.  Fixed by releasing the GIL in more places and added
test that inspects the source to verify GIL/mutex handling.  Thanks to
amicitas reporting this as `issue 31
<http://code.google.com/p/apsw/issues/detail?id=31>`_.

SQLite's API has been extended in 3.6.5 so that errors can be
retrieved in a thread safe manner.  APSW now uses this API.

As a consequence of the prior two changes it is now possible and safe
to use the same :class:`Connection` across as many threads as you want
`concurrently <http://sqlite.org/threadsafe.html>`_.

Documentation is now done using `Sphinx <http://sphinx.pocoo.org>`_
which was adopted by Python 3.  This has allowed for richer
documentation and more output formats such as PDF and `Windows CHM
<http://en.wikipedia.org/wiki/Microsoft_Compiled_HTML_Help>`_ format.

The binary distribution for Windows includes the `full text search
<http://www.sqlite.org/cvstrac/wiki?p=FtsUsage>`_ (FTS) and `Rtree
<http://www.sqlite.org/cvstrac/fileview?f=sqlite/ext/rtree/README>`_
extensions.  See also :ref:`setup_py_flags`.

The source structure and files were reorganized to make it clearer
where things are implemented and to make automatic extraction of
documentation easier.

3.6.3-r1
========

You can now write your own :ref:`VFS` in Python. You can also inherit
from an existing VFS making it easy to augment or override small bits
of behaviour without having to code everything else. See the
:ref:`example <example-vfs>` where database files are obfuscated by
XORing their contents.

:file:`setup.py` now takes an optional :option:`--fetch-sqlite[=ver]`
argument to automatically download and use the latest SQLite
amalgamation (or a specified version). On non-Windows platforms it
will also work out what compile flags SQLite needs (for example
:const:`HAVE_USLEEP`, :const:`HAVE_LOCALTIME_R`). Several other
options to :file:`setup.py` are also available to control
enabling/omitting certains features and functionality. See
:ref:`building <Building>` for further details.

APSW checks that SQLite was compiled to be `threadsafe <http://sqlite.org/c3ref/threadsafe.html>`_

Added new constants:

* :const:`SQLITE_IOERR_ACCESS`, :const:`SQLITE_IOERR_CHECKRESERVEDLOCK` and :const:`SQLITE_IOERR_LOCK` extended result codes
* :const:`SQLITE_OPEN_NOMUTEX` and :const:`SQLITE_OPEN_FULLMUTEX` open flags
* Several new :const:`SQLITE_CONFIG` and :const:`SQLITE_STATUS` codes

Wrapped several new SQLite apis:

* `sqlite3_config <http://sqlite.org/c3ref/config.html>`_
* `sqlite3_initialize/sqlite3_shutdown <http://sqlite.org/c3ref/initialize.html>`_
* `sqlite3_memory_used/sqlite3_memory_highwater <http://sqlite.org/c3ref/memory_highwater.html>`_
* `sqlite3_status <http://sqlite.org/c3ref/status.html>`_
* `sqlite3_soft_heap_limit <http://sqlite.org/c3ref/soft_heap_limit.html>`_
* `sqlite3_release_memory <http://sqlite.org/c3ref/release_memory.html>`_
* `sqlite3_randomness <http://sqlite.org/c3ref/randomness.html>`_


The following experimental apis are not wrapped as there is nothing
useful you can do with them (yet):

* `sqlite3_db_config <http://www.sqlite.org/c3ref/db_config.html>`_
* `sqlite3_db_status <http://www.sqlite.org/c3ref/db_status.html>`_

Restored prior behaviour regarding Python ints and longs returning int
for numbers fitting in signed 32 bit. This only affects Python 2 as
Python 3 uses long exclusively. Thanks to Joe Pham for reporting this
as `issue 24 <http://code.google.com/p/apsw/issues/detail?id=24>`_.

Added :meth:`Connection.getsqlite3pointer` method to help with 
`issue 26 <http://code.google.com/p/apsw/issues/detail?id=26>`_.

3.5.9-r2
========

APSW now works with Python 3 (you need 3.0b1 or later).

(`Issue 17 <http://code.google.com/p/apsw/issues/detail?id=17>`_)
Removed the :const:`SQLITE_MAX_*` constants since they could be
unreliable (eg APSW can't tell what a shared library was compiled
with). A workaround is documented in :func:`Connection.limit`.

3.5.9-r1
========

APSW is now hosted at http://code.google.com/p/apsw

You can use this with SQLite 3.5.9 onwards.

SQLite now provides the source all `amalgamated
<http://www.sqlite.org/cvstrac/wiki?p=TheAmalgamation>`_ into one file
which improves performance and makes compilation and linking of SQLite
far easier. The build instructions are updated.

:const:`SQLITE_COPY` authorizer code and :const:`SQLITE_PROTOCOL`
error code are no longer used by SQLite, but the values are left in
apsw for backwards compatibility

:const:`SQLITE_IOERR_DELETE`, :const:`SQLITE_IOERR_BLOCKED` and :const:`SQLITE_IOERR_NOMEM`

:func:`Connection.interrupt` can be called from any thread

SQLite has implementation limits on string and blob lengths (roughly
constrained to fitting within a signed 32 bit integer - less than 2GB)
which weren't checked. Using a 64 bit Python 2.5+ (as I do) it would
have been possible to destroy memory and crash the
program. Consequently APSW has length checks to ensure it doesn't
happen.  SQLite now has further `limits checking
<http://www.sqlite.org/limits.html>`_ which cover other things as well
such as maximum number of virtual machine opcodes, maximum number of
variables etc. These are very useful if you are taking in SQL
statements from elsewhere. Call :func:`Connection.limit`

A rename method was added for virtual tables.

SQLite 3.5 removed the requirement that all operations on a connection
be done in the same thread. Consequently all code that enforced the
restriction has been removed from APSW.

You no longer have to call :func:`Connection.close`. This was
previously a requirement to ensure that the correct thread was used
(destructors otherwise run in any thread). It is however still a good
idea to do so since you can catch exceptions when close is called
but not if you let the destructor do the closing.

SQLite now has incremental :ref:`blob I/O <blobio>`

`Issue 4 <http://code.google.com/p/apsw/issues/detail?id=4>`_ which
could lead to generic error messages was fixed in SQLite 3.5.9.

Fixed `issue 1 <http://code.google.com/p/apsw/issues/detail?id=1>`_
error in example code for virtual tables which caused filename errors
on Windows.

Fixed `issue 15 <http://code.google.com/p/apsw/issues/detail?id=15>`_
releasing the GIL around calls to sqlite3_prepare.

Fixed `issue 7 <http://code.google.com/p/apsw/issues/detail?id=7>`_
ensuring that extension module filenames are converted to utf8.</li>

Use the `sqlite3_open_v2 <http://sqlite.org/c3ref/open.html>`_
interface which allows specifying which vfs to use. This release does
not allow you to write your own vfs as the SQLite vfs interface is
being changed for SQLite 3.6.

Used new SQLite functions that keep track of when virtual tables and
collations are no longer used so they can be released. Previously APSW
also had to keep track duplicating effort.

Improved test coverage a few more percent.

The statement cache now defaults to the same number of entries as
pysqlite (100). You can however specify more or less as needed.

:func:`Connection.collationneeded` was implemented.


3.3.13-r1
=========

As of this release, APSW is now co-hosted with pysqlite meaning there
is one site to go to for your Python SQLite bindings. Start at
http://initd.org/tracker/pysqlite/wiki/

You can use this with SQLite 3.3.13 onwards. There were no API changes
in SQLite 3.3.10 to 3.3.13 although some internal bugs were fixed and
the 3.3.13 release is recommended over the earlier version.

Thanks to Ed Pasma for highlighting these issues:

* :func:`Connection.interrupt` can be safely called from any thread.

* Empty statements or those consisting entirely of whitespace do not
  cause misuse errors (internally SQLite started returned NULL pointers
  for those statements, and `sqlite3_step
  <http://sqlite.org/c3ref/step.html>`_ didn't like being passed the
  NULL pointer).

* Changed special handling of :const:`SQLITE_BUSY` error to be the same
  as other errors. The special handling previously let you restart on
  receiving busy, but also hung onto statements which could result in
  other statements getting busy errors.
  
3.3.10-r1
=========

You can use this with SQLite 3.3.10 onwards.

Added a statement cache that works in conjunction with the
`sqlite3_prepare_v2 <http://sqlite.org/c3ref/prepare.html>`_ API. A
few issues were exposed in SQLite and hence you must use SQLite 3.3.10
or later.

3.3.9-r1
========
You can use this with SQLite 3.3.9 onwards.

SQLite added `sqlite3_prepare_v2
<http://sqlite.org/c3ref/prepare.html>`_ API. The net effect of this
API update is that you will not get SQLITE_SCHEMA any more. SQLite
will handle it internally.
  

3.3.8-r1
========

You can use this with SQLite 3.3.8 onwards. There was an incompatible
API change for virtual tables in SQLite 3.3.8.

Virtual tables updated for new api.

You must call :func:`~Connection.close` on connections. You can
also call :func:`~Cursor.close` on cursors, but it usually isn't
necessary.

All strings are returned as unicode.

:func:`PyErr_WriteUnraisable` was used for errors in
destructors. Unfortunately it is almost completely useless, merely
printing :func:`str` of the object and exception. This doesn't help in
finding where in your code the issue arose so you could fix it. An
internal APSW implementation generates a traceback and calls
:func:`sys.excepthook`, the default implementation of which prints the
exception and the traceback to sys.stderr.

  .. Note:: The line number reported in the traceback is often off by
            1. This is because the destructors run "between" lines of
            code and so the following line is reported as the current 
            location.

Authorizer codes :const:`SQLITE_CREATE_VTABLE`,
:const:`SQLITE_DROP_VTABLE` and :const:`SQLITE_FUNCTION` added.

SQLite `extended result codes
<http://www.sqlite.org/cvstrac/wiki?p=ExtendedResultCodes>`_ are
available - see :ref:`exceptions` for more detail.

:data:`Connection.hooks` added so you can easily register functions,
virtual tables or similar items with each Connection as it is created.

Added :ref:`mapping dicts <sqliteconstants>` which makes it easy to
map the various constants between strings and ints.

3.3.7-r1
========

Never released as 3.3.8 came along.

You can use this release against SQLite 3.3.7. There were no changes
in the SQLite 3.3.6 API from 3.3.5. In SQLite 3.3.7 an API was added
that allowed removing a chunk of duplicate code. Also added were
`Virtual Tables <http://www.sqlite.org/cvstrac/wiki?p=VirtualTables>`_
and loading of external modules (shared libraries).

APSW had the following changes:

* Even more test cases added (you can't have too many tests :-)
* When exceptions occur, dummy frames are added to the traceback in the
  C code. This makes it a lot easier to tell why code was called if you
  encounter an exception. See :ref:`augmented stack traces
  <augmentedstacktraces` for details.
* String values (traditional and Unicode) work correctly if they have
  embedded NULL characters (ie not truncated at the NULL).
* You can load SQLite shared library extensions.

3.3.5-r1
========

You can use this release against any release of SQLite 3 from 3.3.5
onwards. A bug was also fixed when reporting an error during the
cleanup of an aggregate function if there had also been an error in
the step function.  (:func:`PyErr_WriteUnraisable(NULL)` crashed on
some versions of Python but not others.)

SQLite added several functions for returning metadata about result
column sets. You have to compile SQLite with
:const:`SQLITE_ENABLE_COLUMN_METADATA` to get them. This is not the
default for SQLite. I don't believe these are generally useful except
in some corner cases and so they aren't wrapped. However please shout
if you do need them.  Note that :func:`Cursor.getdescription` will
already give you generally useful information. (Also see the `pragmas
<http://sqlite.org/pragma.html>`_

The test code has been converted into using the unittest module. Run
:command:`python tests.py -v` to get the tests run. There should be no
errors.

Updated code to work correctly with new :ctype:`Py_ssize_t` introduced
in Python 2.5. See :ref:`64 bit hosts, Python 2.5+ <x64bitpy25>` for
more details on how Python and SQLite handle 64 bit sized items.

The following functions were added to SQLite and are wrapped. They are
all functions defined on the :class:`Connection` object or :mod:`apsw`
module:

* `sqlite3_update_hook <http://sqlite.org/c3ref/update_hook.html>`_
* `sqlite3_rollback_hook <http://sqlite.org/c3ref/commit_hook.html>`_
* `sqlite3_enable_shared_cache <http://sqlite.org/c3ref/enable_shared_cache.html>`_
* `sqlite3_get_autocommit <http://sqlite.org/c3ref/get_autocommit.html>`_
* `sqlite3_profile <http://sqlite.org/c3ref/profile.html>`_ This
  callback is run at the end of each statement execution telling you how
  long it took.

3.2.7-r1
========

You can use this release against any release of SQLite 3.

SQLite 3.2.7 has several bug fixes. The undocumented experimental
function :func:`sqlite3_profile` was added, but it not present in apsw
yet.

The author of pysqlite has improved it considerably since APSW was
originally written. The differences section has been updated to
reflect those improvements in pysqlite.

:const:`SQLITE_INTERNAL` and :const:`SQLITE_NOTFOUND` error codes are
not used according to 3.2.7 header file. They are still present in
APSW for backwards compatibility.

Changed the build instructions so configure is run on non-Windows
platforms.

Fixed a bug caused by an overly helpful error message trying to tell
you how many bindings you supplied that crashed if you didn't supply
any.

Changed when an error in the step function for an aggregate is
reported due to limitations in SQLite.

3.2.2-r1
========

You can use this release against any release of SQLite 3.

SQLite 3.2.2 API removed :func:`sqlite3_global_recover`. That function
was not wrapped in APSW. Note that SQLite 3.2.2 contains a bug fix
that applies when you use 64 bit integer primary keys (32 bit ints are
fine).

3.2.1-r1
========

You can use this release against any release of SQLite 3.

There are no changes in APSW except to correct an error in the example
code (collations are registered against the connection not the cursor)

SQLite 3.2.1 had one addition in the stable C API, which was a new
function named :func:`sqlite3_global_recover`. That function is not
applicable for wrapping in APSW.

3.1.3-r1
========

You can use this release against any release of SQLite 3.

The text string returned by apsw.Error used to say
"apsw.APSWException" and has been changed to "apsw.Error".  This is
purely cosmetic and helps make clear what the class is. (The old
string was what the original class name was in an earlier version of
the code.)

Added :const:`SQLITE_ALTER_TABLE` and :const:`SQLITE_REINDEX`
constants for the authorizer function.  (These constants were
introduced in SQLite 3.1.3).

Changed various C++-isms into standard C (eg // comments and the
placing of some :cmacro:`CHECK_THREAD` macro calls).

Added module level function :meth:`~apsw.apswversion` which returns
the version of APSW.

SQLite 3.1.3 had no changes in the stable C API other than what is
mentioned above. There were some new experimental functions added
which are not currently documented on the SQLite website, which are
not wrapped by APSW.  Please contact me if you believe they will
remain in SQLite and you would like them wrapped:

* :cfunc:`sqlite3_sleep` An alternative function which sleeps for a
  specified number of milliseconds can be provided. By default SQLite
  just uses the standard operating system call.
* :cfunc:`sqlite3_expired` This function is internal to statement
  execution. It would apply to the implementation of
  :meth:`Cursor.executemany` and could in theory provide a marginal
  improvement in performance.
* A global variable :cdata:`sqlite3_temp_directory` can be used before
  any databases are opened to set where temporary files are created. By
  default SQLite just uses the standard operating system mechanisms.

3.0.8-r3
========

There are no functional changes. The only changes were to correct some
variable names in the example code (they were cut and pasted from the
test code which used different names) and to make the source zip file
extract its contents into a sub-directory which is the more typical
way of packaging that sort of thing.

3.0.8-r2
========

All remaining functionality in the C API for SQLite 3.0.8 is now
available.

Finished this documentation.

3.0.8-r1
========

Initial release

