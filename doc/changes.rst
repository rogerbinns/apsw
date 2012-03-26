Change History
**************
.. currentmodule:: apsw

3.7.12-r1
=========

Re-enabled the asyncvfs.

3.7.11-r1
=========

Added SQLITE_ABORT_ROLLBACK and SQLITE_FCNTL_PRAGMA constants.

Added :meth:`Connection.readonly`.

Changed :attr:`Connection.filename` which used to return the string
used to open the database and now returns the absolute pathname.

Added :meth:`Connection.db_filename`.

3.7.10-r1
=========

The default sector size returned in VFS routines is 4,096 to match
SQLite's new default.

Several links to SQLite tickets and documentation were updated
(:issue:`122`).

The async vfs is disabled due to a bug in its code that leads to
random memory reads when dealing with filenames.

Added SQLITE_CONFIG_GETPCACHE2, SQLITE_CONFIG_GETPCACHE2,
SQLITE_FCNTL_POWERSAFE_OVERWRITE, SQLITE_FCNTL_VFSNAME and
SQLITE_IOCAP_POWERSAFE_OVERWRITE constants.

Fix shell dumping when SQLite doesn't strip trailing comments from
view declarations (`discussed here
<http://www.sqlite.org/src/info/c04a8b8a4f>`__)

Added a :class:`URIFilename` class to encapsulate how SQLite provides
URI parameters to VFS routines (:issue:`124`).

Compatibility break: Depending on flags your VFS xOpen method may get
a :class:`URIFilename` or a string for the filename.  You can still
pass either to the :class:`VFSFile`.

Compatibility break: The :doc:`vfs` code used to always run strings
you provided through :meth:`VFS.xFullPathname`.  This isn't possible
with URI pathnames so that code has been removed.  If you construct
filenames for :meth:`VFS.xOpen` directly (ie bypassing the SQLite
database open call) then you must call :meth:`VFS.xFullPathname`
yourself first to ensure relative pathnames are turned into absolute
pathnames.  The SQLite API guarantees that filenames passed to
:meth:`VFS.xOpen` are exactly what was returned from
:meth:`VFS.xFullPathname`.

3.7.9-r1
========

Added SQLITE_DBSTATUS_CACHE_HIT, SQLITE_DBSTATUS_CACHE_MISS and
SQLITE_FCNTL_OVERWRITE constants.

3.7.8-r1
========

Updated documentation and tests due to an undocumented change in VFS
xDelete semantics.

Added SQLITE3_FCNTL_PERSIST_WAL and SQLITE3_FCNTL_WIN32_AV_RETRY `file
controls <http://www.sqlite.org/c3ref/c_fcntl_chunk_size.html>`__.

Wrapped sqlite3_sourceid (:issue:`120`)

3.7.7.1-r1
==========

Added `SQLITE_CONFIG_URI
<http://www.sqlite.org/c3ref/c_config_getmalloc.html#sqliteconfiguri>`__
and support for it in :meth:`config`, and the open flag
`SQLITE_OPEN_URI
<http://www.sqlite.org/c3ref/c_open_autoproxy.html>`__.  This makes it
easy to use `URI filenames <http://www.sqlite.org/uri.html>`__.

The :ref:`shell` now uses `URI filenames
<http://www.sqlite.org/uri.html>`__ by default.

New `extended error constants
<http://www.sqlite.org/c3ref/c_busy_recovery.html>`__:
SQLITE_CORRUPT_VTAB, SQLITE_IOERR_SEEK, SQLITE_IOERR_SHMMAP,
SQLITE_READONLY_CANTLOCK and SQLITE_READONLY_RECOVERY.

64 bit platforms
(`LP64 - most non-Windows
<http://en.wikipedia.org/wiki/64-bit#Specific_C-language_data_models>`__)
and Python 2: The Python int type is returned for 64 bit integers
instead of Python long type.

3.7.6.3-r1
==========

When invoking the shell by calling :func:`apsw.main` it will not
become interactive if you supply SQL commands as command line
arguments.  This is to have the same behaviour as the SQLite shell
(:issue:`115`).

The shell has a *.find* command making it easy to search for values
across all columns of some or all tables.

The shell has a *.autoimport* command making it easy to import a data
file automatically deducing separators, column names and data types.

Detect attempted use of a cursor as input data for itself.

3.7.6.2-r1
==========

Fixed :issue:`117` where the
shell could report an I/O error on changing output target for some
operating systems.  Thanks to Edzard Pasma for finding and diagnosing
this.

Added support for VFS version 3 which allows redirecting :meth:`system
calls <VFS.xSetSystemCall>` used by some VFS implementations (eg for
testing or sandboxing).

:exc:`NotFoundError` exception added.

Added :meth:`Connection.config`.

Updated :meth:`Connection.wal_checkpoint` to use `sqlite3_wal_checkpoint_v2
<http://sqlite.org/c3ref/wal_checkpoint_v2.html>`__ which provides
more fine grained control over checkpointing and returns useful
information.

3.7.5-r1
========

Backwards incompatible change in SQLite 3.7.5 for handling of
:meth:`~VFSFile.xFileControl`.  If you implement this method in a VFS
then you must return True or False to indicate if the operation was
understood.  :meth:`Connection.filecontrol` now returns that value.
(Previously you could not tell the difference between an op being
understood and an error resulting, or the op not being understood at
all.)

Windows Python 3.2 binaries now available.

3.7.4-r1
========

Binary downloads for Windows 64 bit Python versions 2.6 and above
including Python 3 are now available.

:meth:`apsw.softheaplimit` now uses `sqlite3_soft_heap_limit64
<http://www.sqlite.org/c3ref/soft_heap_limit64.html>`__ so you can
provide values larger than 2GB.  It is now also able to return the
previous value instead of None.

Improve getting shell timer information for 64 bit Windows.

:meth:`blob.reopen` is implemented.

FTS4 is enabled and in the binary builds.  Note that it is an
augmentation of FTS3 rather than totally separate code and described
in the `SQLite documentation
<http://www.sqlite.org/fts3.html#fts4>`__.

3.7.3-r1
========

You can read blobs into pre-existing buffers using
:meth:`blob.readinto`.  (This is more efficient than allocating new
buffers as :meth:`blob.read` does and then copying.)  (:issue:`109`).

Fixed bug with unicode output in CSV mode in the shell.

`sqlite_create_function_v2
<http://sqlite.org/c3ref/create_function.html>`__ now means that some
housekeeping APSW did can be pushed back onto SQLite and the
consequent deletion of some code

3.7.2-r1
========

No changes to APSW.  Upgrading to this version of SQLite is
`recommended <http://www.sqlite.org/releaselog/3_7_2.html>`__.

3.7.1-r1
========

Updated various constants including `SQLITE_FCNTL_CHUNK_SIZE
<http://sqlite.org/c3ref/c_fcntl_chunk_size.html>`__ used with
:meth:`Connection.filecontrol`.

Fixed Unicode output with some file objects from the shell (:issue:`108`).

With the shell, you can specify handling of characters not present in
the output encoding (eg replace to use '?' or similar, ignore,
xmlcharrefreplace etc).  For example::

    .encoding cp437:replace

3.7.0.1-r1
==========

Fixed issue when using a tracer and a context manager fails to commit.

3.7.0-r1
========

Added several new constants.

`Write Ahead Logging <http://www.sqlite.org/wal.html>`__ is
:ref:`supported <wal>`.  You can make all databases automatically use
WAL mode if available by using :ref:`connection hooks <wal>`.

Added :meth:`format_sql_value` for generating a SQL syntax string from
a value.  This is implemented in C and is significantly faster than
doing the same formatting in Python.

Using the above function and other tweaks the :ref:`shell` dumper is
now three to four times faster.  Thanks to Nikolaus Rath for pointing
out the problem and providing test data.

The shell now does colour highlighting making it easy to visually
distinguish prompts, errors, headers and value types when outputting
to a terminal.  See the :option:`--no-colour` argument and **.colour**
command.  Those of you in the two countries that have not adopted the
metric system may also omit the 'u'.  For Windows users you won't get
colour output unless you install `colorama
<http://pypi.python.org/pypi/colorama>`__

When using the context manager (with statement) of a
:class:`Connection` and the exit commit had an error, then the
transaction is rolled back.  This could occur if SQLite had buffered
the transaction entirely in memory and a non-eager transaction lock
had been obtained.  Thanks to Geoff Ness for finding the problem.
(`Issue 98 <http://code.google.com/p/apsw/issues/detail?id=98>`__).

Fixed bug when an error is returned creating an instance of a virtual
table (eg an invalid column name).  Before the fix you would get the
previous error message or a crash.  Thanks to Jose Gomes for finding
the problem.  (`Issue 103
<http://code.google.com/p/apsw/issues/detail?id=103>`__).

There is now a PPA for Ubuntu users that is kept up to date with APSW
and SQLite at https://launchpad.net/~ubuntu-rogerbinns/+archive/apsw
which has the latest SQLite embedded statically inside (ie system
SQLite is ignored) and has all the extensions enabled: FTS3, RTree,
ICU, asyncvfs

If you open VFS files directly then the filename is always run through
xFullPathname first.  SQLite guarantees this behaviour but the
existing VFS code was not doing that for direct opens.  Opens from
SQLite were doing it.

Fixed error where :attr:`apsw.connection_hooks` were being run before
the :ref:`statement cache <statementcache>` was initialised which would
result in a crash if any hooks executed SQL code.

3.6.23.1-r1
===========

Shell CSV output under Python 3.1 is corrected (work around Python 3.1
StringIO bug/incompatibility with other Python versions).

Simplified access to the shell's :attr:`database <Shell.db>` from the
API.

Added a shell :ref:`example <example-shell>`.


3.6.23-r1
=========

If setup is downloading files and an error occurs then it retries up
to 5 times.

Added SQLITE_CONFIG_LOG and SQLITE_OPEN_AUTOPROXY constants.

Added :attr:`compile_options` which tells you what compilation defines
SQLite was compiled with.

Added :meth:`log` to call the SQLite logging interface, and updated
:meth:`config` so you can set log destination function.

3.6.22-r1
=========

Made it possible to run distutils 'sdist' from an already produced
source that was made from 'sdist'.  This was necessary for some Python
virtual package environments.  Note that the recursive result does not
include the HTML help as distutils has no way of including content in
a reparented location.  (`Issue 89
<http://code.google.com/p/apsw/issues/detail?id=89>`__).

Various settings are output as pragma statements when making a dump
such as page size, encoding, auto_vacuum etc.  The pragmas are
commented out.  (`Issue 90
<http://code.google.com/p/apsw/issues/detail?id=90>`__).

3.6.21-r1
=========

Source and binary files are now digitally signed which means you can
verify they have not been tampered with.  See :ref:`verifydownload`
for instructions.

The pragmas generated for a shell dump are emitted outside the
transaction as they have no effect inside the transaction.

Removed some unintentional logging code left in :ref:`CouchDB virtual
table <couchdb>` code.

3.6.20-r1
=========

Support for Python 3.0 has been dropped as it has been `end of lifed
<http://www.python.org/download/releases/3.0.1/>`__.  Use Python 3.1
onwards.

Changes to how some statements are `prepared
<http://www.sqlite.org/c3ref/prepare.html>`__ to allow the new RANGE
and LIKE optimisations with bound variables introduced in SQLite
3.6.20 to be used.  See `issue 85
<http://code.google.com/p/apsw/issues/detail?id=85>`__ for the long
and gory details.

You can now access `CouchDB <http://couchdb.apache.org>`__ using a
virtual table.  This lets you easily bidirectionally transfer data
between SQLite and CouchDB as well as work on data in both sources at
the same time.  Other example uses are in the :ref:`documentation
<couchdb>`.

:ref:`Shell <shell>` changes:

* .dump command now outputs views in the order they were created
  rather than alphabetical as views could reference each
  other. (`Issue 82
  <http://code.google.com/p/apsw/issues/detail?id=82>`__)

* .dump command now outputs the `user_version
  <http://sqlite.org/pragma.html#version>`__ as a comment.  It is
  used by some programs (such as Firefox) to keep track of the schema
  version.

* Can now output in `JSON <http://json.org>`__.

* Fixed `Issue 83
  <http://code.google.com/p/apsw/issues/detail?id=83>`__ - exception
  if history file didn't exist

* You can right justify output in column mode by specifying negative
  widths. (`Issue 84
  <http://code.google.com/p/apsw/issues/detail?id=84>`__)

* You no longer get a traceback doing completions if there is a
  virtual table in the database but the module is not loaded.
  (`Issue 86
  <http://code.google.com/p/apsw/issues/detail?id=86>`__)

* You can now get detailed tracebacks including local variables using
  the ".exception ON" command.  This is useful when developing
  virtual tables and similar functionality.

* You can now terminate a SQL statement with "go" or "/" on a line
  by itself.

3.6.19-r1
=========

**Backwards incompatible change** Fixed `issue 72
<http://code.google.com/p/apsw/issues/detail?id=72>`__ where APSW
wasn't zero basing virtual table :meth:`~VTTable.BestIndex`
constraints returned as documented.  If you have working BestIndex
code then you need to update it for this release.  Thanks to Lefteris
for finding this issue.

**Backwards incompatible change** The :meth:`~apsw.complete` method
has moved from :class:`Connection` to :mod:`apsw` where it should have
been all along.  You should now call :meth:`apsw.complete` instead.  (It
even had an example showing it to be part of the module and not a
specific connection!)

There is now an :class:`interactive shell <Shell>` very similar to
that `provided by SQLite <http://www.sqlite.org/sqlite.html>`__.  You
can embed it in your own program, inherit from it to provide more
commands and output modes, or just run it like this::

  $ python -c "import apsw ; apsw.main()"

Added the `SQLITE_LIMIT_TRIGGER_DEPTH
<http://www.sqlite.org/c3ref/c_limit_attached.html>`__,
`SQLITE_OPEN_PRIVATECACHE
<http://www.sqlite.org/c3ref/c_open_autoproxy.html>`__ and
`SQLITE_OPEN_SHAREDCACHE
<http://www.sqlite.org/c3ref/c_open_autoproxy.html>`__ constants.

The :file:`setup.py` file now has the various options available made
applicable to appropriate commands only.  Read the :ref:`updated
documentation <setup_py_flags>`.

You can now specify :option:`build --enable=stat2` to :file:`setup.py`
to enable `advanced statistics gathering
<http://www.sqlite.org/compile.html#enable_stat2>`__ for query
planning.

:file:`setup.py` can automatically fetch the :ref:`ext-asyncvfs`
extension for you.  If the source is present when APSW is built then
it will be automatically included and the :meth:`API
<async_initialize>` provided.

A :meth:`fork_checker` is available which turns on detection when you
have used SQLite objects across a fork (a **very** bad thing).  This
is possible on Unix like operating systems, especially if you use the
`multiprocessing module
<http://docs.python.org/library/multiprocessing.html>`__.

Extension loading is now compiled in by default when using the
amalgamation and compiled out when using existing libraries.  This is
more likely to match your machine.  You can use
:option:`--omit=load_extension` or :option:`--enable=load_extension`
to the build/build_ext commands to explicitly disable/enable extension
loading.  (`Issue 67
<http://code.google.com/p/apsw/issues/detail?id=67>`__).

:ref:`setup.py <building>` will now abort on a download that has no
checksum.  See :ref:`more information <fetch_checksums>` on checksums.

:ref:`setup.py <setup_py_flags>` can also fetch the version of SQLite
currently under development before a release.  Use
:option:`--version=fossil`.

Updated which code uses `experimental SQLite APIs
<http://sqlite.org/c3ref/experimental.html>`__ based on changes in
SQLite.  The test suite will also work correctly with experimental on
or off.  (It is on by default.)

3.6.18-r1
=========

The APSW license has been updated to allow you (at your option) to use
any `OSI approved license
<http://opensource.org/licenses/alphabetical>`__.

The :ref:`speedtest` has been updated to (optionally) use unicode
characters and to (optionally) increase the sizes of data items.

Fixed error handling code to not record SQLite error strings in some
situations where it was not necessary.  This results in the code
executing a little faster.

3.6.17-r1
=========

APSW has migrated from Subversion to `Mercurial
<http://mercurial.selenic.com/wiki/>`_ for source code control.
Hosting remains at `Google Code <http://code.google.com/p/apsw/>`_

Updated a test due to VFS xUnlock errors now being ignored sometimes
by SQLite (:cvstrac:`3946`).

The downloads page in the help didn't mention the Windows Python 3.1
installer.

Running the test suite is now integrated into :file:`setup.py` so you
can do the building and testing all in one go.  Sample command line::

  $ python setup.py install test

The test suite will now check the functionality of the FTS3, RTree and
ICU extensions if they were included.  (The Windows binary distribution
includes FTS3 and RTree by default.)

Fixed `issue 55 <http://code.google.com/p/apsw/issues/detail?id=55>`_
where FTS3 was unintentionally omitted from the Windows binary
distribution.

Various documentation updates.

3.6.16-r1
=========

Windows binary distribution includes Python 3.1.

Trivial tweaks to keep MSVC happy.

3.6.15-r1
=========

Fixed `issue 50 <http://code.google.com/p/apsw/issues/detail?id=50>`_
where :meth:`blob.read` was returning :const:`None` on end of file
instead of the documented (and correct) empty string/bytes.

Corrected spelling of option in :ref:`apswtrace <apswtrace>` and only
output CURSORFROM if SQL tracing is on.

3.6.14.2-r1
===========

Updated test code because SQLite 3.6.15 returns a different error code
on trying to register a function with too many arguments (see
:cvstrac:`3875`).

3.6.14.1-r1
===========

Changed some internal symbol names so they won't clash with similar
new ones used by SQLite in the amalgamation.

Added :attr:`apsw.using_amalgamation` so you can tell if APSW was
compiled using the `SQLite amalgamation
<http://www.sqlite.org/cvstrac/wiki?p=TheAmalgamation>`__.  Using the
amalgamation means that SQLite shared libraries are not used and will
not affect your code.

Added a checksums file so that when :file:`setup.py` downloads SQLite,
we know it hasn't been tampered with.  (The :ref:`--fetch-sqlite
<setup_py_flags>` argument can be used to automatically download
SQLite.)

3.6.13-r1
=========

Added SQLITE_LOCKED_SHAREDCACHE `extended error code <http://sqlite.org/c3ref/c_ioerr_access.html>`_.

Updated tests as the VFS delete error handling code in SQLite now
returns the same high level error code between Windows and
non-Windows.

The CHM format help file produced by the Windows HTML Help Compiler is
viewable again under Windows HTML Help Viewer.

3.6.11-r1
=========

You can now use the `hot backup functionality
<http://www.sqlite.org/backup.html>`_ introduced in SQLite 3.6.11.

Updated a VFS test to reflect changes in SQLite underlying error
handling.  (Previously SQLite almost always returned :exc:`FullError`
on any write that had an error but now returns :exc:`SQLError`.)

Changed close methods so that Connections can be released earlier.

In prior releases a :meth:`closed cursor <Cursor.close>` could still be used
(reincarnated).  That is no longer the case and you will get
:exc:`CursorClosedError`.

3.6.10-r1
=========

You can use the database as a `context manager
<http://docs.python.org/reference/datamodel.html#with-statement-context-managers>`_
as defined in :pep:`0343`.  When you use *with* a transaction is
started.  If the block finishes with an exception then the transaction
is rolled back, otherwise it is committed.  See :meth:`Connection.__enter__`
for an example.

Behind the scenes the `savepoint
<http://www.sqlite.org/lang_savepoint.html>`_ functionality introduced
in SQLite 3.6.8 is used.  Consequently :class:`Connection` *with*
blocks can be nested.  If you use Connection level :ref:`execution
tracers <executiontracer>` then they will be called with the savepoint
SQL statements.

You can also use :class:`blobs <blob>` as a context manager which
ensures it is always closed when finished using it.  See
:meth:`blob.__enter__` for an example.

Added :ref:`constants <sqliteconstants>`:

  * SQLITE_SAVEPOINT (authorizer code)
  * SQLITE_IOERR_CLOSE (extended result code)
  * SQLITE_IOERR_DIR_CLOSE (extended result code)
  * New mapping: SQLITE_FCNTL_LOCKSTATE, SQLITE_GET_LOCKPROXYFILE, SQLITE_SET_LOCKPROXYFILE, SQLITE_LAST_ERRNO.  SQLite does not document the purpose of these except the first one.

Updated :ref:`vfs` test code.  SQLite's routines that call
:meth:`VFSFile.xTruncate` used to ignore errors but now return an
error to the caller.  :meth:`VFSFile.xFileControl` is now called so a
user implemented one must call any base it inherits from for SQLite to
function normally.

Updated the xDlSym VFS routine to have the different but compatible
type signature as changed in SQLite 3.6.7 to deal with pedantic
compiler warnings.

Fixed bug in :ref:`apswtrace <apswtrace>` that could result in poorly
formatted times.  Leading comments are also stripped for queries
printed in the final reports.  You can also request subsets of the
reports.

The :ref:`speedtest` script will now fallback to the Python builtin
sqlite3 module if it can't find an externally installed pysqlite.

3.6.6.2-r1
==========

Windows binary download for Python 3.0 is :ref:`available
<source_and_binaries>`.

Various changes in data structures and containers to reduce code size.

Changed the code to handle SQLite errors to only use Python
functionality and no operating system functionality (thread local
storage).  This also addresses `issue 36
<http://code.google.com/p/apsw/issues/detail?id=36>`_ where Vista was
not binary compatible with XP.  Thanks to Rudolf Gaertner for
assistance in detecting and diagnosing this issue.

:class:`Connections <Connection>`, :class:`cursors <Cursor>` and
:class:`blobs <blob>` can be used by `weak references
<http://docs.python.org/library/weakref.html>`_.

You can now install :class:`Connection` wide :meth:`execution
<Connection.setexectrace>` and :meth:`row <Connection.setrowtrace>`
:ref:`tracers <tracing>`.

The callbacks for execution and row tracers have a different signature
to include the cursor the execution or row happened on.  This is a
backwards incompatible change.  See :ref:`tracing <tracing>` for
details.

Due to popular demand, added :meth:`Cursor.fetchall`.  This is a
longer way of typing ``list(cursor)``.

Added attributes to the :class:`Connection` class -
:attr:`~Connection.filename`, :attr:`~Connection.open_flags` and
:attr:`~Connection.open_vfs`.  These let you track how the database
was opened.

Added a :ref:`apswtrace <apswtrace>` script to allow easy SQL tracing
without having to modify your code.

Revert to using older SQLite APIs in order to work around
:cvstrac:`2158`.  (This also saves a little bit of SQLite memory
usage).  The user visible effect was that you could get different
exceptions and error text depending on whether a query was already in
the :ref:`statement cache <statementcache>` or if you were
multi-threading.  As an example, if you have a query that used an
unknown collation then SQLite's `prepare
<http://www.sqlite.org/c3ref/prepare.html>`_ returns
:const:`SQLITE_ERROR` with error text about the bad collation.  If a
query had already been prepared, the collation removed and then `run
<http://www.sqlite.org/c3ref/step.html>`_ the new SQLite routines are
returning :const:`SQLITE_SCHEMA` and generic ``schema changed`` error
text.  Changing user defined functions could also cause a previously
correct query to become invalid.

3.6.5-r1
========

The distribution now includes a :ref:`speedtest` script.  You can use
this to see how APSW performs relative to pysqlite, or to track
performance differences between SQLite versions.  The underlying
queries are derived from `SQLite's speed test
<http://www.sqlite.org/src/finfo?name=tool/mkspeedsql.tcl>`_

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
which was adopted by Python 2.6 and 3.  This has allowed for richer
documentation and more output formats such as PDF and `Windows CHM
<http://en.wikipedia.org/wiki/Microsoft_Compiled_HTML_Help>`_ format.

The binary distribution for Windows includes the `full text search
<http://www.sqlite.org/fts3.html>`__ (FTS) and `Rtree
<http://www.sqlite.org/src/finfo?name=ext/rtree/README>`_ extensions.
See also :ref:`setup_py_flags`.

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
ensuring that extension module filenames are converted to utf8.

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
is one site to go to for your Python SQLite bindings. (Both projects
subsequently moved to Google Code.)

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
  <augmentedstacktraces>` for details.
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
<http://sqlite.org/pragma.html>`_)

The test code has been converted into using the unittest module. Run
:command:`python tests.py -v` to get the tests run. There should be no
errors.

Updated code to work correctly with new :c:type:`Py_ssize_t` introduced
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
placing of some :c:macro:`CHECK_THREAD` macro calls).

Added module level function :meth:`~apsw.apswversion` which returns
the version of APSW.

SQLite 3.1.3 had no changes in the stable C API other than what is
mentioned above. There were some new experimental functions added
which are not currently documented on the SQLite website, which are
not wrapped by APSW.  Please contact me if you believe they will
remain in SQLite and you would like them wrapped:

* :c:func:`sqlite3_sleep` An alternative function which sleeps for a
  specified number of milliseconds can be provided. By default SQLite
  just uses the standard operating system call.
* :c:func:`sqlite3_expired` This function is internal to statement
  execution. It would apply to the implementation of
  :meth:`Cursor.executemany` and could in theory provide a marginal
  improvement in performance.
* A global variable :c:data:`sqlite3_temp_directory` can be used before
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

