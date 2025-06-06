Change History
**************

.. currentmodule:: apsw

You may also be interested in the `SQLite release history
<https://www.sqlite.org/changes.html>`__ and `Python release
history <https://devguide.python.org/versions/>`__.

APSW changes by version
-----------------------

3.50.1.0
========

No user visible changes.

3.50.0.0
========

Full support for the :doc:`session`

Added :meth:`Connection.setlk_timeout`,
:attr:`apsw.mapping_setlk_timeout_flags`, and enabled the timeout for
amalgamation builds such as PyPI.

Shell :ref:`open command <shell-cmd-open>` allows specifying flags to
open a connection, and :ref:`connection command
<shell-cmd-connection>` shows flags used for each open. (:issue:`557`)

Type stubs updated to :class:`collections.abc.Buffer` (Python 3.12+)
wherever some bytes are taken.  `Buffers
<https://docs.python.org/3/c-api/buffer.html>`__ have always been
used, but Python 3.12 added typing.

3.49.2.0
========

Shell dump command handles generated columns correctly. (:issue:`556`)

3.49.1.0
========

No APSW changes.

3.49.0.0
========

:meth:`Connection.set_progress_handler` allows multiple callbacks
(multiplexed by APSW).

Added :class:`apsw.ext.query_limit` to limit total row count and
execution time within a block. (:issue:`520`)

:meth:`Connection.config` updated with new DBCONFIG options

Adjustments for SQLite's new build process.

3.48.0.0
========

You can :ref:`pass any Python objects <pyobject>` into SQLite, and
return them when used as runtime values such as functions.  SQLite's
`pointer passing interface <https://www.sqlite.org/bindptr.html>`__ is
used behind the scenes. (:issue:`521`)

:ref:`Source releases <sources>` are also available in tar format
(:issue:`548`), and have updated source release signing
:ref:`instructions <verifydownload>`. (:issue:`549`)

`Shared cache
<https://www.sqlite.org/compile.html#omit_shared_cache>`__ (2006) is
omitted when APSW includes the amalgamation like PyPI builds.  This is
`recommended by SQLite
<https://www.sqlite.org/compile.html#recommended_compile_time_options>`__,
has been `discouraged for a long time
<https://sqlite.org/sharedcache.html#use_of_shared_cache_is_discouraged>`__.
:meth:`apsw.enable_shared_cache` will raise an exception if called and
the shared cache has been omitted.  You can see what options are in
effect in :attr:`apsw.compile_options`.  If you were using it for
shared memory databases then :ref:`use the memdb VFS <memdb>`.

3.47.2.0
========

Added :func:`apsw.ext.page_usage_to_svg` which shows database usage as
SVG (`example <_static/samples/chinook.svg>`__).  Available as shell
:ref:`.pages-svg command <shell-cmd-pages-svg>`.

3.47.1.0
========

Documentation on how to :ref:`build for packagers <packagers>` such as
those maintaining Linux and BSD distributions.

Documentation on how to :ref:`build for pyodide <pyodide>`, the Python
WASM implementation that runs in the browser and NPM.  PyPI does not
accept pyodide packages yet.

A command line tool ``apsw`` is defined which invokes the :doc:`shell
<shell>`.  This also allows using `uvx apsw
<https://docs.astral.sh/uv/guides/tools/>`__ without having to
explicitly install APSW.

Added :func:`apsw.ext.analyze_pages` which uses `dbstat
<https://www.sqlite.org/dbstat.html>`__ to provide useful information
about the pages making up the database, and fragmentation.  The shell
:ref:`.pages command <shell-cmd-pages>` shows it in a pretty form.

3.47.0.0
========

Support for Python 3.8 removed (:issue:`539`).

The readonly database statistics virtual table (`dbstat
<https://www.sqlite.org/dbstat.html>`__) is enabled by default for
PyPI builds, and when ``--enable-all-extensions`` is passed to manual
:ref:`builds <build>`.

Added :func:`recursive triggers
<apsw.bestpractice.connection_recursive_triggers>` and :func:`optimize
<apsw.bestpractice.connection_optimize>` to :mod:`apsw.bestpractice`.

Multiple callbacks can be present for :meth:`Connection.trace_v2` with
APSW ensuring they are all called (:issue:`502`)

:meth:`Connection.trace_v2` callback information now has ``trigger``,
``id``, and ``total_changes`` fields.

Added :attr:`Connection.data_version` for getting a change counter.
`pragma data_version
<https://sqlite.org/pragma.html#pragma_data_version>`__ doesn't update when
changes are made on the same connection, only others.

Added :func:`apsw.ext.ShowResourceUsage` for getting resource and
SQLite usage in a context block, and also use it for the shell
:ref:`timer <shell-cmd-timer>` command.

Added :func:`apsw.ext.Trace` for tracing SQL execution, row and change
counting, and timing per statement for use in a context block.

Added :doc:`FTS5 support <textsearch>` including registering and
calling tokenizers, and auxiliary functions.  The :mod:`apsw.fts5`
module provides many additional classes and methods for working with
FTS5, including tokenizers for HTML, JSON, regular expressions,
support tokenizers for synonyms, stop words, transformers, and a
:class:`~apsw.fts5.Table` class that wraps access to a FTS5 table
(including :meth:`creating one <apsw.fts5.Table.create>`) with
:meth:`~apsw.fts5.Table.search`, :meth:`~apsw.fts5.Table.more_like`,
and :meth:`~apsw.fts5.Table.query_suggest`.  :mod:`apsw.fts5query` can
parse, modify, and reconstruct queries.  The shell gets a :ref:`ftsq
<shell-cmd-ftsq>` command for issuing queries.

Added :mod:`apsw.unicode` which implements Unicode algorithms for
determining codepoint groups making up a user perceived character,
word and sentence splitting, and where line breaks can be made.  These
are used to make provided FTS5 tokenizers and auxiliary functions
fully Unicode aware.  There are many additional methods such as
getting categories, stripping diacritics, case folding, width when
output to a terminal, text wrapping, and more.

:func:`apsw.ext.format_query_table` uses :mod:`apsw.unicode` to get
widths and line breaks more accurate.  As a side effect it loses the
`word_wrap` parameter (breaking change).


3.46.1.0
========

The shell :ref:`dump <shell-cmd-dump>` command outputs the
`application_id
<https://www.sqlite.org/pragma.html#pragma_application_id>`__ in
addition to the `user_version
<https://www.sqlite.org/pragma.html#pragma_user_version>`__.

`PyPI <https://pypi.org/project/apsw/>`__ binary builds for `Python
3.13 now available
<https://github.com/pypa/cibuildwheel/releases/tag/v2.20.0>`__, as
well as `older Python ARM64
<https://github.com/rogerbinns/apsw/pull/530>`__ are available.

3.46.0.1
========

:func:`apsw.ext.query_info` provides the count and names of bindings
parameters.  (:issue:`528`)

Address how errors are handled in VFS xRandomness routine, that is
only called once by SQLite to seed its random number generator.
(:issue:`526`)

Added :meth:`Connection.vfsname` and updated corresponding shell
command to get the diagnostic names of the vfs stack for the
connection.  (:issue:`525`)

Do not cache :meth:`Connection.pragma` statements to avoid encryption
keys, or pragmas that run during prepare from being retained.
(:issue:`522`)

:meth:`Connection.pragma` adds keyword ``schema`` argument to run
pragma against attached databases.  (:issue:`524`)

3.46.0.0
========

Adjusted `levels
<https://docs.python.org/3/library/logging.html#levels>`__ in
:func:`apsw.ext.log_sqlite` to be lower for some SQLite messages like
`SQLITE_SCHEMA` and `SQLITE_NOTICE_RECOVER_WAL` (:issue:`518`)

Previous source releases were signed with `PGP
<https://en.wikipedia.org/wiki/Pretty_Good_Privacy>`__.  Starting with
this release `Sigstore's <https://www.sigstore.dev/>`__ `cosign tool
<https://docs.sigstore.dev/cosign/>`__ is used. (:issue:`512`)

3.45.3.0
========

No APSW changes.

3.45.2.0
========

Minor doc and tests change due to changed behaviour of
`sqlite3_serialize <https://sqlite.org/c3ref/serialize.html>`__ on an
empty database, used by :meth:`Connection.serialize`.

3.45.1.0
========

No APSW changes.

3.45.0.0
========

Correctly handle NULL/None VFS filenames (:issue:`506`)

3.44.2.0
========

Added `logger` parameter to :func:`apsw.ext.log_sqlite` to use a
specific :class:`logging.Logger` (:issue:`493`)

Added :func:`apsw.ext.result_string` to turn an result code into
a string, taking into account if it is extended or not.

Provide detail when C implemented objects are printed. For example
:class:`connections <Connection>` include the filename.
(:issue:`494`)

Added :meth:`URIFilename.parameters` (:issue:`496`)

:class:`URIFilename` are only valid for the duration of the
:meth:`VFS.xOpen` call.  If you save and use the object later you will
get an exception.  (:issue:`501`)

3.44.0.0
========

Added virtual table :meth:`VTTable.Integrity` support.

On 64 bit platforms with the amalgamation, `SQLITE_MAX_MMAP_SIZE
<https://www.sqlite.org/mmap.html>`__ is set to 256 terabytes.
SQLite's default limit is 2GB.  (:issue:`491`)

3.43.2.0
========

:meth:`Connection.create_aggregate_function` can take a class with step
and final methods. (:issue:`421`)

Corrected non :pep:`8` :ref:`compliant names <renaming>`.  The old
names remain as aliases to the new ones, and your code will not break.

3.43.1.1
========

:doc:`Exception <exceptions>` handling has been updated, with multiple
exceptions in the same SQLite control flow being chained together.
Previously more would have used the :ref:`unraisable <unraisable>`
mechanism.  (:issue:`489`)

Only use alloca with msvc because it doesn't support `VLA
<https://en.wikipedia.org/wiki/Variable-length_array>`__.  The arrays
are used for fastcall. (:issue:`487`)

3.43.1.0
========

All C code calling into Python and all C code called by Python uses
vectorcall / fastcall (see :pep:`590`) which reduces the overhead of
passing and receiving positional and keyword arguments. (:issue:`477`,
:issue:`446`):

* Conversion of arguments from Python values to C values drops generic
  :c:func:`PyArg_ParseTupleAndKeywords` in favour of direct processing
  which is more efficient and allows better exception messages.

* Running :ref:`speedtest` with a VFS that inherits all methods went
  from being 17% slower than pure SQLite to 2% slower.

* A :source:`virtual table benchmark <tools/vtbench.py>` takes 35%
  less time.  (Remember that benchmarks are best case!)

The :doc:`shell <shell>` JSON output modes have been fixed.  Mode
'json' outputs a json array, while mode 'jsonl' does newline delimited
json objects, aka `json lines <https://jsonlines.org/>`__.
(:issue:`483`)

3.43.0.0
========

This is the last version that supports Python 3.6 and Python 3.7 (both
end of life).  The policy as stated in the :doc:`about <about>` page
is that there will be one more APSW release after a Python version
goes end of life supporting that Python version.  (:issue:`471`)

Added :doc:`best practice <bestpractice>` module (:issue:`460`)

:meth:`apsw.ext.log_sqlite` outputs SQLite warnings at warning level.
(:issue:`472`)

`sqlite3_stmt_explain <https://sqlite.org/c3ref/stmt_explain.html>`__
is wrapped available as a `explain` keyword parameter on
execute/executemany methods. (:issue:`474`)

Added documentation and :class:`helper class <VFSFcntlPragma>` for
implementing custom `pragmas <https://sqlite.org/pragma.html>`__ in
your own :ref:`VFS` (:issue:`464`)

Reduced overhead of the Column method when using
:meth:`apsw.ext.make_virtual_module` (:issue:`465`)

3.42.0.1
========

Work with SQLite compiled with `SQLITE_OMIT_DEPRECATED
<https://www.sqlite.org/compile.html#omit_deprecated>`__.
:meth:`Connection.set_profile` was changed from using the deprecated
`sqlite3_profile <https://sqlite.org/c3ref/profile.html>`__ to
`sqlite3_trace_v2 <https://sqlite.org/c3ref/trace_v2.html>`__ giving
the same results.  When including the amalgamation,
SQLITE_OMIT_DEPRECATED is defined. (:issue:`443`)

:doc:`shell` updates adding :ref:`various commands <shell-commands>`
to match the SQLite shell, as well as code and documentation
improvements. (:issue:`397`)

Added :meth:`Connection.read` and :func:`apsw.ext.dbinfo` to provide
information from the database and journal/wal files.  The shell
command :ref:`.dbinfo <shell-cmd-dbinfo>` displays it.

Added :meth:`apsw.vfs_details`.  The shell command
:ref:`.vfslist <shell-cmd-vfslist>` displays it.

Implemented `VFS method xCurrentTimeInt64
<https://sqlite.org/c3ref/vfs.html>`__.  The default SQLite VFS no
longer provide ``xCurrentTime`` (floating point version) if
``SQLITE_OMIT_DEPRECATED`` is defined, so this is needed for
inheritance to work. (:issue:`451`)

**Backwards incompatible change**: *VFS* If you override
``xCurrentTime``, then you will need to override ``xCurrentTimeInt64``
in the same way, or ``exclude`` ``xCurrentTimeInt64`` in :class:`VFS`,
or use ``iVersion`` of ``1``.

:ref:`speedtest` now shows summary statistics, and improved help text.
(:issue:`444`)

3.42.0.0
========

`SQLITE_ENABLE_COLUMN_METADATA
<https://www.sqlite.org/compile.html#enable_column_metadata>`__ is
enabled when installing  APSW from `PyPI <https://pypi.org/project/apsw/>`__
(binary or source). (:issue:`435`)

:ref:`Type stubs <type_stubs>` and typing information in the
documentation use newer Python conventions such as `|` instead of
`Union` and `list` instead of `typing.List`, being more concise and
readable.  (A recent Python is required to use them, but they have no
effect at runtime.)  (:issue:`438`)

Shell: Errors when SQLite are preparing a statement now show the
relevant extract of the query, and where the error was detected.

Shell: Output modes table (ASCII line drawing, lots of sanitization),
box (Unicode line drawing) and qbox (box with quoted values) available.
Python 3.7+ (:issue:`420`)

Shell: if started interactively then box is the default mode (list remains
the default in non-interactive)

Added :meth:`Connection.pragma` to execute pragmas
and get results. (:issue:`432`)

Added :attr:`Cursor.get` returning query results with the
least amount of structure.  (:issue:`389`)

Fixed execution tracers should return comment text for comment
only queries, and add :attr:`Cursor.has_vdbe`. (:issue:`433`)

Ensure that all applicable options are implemented for
:func:`apsw.config`, :meth:`Connection.config` and similar.
(:issue:`431`)

Added :func:`apsw.sleep` (:issue:`419`)

Strings for :meth:`apsw.VFS.xNextSystemCall` are :c:func:`interned
<PyUnicode_InternInPlace>` avoiding memory leaks. (:issue:`430`)

Detect unbound recursion not handled by CPython, and handle better.
(:issue:`425`)

3.41.2.0
========

Fixed :issue:`412` in :meth:`apsw.ext.make_virtual_module`.

Added :meth:`apsw.connections` to get all connections. (:issue:`416`)

:func:`sys.unraisablehook` is called correctly (:issue:`410`)

Be stricter where :class:`bool` values are expected (eg
:meth:`VTTable.BestIndex`), only accepting :class:`int` and
:class:`bool`.  Previously you could for example supply strings and
lists, which were almost certainly unintended errors.

3.41.0.0
========

**Backwards incompatible change**:  Bindings using a dictionary with a
missing key now result in a :exc:`KeyError` exception.  You can use
:meth:`allow_missing_dict_bindings` to restore the old behaviour.
(:issue:`392`)

Virtual table updates:

* :meth:`VTTable.BestIndexObject` is now available which provides
  :class:`IndexInfo` exposing full control (:issue:`332`, :issue:`329`,
  :issue:`278`, :issue:`188`)

* :meth:`IndexInfo.set_aConstraintUsage_in` can have *in* values
  passed all at once to :meth:`VTCursor.Filter`

* Exceptions in :meth:`VTTable.FindFunction` are now reported as
  an :ref:`unraisable exception <unraisable>` because it isn't
  possible to tell SQLite about the error.

* :meth:`VTTable.FindFunction` can now return (int, callable)
  to allow for virtual table specific function overloads. (:issue:`269`)

* Added :meth:`Connection.vtab_config` and
  :meth:`Connection.vtab_on_conflict` (:issue:`189`, :issue:`190`)

* :meth:`Connection.create_module` lets you have `eponymous
  <https://sqlite.org/vtab.html#eponymous_virtual_tables>`__,
  `eponymous_only
  <https://sqlite.org/vtab.html#eponymous_only_virtual_tables>`__, and
  read_only modules. (:issue:`196`)

* Virtual table updates can avoid having to provide all column
  values when only a subset are changing.  See :attr:`apsw.no_change`,
  :meth:`Connection.create_module` *use_no_change* parameter,
  :meth:`VTCursor.ColumnNoChange` and :meth:`VTTable.UpdateChangeRow`
  (:issue:`402`)

* All `virtual table methods <https://www.sqlite.org/c3ref/module.html>`__
  are supported - added *iVersion* 2 and 3.  You can specify the
  *iVersion* in :meth:`Connection.create_module` (:issue:`128`)

* :meth:`apsw.ext.make_virtual_module` makes it very easy to turn
  a Python function into a virtual table module.

* :meth:`apsw.ext.generate_series` and :meth:`apsw.ext.generate_series_sqlite`
  added. (:issue:`380`)

:meth:`apsw.format_sql_value` now outputs floating point NaN, positive
and negative infinity, and signed zero exactly as SQLite does (:issue:`398`)

Added :meth:`apsw.ext.format_query_table` for handy table output
with auto column sizes, colour, word wrap etc.

Added :meth:`Connection.is_interrupted`.

3.40.1.0
========

Implemented `window functions
<https://www.sqlite.org/windowfunctions.html#udfwinfunc>`__
(:issue:`292`)

`Function flags <https://www.sqlite.org/c3ref/c_deterministic.html>`__
can be specified to :meth:`Connection.create_scalar_function` and
:meth:`Connection.create_aggregate_function`. Added
:attr:`apsw.mapping_function_flags`. (:issue:`384`)

Added :meth:`Connection.trace_v2` with :attr:`apsw.mapping_trace_codes`
and :attr:`apsw.mapping_statement_status` (:issue:`383`)

Ensure all SQLite APIs are wrapped. :attr:`Connection.system_errno`,
:meth:`apsw.strlike`, :meth:`apsw.strglob`, :meth:`apsw.stricmp`,
:meth:`apsw.strnicmp`, :attr:`Connection.filename_wal`,
:attr:`Connection.filename_journal`, :meth:`Connection.table_exists`,
:meth:`Connection.column_metadata`, :attr:`Error.error_offset`,
:meth:`Connection.cache_flush`, :meth:`Connection.release_memory`,
:meth:`apsw.hard_heap_limit`. :meth:`Connection.drop_modules`
(:issue:`382`)

When an :ref:`unraisable exception <unraisable>` happens, `sqlite3_log
<https://www.sqlite.org/c3ref/log.html>`__ is now called so you will
have context within SQLite's actions.  :func:`sys.unraisablehook` is
now called first, and if it doesn't exist then :func:`sys.excepthook`
as before.  (:issue:`385`)

When the wrong type is given for a function argument, the error
message now includes the parameter name and function signature.
(:issue:`358`)

Let SQLite do size checking instead of APSW for strings and blobs.
(:issue:`387`)

Added :meth:`apsw.ext.log_sqlite` which installs a handler that
forwards SQLite messages to the :mod:`logging module <logging>`.

Added :meth:`set_default_vfs` and :meth:`unregister_vfs` taking vfs
names.  The test suite also unregisters `ZipVFS
<https://www.sqlite.org/zipvfs/doc/trunk/www/index.wiki>`__
(:issue:`394`)

3.40.0.0
========

Fixed regression in statement cache update (version 3.38.1-r1) where
trailing whitespace in queries would be incorrectly treated as
incomplete execution (:issue:`376`)

Added :doc:`ext` (:issue:`369`)

Added more Pythonic attributes as an alternative to getters and
setters, including :attr:`Connection.in_transaction`,
:attr:`Connection.exec_trace`, :attr:`Connection.row_trace`,
:attr:`Cursor.exec_trace`, :attr:`Cursor.row_trace`,
:attr:`Cursor.connection` (:issue:`371`)

Completed: To the extent permitted by CPython APIs every item has the
same docstring as this documentation.  Every API can use named
parameters.  The :source:`type stubs <apsw/__init__.pyi>` cover
everything including constants.  The type stubs also include
documentation for everything, which for example Visual Studio Code
displays as you type or hover.  There is a single source of
documentation in the source code, which is then automatically
extracted to make this documentation, docstrings, and docstrings in
the type stubs.

:doc:`example` updated and appearance improved (:issue:`367`).

3.39.4.0
========

Added :meth:`Connection.cache_stats` to provide more information about
the statement cache.

:meth:`Cursor.execute` now uses `sqlite_prepare_v3
<https://sqlite.org/c3ref/prepare.html>`__ which allows supplying
`flags <https://sqlite.org/c3ref/c_prepare_normalize.html#sqlitepreparenormalize>`__.

:meth:`Cursor.execute` has a new `can_cache` parameter to control
whether the query can use the statement cache.  One example use is
with :meth:`authorizers <Connection.set_authorizer>` because they only
run during prepare, which doesn't happen with already cached
statements.

(The :meth:`Cursor.execute` additional parameters are keyword only and
also present in :meth:`Cursor.executemany`, and the corresponding
:meth:`Connection.execute` and :meth:`Connection.executemany`
methods.)

Added :attr:`Cursor.is_readonly`, :attr:`Cursor.is_explain`, and
:attr:`Cursor.expanded_sql`.

Updated processing named bindings so that types registered with
:class:`collections.abc.Mapping` (such as
:class:`collections.UserDict`) will also be treated as dictionaries.
(:issue:`373`)

3.39.3.0
========

Test no longer fails if APSW was compiled without
SQLITE_ENABLE_COLUMN_METADATA but sqlite3 was separately compiled with
it.  APSW should be compiled with the same flags as sqlite3 to match
functionality and APIs. (:issue:`363`)

`--use-system-sqlite-config` setup.py `build_ext` option added to
allow :ref:`matching_sqlite_options`. (:issue:`364`)

3.39.2.1
========

PyPI now includes Python 3.11 builds.

Instead of using scripts, you can now run several tools directly:

* :ref:`tests <testing>`:  python3 **-m apsw.tests** *[options]*

* :ref:`tracer <apswtrace>`:  python3 **-m apsw.trace** *[options]*

* :ref:`speed tester <speedtest>`:  python3 **-m apsw.speedtest** *[options]*

* :ref:`shell <shell>`:  python3 **-m apsw** *[options]*

The shell class has moved from apsw.Shell to :class:`apsw.shell.Shell`
(:issue:`356`).  You can still reference it via the old name (ie
existing code will not break, except on Python 3.6).

:ref:`shell`: On Windows the native console support for colour is now used
(previously a third party module was supported).

You :ref:`can use --definevalues in setup.py build_ext
<setup_build_flags>` to provide compiler defines used for configuring
SQLite. (:issue:`357`)

If SQLITE_ENABLE_COLUMN_METADATA is enabled then
:attr:`Cursor.description_full` is available providing all the column
metadata available. (:issue:`354`)

:attr:`Connection.cursor_factory` attribute is now present and is used
when :meth:`Connection.cursor` is called.  Added
:meth:`Connection.execute` and :meth:`Connection.executemany` which
automatically obtain the underlying cursor.  See :ref:`customizing
connections and cursors <customizing_connection_cursor>` in the
:doc:`tips`.  (:issue:`361`)



3.39.2.0
========

**Version numbering scheme change:** Instead of a *-r1* style suffix,
there is *.0* style suffix (:issue:`340`)

Updated building for PyPI to include more compiled platforms,
including aarch64 (Linux) and universal (MacOS).  Windows binaries are
no longer separately provided since PyPI has them.

When the amalgamation is included into APSW, `SQLITE_MAX_ATTACHED
<https://www.sqlite.org/limits.html#max_attached>`__ is set to 125 if
not defined, up from the default of 10.

Updated typing information stubs with more detail and include docstrings.
This is still ongoing, but core functionality is well covered.
(:issue:`338`) (:issue:`381`)

Corrected the :ref:`tips <diagnostics_tips>` log handler of extended
result code (:issue:`342`)

Added :func:`Connection.db_names` (:issue:`343`)

3.38.5-r1
=========

APSW is now on PyPI, so you can::

   pip install apsw

(Thanks to several people behind the scenes who helped with the various pieces
to make this happen.)

Removed support for setup.py downloading the in-development (aka
fossil) version of SQLite.

Shell exit for --version etc cleanly exits (:issue:`210`)

Python 3.11 (:issue:`326`) now works.

PyPy3 compiles and mostly works (:issue:`323`).

3.38.1-r1
=========

All items now have full docstrings including type information.
(Previously just one line summaries).  Note the C implemented
functions and data (ie almost all of APSW) can't provide the same
level of type information as regular Python code.

A pyi file is included which does provide all the typing
information in `type stub
<https://typing.readthedocs.io/en/latest/source/stubs.html>`__ format,
and shown by most IDEs.

Removal of code, tests, and documentation only relevant to CPython
before 3.7.  (Python 3.6 does still work, but is end of life.)

Keyword arguments can be used everywhere.

The statement cache implementation changed from a dictionary to a
list.  This allows the cache to be used for the same query text
multiple times.  (The code is also a quarter of the size and simpler).

The default for setup.py's fetch command is to get the SQLite version
corresponding to APSW's release.  (Previously it got the latest release.)

Added constants:

* SQLITE_INDEX_CONSTRAINT_OFFSET, SQLITE_INDEX_CONSTRAINT_LIMIT

3.37.0-r1
=========

Allow breaking of reference cycles between objects that contain a
:obj:`Connection` or :obj:`Cursor`, and also use callbacks from that
object (eg busy handler). (:issue:`314`)

This is the last release supporting Python 2 and Python 3 before 3.7.
If you still use those Python versions then you should pin to this
APSW version.  (`More information
<https://www.rogerbinns.com/blog/apsw-ending-python2early3.html>`__).

Windows Python 3.10 binaries are available to download.  The .exe format
is no longer available with this Python version.

Fixed custom VFS extension loading failure could leave the error
message unterminated.

Updated size of mutex array used by the :func:`fork checker <fork_checker>`

Connections are opened with SQLITE_OPEN_EXRESCODE so open errors will
also include extended result codes.

:meth:`Connection.changes` and :meth:`Connection.total_changes` use the
new SQLite APIs that return 64 bit values (ie can now return values
greater than 2 billion).

Added :meth:`Connection.autovacuum_pages`.

Added constants:

* SQLITE_CONSTRAINT_DATATYPE, SQLITE_OPEN_EXRESCODE

3.36.0-r1
=========

Implemented :meth:`Connection.serialize` and
:meth:`Connection.deserialize`.  They turn a database into bytes, and
bytes into a database respectively.

Allow any subclass of VFS to implement WAL, not just direct
subclasses.  (:issue:`311`)

Added constants:

* SQLITE_FCNTL_EXTERNAL_READER, SQLITE_FCNTL_CKSM_FILE

3.35.4-r1
=========

Updates for SQLite download url (the year is part of the urls).

Added enable flag for built-in SQL math functions, and enable it
by default with --enable-all-extensions.

Use the newer buffer API for Python 3 (old API removed in Python
3.10).

3.34.0-r1
=========

Windows MSI installer files are now provided in addition to the exe
files (:issue:`294`), as well as wheels for Python 3.6+.  Python 3.9
binaries are also now available.  The wheels can be installed via pip.

Added :meth:`Connection.txn_state`

Added constants:

* SQLITE_IOERR_CORRUPTFS

3.33.0-r1
=========

Small performance improvement in string handling

apsw module exposes Cursor, Blob, and Backup types (:issue:`273`)

pkg-config is used to detect `International Components for Unicode
(ICU) sdk
<https://unicode-org.github.io/icu/userguide/icu/howtouseicu.html>`__
when the `SQLite ICU extension
<https://www.sqlite.org/src/artifact?ci=trunk&filename=ext/icu/README.txt>`__
is enabled.  It falls back to icu-config as before. (:issue:`268`).

Added constants:

* SQLITE_OPEN_SUPER_JOURNAL

3.32.2-r1
=========

Added constants:

* SQLITE_IOERR_DATA, SQLITE_CORRUPT_INDEX, SQLITE_BUSY_TIMEOUT, SQLITE_FCNTL_CKPT_START,
  SQLITE_FCNTL_RESERVE_BYTES

Minor documentation updates

3.31.1-r1
=========

Various updates due to year change

Fix deprecated universal newline use in shell (:issue:`283`)

Shell now uses `pragma function_list` to get list of functions for tab completion

Added constants:

* SQLITE_DBCONFIG_TRUSTED_SCHEMA, SQLITE_DBCONFIG_LEGACY_FILE_FORMAT,
  SQLITE_CONSTRAINT_PINNED, SQLITE_OK_SYMLINK, SQLITE_CANTOPEN_SYMLINK,
  SQLITE_FCNTL_CKPT_DONE, SQLITE_OPEN_NOFOLLOW, SQLITE_VTAB_DIRECTONLY


3.30.1-r1
=========

Added constants:

* SQLITE_DBCONFIG_ENABLE_VIEW

Updated hashing of SQL statements (:issue:`274`)

Python 3.8 Windows binaries available.

3.29.0-r1
=========

Added constants:

* SQLITE_DBCONFIG_DQS_DML, SQLITE_DBCONFIG_DQS_DDL, SQLITE_DBCONFIG_LEGACY_ALTER_TABLE

Updated :meth:`Connection.config` with all current `SQLITE_DBCONFIG
<https://sqlite.org/c3ref/c_dbconfig_defensive.html>`__ constants. Also fixes
:issue:`249`.

3.28.0-r1
=========

Added constant:

* SQLITE_DBCONFIG_WRITABLE_SCHEMA

3.27.2-r1
=========

Added constants:

* SQLITE_CONFIG_MEMDB_MAXSIZE, SQLITE_FCNTL_SIZE_LIMIT

Added support for the geopoly extension (:issue:`253`)

Removed hash optimisation that isn't useful any more (:issue:`256`)

3.26.0-r1
=========

Added constant:

* SQLITE_DBCONFIG_DEFENSIVE

3.25.2-r1
=========

Added constants:

* SQLITE_INDEX_CONSTRAINT_FUNCTION, SQLITE_CANTOPEN_DIRTYWAL, SQLITE_ERROR_SNAPSHOT, SQLITE_FCNTL_DATA_VERSION

Shell output mode now has lines and columns for compatibility (:issue:`214`)

Example now runs under both Python 2 and 3.


3.24.0-r1
=========

Added constants:

* SQLITE_DBCONFIG_RESET_DATABASE, and support for it in :meth:`Connection.config`

* SQLITE_LOCKED_VTAB, and SQLITE_CORRUPT_SEQUENCE extended result codes

Added :attr:`keywords` and updated the shell to use it.

Python 3.7 Windows binaries are provided.

3.23.1-r1
=========

Added constants:

* SQLITE_DBSTATUS_CACHE_SPILL, SQLITE_FCNTL_LOCK_TIMEOUT

3.22.0-r1
=========

Added constants:

* SQLITE_DBCONFIG_TRIGGER_EQP, SQLITE_DBCONFIG_MAX

* SQLITE_READONLY_CANTINIT, SQLITE_ERROR_RETRY, SQLITE_ERROR_MISSING_COLLSEQ, SQLITE_READONLY_DIRECTORY

3.21.0-r1
=========

Added constants:

* SQLITE_INDEX_CONSTRAINT_ISNULL, SQLITE_INDEX_CONSTRAINT_ISNOT,
  SQLITE_INDEX_CONSTRAINT_ISNOTNULL, SQLITE_INDEX_CONSTRAINT_IS and
  SQLITE_INDEX_CONSTRAINT_NE

* SQLITE_CONFIG_SMALL_MALLOC

* SQLITE_IOCAP_BATCH_ATOMIC

* SQLITE_IOERR_ROLLBACK_ATOMIC, SQLITE_IOERR_COMMIT_ATOMIC and
  SQLITE_IOERR_BEGIN_ATOMIC

* SQLITE_FCNTL_COMMIT_ATOMIC_WRITE,  SQLITE_FCNTL_ROLLBACK_ATOMIC_WRITE and
  SQLITE_FCNTL_BEGIN_ATOMIC_WRITE

Many spelling fixes (thanks to Edward Betts for the review)

3.20.1-r1
=========

Added `SQLITE_DBCONFIG_ENABLE_QPSG <https://www.sqlite.org/c3ref/c_dbconfig_enable_fkey.html>`__ constant.

Added shell .open command (:issue:`240`)

3.19.3-r1
=========

No APSW changes.

3.18.0-r1
=========

Updated completions in shell (eg added pragmas).

`Resumable Bulk Update (RBU) <https://www.sqlite.org/rbu.html>`__
extension is now built by default for :code:`--enable-all-extensions`.

Added :meth:`Connection.set_last_insert_rowid`.

3.17.0-r1
=========

No APSW changes.

3.16.2-r1
=========

Python 3.6 builds added.

Added SQLITE_DBCONFIG_NO_CKPT_ON_CLOSE and SQLITE_FCNTL_PDB constants.

3.15.2-r1
=========

No APSW changes.

3.15.1-r1
=========

Added SQLITE_FCNTL_WIN32_GET_HANDLE constant.

3.15.0-r1
=========

Added SQLITE_DBCONFIG_MAINDBNAME constant.

3.14.1-r1
=========

Added SQLITE_DBSTATUS_CACHE_USED_SHARED and SQLITE_OK_LOAD_PERMANENTLY constants.

3.13.0-r1
=========

Added SQLITE_DBCONFIG_ENABLE_LOAD_EXTENSION constant.

Added a pip command line in the download page.

3.12.2-r1
=========

Call `PyUnicode_READY <https://www.python.org/dev/peps/pep-0393/#new-api>`__ for
Python 3.3 onwards.  Fixes :issue:`208`, :issue:`132`, :issue:`168`.

SQLite 3.12 completely changed the semantics of :meth:`VFS.xGetLastError` in an
incompatible way.  This required a rewrite of the relevant C, Python and test
code.  If you implement or use this method then you have to rewrite your code
too.  Also note that running the test suite from an earlier version of APSW
against this or future SQLite versions will result in consuming all memory, swap
or address space (an underlying integer changed meaning).

Added SQLITE_CONFIG_STMTJRNL_SPILL and SQLITE_DBCONFIG_ENABLE_FTS3_TOKENIZER
constants.

Added support for SQLITE_CONFIG_STMTJRNL_SPILL in :meth:`apsw.config`.

3.11.1-r1
=========

setup.py attempts to use setuptools if present, before falling back to
distutils. This allows setuptools only commands such as bdist_wheel to work. You
can force use of distutils by setting the environment variable
APSW_FORCE_DISTUTILS to any value. Note that setuptools may also affect the
output file names. (:issue:`207`)

3.11.0-r1
==========

The shell dump command now outputs the page size and user version.  They were
both output before as comments.

Updated SQLite download logic for 2016 folder.

Updated VFS test suite due to changes in SQLite default VFS implemented methods.

Added SQLITE_INDEX_CONSTRAINT_LIKE, SQLITE_INDEX_CONSTRAINT_REGEXP,
SQLITE_INDEX_CONSTRAINT_GLOB, SQLITE_IOERR_AUTH, SQLITE_FCNTL_JOURNAL_POINTER,
and SQLITE_FCNTL_VFS_POINTER constants.

Allow :class:`Connection` subclasses for backup api (:issue:`199`).

`FTS5 <https://www.sqlite.org/fts5.html>`__ and `JSON1
<https://www.sqlite.org/json1.html>`__ extensions are now built by default for
:code:`--enable-all-extensions`.  It is recommended you wait a few more
releases for these extensions to mature.

Added a mapping for `virtual table scan flags
<https://sqlite.org/c3ref/c_index_scan_unique.html>`__

Use `SQLITE_ENABLE_API_ARMOR
<https://www.sqlite.org/compile.html#enable_api_armor>`__ for extra error
checking.

3.9.2-r1
========

Added SQLITE_IOERR_VNODE constant.

Windows builds for Python 3.5 are now provided.

3.8.11.1-r1
===========

Added SQLITE_FCNTL_RBU and SQLITE_FCNTL_ZIPVFS constants.

setup's fetch command can now get arbitrary fossil versions.  For
example specify ``fossil-e596a6b6``.

Update tests due to a change in Python 3.5 (exception returned with
invalid strings for system calls changed from TypeError to
ValueError).

Adjusted some internal detection related to the :func:`fork checker
<fork_checker>`

3.8.10.1-r1
===========

Added deterministic parameter to
:func:`Connection.create_scalar_function` (:issue:`187`)

Switched to new SQLite API returning 64 bit values for :func:`status`
(:issue:`191`)

3.8.9-r1
========

Fixed column description caching which could be preserved between
multiple statements in the same execution (:issue:`186`)

Updated documentation building tool to use new database of information
from the SQLite site.  This is simpler and more reliable.  (Previously
used site scraping.)

Added SQLITE_AUTH_USER, SQLITE_FCNTL_LAST_ERRNO,
SQLITE_FCNTL_WAL_BLOCK, SQLITE_FCNTL_GET_LOCKPROXYFILE, and
SQLITE_FCNTL_SET_LOCKPROXYFILE constants.

Removed SQLITE_GET_LOCKPROXYFILE, SQLITE_SET_LOCKPROXYFILE,
SQLITE_LAST_ERRNO file control constants.  These are deprecated - use
the versions with FCNTL in their name (eg
SQLITE_FCNTL_GET_LOCKPROXYFILE).

Added :ref:`mappings <sqliteconstants>` for conflict resolution modes,
virtual table configuration options and xShmLock VFS flags.


3.8.8.2-r1
==========

No APSW changes.

3.8.8.1-r1
==========

The column description is now cached on first request during a query
so getting it is quick if called for every row.

Added SQLITE_CONFIG_PCACHE_HDRSZ and SQLITE_CONFIG_PMASZ constants, and
support for them in :func:`config`.

Added SQLITE_CHECKPOINT_TRUNCATE constant.

Update year in various places to 2015.

3.8.7.3-r1
==========

No APSW changes.

3.8.7.2-r1
==========

Fixed parsing of icu-config flags

3.8.7.1-r1
==========

Added SQLITE_LIMIT_WORKER_THREADS constant

3.8.6-r1
========

Updated test suite for Python 3.4 unittest garbage collection changes
(:issue:`164` :issue:`169`)

Using the recommended build option of
**--enable-all-extensions** turns on `STAT4
<https://sqlite.org/compile.html#enable_stat4>`__.  Windows binaries
include this too.

3.8.5-r1
========

Added SQLITE_IOCAP_IMMUTABLE and SQLITE_FCNTL_WIN32_SET_HANDLE
constants.

3.8.4.3-r1
==========

Added :meth:`Cursor.fetchone`

3.8.4.2-r1
==========

No APSW code changes.  Rebuild due to updated SQLite version.

3.8.4.1-r1
==========

Windows 64 bit binary builds for Python 3.3+ are back - thanks to
Mike C. Fletcher for `pointing the way <http://vrplumber.com>`__

Correct detection of current SQLite version from download page for
:file:`setup.py` fetch command

Tested against Python 3.4 and binaries for Windows.

3.8.3.1-r1
==========

Updated :doc:`shell` completions for keywords, functions and pragmas.

3.8.3-r1
========

APSW is now hosted at Github - https://github.com/rogerbinns/apsw

Added SQLITE_RECURSIVE, SQLITE_READONLY_DBMOVED,
SQLITE_FCNTL_COMMIT_PHASETWO, SQLITE_FCNTL_HAS_MOVED and
SQLITE_FCNTL_SYNC constants.

3.8.2-r1
========

Added SQLITE_CONFIG_WIN32_HEAPSIZE, SQLITE_CONSTRAINT_ROWID and
SQLITE_FCNTL_TRACE constants.

3.8.1-r1
========

Added SQLITE_CANTOPEN_CONVPATH and SQLITE_IOERR_CONVPATH extended
error codes.

Updated pysqlite urls to point to github.

Various minor build/download documentation updates.

3.8.0.2-r1
==========

No APSW code changes.  Rebuild due to updated SQLite version.

Updated documentation tips to show how to :ref:`get detailed
diagnostics <diagnostics_tips>`.

3.8.0.1-r1
==========

No APSW changes.  Rebuild due to updated SQLite version.

Windows binaries for Python 3.3 64 bit are no longer available as a
Visual Studio update obliterated the ability to compile them, and I
have no patience left to fight Microsoft's tools.

3.8.0-r2
========

No APSW changes - updated checksums because SQLite changed the
released archive to address an autoconf issue on some platforms

3.8.0-r1
========

Windows binaries for Python 3.3 64 bit are now available after
managing to get several pieces of Microsoft software to cooperate.

Fixed shell dump issue when system routines (eg timestamp, username,
hostname) couldn't automatically be promoted to unicode.  They are
used in comments in the output.  (:issue:`142`)

Added SQLITE_DBSTATUS_DEFERRED_FKS, SQLITE_IOERR_GETTEMPPATH,
SQLITE_WARNING_AUTOINDEX and SQLITE_BUSY_SNAPSHOT constants.

3.7.17-r1
=========

Removed tests that checked directly calling VFS read/write with
negative offsets or amounts returns errors.  This version of SQLite no
longer returns errors in those circumstances and typically crashes
instead.

Various new constants.

3.7.16.2-r1
===========

No APSW changes - just a binary rebuild.  Windows users are
recommended to upgrade their SQLite version.

3.7.16.1-r1
===========

Updated tables of functions and pragmas in the :doc:`shell` to match
current SQLite version.

3.7.16-r1
=========

Adjust to different SQLite download URLs

Added SQLITE_CONSTRAINT_* and SQLITE_READONLY_ROLLBACK `extended error
codes <https://www.sqlite.org/c3ref/c_abort_rollback.html>`__

Removed CouchDB virtual table

3.7.15.2-r1
===========

No APSW changes - binary rebuild to pickup new SQLite version

3.7.15.1-r1
===========

Use https (SSL) for SQLite web site references (downloads and
documentation links).  On some platforms/versions/SSL libraries,
Python's SSL module `doesn't work
<https://www.google.com/webhp?q=python%20ssl%20EOF%20occurred%20in%20violation%20of%20protocol>`__
with the SQLite website so a fallback to http is used - the downloads
still have their checksum verified.

3.7.15-r1
=========

Work around changed semantics for error handling when the VFS xDelete
method is asked to delete a file that does not exist.

Completely removed all `AsyncVFS
<https://sqlite.org/asyncvfs.html>`__ related code.  This extension
is `no longer maintained nor supported
<https://sqlite.org/src/info/3d548db7eb>`__ by the SQLite team.
`WAL <https://sqlite.org/wal.html>`__ is a good way of getting
similar functionality.

Added :func:`config` support for SQLITE_CONFIG_COVERING_INDEX_SCAN.

Added several new constants: SQLITE_CONFIG_COVERING_INDEX_SCAN,
SQLITE_CONFIG_SQLLOG, SQLITE_FCNTL_BUSYHANDLER,
SQLITE_FCNTL_TEMPFILENAME, SQLITE_CANTOPEN_FULLPATH,
SQLITE_IOERR_DELETE_NOENT

3.7.14.1-r1
===========

Updated setup and test suite so that all files are explicitly closed
instead of relying on garbage collection.

Added Windows binaries for Python 3.3.  (Only 32 bit as Python doesn't
provide a free way of making 64 bit Windows binaries.)

Updated setup.py to work with changed SQLite download page formatting
when detecting latest version.

Due to a `Python 3.3.0 regression bug
<https://bugs.python.org/issue16145>`__ using the csv output mode in the
shell can result in bad data or Python crashing.  The bug has been
fixed for Python 3.3.1 which is due in November 2012.

3.7.14-r2
=========

Fixed an issue with the GIL in the destructor for functions.  The bug
would be encountered if you create a function with the same name as an
existing function and are using an upcoming version of Python (eg
2.7.4).  Thanks to Arfrever Frehtes Taifersar Arahesis for finding it
(:issue:`134`).

Added shell .print command to match upcoming SQLite shell changes.

3.7.14-r1
=========

Added support for :meth:`Connection.status` (calls `sqlite3_db_status
<https://sqlite.org/c3ref/db_status.html>`__).

The legacy Windows `Compiled Help Format
<https://en.wikipedia.org/wiki/Microsoft_Compiled_HTML_Help>`__
documentation is no longer produced - the help compiler setup program
can't cope with modern machines.

3.7.13-r1
=========

Do not free a structure on failure to register a virtual table module
as SQLite does that anyway.

Added SQLITE_OPEN_MEMORY constant.

3.7.12.1-r1
===========

No changes to APSW.  Binary rebuilds due to SQLite bugfixes.

3.7.12-r1
=========

Re-enabled the asyncvfs.

Added :attr:`Cursor.description` to make DB API interoperability a
little easier (:issue:`131`).

Added SQLITE_DBSTATUS_CACHE_WRITE and SQLITE_CANTOPEN_ISDIR constants.

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
<https://sqlite.org/src/info/c04a8b8a4f>`__)

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
controls <https://sqlite.org/c3ref/c_fcntl_chunk_size.html>`__.

Wrapped sqlite3_sourceid (:issue:`120`)

3.7.7.1-r1
==========

Added `SQLITE_CONFIG_URI
<https://sqlite.org/c3ref/c_config_getmalloc.html#sqliteconfiguri>`__
and support for it in :meth:`config`, and the open flag
`SQLITE_OPEN_URI
<https://sqlite.org/c3ref/c_open_autoproxy.html>`__.  This makes it
easy to use `URI filenames <https://sqlite.org/uri.html>`__.

The :ref:`shell` now uses `URI filenames
<https://sqlite.org/uri.html>`__ by default.

New `extended error constants
<https://sqlite.org/c3ref/c_busy_recovery.html>`__:
SQLITE_CORRUPT_VTAB, SQLITE_IOERR_SEEK, SQLITE_IOERR_SHMMAP,
SQLITE_READONLY_CANTLOCK and SQLITE_READONLY_RECOVERY.

64 bit platforms
(`LP64 - most non-Windows
<https://en.wikipedia.org/wiki/64-bit_computing#64-bit_data_models>`__)
and Python 2: The Python int type is returned for 64 bit integers
instead of Python long type.

3.7.6.3-r1
==========

When invoking the shell by calling :func:`apsw.shell.main` it will not
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
<https://sqlite.org/c3ref/wal_checkpoint_v2.html>`__ which provides
more fine grained control over checkpointing and returns useful
information.

3.7.5-r1
========

Backwards incompatible change in SQLite 3.7.5 for handling of
:meth:`~VFSFile.xFileControl`.  If you implement this method in a VFS
then you must return True or False to indicate if the operation was
understood.  :meth:`Connection.file_control` now returns that value.
(Previously you could not tell the difference between an op being
understood and an error resulting, or the op not being understood at
all.)

Windows Python 3.2 binaries now available.

3.7.4-r1
========

Binary downloads for Windows 64 bit Python versions 2.6 and above
including Python 3 are now available.

:meth:`apsw.soft_heap_limit` now uses `sqlite3_soft_heap_limit64
<https://sqlite.org/c3ref/soft_heap_limit64.html>`__ so you can
provide values larger than 2GB.  It is now also able to return the
previous value instead of None.

Improve getting shell timer information for 64 bit Windows.

:meth:`Blob.reopen` is implemented.

FTS4 is enabled and in the binary builds.  Note that it is an
augmentation of FTS3 rather than totally separate code and described
in the `SQLite documentation
<https://sqlite.org/fts3.html#fts4>`__.

3.7.3-r1
========

You can read blobs into pre-existing buffers using
:meth:`Blob.read_into`.  (This is more efficient than allocating new
buffers as :meth:`Blob.read` does and then copying.)  (:issue:`109`).

Fixed bug with unicode output in CSV mode in the shell.

`sqlite_create_function_v2
<https://sqlite.org/c3ref/create_function.html>`__ now means that some
housekeeping APSW did can be pushed back onto SQLite and the
consequent deletion of some code

3.7.2-r1
========

No changes to APSW.  Upgrading to this version of SQLite is
`recommended <https://sqlite.org/releaselog/3_7_2.html>`__.

3.7.1-r1
========

Updated various constants including `SQLITE_FCNTL_CHUNK_SIZE
<https://sqlite.org/c3ref/c_fcntl_chunk_size.html>`__ used with
:meth:`Connection.file_control`.

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

`Write Ahead Logging <https://sqlite.org/wal.html>`__ is
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
to a terminal.  See the `--no-colour` argument and **.colour**
command.  Those of you in the two countries that have not adopted the
metric system may also omit the 'u'.  For Windows users you won't get
colour output unless you install `colorama
<https://pypi.python.org/pypi/colorama>`__

When using the context manager (with statement) of a
:class:`Connection` and the exit commit had an error, then the
transaction is rolled back.  This could occur if SQLite had buffered
the transaction entirely in memory and a non-eager transaction lock
had been obtained.  Thanks to Geoff Ness for finding the problem.
(:issue:`98`).

Fixed bug when an error is returned creating an instance of a virtual
table (eg an invalid column name).  Before the fix you would get the
previous error message or a crash.  Thanks to Jose Gomes for finding
the problem.  :issue:`103`

There is now a PPA for Ubuntu users that is kept up to date with APSW
and SQLite at \https://launchpad.net/~ubuntu-rogerbinns/+archive/apsw
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

Simplified access to the shell's :attr:`database <shell.Shell.db>` from the
API.

Added a shell :ref:`example <example_shell>`.


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
a reparented location.  :issue:`89`

Various settings are output as pragma statements when making a dump
such as page size, encoding, auto_vacuum etc.  The pragmas are
commented out.  :issue:`90`

3.6.21-r1
=========

Source and binary files are now digitally signed which means you can
verify they have not been tampered with.  See :ref:`verifydownload`
for instructions.

The pragmas generated for a shell dump are emitted outside the
transaction as they have no effect inside the transaction.

Removed some unintentional logging code left in CouchDB virtual
table code.

3.6.20-r1
=========

Support for Python 3.0 has been dropped as it has been `end of lifed
<https://www.python.org/download/releases/3.0.1/>`__.  Use Python 3.1
onwards.

Changes to how some statements are `prepared
<https://sqlite.org/c3ref/prepare.html>`__ to allow the new RANGE and
LIKE optimisations with bound variables introduced in SQLite 3.6.20 to
be used.  See :issue:`85` for the long and gory details.

You can now access `CouchDB <https://couchdb.apache.org>`__ using a
virtual table.  This lets you easily bidirectionally transfer data
between SQLite and CouchDB as well as work on data in both sources at
the same time.  Other example uses are in the documentation.

:ref:`Shell <shell>` changes:

* .dump command now outputs views in the order they were created
  rather than alphabetical as views could reference each
  other. :issue:`82`

* .dump command now outputs the `user_version
  <https://sqlite.org/pragma.html#pragma_user_version>`__ as a comment.  It is
  used by some programs (such as Firefox) to keep track of the schema
  version.

* Can now output in `JSON <https://json.org>`__.

* Fixed :issue:`83` - exception if history file didn't exist

* You can right justify output in column mode by specifying negative
  widths. :issue:`84`

* You no longer get a traceback doing completions if there is a
  virtual table in the database but the module is not loaded.
  :issue:`86`

* You can now get detailed tracebacks including local variables using
  the ".exception ON" command.  This is useful when developing
  virtual tables and similar functionality.

* You can now terminate a SQL statement with "go" or "/" on a line
  by itself.

3.6.19-r1
=========

**Backwards incompatible change** Fixed :issue:`72` where APSW wasn't
zero basing virtual table :meth:`~VTTable.BestIndex` constraints
returned as documented.  If you have working BestIndex code then you
need to update it for this release.  Thanks to Lefteris for finding
this issue.

**Backwards incompatible change** The :meth:`~apsw.complete` method
has moved from :class:`Connection` to :mod:`apsw` where it should have
been all along.  You should now call :meth:`apsw.complete` instead.  (It
even had an example showing it to be part of the module and not a
specific connection!)

There is now an :class:`interactive shell <shell.Shell>` very similar to
that `provided by SQLite <https://sqlite.org/sqlite.html>`__.  You
can embed it in your own program, inherit from it to provide more
commands and output modes, or just run it like this::

  $ python -c "import apsw ; apsw.main()"

Added the `SQLITE_LIMIT_TRIGGER_DEPTH
<https://sqlite.org/c3ref/c_limit_attached.html>`__,
`SQLITE_OPEN_PRIVATECACHE
<https://sqlite.org/c3ref/c_open_autoproxy.html>`__ and
`SQLITE_OPEN_SHAREDCACHE
<https://sqlite.org/c3ref/c_open_autoproxy.html>`__ constants.

The :file:`setup.py` file now has the various options available made
applicable to appropriate commands only.  Read the :ref:`updated
documentation <setup_py_flags>`.

You can now specify `build --enable=stat2` to :file:`setup.py`
to enable `advanced statistics gathering
<https://sqlite.org/compile.html#enable_stat2>`__ for query
planning.

:file:`setup.py` can automatically fetch the asyncvfs
extension for you.  If the source is present when APSW is built then
it will be automatically included and *async_initialize* called.

A :meth:`fork_checker` is available which turns on detection when you
have used SQLite objects across a fork (a **very** bad thing).  This
is possible on Unix like operating systems, especially if you use the
:mod:`multiprocessing module <multiprocessing>`.

Extension loading is now compiled in by default when using the
amalgamation and compiled out when using existing libraries.  This is
more likely to match your machine.  You can use
`--omit=load_extension` or `--enable=load_extension`
to the build/build_ext commands to explicitly disable/enable extension
loading.  :issue:`67`

:file:`setup.py` will now abort on a download that has no
checksum.  See :ref:`more information <fetch_checksums>` on checksums.

:ref:`setup.py <setup_py_flags>` can also fetch the version of SQLite
currently under development before a release.  Use
`--version=fossil`.

Updated which code uses `experimental SQLite APIs
<https://sqlite.org/c3ref/experimental.html>`__ based on changes in
SQLite.  The test suite will also work correctly with experimental on
or off.  (It is on by default.)

3.6.18-r1
=========

The APSW license has been updated to allow you (at your option) to use
any `OSI approved license
<https://opensource.org/licenses/alphabetical>`__.

The :ref:`speedtest` has been updated to (optionally) use unicode
characters and to (optionally) increase the sizes of data items.

Fixed error handling code to not record SQLite error strings in some
situations where it was not necessary.  This results in the code
executing a little faster.

3.6.17-r1
=========

APSW has migrated from Subversion to `Mercurial
<https://en.wikipedia.org/wiki/Mercurial>`_ for source code control.
Hosting remains at `Google Code <https://code.google.com/p/apsw/>`_

Updated a test due to VFS xUnlock errors now being ignored sometimes
by SQLite (cvstrac 3946).

The downloads page in the help didn't mention the Windows Python 3.1
installer.

Running the test suite is now integrated into :file:`setup.py` so you
can do the building and testing all in one go.  Sample command line::

  $ python setup.py install test

The test suite will now check the functionality of the FTS3, RTree and
ICU extensions if they were included.  (The Windows binary distribution
includes FTS3 and RTree by default.)

Fixed :issue:`55` where FTS3 was unintentionally omitted from the
Windows binary distribution.

Various documentation updates.

3.6.16-r1
=========

Windows binary distribution includes Python 3.1.

Trivial tweaks to keep MSVC happy.

3.6.15-r1
=========

Fixed :issue:`50` where :meth:`Blob.read` was returning *None*
on end of file instead of the documented (and correct) empty
string/bytes.

Corrected spelling of option in :ref:`apswtrace <apswtrace>` and only
output CURSORFROM if SQL tracing is on.

3.6.14.2-r1
===========

Updated test code because SQLite 3.6.15 returns a different error code
on trying to register a function with too many arguments (see
cvstrac 3875).

3.6.14.1-r1
===========

Changed some internal symbol names so they won't clash with similar
new ones used by SQLite in the amalgamation.

Added :attr:`apsw.using_amalgamation` so you can tell if APSW was
compiled using the `SQLite amalgamation
<https://www.sqlite.org/amalgamation.html>`__.  Using the
amalgamation means that SQLite shared libraries are not used and will
not affect your code.

Added a checksums file so that when :file:`setup.py` downloads SQLite,
we know it hasn't been tampered with.  (The :ref:`--fetch-sqlite
<setup_py_flags>` argument can be used to automatically download
SQLite.)

3.6.13-r1
=========

Added SQLITE_LOCKED_SHAREDCACHE `extended error code <https://sqlite.org/c3ref/c_ioerr_access.html>`_.

Updated tests as the VFS delete error handling code in SQLite now
returns the same high level error code between Windows and
non-Windows.

The CHM format help file produced by the Windows HTML Help Compiler is
viewable again under Windows HTML Help Viewer.

3.6.11-r1
=========

You can now use the `hot backup functionality
<https://sqlite.org/backup.html>`_ introduced in SQLite 3.6.11.

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
<https://docs.python.org/3/reference/datamodel.html#with-statement-context-managers>`_
as defined in :pep:`0343`.  When you use *with* a transaction is
started.  If the block finishes with an exception then the transaction
is rolled back, otherwise it is committed.  See :meth:`Connection.__enter__`
for an example.

Behind the scenes the `savepoint
<https://sqlite.org/lang_savepoint.html>`_ functionality introduced
in SQLite 3.6.8 is used.  Consequently :class:`Connection` *with*
blocks can be nested.  If you use Connection level :ref:`execution
tracers <executiontracer>` then they will be called with the savepoint
SQL statements.

You can also use :class:`blobs <Blob>` as a context manager which
ensures it is always closed when finished using it.  See
:meth:`Blob.__enter__` for an example.

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

Windows binary download for Python 3.0 is available.

Various changes in data structures and containers to reduce code size.

Changed the code to handle SQLite errors to only use Python
functionality and no operating system functionality (thread local
storage).  This also addresses :issue:`36` where Vista was not binary
compatible with XP.  Thanks to Rudolf Gaertner for assistance in
detecting and diagnosing this issue.

:class:`Connections <Connection>`, :class:`cursors <Cursor>` and
:class:`blobs <Blob>` can be used by :mod:`weak references <weakref>`.

You can now install :class:`Connection` wide :meth:`execution
<Connection.set_exec_trace>` and :meth:`row <Connection.set_row_trace>`
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
cvstrac 2158.  (This also saves a little bit of SQLite memory
usage).  The user visible effect was that you could get different
exceptions and error text depending on whether a query was already in
the :ref:`statement cache <statementcache>` or if you were
multi-threading.  As an example, if you have a query that used an
unknown collation then SQLite's `prepare
<https://sqlite.org/c3ref/prepare.html>`_ returns
*SQLITE_ERROR* with error text about the bad collation.  If a
query had already been prepared, the collation removed and then `run
<https://sqlite.org/c3ref/step.html>`_ the new SQLite routines are
returning *SQLITE_SCHEMA* and generic ``schema changed`` error
text.  Changing user defined functions could also cause a previously
correct query to become invalid.

3.6.5-r1
========

The distribution now includes a :ref:`speedtest` script.  You can use
this to see how APSW performs relative to pysqlite, or to track
performance differences between SQLite versions.  The underlying
queries are derived from `SQLite's speed test
<https://sqlite.org/src/finfo?name=tool/mkspeedsql.tcl>`_

The statement cache was completely rewritten.  It uses less memory and
scales significantly better.

It was possible to get a deadlock between the Python GIL and the
SQLite database mutex when using the same :class:`Connection` across
multiple threads.  Fixed by releasing the GIL in more places and added
test that inspects the source to verify GIL/mutex handling.  Thanks to
amicitas reporting this as :issue:`31`

SQLite's API has been extended in 3.6.5 so that errors can be
retrieved in a thread safe manner.  APSW now uses this API.

As a consequence of the prior two changes it is now possible and safe
to use the same :class:`Connection` across as many threads as you want
`concurrently <https://sqlite.org/threadsafe.html>`_.

Documentation is now done using `Sphinx <https://www.sphinx-doc.org/>`_
which was adopted by Python 2.6 and 3.  This has allowed for richer
documentation and more output formats such as PDF and `Windows CHM
<https://en.wikipedia.org/wiki/Microsoft_Compiled_HTML_Help>`_ format.

The binary distribution for Windows includes the `full text search
<https://sqlite.org/fts3.html>`__ (FTS) and `Rtree
<https://sqlite.org/src/finfo?name=ext/rtree/README>`_ extensions.
See also :ref:`setup_py_flags`.

The source structure and files were reorganized to make it clearer
where things are implemented and to make automatic extraction of
documentation easier.

3.6.3-r1
========

You can now write your own :ref:`VFS` in Python. You can also inherit
from an existing VFS making it easy to augment or override small bits
of behaviour without having to code everything else. See the
:ref:`example <example_vfs>` where database files are obfuscated by
XORing their contents.

:file:`setup.py` now takes an optional `--fetch-sqlite[=ver]`
argument to automatically download and use the latest SQLite
amalgamation (or a specified version). On non-Windows platforms it
will also work out what compile flags SQLite needs (for example
*HAVE_USLEEP*, *HAVE_LOCALTIME_R*). Several other
options to :file:`setup.py` are also available to control
enabling/omitting certains features and functionality. See
:ref:`building <Building>` for further details.

APSW checks that SQLite was compiled to be `threadsafe <https://sqlite.org/c3ref/threadsafe.html>`_

Added new constants:

* *SQLITE_IOERR_ACCESS*, *SQLITE_IOERR_CHECKRESERVEDLOCK* and *SQLITE_IOERR_LOCK* extended result codes
* *SQLITE_OPEN_NOMUTEX* and *SQLITE_OPEN_FULLMUTEX* open flags
* Several new *SQLITE_CONFIG* and *SQLITE_STATUS* codes

Wrapped several new SQLite apis:

* `sqlite3_config <https://sqlite.org/c3ref/config.html>`_
* `sqlite3_initialize/sqlite3_shutdown <https://sqlite.org/c3ref/initialize.html>`_
* `sqlite3_memory_used/sqlite3_memory_highwater <https://sqlite.org/c3ref/memory_highwater.html>`_
* `sqlite3_status <https://sqlite.org/c3ref/status.html>`_
* `sqlite3_soft_heap_limit <https://sqlite.org/c3ref/soft_heap_limit.html>`_
* `sqlite3_release_memory <https://sqlite.org/c3ref/release_memory.html>`_
* `sqlite3_randomness <https://sqlite.org/c3ref/randomness.html>`_


The following experimental apis are not wrapped as there is nothing
useful you can do with them (yet):

* `sqlite3_db_config <https://sqlite.org/c3ref/db_config.html>`_
* `sqlite3_db_status <https://sqlite.org/c3ref/db_status.html>`_

Restored prior behaviour regarding Python ints and longs returning int
for numbers fitting in signed 32 bit. This only affects Python 2 as
Python 3 uses long exclusively. Thanks to Joe Pham for reporting this
as :issue:`24`

Added :meth:`Connection.sqlite3_pointer` method to help with
:issue:`26`

3.5.9-r2
========

APSW now works with Python 3 (you need 3.0b1 or later).

(:issue:`17`)
Removed the *SQLITE_MAX_* constants since they could be
unreliable (eg APSW can't tell what a shared library was compiled
with). A workaround is documented in :func:`Connection.limit`.

3.5.9-r1
========

APSW is now hosted at \https://code.google.com/p/apsw

You can use this with SQLite 3.5.9 onwards.

SQLite now provides the source all `amalgamated
<https://www.sqlite.org/amalgamation.html>`_ into one file
which improves performance and makes compilation and linking of SQLite
far easier. The build instructions are updated.

*SQLITE_COPY* authorizer code and *SQLITE_PROTOCOL*
error code are no longer used by SQLite, but the values are left in
apsw for backwards compatibility

*SQLITE_IOERR_DELETE*, *SQLITE_IOERR_BLOCKED* and *SQLITE_IOERR_NOMEM*

:func:`Connection.interrupt` can be called from any thread

SQLite has implementation limits on string and blob lengths (roughly
constrained to fitting within a signed 32 bit integer - less than 2GB)
which weren't checked. Using a 64 bit Python 2.5+ (as I do) it would
have been possible to destroy memory and crash the
program. Consequently APSW has length checks to ensure it doesn't
happen.  SQLite now has further `limits checking
<https://sqlite.org/limits.html>`_ which cover other things as well
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

:issue:`4` which could lead to generic error messages was fixed in
SQLite 3.5.9.

Fixed :issue:`1` error in example code for virtual tables which caused
filename errors on Windows.

Fixed :issue:`15` releasing the GIL around calls to sqlite3_prepare.

Fixed :issue:`7` ensuring that extension module filenames are
converted to utf8.

Use the `sqlite3_open_v2 <https://sqlite.org/c3ref/open.html>`_
interface which allows specifying which vfs to use. This release does
not allow you to write your own vfs as the SQLite vfs interface is
being changed for SQLite 3.6.

Used new SQLite functions that keep track of when virtual tables and
collations are no longer used so they can be released. Previously APSW
also had to keep track duplicating effort.

Improved test coverage a few more percent.

The statement cache now defaults to the same number of entries as
pysqlite (100). You can however specify more or less as needed.

:func:`Connection.collation_needed` was implemented.


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
  <https://sqlite.org/c3ref/step.html>`_ didn't like being passed the
  NULL pointer).

* Changed special handling of *SQLITE_BUSY* error to be the same
  as other errors. The special handling previously let you restart on
  receiving busy, but also hung onto statements which could result in
  other statements getting busy errors.

3.3.10-r1
=========

You can use this with SQLite 3.3.10 onwards.

Added a statement cache that works in conjunction with the
`sqlite3_prepare_v2 <https://sqlite.org/c3ref/prepare.html>`_ API. A
few issues were exposed in SQLite and hence you must use SQLite 3.3.10
or later.

3.3.9-r1
========
You can use this with SQLite 3.3.9 onwards.

SQLite added `sqlite3_prepare_v2
<https://sqlite.org/c3ref/prepare.html>`_ API. The net effect of this
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

:c:func:`PyErr_WriteUnraisable` was used for errors in destructors.
Unfortunately it is almost completely useless, merely printing ``str``
of the object and exception.  This doesn't help in finding where in
your code the issue arose so you could fix it. An internal APSW
implementation generates a traceback and calls :func:`sys.excepthook`,
the default implementation of which prints the exception and the
traceback to sys.stderr.

  .. Note:: The line number reported in the traceback is often off by
            1. This is because the destructors run "between" lines of
            code and so the following line is reported as the current
            location.

Authorizer codes *SQLITE_CREATE_VTABLE*,
*SQLITE_DROP_VTABLE* and *SQLITE_FUNCTION* added.

SQLite `extended result codes
<https://www.sqlite.org/rescode.html#extrc>`_ are
available - see :ref:`exceptions` for more detail.

:data:`apsw.connection_hooks` added so you can easily register functions,
virtual tables or similar items with each Connection as it is created.

Added :ref:`mapping dicts <sqliteconstants>` which makes it easy to
map the various constants between strings and ints.

3.3.7-r1
========

Never released as 3.3.8 came along.

You can use this release against SQLite 3.3.7. There were no changes
in the SQLite 3.3.6 API from 3.3.5. In SQLite 3.3.7 an API was added
that allowed removing a chunk of duplicate code. Also added were
`Virtual Tables <https://www.sqlite.org/vtab.html>`_
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
the step function.  (:c:func:`PyErr_WriteUnraisable(NULL)
<PyErr_WriteUnraisable>` crashed on some versions of Python but not
others.)

SQLite added several functions for returning metadata about result
column sets. You have to compile SQLite with
*SQLITE_ENABLE_COLUMN_METADATA* to get them. This is not the
default for SQLite. I don't believe these are generally useful except
in some corner cases and so they aren't wrapped. However please shout
if you do need them.  Note that :meth:`Cursor.get_description` will
already give you generally useful information. (Also see the `pragmas
<https://sqlite.org/pragma.html>`_)

The test code has been converted into using the unittest module. Run
:command:`python tests.py -v` to get the tests run. There should be no
errors.

Updated code to work correctly with new :c:type:`Py_ssize_t` introduced
in Python 2.5. See 64 bit hosts, Python 2.5+ for
more details on how Python and SQLite handle 64 bit sized items.

The following functions were added to SQLite and are wrapped. They are
all functions defined on the :class:`Connection` object or :mod:`apsw`
module:

* `sqlite3_update_hook <https://sqlite.org/c3ref/update_hook.html>`_
* `sqlite3_rollback_hook <https://sqlite.org/c3ref/commit_hook.html>`_
* `sqlite3_enable_shared_cache <https://sqlite.org/c3ref/enable_shared_cache.html>`_
* `sqlite3_get_autocommit <https://sqlite.org/c3ref/get_autocommit.html>`_
* `sqlite3_profile <https://sqlite.org/c3ref/profile.html>`_ This
  callback is run at the end of each statement execution telling you how
  long it took.

3.2.7-r1
========

You can use this release against any release of SQLite 3.

SQLite 3.2.7 has several bug fixes. The undocumented experimental
function *sqlite3_profile* was added, but it not present in apsw
yet.

The author of pysqlite has improved it considerably since APSW was
originally written. The differences section has been updated to
reflect those improvements in pysqlite.

*SQLITE_INTERNAL* and *SQLITE_NOTFOUND* error codes are
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

SQLite 3.2.2 API removed *sqlite3_global_recover*. That function
was not wrapped in APSW. Note that SQLite 3.2.2 contains a bug fix
that applies when you use 64 bit integer primary keys (32 bit ints are
fine).

3.2.1-r1
========

You can use this release against any release of SQLite 3.

There are no changes in APSW except to correct an error in the example
code (collations are registered against the connection not the cursor)

SQLite 3.2.1 had one addition in the stable C API, which was a new
function named *sqlite3_global_recover*. That function is not
applicable for wrapping in APSW.

3.1.3-r1
========

You can use this release against any release of SQLite 3.

The text string returned by apsw.Error used to say
"apsw.APSWException" and has been changed to "apsw.Error".  This is
purely cosmetic and helps make clear what the class is. (The old
string was what the original class name was in an earlier version of
the code.)

Added *SQLITE_ALTER_TABLE* and *SQLITE_REINDEX*
constants for the authorizer function.  (These constants were
introduced in SQLite 3.1.3).

Changed various C++-isms into standard C (eg // comments and the
placing of some *CHECK_THREAD* macro calls).

Added module level function :meth:`~apsw.apsw_version` which returns
the version of APSW.

SQLite 3.1.3 had no changes in the stable C API other than what is
mentioned above. There were some new experimental functions added
which are not currently documented on the SQLite website, which are
not wrapped by APSW.  Please contact me if you believe they will
remain in SQLite and you would like them wrapped:

* *sqlite3_sleep* An alternative function which sleeps for a
  specified number of milliseconds can be provided. By default SQLite
  just uses the standard operating system call.
* *sqlite3_expired* This function is internal to statement
  execution. It would apply to the implementation of
  :meth:`Cursor.executemany` and could in theory provide a marginal
  improvement in performance.
* A global variable *sqlite3_temp_directory* can be used before
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

.. _renaming:

Renaming
--------

Early APSW exposed some method and attribute names not complying with
Python naming conventions as documented in :pep:`8`.  For example
``exceptionfor`` instead of ``exception_for``.  This has now been
corrected with the compliant names being documented and used in
examples.  The old names are still available maintaining
:ref:`backwards compatibility <backcompat>`.

The change happened in version 3.43.2.0.

.. include:: ../doc/renames.rstgen