====
apsw
====

-----------------------------------------------------------------------------
A terminal interface to the APSW SQLite shell modelled after the SQLite shell
-----------------------------------------------------------------------------

:version: apsw 3.50.0.0
:date: 30 May 2025
:manual section: 1
:manual group: General Commands Manual

SYNOPSIS
========

``apsw``  ``[OPTIONS]`` ``FILENAME`` ``[SQL|CMD]`` ``[SQL|CMD]...``

SUMMARY
=======

``apsw`` is a terminal interface to the APSW Python library that wraps
the SQLite library.  It is interoperable with the SQLite shell
``sqlite3`` but with the following improvements in interactive mode:

* Comprehensive tab completion
* Attractive Unicode aware table layout
* Colour

The following commands:

* ``.dump`` has nicer output including more metadata like user_version
* Multiple connections open at the same time, and you can switch
  amongst them
* ``.autoimport`` imports all flavours of comma and tab separated
  files doing a first pass to determine the type of each column
  including various date formats
* ``.find`` to search all tables for values or text
* ``.py`` command to run one line of Python code, or drop into a
  Python REPL

OPTIONS
=======

.. options-begin:

``FILENAME`` is the name of a SQLite database. A new database is
created if the file does not exist. If omitted or an empty
string then an in-memory database is created.

``-init`` ``filename``
    read/process named file
    
``-echo``
    print commands before execution
    
``-[no]header``
    turn headers on or off
    
``-bail``
    stop after hitting the first error
    
``-interactive``
    force interactive I/O (command editing and colour)
    
``-batch``
    force batch I/O (no banners or editing)
    
``-column``
    set output mode to ``column``
    
``-csv``
    set output mode to ``csv``
    
``-html``
    set output mode to ``html``
    
``-line``
    set output mode to ``line``
    
``-list``
    set output mode to ``list``
    
``-python``
    set output mode to ``python``
    
``-jsonl``
    set output mode to ``jsonl``
    
``-separator`` ``'x'``
    set output field separator (|)
    
``-nullvalue`` ``'text'``
    set text string for ``NULL`` values
    
``-version``
    show SQLite version
    
``-encoding`` ``'name'``
    the encoding to use for files opened via .import, .read & .output
    
``-nocolour``
    disables interactive colour output, as does setting ``NO_COLOR`` environment
    variable
    


.. options-end:

SQL
===

Issue SQL commands.  You will get a continuation prompt for incomplete
SQL.

The prompt can be changed via the ``.prompt`` command.

Use the ``.mode`` command to change how the results are displayed,
and ``.help mode`` to see the list of output modes.

COMMANDS OVERVIEW
=================

Commands begin with a ``.`` (dot, period) so they can be distinguished
from SQL.  You can use ``.help`` to see a list of all commands and
``.help command`` to see detailed help for the specific ``command``.

.. commands-begin:

``.autoimport`` ``FILENAME`` ``?TABLE?``
    Imports filename creating a table and automatically working out separators and
    data types (alternative to .import command)
    
``.backup`` ``?DB?`` ``FILE``
    Backup ``DB`` (default "main") to ``FILE``
    
``.bail`` ``ON|OFF``
    Stop after hitting an error (default ``OFF``)
    
``.cd`` ``?DIR?``
    Changes current directory
    
``.changes`` ``ON|OFF``
    Show changes from last SQL and total changes (default ``OFF``)
    
``.close``
    Closes the current database
    
``.colour`` ``SCHEME``
    Selects a colour scheme from default, off
    
``.connection`` ``?NUMBER?``
    List connections, or switch active connection
    
``.databases``
    Lists names and files of attached databases
    
``.dbconfig`` ``?NAME`` ``VALUE?``
    Show all dbconfig, or set a specific one
    
``.dbinfo`` ``?NAME?``
    Shows summary and file information about the database
    
``.dump`` ``?TABLE?`` ``[TABLE...]``
    Dumps all or specified tables in SQL text format
    
``.echo`` ``ON|OFF``
    If ``ON`` then each SQL statement or command is printed before execution
    (default ``OFF``)
    
``.encoding`` ``ENCODING``
    Set the encoding used for new files opened via .output and imports
    
``.exceptions`` ``ON|OFF``
    If ``ON`` then detailed tracebacks are shown on exceptions (default ``OFF``)
    
``.exit`` ``?CODE?``
    Exit this program with optional exit code
    
``.find`` ``value`` ``?TABLE?``
    Searches all columns of all tables for a value
    
``.ftsq`` ``TABLE`` ``query``
    Issues the query against the named FTS5 table
    
``.header(s)`` ``ON|OFF``
    Display the column names in output (default ``OFF``)
    
``.help`` ``?COMMAND?``
    Shows list of commands and their usage
    
``.import`` ``FILE`` ``TABLE``
    Imports separated data from ``FILE`` into ``TABLE``
    
``.indices`` ``TABLE``
    Lists all indices on table ``TABLE``
    
``.load`` ``FILE`` ``?ENTRY?``
    Loads a SQLite extension library
    
``.log`` ``ON|OFF``
    Shows SQLite log messages (default off)
    
``.mode`` ``MODE`` ``?OPTIONS?``
    Sets output mode to one of box column columns csv html insert json jsonl line
    lines list python qbox table tabs tcl
    
``.nullvalue`` ``STRING``
    Print ``STRING`` in place of null values
    
``.open`` ``?OPTIONS?`` ``?FILE?``
    Opens a database connection
    
``.output`` ``FILENAME``
    Send output to ``FILENAME`` (or stdout)
    
``.pages`` ``SCOPE``
    Shows page usage summary in human units
    
``.pages-svg`` ``?OUTFILENAME?``
    Shows space usage in a graphic
    
``.parameter`` ``CMD`` ``...``
    Maintain named bindings you can use in your queries.
    
``.print`` ``STRING``
    print the literal ``STRING``
    
``.prompt`` ``MAIN`` ``?CONTINUE?``
    Changes the prompts for first line and continuation lines
    
``.py`` ``?PYTHON?``
    Starts a python ``REPL`` or runs the Python statement provided
    
``.read`` ``FILENAME``
    Processes SQL and commands in ``FILENAME`` (or Python if ``FILENAME`` ends with
    .py)
    
``.restore`` ``?DB?`` ``FILE``
    Restore database from ``FILE`` into ``DB`` (default "main")
    
``.schema`` ``?TABLE?`` ``[TABLE...]``
    Shows SQL for table
    
``.separator`` ``STRING``
    Change separator for output mode and .import
    
``.shell`` ``CMD`` ``ARGS...``
    Run ``CMD`` ``ARGS`` in a system shell
    
``.show``
    Show the current values for various settings.
    
``.tables`` ``?PATTERN?``
    Lists names of tables matching ``LIKE`` pattern
    
``.timeout`` ``MS``
    Try opening locked tables for ``MS`` milliseconds
    
``.timer`` ``ON|OFF``
    Control printing of time and resource usage after each query
    
``.version``
    Displays SQLite, APSW, and Python version information
    
``.vfsinfo``
    Shows detailed information about the VFS for the database
    
``.vfslist``
    Shows detailed information about all the VFS available
    
``.vfsname``
    VFS name for database, or attached names
    
``.width`` ``NUM`` ``NUM`` ``...``
    Set the column widths for "column" mode
    

COMMANDS
========

.autoimport FILENAME ?TABLE?
----------------------------

Imports filename creating a table and automatically working out separators and
data types (alternative to .import command)

The import command requires that you precisely pre-setup the table and schema,
and set the data separators (eg commas or tabs).  This command figures out the
separator and csv dialect automatically.  There must be at least two columns and
two rows.

If the table is not specified then the basename of the file will be used.

Additionally the type of the contents of each column is also deduced - for
example if it is a number or date.  Empty values are turned into nulls.  Dates
are normalized into ``YYYY``-``MM``-``DD`` format and DateTime are normalized
into ISO8601 format to allow easy sorting and searching.  4 digit years must be
used to detect dates.  US (swapped day and month) versus rest of the world is
also detected providing there is at least one value that resolves the ambiguity.

Care is taken to ensure that columns looking like numbers are only treated as
numbers if they do not have unnecessary leading zeroes or plus signs.  This is
to avoid treating phone numbers and similar number like strings as integers.

This command can take quite some time on large files as they are effectively
imported twice.  The first time is to determine the format and the types for
each column while the second pass actually imports the data.


.backup ?DB? FILE
-----------------

Backup ``DB`` (default "main") to ``FILE``

Copies the contents of the current database to ``FILE`` overwriting whatever was
in ``FILE``.  If you have attached databases then you can specify their name
instead of the default of "main".

The backup is done at the page level - SQLite copies the pages as is.  There is
no round trip through SQL code.


.bail ON|OFF
------------

Stop after hitting an error (default ``OFF``)

If an error is encountered while processing commands or SQL then exit.  (Note
this is different than SQLite shell which only exits for errors in SQL.)


.cd ?DIR?
---------

Changes current directory

If no directory supplied then change to home directory


.changes ON|OFF
---------------

Show changes from last SQL and total changes (default ``OFF``)

After executing SQL that makes changes, the number of affected rows is displayed
as well as a running count of all changes.


.close
------

Closes the current database

Use .open to open a database, or .connection to switch to another connection


.colour SCHEME
--------------

Selects a colour scheme from default, off

If using a colour terminal in interactive mode then output is automatically
coloured to make it more readable.  Use ``off`` to turn off colour, and no name
or ``default`` for the default colour scheme.


.connection ?NUMBER?
--------------------

List connections, or switch active connection

This covers all connections, not just those started in this shell.  Closed
connections are not shown.

For each connection, its index for switching active connection, (VFS used),
"filename", and open flags are shown.


.databases
----------

Lists names and files of attached databases

.dbconfig ?NAME VALUE?
----------------------

Show all dbconfig, or set a specific one

With no arguments lists all settings.  Supply a name and integer value to
change.  For example::

    .dbconfig enable_fkey 1


.dbinfo ?NAME?
--------------

Shows summary and file information about the database

This includes the numbers of tables, indices etc, as well as fields from the
file headers.

``NAME`` defaults to ``main``, and can be the attached name of a database.


.dump ?TABLE? [TABLE...]
------------------------

Dumps all or specified tables in SQL text format

The table name is treated as like pattern so you can use ``%`` as a wildcard.
You can use dump to make a text based backup of the database.  It is also useful
for comparing differences or making the data available to other databases.
Indices and triggers for the table(s) are also dumped.  Finally views matching
the table pattern name are dumped.

Note that if you are dumping virtual tables such as used by the FTS5 module then
they may use other tables to store information.  For example if you create a
FTS5 table named *recipes* then it also creates *recipes_content*,
*recipes_segdir* etc.  Consequently to dump this example correctly use::

   .dump recipes recipes_%

If the database is empty or no tables/views match then there is no output.


.echo ON|OFF
------------

If ``ON`` then each SQL statement or command is printed before execution
(default ``OFF``)

The SQL statement or command is sent to error output so that it is not
intermingled with regular output.


.encoding ENCODING
------------------

Set the encoding used for new files opened via .output and imports

SQLite and APSW/Python work internally using Unicode and characters. Files
however are a sequence of bytes.  An encoding describes how to convert between
bytes and characters.  The default encoding is utf8 and that is generally the
best value to use when other programs give you a choice.

You can also specify an error handler.  For example `cp437:replace` will use
code page 437 and any Unicode codepoints not present in cp437 will be replaced
(typically with something like a question mark).  Other error handlers include
`ignore`, `strict` (default) and `xmlcharrefreplace`.

This command affects files opened after setting the encoding as well as imports.


.exceptions ON|OFF
------------------

If ``ON`` then detailed tracebacks are shown on exceptions (default ``OFF``)

Normally when an exception occurs the error string only is displayed.  However
it is sometimes useful to get a full traceback.  An example would be when you
are developing virtual tables and using the shell to exercise them.  In addition
to displaying each stack frame, the local variables within each frame are also
displayed.


.exit ?CODE?
------------

Exit this program with optional exit code

.find value ?TABLE?
-------------------

Searches all columns of all tables for a value

The find command helps you locate data across your database for example to find
a string or any references to an id.

You can specify a like pattern to limit the search to a subset of tables (eg
specifying ``CUSTOMER%`` for all tables beginning with ``CUSTOMER``).

The value will be treated as a string and/or integer if possible.  If value
contains ``%`` or ``_`` then it is also treated as a like pattern.

This command can take a long time to execute needing to scan all of the relevant
tables, rows, and columns.


.ftsq TABLE query
-----------------

Issues the query against the named FTS5 table

The top 20 results are shown.  Text after the table name is used exactly as the
query - do not extra shell quote it.


.header(s) ON|OFF
-----------------

Display the column names in output (default ``OFF``)

.help ?COMMAND?
---------------

Shows list of commands and their usage

If ``COMMAND`` is specified then shows detail about that ``COMMAND``. ``.help
all`` will show detailed help about all commands.


.import FILE TABLE
------------------

Imports separated data from ``FILE`` into ``TABLE``

Reads data from the file into the named table using the current separator and
encoding.  For example if the separator is currently a comma then the file
should be CSV (comma separated values).

All values read in are supplied to SQLite as strings.  If you want SQLite to
treat them as other types then declare your columns appropriately.  For example
declaring a column ``REAL`` will result in the values being stored as floating
point if they can be safely converted.

Another alternative is to create a temporary table, insert the values into that
and then use casting.::

  CREATE TEMPORARY TABLE import(a,b,c);
  .import filename import
  CREATE TABLE final AS SELECT cast(a as BLOB), cast(b as INTEGER),
       cast(c as CHAR) from import;
  DROP TABLE import;

You can also get more sophisticated using the SQL ``CASE`` operator.  For
example this will turn zero length strings into null::

  SELECT CASE col WHEN '' THEN null ELSE col END FROM ...


.indices TABLE
--------------

Lists all indices on table ``TABLE``

.load FILE ?ENTRY?
------------------

Loads a SQLite extension library

Note: Extension loading may not be enabled in the SQLite library version you are
using.

By default sqlite3_extension_init is called in the library but you can specify
an alternate entry point.

If you get an error about the extension not being found you may need to
explicitly specify the directory.  For example if it is in the current directory
then use::

  .load ./extension.so


.log ON|OFF
-----------

Shows SQLite log messages (default off)

.mode MODE ?OPTIONS?
--------------------

Sets output mode to one of box column columns csv html insert json jsonl line
lines list python qbox table tabs tcl

box: Outputs using line drawing and auto sizing columns

columns: Items left aligned in space padded columns. They are truncated if they
do not fit. If the width hasn't been specified for a column then 10 is used
unless the column name (header) is longer in which case that width is used. Use
the .width command to change column sizes.

csv: Items in csv format (comma separated). Use tabs mode for tab separated. You
can use the .separator command to use a different one after switching mode. A
separator of comma uses double quotes for quoting while other separators do not
do any quoting. The Python csv library used for this only supports single
character separators.

html: HTML table style

insert: Lines as SQL insert statements. The table name is "table" unless you
specified a different one as the second parameter to the .mode command.

json: Output a JSON array. Blobs are output as base64 encoded strings.

jsonl: Output as JSON objects, newline separated. Blobs are output as base64
encoded strings.

lines: One value per line in the form 'column = value' with a blank line between
rows.

list: All items on one line with separator

python: Tuples in Python source form for each row

qbox: Outputs using line drawing and auto sizing columns quoting values

table: Outputs using ascii line drawing and strongly sanitized text

tcl: Outputs TCL/C style strings using current separator


.nullvalue STRING
-----------------

Print ``STRING`` in place of null values

This affects textual output modes like column and list and sets how SQL null
values are shown.  The default is a zero length string.  Insert mode and dumps
are not affected by this setting.  You can use double quotes to supply a zero
length string.  For example::

  .nullvalue ""         # the default
  .nullvalue <NULL>     # rather obvious
  .nullvalue " \\t "     # A tab surrounded by spaces


.open ?OPTIONS? ?FILE?
----------------------

Opens a database connection

Options are:

--wipe         Closes any existing connections in this process referring to
               the same file and deletes the database file, journals etc
               before opening

--vfs VFS      Which vfs to use when opening

--flags FLAGS  Open flags to use, in lower or upper case.  Use | to
               combine. Default is  READWRITE|CREATE|URI

If ``FILE`` is omitted then a memory database is opened


.output FILENAME
----------------

Send output to ``FILENAME`` (or stdout)

If the ``FILENAME`` is ``stdout`` then output is sent to standard output from
when the shell was started.  The file is opened using the current encoding
(change with ``encoding`` command).


.pages SCOPE
------------

Shows page usage summary in human units

``SCOPE`` is a number 0, 1, or 2.

0 - shows the database as a whole. 1 - groups by each table, including its
indices.  2 - shows each table and index separately.


.pages-svg ?OUTFILENAME?
------------------------

Shows space usage in a graphic

If you do not specify a filename, then a temporary file is created and the
browser invoked to show it.


.parameter CMD ...
------------------

Maintain named bindings you can use in your queries.

Specify a subcommand::

   list            -- shows current bindings
   clear           -- deletes all bindings
   unset NAME      -- deletes named binding
   set NAME VALUE  -- sets binding to VALUE

The value must be a valid SQL literal or expression.  For example `3` will be an
integer 3 while ``'3'`` will be a string.

Example:::

  .parameter set floor 10.99
  .parameter set text 'Acme''s Glove'
  SELECT * FROM sales WHERE price > $floor AND description != $text;


.print STRING
-------------

print the literal ``STRING``

If more than one argument is supplied then they are printed space separated.
You can use backslash escapes such as \\n and \\t.


.prompt MAIN ?CONTINUE?
-----------------------

Changes the prompts for first line and continuation lines

The default is to print 'sqlite> ' for the main prompt where you can enter a dot
command or a SQL statement.  If the SQL statement is not complete then you are
prompted for more using the continuation prompt which defaults to ' ..> '.
Example::

  .prompt "command> " "more command> "

You can use backslash escapes such as \\n and \\t.


.py ?PYTHON?
------------

Starts a python ``REPL`` or runs the Python statement provided

The namespace provided includes ``apsw`` for the module, ``shell`` for this
shell and ``db`` for the current database.

Using the .output command does not affect output from this command.  You can
write to `shell.stdout` and `shell.stderr`.


.read FILENAME
--------------

Processes SQL and commands in ``FILENAME`` (or Python if ``FILENAME`` ends with
.py)

Treats the specified file as input (a mixture or SQL and/or dot commands).  If
the filename ends in .py then it is treated as Python code instead.

For Python code the symbol ``db`` refers to the current database, ``shell``
refers to the instance of the shell and ``apsw`` is the apsw module.


.restore ?DB? FILE
------------------

Restore database from ``FILE`` into ``DB`` (default "main")

Copies the contents of ``FILE`` to the current database (default "main"). The
backup is done at the page level - SQLite copies the pages as is.  There is no
round trip through SQL code.


.schema ?TABLE? [TABLE...]
--------------------------

Shows SQL for table

If you give one or more tables then their schema is listed (including indices).
If you don't specify any then all schemas are listed. ``TABLE`` is a like
pattern so you can use ``%`` for wildcards.


.separator STRING
-----------------

Change separator for output mode and .import

You can use quotes and backslashes.  For example to set the separator to space
tab space you can use::

  .separator " \\t "

The setting is automatically changed when you switch to csv or tabs output mode.
You should also set it before doing an import (ie , for CSV and \\t for TSV).


.shell CMD ARGS...
------------------

Run ``CMD`` ``ARGS`` in a system shell

Note that output goes to the process standard output, not whatever the shell
.output command has configured.


.show
-----

Show the current values for various settings.

.tables ?PATTERN?
-----------------

Lists names of tables matching ``LIKE`` pattern

This also returns views.


.timeout MS
-----------

Try opening locked tables for ``MS`` milliseconds

If a database is locked by another process SQLite will keep retrying.  This sets
how many thousandths of a second it will keep trying for.  If you supply zero or
a negative number then all busy handlers are disabled.


.timer ON|OFF
-------------

Control printing of time and resource usage after each query

The values displayed are in seconds when shown as floating point or an absolute
count.  Only items that have changed since starting the query are shown.  On
non-Windows platforms considerably more information can be shown.  SQLite
statistics are also included.


.version
--------

Displays SQLite, APSW, and Python version information

.vfsinfo
--------

Shows detailed information about the VFS for the database

.vfslist
--------

Shows detailed information about all the VFS available

.vfsname
--------

VFS name for database, or attached names

.width NUM NUM ...
------------------

Set the column widths for "column" mode

In "column" output mode, each column is a fixed width with values truncated to
fit.  Specify new widths using this command.  Use a negative number to right
justify and zero for default column width.



.. commands-end:

SEE ALSO
========

https://rogerbinns.github.io/apsw/

   APSW HTML documentation

https://github.com/rogerbinns/apsw

   APSW source repository

https://sqlite.org/cli.html

   SQLite's terminal interface

.. copyright-begin:

COPYRIGHT AND LICENSE
=====================

Copyright (C) 2004-2025 `Roger Binns <https://www.rogerbinns.com>`__


This software is provided 'as-is', without any express or implied
warranty. In no event will the authors be held liable for any damages
arising from the use of this software.

Permission is granted to anyone to use this software for any purpose,
including commercial applications, and to alter it and redistribute it
freely, subject to the following restrictions:

* The origin of this software must not be misrepresented; you must not
  claim that you wrote the original software. If you use this software
  in a product, an acknowledgment in the product documentation would be
  appreciated but is not required.

* Altered source versions must be plainly marked as such, and must not
  be misrepresented as being the original software.

* This notice may not be removed or altered from any source
  distribution.


Alternatively you may strike the license above and use it under any
OSI approved open source license such as those listed at
https://opensource.org/licenses/alphabetical

SPDX-License-Identifier: any-OSI

.. copyright-end:
