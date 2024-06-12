.. currentmodule:: apsw

.. _shell:

Shell
*****

The shell provides a convenient way for you to interact with SQLite,
perform administration, and supply SQL for execution.  It is modelled
after the `shell that comes with SQLite
<https://sqlite.org/cli.html>`__ which doesn't interoperate with Python.

Notable improvements include:

* You can invoke this shell programmatically - very useful for
  development and debugging
* Output is in colour
* Tab completion is available
* Nicer text dump output, including metadata like user_version
* All open APSW :class:`connections <Connection>` are available
  and you can switch between them
* :ref:`.py command <shell-cmd-py>` gets you a Python REPL or
  runs one line of Python code
* Very useful :ref:`autoimport <shell-cmd-autoimport>` and :ref:`find
  <shell-cmd-find>` commands

Notes
=====

To interrupt the shell press Control-C. (On Windows if you press
Control-Break then the program will be terminated.)

For Windows users you won't have command line editing and completion
unless you install a :mod:`readline module <readline>`. You can pip
install `pyreadline3 <https://pypi.org/project/pyreadline3/>`__ to get
full functionality.

For Windows users, the builtin console support for colour is used.  It
is enabled by default in current versions of Windows, and a registry
key enables for older versions `(details)
<https://github.com/kiedtl/winfetch/wiki/ANSI-Colors>`__.

Command Line Usage
==================

You can use the shell directly from the command line.

.. usage-begin:

.. code-block:: text

  Usage: python3 -m apsw [OPTIONS] FILENAME [SQL|CMD] [SQL|CMD]...
  FILENAME is the name of a SQLite database. A new database is
  created if the file does not exist. If omitted or an empty
  string then an in-memory database is created.
  OPTIONS include:
  
     -init filename       read/process named file
     -echo                print commands before execution
     -[no]header          turn headers on or off
     -bail                stop after hitting the first error
     -interactive         force interactive I/O (command editing and colour)
     -batch               force batch I/O (no banners or editing)
     -column              set output mode to 'column'
     -csv                 set output mode to 'csv'
     -html                set output mode to 'html'
     -line                set output mode to 'line'
     -list                set output mode to 'list'
     -python              set output mode to 'python'
     -jsonl               set output mode to 'jsonl'
     -separator 'x'       set output field separator (|)
     -nullvalue 'text'    set text string for NULL values
     -version             show SQLite version
     -encoding 'name'     the encoding to use for files
                          opened via .import, .read & .output
     -nocolour            disables interactive colour output
  

.. usage-end:

Programmatic Usage
==================

You can also use the shell programmatically (or even interactively and
programmatically at the same time).  See the :ref:`example
<example_shell>` for using the API.

To quickly invoke the shell similar to the Python debugger, do this::

  apsw.shell.Shell(db=database_of_interest).cmdloop()

You can use :ref:`.connection <shell-cmd-connection>` to switch
amongst connections.  Press Control-D at the prompt (Control-Z on
Windows) will exit the shell.

.. _shell-commands:

Commands
========

In addition to executing SQL, these are the commands available with
their description.  Commands are distinguished from SQL by having a
leading ``.`` (period) - for example::

  .help
  .mode qbox
  .find winchester

.. help-begin:

.. hlist::
  :columns: 3

  * :ref:`autoimport <shell-cmd-autoimport>`
  * :ref:`backup <shell-cmd-backup>`
  * :ref:`bail <shell-cmd-bail>`
  * :ref:`cd <shell-cmd-cd>`
  * :ref:`changes <shell-cmd-changes>`
  * :ref:`close <shell-cmd-close>`
  * :ref:`colour <shell-cmd-colour>`
  * :ref:`connection <shell-cmd-connection>`
  * :ref:`databases <shell-cmd-databases>`
  * :ref:`dbconfig <shell-cmd-dbconfig>`
  * :ref:`dbinfo <shell-cmd-dbinfo>`
  * :ref:`dump <shell-cmd-dump>`
  * :ref:`echo <shell-cmd-echo>`
  * :ref:`encoding <shell-cmd-encoding>`
  * :ref:`exceptions <shell-cmd-exceptions>`
  * :ref:`exit <shell-cmd-exit>`
  * :ref:`find <shell-cmd-find>`
  * :ref:`header <shell-cmd-header>`
  * :ref:`help <shell-cmd-help>`
  * :ref:`import <shell-cmd-import>`
  * :ref:`indices <shell-cmd-indices>`
  * :ref:`load <shell-cmd-load>`
  * :ref:`log <shell-cmd-log>`
  * :ref:`mode <shell-cmd-mode>`
  * :ref:`nullvalue <shell-cmd-nullvalue>`
  * :ref:`open <shell-cmd-open>`
  * :ref:`output <shell-cmd-output>`
  * :ref:`parameter <shell-cmd-parameter>`
  * :ref:`print <shell-cmd-print>`
  * :ref:`prompt <shell-cmd-prompt>`
  * :ref:`py <shell-cmd-py>`
  * :ref:`read <shell-cmd-read>`
  * :ref:`restore <shell-cmd-restore>`
  * :ref:`schema <shell-cmd-schema>`
  * :ref:`separator <shell-cmd-separator>`
  * :ref:`shell <shell-cmd-shell>`
  * :ref:`show <shell-cmd-show>`
  * :ref:`tables <shell-cmd-tables>`
  * :ref:`timeout <shell-cmd-timeout>`
  * :ref:`timer <shell-cmd-timer>`
  * :ref:`version <shell-cmd-version>`
  * :ref:`vfsinfo <shell-cmd-vfsinfo>`
  * :ref:`vfslist <shell-cmd-vfslist>`
  * :ref:`vfsname <shell-cmd-vfsname>`
  * :ref:`width <shell-cmd-width>`

.. _shell-cmd-autoimport:
.. index::
    single: autoimport (Shell command)

autoimport FILENAME ?TABLE?
---------------------------

*Imports filename creating a table and automatically working out separators and data types (alternative to .import command)*

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

.. _shell-cmd-backup:
.. index::
    single: backup (Shell command)

backup ?DB? FILE
----------------

*Backup DB (default "main") to FILE*

Copies the contents of the current database to ``FILE`` overwriting whatever was
in ``FILE``.  If you have attached databases then you can specify their name
instead of the default of "main".

The backup is done at the page level - SQLite copies the pages as is.  There is
no round trip through SQL code.

.. _shell-cmd-bail:
.. index::
    single: bail (Shell command)

bail ON|OFF
-----------

*Stop after hitting an error (default OFF)*

If an error is encountered while processing commands or SQL then exit.  (Note
this is different than SQLite shell which only exits for errors in SQL.)

.. _shell-cmd-cd:
.. index::
    single: cd (Shell command)

cd ?DIR?
--------

*Changes current directory*

If no directory supplied then change to home directory

.. _shell-cmd-changes:
.. index::
    single: changes (Shell command)

changes ON|OFF
--------------

*Show changes from last SQL and total changes (default OFF)*

After executing SQL that makes changes, the number of affected rows is displayed
as well as a running count of all changes.

.. _shell-cmd-close:
.. index::
    single: close (Shell command)

close
-----

*Closes the current database*

Use .open to open a database, or .connection to switch to another connection

.. _shell-cmd-colour:
.. index::
    single: colour (Shell command)

colour SCHEME
-------------

*Selects a colour scheme from default, off*

If using a colour terminal in interactive mode then output is automatically
coloured to make it more readable.  Use ``off`` to turn off colour, and no name
or ``default`` for the default colour scheme.

.. _shell-cmd-connection:
.. index::
    single: connection (Shell command)

connection ?NUMBER?
-------------------

*List connections, or switch active connection*

This covers all connections, not just those started in this shell.  Closed
connections are not shown.

.. _shell-cmd-databases:
.. index::
    single: databases (Shell command)

databases
---------

*Lists names and files of attached databases*

.. _shell-cmd-dbconfig:
.. index::
    single: dbconfig (Shell command)

dbconfig ?NAME VALUE?
---------------------

*Show all dbconfig, or set a specific one*

With no arguments lists all settings.  Supply a name and integer value to
change.  For example::

    .dbconfig enable_fkey 1

.. _shell-cmd-dbinfo:
.. index::
    single: dbinfo (Shell command)

dbinfo ?NAME?
-------------

*Shows summary and file information about the database*

This includes the numbers of tables, indices etc, as well as fields from the
files as returned by :func:`apsw.ext.dbinfo`.

``NAME`` defaults to ``main``, and can be the attached name of a database.

.. _shell-cmd-dump:
.. index::
    single: dump (Shell command)

dump ?TABLE? [TABLE...]
-----------------------

*Dumps all or specified tables in SQL text format*

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

.. _shell-cmd-echo:
.. index::
    single: echo (Shell command)

echo ON|OFF
-----------

*If ON then each SQL statement or command is printed before execution (default OFF)*

The SQL statement or command is sent to error output so that it is not
intermingled with regular output.

.. _shell-cmd-encoding:
.. index::
    single: encoding (Shell command)

encoding ENCODING
-----------------

*Set the encoding used for new files opened via .output and imports*

SQLite and APSW/Python work internally using Unicode and characters. Files
however are a sequence of bytes.  An encoding describes how to convert between
bytes and characters.  The default encoding is utf8 and that is generally the
best value to use when other programs give you a choice.

You can also specify an error handler.  For example `cp437:replace` will use
code page 437 and any Unicode codepoints not present in cp437 will be replaced
(typically with something like a question mark).  Other error handlers include
`ignore`, `strict` (default) and `xmlcharrefreplace`.

This command affects files opened after setting the encoding as well as imports.

.. _shell-cmd-exceptions:
.. index::
    single: exceptions (Shell command)

exceptions ON|OFF
-----------------

*If ON then detailed tracebacks are shown on exceptions (default OFF)*

Normally when an exception occurs the error string only is displayed.  However
it is sometimes useful to get a full traceback.  An example would be when you
are developing virtual tables and using the shell to exercise them.  In addition
to displaying each stack frame, the local variables within each frame are also
displayed.

.. _shell-cmd-exit:
.. index::
    single: exit (Shell command)

exit ?CODE?
-----------

*Exit this program with optional exit code*

.. _shell-cmd-find:
.. index::
    single: find (Shell command)

find value ?TABLE?
------------------

*Searches all columns of all tables for a value*

The find command helps you locate data across your database for example to find
a string or any references to an id.

You can specify a like pattern to limit the search to a subset of tables (eg
specifying ``CUSTOMER%`` for all tables beginning with ``CUSTOMER``).

The value will be treated as a string and/or integer if possible.  If value
contains ``%`` or ``_`` then it is also treated as a like pattern.

This command can take a long time to execute needing to scan all of the relevant
tables, rows, and columns.

.. _shell-cmd-header:
.. index::
    single: header(s) (Shell command)

header(s) ON|OFF
----------------

*Display the column names in output (default OFF)*

.. _shell-cmd-help:
.. index::
    single: help (Shell command)

help ?COMMAND?
--------------

*Shows list of commands and their usage*

If ``COMMAND`` is specified then shows detail about that ``COMMAND``. ``.help
all`` will show detailed help about all commands.

.. _shell-cmd-import:
.. index::
    single: import (Shell command)

import FILE TABLE
-----------------

*Imports separated data from FILE into TABLE*

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

.. _shell-cmd-indices:
.. index::
    single: indices (Shell command)

indices TABLE
-------------

*Lists all indices on table TABLE*

.. _shell-cmd-load:
.. index::
    single: load (Shell command)

load FILE ?ENTRY?
-----------------

*Loads a SQLite extension library*

Note: Extension loading may not be enabled in the SQLite library version you are
using.

By default sqlite3_extension_init is called in the library but you can specify
an alternate entry point.

If you get an error about the extension not being found you may need to
explicitly specify the directory.  For example if it is in the current directory
then use::

  .load ./extension.so

.. _shell-cmd-log:
.. index::
    single: log (Shell command)

log ON|OFF
----------

*Shows SQLite log messages (default off)*

.. _shell-cmd-mode:
.. index::
    single: mode (Shell command)

mode MODE ?OPTIONS?
-------------------

*Sets output mode to one of box column columns csv html insert json jsonl line lines list python qbox table tabs tcl*

box: Outputs using line drawing and auto sizing columns

columns: Items left aligned in space padded columns. They are truncated if they
do not fit. If the width hasn't been specified for a column then 10 is used
unless the column name (header) is longer in which case that width is used. Use
the .width command to change column sizes.

csv: Items in csv format (comma separated). Use tabs mode for tab separated. You
can use the .separator command to use a different one after switching mode.
``A`` separator of comma uses double quotes for quoting while other separators
do not do any quoting. The Python csv library used for this only supports single
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

.. _shell-cmd-nullvalue:
.. index::
    single: nullvalue (Shell command)

nullvalue STRING
----------------

*Print STRING in place of null values*

This affects textual output modes like column and list and sets how SQL null
values are shown.  The default is a zero length string.  Insert mode and dumps
are not affected by this setting.  You can use double quotes to supply a zero
length string.  For example::

  .nullvalue ""         # the default
  .nullvalue <NULL>     # rather obvious
  .nullvalue " \\t "     # A tab surrounded by spaces

.. _shell-cmd-open:
.. index::
    single: open (Shell command)

open ?OPTIONS? ?FILE?
---------------------

*Opens a database connection*

Options are:

--wipe     Closes any existing connections in this process referring to
           the same file  and deletes the database file, journals etc
           before opening

--vfs VFS  Which vfs to use when opening

If ``FILE`` is omitted then a memory database is opened

.. _shell-cmd-output:
.. index::
    single: output (Shell command)

output FILENAME
---------------

*Send output to FILENAME (or stdout)*

If the ``FILENAME`` is ``stdout`` then output is sent to standard output from
when the shell was started.  The file is opened using the current encoding
(change with ``encoding`` command).

.. _shell-cmd-parameter:
.. index::
    single: parameter (Shell command)

parameter CMD ...
-----------------

*Maintain named bindings you can use in your queries.*

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

.. _shell-cmd-print:
.. index::
    single: print (Shell command)

print STRING
------------

*print the literal STRING*

If more than one argument is supplied then they are printed space separated.
You can use backslash escapes such as \\n and \\t.

.. _shell-cmd-prompt:
.. index::
    single: prompt (Shell command)

prompt MAIN ?CONTINUE?
----------------------

*Changes the prompts for first line and continuation lines*

The default is to print 'sqlite> ' for the main prompt where you can enter a dot
command or a SQL statement.  If the SQL statement is not complete then you are
prompted for more using the continuation prompt which defaults to ' ..> '.
Example::

  .prompt "command> " "more command> "

You can use backslash escapes such as \\n and \\t.

.. _shell-cmd-py:
.. index::
    single: py (Shell command)

py ?PYTHON?
-----------

*Starts a python REPL or runs the Python statement provided*

The namespace provided includes ``apsw`` for the module, ``shell`` for this
shell and ``db`` for the current database.

Using the .output command does not affect output from this command.  You can
write to `shell.stdout` and `shell.stderr`.

.. _shell-cmd-read:
.. index::
    single: read (Shell command)

read FILENAME
-------------

*Processes SQL and commands in FILENAME (or Python if FILENAME ends with .py)*

Treats the specified file as input (a mixture or SQL and/or dot commands).  If
the filename ends in .py then it is treated as Python code instead.

For Python code the symbol ``db`` refers to the current database, ``shell``
refers to the instance of the shell and ``apsw`` is the apsw module.

.. _shell-cmd-restore:
.. index::
    single: restore (Shell command)

restore ?DB? FILE
-----------------

*Restore database from FILE into DB (default "main")*

Copies the contents of ``FILE`` to the current database (default "main"). The
backup is done at the page level - SQLite copies the pages as is.  There is no
round trip through SQL code.

.. _shell-cmd-schema:
.. index::
    single: schema (Shell command)

schema ?TABLE? [TABLE...]
-------------------------

*Shows SQL for table*

If you give one or more tables then their schema is listed (including indices).
If you don't specify any then all schemas are listed. ``TABLE`` is a like
pattern so you can use ``%`` for wildcards.

.. _shell-cmd-separator:
.. index::
    single: separator (Shell command)

separator STRING
----------------

*Change separator for output mode and .import*

You can use quotes and backslashes.  For example to set the separator to space
tab space you can use::

  .separator " \\t "

The setting is automatically changed when you switch to csv or tabs output mode.
You should also set it before doing an import (ie , for CSV and \\t for TSV).

.. _shell-cmd-shell:
.. index::
    single: shell (Shell command)

shell CMD ARGS...
-----------------

*Run CMD ARGS in a system shell*

Note that output goes to the process standard output, not whatever the shell
.output command has configured.

.. _shell-cmd-show:
.. index::
    single: show (Shell command)

show
----

*Show the current values for various settings.*

.. _shell-cmd-tables:
.. index::
    single: tables (Shell command)

tables ?PATTERN?
----------------

*Lists names of tables matching LIKE pattern*

This also returns views.

.. _shell-cmd-timeout:
.. index::
    single: timeout (Shell command)

timeout MS
----------

*Try opening locked tables for MS milliseconds*

If a database is locked by another process SQLite will keep retrying.  This sets
how many thousandths of a second it will keep trying for.  If you supply zero or
a negative number then all busy handlers are disabled.

.. _shell-cmd-timer:
.. index::
    single: timer (Shell command)

timer ON|OFF
------------

*Control printing of time and resource usage after each query*

The values displayed are in seconds when shown as floating point or an absolute
count.  Only items that have changed since starting the query are shown.  On
non-Windows platforms considerably more information can be shown.

.. _shell-cmd-version:
.. index::
    single: version (Shell command)

version
-------

*Displays SQLite, APSW, and Python version information*

.. _shell-cmd-vfsinfo:
.. index::
    single: vfsinfo (Shell command)

vfsinfo
-------

*Shows detailed information about the VFS for the database*

.. _shell-cmd-vfslist:
.. index::
    single: vfslist (Shell command)

vfslist
-------

*Shows detailed information about all the VFS available*

.. _shell-cmd-vfsname:
.. index::
    single: vfsname (Shell command)

vfsname
-------

*VFS name for database, or attached names*

.. _shell-cmd-width:
.. index::
    single: width (Shell command)

width NUM NUM ...
-----------------

*Set the column widths for "column" mode*

In "column" output mode, each column is a fixed width with values truncated to
fit.  Specify new widths using this command.  Use a negative number to right
justify and zero for default column width.

.. help-end:

Shell class
===========

This is the API should you want extend the shell with your own commands
and output modes. Not shown here are the functions that implement various
commands.  They are named after the command.  For example .exit is
implemented by command_exit.  You can add new commands by having your
subclass have the relevant functions.  The doc string of the function is
used by the help command.  Output modes work in a similar way.  For example
there is an output_html method and again doc strings are used by the help
function and you add more by just implementing an appropriately named
method.

Note that in addition to extending the shell, you can also use the
**.read** command supplying a filename with a **.py** extension.  You
can then `monkey patch <https://en.wikipedia.org/wiki/Monkey_patch>`__
the shell as needed.

.. automodule:: apsw.shell
     :synopsis: Convenient way for you to interact with SQLite
     :members:
     :undoc-members: