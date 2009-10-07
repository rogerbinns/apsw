.. currentmodule: apsw

.. _shell:

Shell
*****

The shell provides a convenient way for you to interact with SQLite,
perform administration and supply SQL for execution.  It is modelled
after the `shell that comes with SQLite
<http://www.sqlite.org/sqlite.html>`__ which requires separate
compilation and installation.

A number of the quirks and bugs in the SQLite shell are also
addressed.  It provides command line editing and completion.  You can
easily include it into your own program to provide SQLite interaction
and add your own commands.

Commands
========

In addition to executing SQL, these are the commands available:

.. help-begin:

.. code-block:: text

  
  .backup ?DB? FILE           Backup DB (default "main") to FILE
  .bail ON|OFF                Stop after hitting an error (default OFF)
  .databases                  Lists names and files of attached databases
  .dump ?TABLE? [TABLE...]    Dumps all or specified tables in SQL text format
  .echo ON|OFF                If ON then each SQL statement or command is printed
                              before execution (default OFF)
  .encoding ENCODING          Set the encoding used for new files opened via
                              .output and imports
  .exit                       Exit this program
  .explain ON|OFF             Set output mode suitable for explain (default OFF)
  .header(s) ON|OFF           Display the column names in output (default OFF)
  .help ?COMMAND?             Shows list of commands and their usage.  If COMMAND
                              is specified then shows detail about that COMMAND.
                              ('.help all' will show detailed help about all
                              commands.)
  .import FILE TABLE          Imports separated data from FILE into TABLE
  .indices TABLE              Lists all indices on table TABLE
  .load FILE ?ENTRY?          Loads a SQLite extension library
  .mode MODE ?TABLE?          Sets output mode to one of column csv html insert
                              line list python tabs tcl
  .nullvalue STRING           Print STRING in place of null values
  .output FILENAME            Send output to FILENAME (or stdout)
  .prompt MAIN ?CONTINUE?     Changes the prompts for first line and continuation
                              lines
  .quit                       Exit this program
  .read FILENAME              Processes SQL and commands in FILENAME (or Python if
                              FILENAME ends with .py)
  .restore ?DB? FILE          Restore database from FILE into DB (default "main")
  .schema ?TABLE? [TABLE...]  Shows SQL for table
  .separator STRING           Change separator for output mode and .import
  .show                       Show the current values for various settings.
  .tables ?PATTERN?           Lists names of tables matching LIKE pattern
  .timeout MS                 Try opening locked tables for MS milliseconds
  .timer ON|OFF               Control printing of time and resource usage after
                              each query
  .width NUM NUM ...          Set the column widths for "column" mode
  
  

.. help-end:

Command Line Usage
==================

You can use the shell directly from the command line.  Invoke it like
this::

  $ python -c "import apsw;apsw.main()"  [options and arguments]

The following command lne options are accepted:

.. usage-begin:

.. code-block:: text

  Usage: program [OPTIONS] FILENAME [SQL|CMD] [SQL|CMD]...
  FILENAME is the name of a SQLite database. A new database is
  created if the file does not exist.
  OPTIONS include:
     -init filename       read/process named file
     -echo                print commands before execution
     -[no]header          turn headers on or off
     -bail                stop after hitting an error
     -interactive         force interactive I/O
     -batch               force batch I/O
     -column              set output mode to 'column'
     -csv                 set output mode to 'csv'
     -html                set output mode to 'html'
     -line                set output mode to 'line'
     -list                set output mode to 'list'
     -separator 'x'       set output field separator (|)
     -nullvalue 'text'    set text string for NULL values
     -version             show SQLite version
     -encoding 'name'     treat terminal and text/files as this encoding
                          unless Python already detected it from environment
                          variables etc (default usually utf8)
  

.. usage-end:

Notes
=====

To interrupt the shell press Control-C. (On Windows if you press
Control-Break then the program will be instantly aborted.)

For Windows users you won't have command line editing and completion
unless you install a ` readline module
<http://docs.python.org/library/readline.html>`__.  Fortunately there
is one at http://ipython.scipy.org/moin/PyReadline/Intro which works.
However if the shell offers no completions it will start matching
filenames even if they make no sense in the context.

Shell class
===========

This is the API should you want to integrate the code into your shell.

.. autoclass:: apsw.Shell
     :members:
     :undoc-members: