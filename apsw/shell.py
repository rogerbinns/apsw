#!/usr/bin/env python3

from __future__ import annotations

# mypy: ignore-errors

import argparse
import base64
import code
import codecs
import contextlib
import csv
import dataclasses
import inspect
import io
import json
import os
import re
import shlex
import sys
import textwrap
import time
import traceback
from typing import Optional, TextIO

import apsw
import apsw.ext
import apsw.fts5


class Shell:
    """Implements a SQLite shell

    :param stdin: Where to read input from (default sys.stdin)
    :param stdout: Where to send output (default sys.stdout)
    :param stderr: Where to send errors (default sys.stderr)
    :param encoding: Default encoding for files opened/created by the
      Shell.  If you want stdin/out/err to use a particular encoding
      then you need to provide them `already configured
      <https://docs.python.org/3/library/codecs.html#codecs.open>`__ that way.
    :param args: This should be program arguments only (ie if
      passing in sys.argv do not include sys.argv[0] which is the
      program name.  You can also pass in None and then call
      :meth:`process_args` if you want to catch any errors
      in handling the arguments yourself.
    :param db: A existing :class:`~apsw.Connection` you wish to use

    Errors and diagnostics are only ever sent to error output
    (self.stderr) and never to the regular output (self.stdout).

    Shell commands begin with a dot (eg .help).  They are implemented
    as a method named after the command (eg command_help).  The method
    is passed one parameter which is the list of arguments to the
    command.

    Output modes are implemented by functions named after the mode (eg
    output_column for columns).

    When you request help the help information is automatically
    generated from the docstrings for the command and output
    functions.

    You should not use a Shell object concurrently from multiple
    threads.  It is one huge set of state information which would
    become inconsistent if used simultaneously.
    """

    class Error(Exception):
        """Class raised on errors.  The expectation is that the error
        will be displayed by the shell as text so there are no
        specific subclasses as the distinctions between different
        types of errors doesn't matter."""
        pass

    def __init__(self,
                 stdin: TextIO | None = None,
                 stdout: TextIO | None = None,
                 stderr: TextIO | None = None,
                 encoding: str = "utf8",
                 args: list[str] | None = None,
                 db: apsw.Connection | None = None):
        """Create instance, set defaults and do argument processing."""
        # The parameter doc has to be in main class doc as sphinx
        # ignores any described here
        self.exceptions = False
        self.history_file = "~/.sqlite_history"
        self.bindings = {}
        self._db = None
        self.dbfilename = None
        if db:
            self.db = db, db.filename
        else:
            self.db = None, None
        # keep a reference around allowing switching connections
        self.db_references = set()
        self.prompt = "sqlite> "
        self.moreprompt = "    ..> "
        self.separator = "|"
        self.bail = False
        self.changes = False
        self.echo = False
        self.timer = False
        self.header = False
        self.nullvalue = ""
        self.output : Callable = self.output_list
        self._output_table : str = self._fmt_sql_identifier("table")
        self.widths = []
        # do we truncate output in list mode?
        self.truncate = True
        # a stack of previous outputs
        self._output_stack = []

        # other stuff
        self.set_encoding(encoding)
        self.stdin = stdin or sys.stdin
        self._original_stdout = self.stdout = stdout or sys.stdout
        self.stderr = stderr or sys.stderr

        # default to box output
        if self._using_a_terminal():
            self.output = self.output_box
            self.box_options = {
                "quote": False,
                "string_sanitize": 0,
                "null": "NULL",
                "truncate": 4096,
                "text_width": self._terminal_width(32),
                "use_unicode": True
            }

        # we don't become interactive until the command line args are
        # successfully parsed and acted upon
        self.interactive = None
        # current colouring object
        self.command_colour()  # set to default
        self._using_readline = False
        self._input_stack = []
        self.input_line_number = 0
        self._calculate_output_modes()
        self.push_input()
        self.push_output()
        self._input_descriptions = []

        if args:
            try:
                self.process_args(args)
            except Exception:
                if len(self._input_descriptions):
                    self._input_descriptions.append("Processing command line arguments")
                self.handle_exception()
                raise

        if self.interactive is None:
            self.interactive = self._using_a_terminal()

    def _using_a_terminal(self):
        return getattr(self.stdin, "isatty", None) and self.stdin.isatty() and getattr(self.stdout, "isatty",
                                                                                       None) and self.stdout.isatty()

    def _apply_fts(self):
        # Applies the default apsw fts tokenizers and functions
        # very useful opening databases from CLI shell
        if self._db is not None:
            try:
                apsw.fts5.register_tokenizers(self._db, apsw.fts5.map_tokenizers)
                apsw.fts5.register_functions(self._db, apsw.fts5.map_functions)
            except apsw.NoFTS5Error:
                pass


    def _ensure_db(self):
        "The database isn't opened until first use.  This function ensures it is now open."
        if not self._db:
            if not self.dbfilename:
                self.dbfilename = ":memory:"
            self._db = apsw.Connection(self.dbfilename,
                                       flags=apsw.SQLITE_OPEN_URI | apsw.SQLITE_OPEN_READWRITE
                                       | apsw.SQLITE_OPEN_CREATE)
            self._apply_fts()
        return self._db

    def _set_db(self, newv):
        "Sets the open database (or None) and filename"
        (db, dbfilename) = newv
        if self._db:
            self._db.close(True)
            self._db = None
        self._db = db
        self.dbfilename = dbfilename
        self._apply_fts()

    db = property(_ensure_db, _set_db, None, "The current :class:`~apsw.Connection`")

    def process_args(self, args):
        """Process command line options specified in args.  It is safe to
        call this multiple times.  We try to be compatible with SQLite shell
        argument parsing.

        :param args: A list of string options.  Do not include the
           program as args[0]

        :returns: A tuple of (databasefilename, initfiles,
           sqlncommands).  This is provided for informational purposes
           only - they have already been acted upon.  An example use
           is that the SQLite shell does not enter the main interactive
           loop if any sql/commands were provided.

        The first non-option is the database file name.  Each
        remaining non-option is treated as a complete input (ie it
        isn't joined with others looking for a trailing semi-colon).

        The SQLite shell uses single dash in front of options.  We
        allow both single and double dashes.  When an unrecognized
        argument is encountered then
        :meth:`process_unknown_args` is called.
        """
        # we don't use argparse as we need to be compatible with what
        # SQLite's C code does
        if not args:
            return None, [], []

        # are options still valid?
        options = True
        # have we seen the database name?
        havedbname = False
        # List of init files to read
        inits = []
        # List of sql/dot commands
        sqls = []

        while args:
            if not options or not args[0].startswith("-"):
                options = False
                if not havedbname:
                    # grab new database
                    self.db = None, args[0]
                    havedbname = True
                else:
                    sqls.append(args[0])
                args = args[1:]
                continue

            # remove initial single or double dash
            args[0] = args[0][1:]
            if args[0].startswith("-"):
                args[0] = args[0][1:]

            if args[0] == "init":
                if len(args) < 2:
                    raise self.Error("You need to specify a filename after -init")
                inits.append(args[1])
                args = args[2:]
                continue

            if args[0] == "header" or args[0] == "noheader":
                self.header = args[0] == "header"
                args = args[1:]
                continue

            if args[0] in ("echo", "bail", "interactive"):
                setattr(self, args[0], True)
                args = args[1:]
                continue

            if args[0] == "batch":
                self.interactive = False
                args = args[1:]
                continue

            if args[0] in ("separator", "nullvalue", "encoding"):
                if len(args) < 2:
                    raise self.Error("You need to specify a value after -" + args[0])
                getattr(self, "command_" + args[0])([args[1]])
                args = args[2:]
                continue

            if args[0] == "version":
                self.write(self.stdout, apsw.sqlite_lib_version() + "\n")
                # A pretty gnarly thing to do
                sys.exit(0)

            if args[0] == "help":
                self.write(self.stderr, self.usage())
                sys.exit(0)

            if args[0] in ("no-colour", "no-color", "nocolour", "nocolor"):
                self.colour_scheme = "off"
                self._out_colour()
                args = args[1:]
                continue

            # only remaining known args are output modes
            if getattr(self, "output_" + args[0], None):
                self.command_mode(args[:1])
                args = args[1:]
                continue

            newargs = self.process_unknown_args(args)
            if newargs is None:
                raise self.Error("Unrecognized argument '" + args[0] + "'")
            args = newargs

        for f in inits:
            self.command_read([f])

        for s in sqls:
            self.process_complete_line(s)

        return self.dbfilename, inits, sqls

    def process_unknown_args(self, args):
        """This is called when :meth:`process_args` encounters an
        argument it doesn't understand.  Override this method if you
        want to be able to understand additional command line arguments.

        :param args: A list of the remaining arguments.  The initial one will
           have had the leading dashes removed (eg if it was --foo on the command
           line then args[0] will be "foo"
        :returns: None if you don't recognize the argument either.  Otherwise
           return the list of remaining arguments after you have processed
           yours.
        """
        return None

    def usage(self):
        "Returns the usage message."

        msg = """
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
"""
        return msg.lstrip()

    ###
    ### Value formatting routines.  They take a value and return a
    ### text formatting of them.  Mostly used by the various output's
    ### but also by random other pieces of code.
    ###

    # bytes that are ok in C strings - no need for quoting
    _printable = [
        ord(x) for x in "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789~!@#$%^&*()`_-+={}[]:;,.<>/?|"
    ]

    def _fmt_c_string(self, v: apsw.SQLiteValue) -> str:
        "Format as a C string including surrounding double quotes"
        if isinstance(v, str):
            op = ['"']
            for c in v:
                if c == "\\":
                    op.append("\\\\")
                elif c == "\r":
                    op.append("\\r")
                elif c == "\n":
                    op.append("\\n")
                elif c == "\t":
                    op.append("\\t")
                elif ord(c) not in self._printable:
                    op.append("\\" + c)
                else:
                    op.append(c)
            op.append('"')
            return "".join(op)
        elif v is None:
            return '"' + self.nullvalue + '"'
        elif isinstance(v, bytes):
            o = lambda x: x
            fromc = chr
            res = ['"']
            for c in v:
                if o(c) in self._printable:
                    res.append(fromc(c))
                else:
                    res.append("\\x%02X" % (o(c), ))
            res.append('"')
            return "".join(res)
        else:
            # number of some kind
            return '"%s"' % (v, )

    def _fmt_html_col(self, v):
        "Format as HTML (mainly escaping &/</>"
        return self._fmt_text_col(v).\
           replace("&", "&amp;"). \
           replace(">", "&gt;"). \
           replace("<", "&lt;"). \
           replace("'", "&apos;"). \
           replace('"', "&quot;")

    def _fmt_json_value(self, v):
        "Format a value."
        # JSON doesn't have a binary type so we base64 encode it
        if isinstance(v, bytes):
            return '"' + base64.encodebytes(v).decode("ascii").strip() + '"'
        return json.dumps(v, ensure_ascii=True)

    def _fmt_python(self, v):
        "Format as python literal"
        if v is None:
            return "None"
        elif isinstance(v, str):
            return repr(v)
        elif isinstance(v, bytes):
            res = ['b"']
            for i in v:
                if i in self._printable:
                    res.append(chr(i))
                else:
                    res.append("\\x%02X" % (i, ))
            res.append('"')
            return "".join(res)
        else:
            return "%s" % (v, )

    def _fmt_sql_identifier(self, v):
        "Return the identifier quoted in SQL syntax if needed (eg table and column names)"
        if not len(v):  # yes sqlite does allow zero length identifiers
            return '""'
        nonalnum = re.sub("[A-Za-z_0-9]+", "", v)
        if len(nonalnum) == 0:
            if v.upper() not in self._sqlite_reserved:
                # Ok providing it doesn't start with a digit
                if v[0] not in "0123456789":
                    return v
        # double quote it unless there are any double quotes in it
        if '"' in nonalnum:
            return "[%s]" % (v, )
        return '"%s"' % (v, )

    def _fmt_text_col(self, v):
        "Regular text formatting"
        if v is None:
            return self.nullvalue
        elif isinstance(v, str):
            return v
        elif isinstance(v, bytes):
            # sqlite gives back raw bytes!
            return "<Binary data>"
        else:
            return "%s" % (v, )

    ###
    ### The various output routines.  They are always called with the
    ### header irrespective of the setting allowing for some per query
    ### setup. (see output_column for example).  The doc strings are
    ### used to generate help.
    ###

    def output_column(self, header, line):
        """
        Items left aligned in space padded columns.  They are
        truncated if they do not fit. If the width hasn't been
        specified for a column then 10 is used unless the column name
        (header) is longer in which case that width is used.  Use the
        .width command to change column sizes.
        """
        # as an optimization we calculate self._actualwidths which is
        # reset for each query
        if header:

            def gw(n):
                if n < len(self.widths) and self.widths[n] != 0:
                    return self.widths[n]
                # if width is not present or 0 then autosize
                text = self._fmt_text_col(line[n])
                return max(len(text), 10)

            widths = [gw(i) for i in range(len(line))]

            if self.truncate:
                self._actualwidths = ["%" + ("-%d.%ds", "%d.%ds")[w < 0] % (abs(w), abs(w)) for w in widths]
            else:
                self._actualwidths = ["%" + ("-%ds", "%ds")[w < 0] % (abs(w), ) for w in widths]

            if self.header:
                # output the headers
                c = self.colour
                cols = [
                    c.header + (self._actualwidths[i] % (self._fmt_text_col(line[i]), )) + c.header_
                    for i in range(len(line))
                ]
                # sqlite shell uses two spaces between columns
                self.write(self.stdout, "  ".join(cols) + "\n")
                if c is self._colours["off"]:
                    self.output_column(False, ["-" * abs(widths[i]) for i in range(len(widths))])
            return
        cols = [
            self.colour.colour_value(line[i], self._actualwidths[i] % (self._fmt_text_col(line[i]), ))
            for i in range(len(line))
        ]
        # sqlite shell uses two spaces between columns
        self.write(self.stdout, "  ".join(cols) + "\n")

    output_columns = output_column

    def output_csv(self, header, line):
        """
        Items in csv format (comma separated).  Use tabs mode for tab
        separated.  You can use the .separator command to use a
        different one after switching mode.  A separator of comma uses
        double quotes for quoting while other separators do not do any
        quoting.  The Python csv library used for this only supports
        single character separators.
        """

        # we use self._csv for the work, setup when header is
        # supplied. _csv is a tuple of a StringIO and the csv.writer
        # instance.

        fixdata = lambda x: x

        if header:
            s = io.StringIO()
            kwargs = {}
            if self.separator == ",":
                kwargs["dialect"] = "excel"
            elif self.separator == "\t":
                kwargs["dialect"] = "excel-tab"
            else:
                kwargs["quoting"] = csv.QUOTE_NONE
                kwargs["delimiter"] = fixdata(self.separator)
                kwargs["doublequote"] = False
                # csv module is bug ridden junk - I already say no
                # quoting so it still looks for the quotechar and then
                # gets upset that it can't be quoted.  Which bit of no
                # quoting was ambiguous?
                kwargs["quotechar"] = "\x00"

            writer = csv.writer(s, **kwargs)
            self._csv = (s, writer)
            if self.header:
                self.output_csv(None, line)
            return

        if header is None:
            c = self.colour
            line = [c.header + fixdata(self._fmt_text_col(l)) + c.header_ for l in line]
        else:
            fmt = lambda x: self.colour.colour_value(x, fixdata(self._fmt_text_col(x)))
            line = [fmt(l) for l in line]
        self._csv[1].writerow(line)
        t = self._csv[0].getvalue()
        # csv lib always does DOS eol
        assert (t.endswith("\r\n"))
        t = t[:-2]
        # should not be other eol irregularities
        assert (not t.endswith("\r") and not t.endswith("\n"))
        self.write(self.stdout, t + "\n")
        self._csv[0].truncate(0)
        self._csv[0].seek(0)

    def output_html(self, header, line):
        "HTML table style"
        if header:
            if not self.header:
                return
            fmt = lambda x: self.colour.header + self._fmt_html_col(x) + self.colour.header_
        else:
            fmt = lambda x: self.colour.colour_value(x, self._fmt_html_col(x))
        line = [fmt(l) for l in line]
        out = ["<TR>"]
        for l in line:
            out.append(("<TD>", "<TH>")[header])
            out.append(l)
            out.append(("</TD>\n", "</TH>\n")[header])
        out.append("</TR>\n")
        self.write(self.stdout, "".join(out))

    def output_insert(self, header, line):
        """
        Lines as SQL insert statements.  The table name is "table"
        unless you specified a different one as the second parameter
        to the .mode command.
        """
        if header:
            return
        fmt = lambda x: self.colour.colour_value(x, apsw.format_sql_value(x))
        out = "INSERT INTO " + self._output_table + " VALUES(" + ",".join([fmt(l) for l in line]) + ");\n"
        self.write(self.stdout, out)

    def output_json(self, header, line: Shell.Row):
        """
        Output a JSON array.  Blobs are output as base64 encoded strings.
        """
        if header: return
        fmt = lambda x: self.colour.colour_value(x, self._fmt_json_value(x))
        out = ["%s: %s" % (self._fmt_json_value(k), fmt(line.row[i])) for i, k in enumerate(line.columns)]
        self.write(self.stdout,
                   ("[" if line.is_first else "") + "{ " + ", ".join(out) + "}" + ("]" if line.is_last else ",") + "\n")

    def output_jsonl(self, header, line: Shell.Row):
        """
        Output as JSON objects, newline separated.  Blobs are output as base64 encoded strings.
        """
        if header: return
        fmt = lambda x: self.colour.colour_value(x, self._fmt_json_value(x))
        out = ["%s: %s" % (self._fmt_json_value(k), fmt(line.row[i])) for i, k in enumerate(line.columns)]
        self.write(self.stdout, "{ " + ", ".join(out) + "}\n")

    def output_line(self, header, line):
        """
        One value per line in the form 'column = value' with a blank
        line between rows.
        """
        if header:
            w = 5
            for l in line:
                if len(l) > w:
                    w = len(l)
            self._line_info = (w, line)
            return
        fmt = lambda x: self.colour.colour_value(x, self._fmt_text_col(x))
        w = self._line_info[0]
        for i in range(len(line)):
            self.write(self.stdout, "%*s = %s\n" % (w, self._line_info[1][i], fmt(line[i])))
        self.write(self.stdout, "\n")

    output_lines = output_line

    def output_list(self, header, line):
        "All items on one line with separator"
        if header:
            if not self.header:
                return
            c = self.colour
            fmt = lambda x: c.header + x + c.header_
        else:
            fmt = lambda x: self.colour.colour_value(x, self._fmt_text_col(x))
        self.write(self.stdout, self.separator.join([fmt(x) for x in line]) + "\n")

    def output_python(self, header, line):
        "Tuples in Python source form for each row"
        if header:
            if not self.header:
                return
            c = self.colour
            fmt = lambda x: c.header + self._fmt_python(x) + c.header_
        else:
            fmt = lambda x: self.colour.colour_value(x, self._fmt_python(x))
        self.write(self.stdout, '(' + ", ".join([fmt(l) for l in line]) + "),\n")

    def output_tcl(self, header, line):
        "Outputs TCL/C style strings using current separator"
        # In theory you could paste the output into your source ...
        if header:
            if not self.header:
                return
            c = self.colour
            fmt = lambda x: c.header + self._fmt_c_string(x) + c.header_
        else:
            fmt = lambda x: self.colour.colour_value(x, self._fmt_c_string(x))
        self.write(self.stdout, self.separator.join([fmt(l) for l in line]) + "\n")

    _fqt_kwargs = None

    def output_box(self, column_names, rows):
        "Outputs using line drawing and auto sizing columns"
        if self._fqt_kwargs is None:
            # figure out default args
            sig = inspect.signature(apsw.ext.format_query_table)
            self._fqt_kwargs = {
                k: v.default
                for k, v in sig.parameters.items()
                if v.default is not inspect.Signature.empty and k not in {"db", "query", "bindings"}
            }
        kwargs = self._fqt_kwargs.copy()
        kwargs.update(self.box_options)
        if kwargs["text_width"] < 1:
            kwargs["text_width"] = self._terminal_width(32)

        kwargs.update({"colour": self.colour != self._colours["off"]})

        rows = list(list(row) for row in rows)

        self.stdout.write(apsw.ext.format_query_table._format_table(column_names, rows, **kwargs))

    output_box.all_at_once = True

    def output_table(self):
        "Outputs using ascii line drawing and strongly sanitized text"
        # this function isn't actually called - output_box is used
        1 / 0

    def output_qbox(self):
        "Outputs using line drawing and auto sizing columns quoting values"
        # this function isn't actually called - output_box is used
        1 / 0

    def _output_summary(self, summary):
        # internal routine to output a summary line or two
        self.write(self.stdout, self.colour.summary + summary + self.colour.summary_)

    ###
    ### Various routines
    ###

    def cmdloop(self, intro=None, transient=None):
        """Runs the main interactive command loop.

        :param intro: Initial text banner to display instead of the
           default.  Make sure you newline terminate it.
        :param transient: Additional message about being connected to
          a transient in memory database
        """
        if intro is None:
            intro = f"""
SQLite version { apsw.sqlite_lib_version() } (APSW { apsw.apsw_version() })
Enter ".help" for instructions
"""
            intro = intro.lstrip()
        if self.interactive and intro:
            c = self.colour
            self.write(self.stdout, c.intro + intro + c.intro_)
            if not self.dbfilename:
                transient = transient or "Connected to a transient in-memory database.\n"
                self.write(self.stdout, c.transient + transient + c.transient_)

        using_readline = False
        try:
            if self.interactive and self.stdin is sys.stdin:
                import readline
                old_completer = readline.get_completer()
                readline.set_completer(self.complete)
                readline.parse_and_bind("tab: complete")
                using_readline = True
                try:
                    readline.read_history_file(os.path.expanduser(self.history_file))
                except IOError:
                    pass
        except ImportError:
            pass

        try:
            while True:
                self._input_descriptions = []
                if using_readline:
                    # we drop completion cache because it contains
                    # table and column names which could have changed
                    # with last executed SQL
                    self._completion_cache = None
                    self._using_readline = True
                try:
                    command = self.get_complete_line()
                    if command is None:  # EOF
                        if self.interactive:
                            self.write(self.stdout, "\n")
                        return
                    self.process_complete_line(command)
                except:
                    self._append_input_description()
                    try:
                        self.handle_exception()
                    except UnicodeDecodeError:
                        self.handle_exception()
        finally:
            if using_readline:
                readline.set_completer(old_completer)
                readline.set_history_length(256)
                readline.write_history_file(os.path.expanduser(self.history_file))

    def handle_exception(self):
        """Handles the current exception, printing a message to stderr as appropriate.
        It will reraise the exception if necessary (eg if bail is true)"""
        eclass, eval, etb = sys.exc_info()

        if isinstance(eval, SystemExit):
            eval._handle_exception_saw_this = True
            raise

        if not getattr(eval, "_handle_exception_saw_this", False):
            self._out_colour()
            self.write(self.stderr, self.colour.error)

            if isinstance(eval, KeyboardInterrupt):
                self.handle_interrupt()
                text = "Interrupted"
            else:
                text = str(eval)

            if not text.strip():
                text = "(Exception)"

            if not text.endswith("\n"):
                text = text + "\n"

            if len(self._input_descriptions):
                for i in range(len(self._input_descriptions)):
                    if i == 0:
                        pref = "At "
                    else:
                        pref = " " * i + "From "
                    self.write(self.stderr, pref + self._input_descriptions[i] + "\n")

            self.write(self.stderr, text)
            if self.exceptions:
                stack = []
                while etb:
                    stack.append(etb.tb_frame)
                    etb = etb.tb_next

                for frame in stack:
                    self.write(
                        self.stderr, "\nFrame %s in %s at line %d\n" %
                        (frame.f_code.co_name, frame.f_code.co_filename, frame.f_lineno))
                    vars = list(frame.f_locals.items())
                    vars.sort()
                    for k, v in vars:
                        try:
                            v = repr(v)[:80]
                        except Exception:
                            v = "<Unable to convert to string>"
                        self.write(self.stderr, "%10s = %s\n" % (k, v))
                self.write(self.stderr, "\n%s: %s\n" % (eclass, repr(eval)))

            self.write(self.stderr, self.colour.error_)

        eval._handle_exception_saw_this = True
        if self.bail:
            raise

    @dataclasses.dataclass(**({"slots": True, "frozen": True} if sys.version_info >= (3, 10) else {}))
    class _qd:
        query: str | None
        remaining: str | None
        error_text: str | None
        error_offset: int | None
        exception: Exception | None
        explain: int | None

    def _query_details(self, sql, bindings) -> _qd:
        "Internal routine to iterate over statements"
        cur = self.db.cursor()
        saved = sql
        explain = None

        def et(cursor, statement, bindings):
            nonlocal saved, explain
            saved = statement
            explain = cursor.is_explain
            return False

        cur.exec_trace = et

        try:
            cur.execute(sql, bindings)
        except apsw.ExecTraceAbort:
            pass
        except apsw.BindingsError as e:
            return Shell._qd(None, None, "You must used named bindings (eg $name) and .parameter set", -1, e, explain)
        except apsw.Error as e:
            return Shell._qd(sql[: len(saved)], sql[len(saved) :], str(e), getattr(e, "error_offset", -1), e, explain)
        except KeyError as e:
            var = e.args[0]
            return Shell._qd(
                None,
                None,
                f"No binding present for '{ var }' - use .parameter set { var } VALUE to provide one",
                -1,
                e,
                explain,
            )
        return Shell._qd(sql[:len(saved)], sql[len(saved):], None, None, None, explain)

    def process_sql(self, sql: str, bindings=None, internal=False, summary=None):
        """Processes SQL text consisting of one or more statements

        :param sql: SQL to execute

        :param bindings: bindings for the *sql*

        :param internal: If True then this is an internal execution
          (eg the .tables or .database command).  When executing
          internal sql timings are not shown nor is the SQL echoed.

        :param summary: If not None then should be a tuple of two
          items.  If the ``sql`` returns any data then the first item
          is printed before the first row, and the second item is
          printed after the last row.  An example usage is the .find
          command which shows table names.
        """

        changes_start = self.db.total_changes()

        def fixws(s: str):
            return re.sub(r"\s", " ", s, flags=re.UNICODE)

        def fmt_sql(s):
            s = s.strip()
            if len(s) > 4096:
                s = s[:4096] + "..."
            return s

        if not internal and bindings is None:
            bindings = self.bindings

        while sql.strip():
            qd = self._query_details(sql, bindings)
            sql = qd.remaining
            if not internal:
                if self.echo:
                    self.write_error(fmt_sql(qd.query) + "\n")
            if qd.error_text:
                self.write_error(f"{ qd.error_text }\n")
                if qd.error_offset >= 0:
                    offset = qd.error_offset
                    query = qd.query.encode("utf8")
                    before, after = fixws(query[:offset][-35:].decode("utf8")), \
                        fixws(query[offset:][:35].decode("utf8"))
                    print("  ", before + after, file=self.stderr)
                    print("   " + (" " * len(before)) + "^--- error here", file=self.stderr)
                qd.exception._handle_exception_saw_this = True
                raise qd.exception

            if qd.explain == 1:  # explain
                self.push_output()
                self.header = True
                self.widths = [-4, 13, 4, 4, 4, 13, 2, 13]
                self.truncate = False
                self.output = self.output_column
            elif qd.explain == 2:  # explain query plan
                self.push_output()
                self.header = True
                self.widths = [-4, -6, 22]
                self.truncate = False
                self.output = self.output_column

            use_prow = False
            sig = inspect.signature(self.output)
            param_name = list(sig.parameters.keys())[1]
            p = sig.parameters[param_name]
            use_prow = p.annotation == "Shell.Row"

            with apsw.ext.ShowResourceUsage(file = self.stderr if self.timer else None, db=self.db, scope="thread", indent="* "):
                column_names = None
                rows = [] if getattr(self.output, "all_at_once", False) else None

                cur = self.db.cursor()
                if self.db.exec_trace:
                    cur.exec_trace = lambda *args: True
                if self.db.row_trace:
                    cur.row_trace = lambda x, y: y

                for prow in Shell.PositionRow(cur.execute(qd.query, bindings)):
                    row = prow.row
                    if column_names is None:
                        column_names = prow.columns
                        if qd.explain == 2:
                            # column 2 is "notused"
                            column_names = tuple(c for i, c in enumerate(column_names) if i != 2)
                        if summary:
                            self._output_summary(summary[0])
                        if rows is None:
                            self.output(True, column_names)
                    if qd.explain == 2:
                        row = tuple(c for i, c in enumerate(row) if i != 2)

                    row = prow if use_prow else row
                    if rows is None:
                        self.output(False, row)
                    else:
                        rows.append(row)

                if column_names and rows:
                    self.output(column_names, rows)

                if column_names and summary:
                    self._output_summary(summary[1])

            if qd.explain:
                self.pop_output()

        changes = self.db.total_changes() - changes_start
        if not internal and changes and self.changes:
            text = ("changes: " + self.colour.colour_value(changes, str(changes)) + "\t" + "total changes: " +
                    self.colour.colour_value(self.db.total_changes(), str(self.db.total_changes())) + "\n")
            self.write(self.stdout, text)

    def process_command(self, command):
        """Processes a dot command.

        It is split into parts using :func:`shlex.split`
        """
        if self.echo:
            self.write(self.stderr, command + "\n")
        cmd = shlex.split(command)
        assert cmd[0][0] == "."
        cmd[0] = cmd[0][1:]
        fn = getattr(self, "command_" + cmd[0], None)
        if not fn:
            raise self.Error("Unknown command \"%s\".  Enter \".help\" for help" % (cmd[0], ))
        # special handling for .parameter set because we need the value to preserve quoting
        # '33' and 33 are different
        if len(cmd) > 3 and cmd[0] == "parameter" and cmd[1] == "set":
            pos = command.index(cmd[2], command.index("set") + 4) + len(cmd[2]) + 1
            cmd = cmd[:3] + [command[pos:]]
        # special handling for shell / py because we want to preserve the text exactly
        if cmd[0] in {"shell", "py"}:
            rest = command[command.index(cmd[0]) + len(cmd[0]):].strip()
            if rest:
                cmd = [cmd[0], rest]
        # special handling for ftsq TABLE to preserve exact query
        if len(cmd) > 2 and cmd[0] == "ftsq":
            pos = command.index(cmd[1]) + len(cmd[1])
            while command[pos].strip(): # test it isn't whitespace, eq quoting
                pos += 1
            cmd = [cmd[0], cmd[1], command[pos:].strip()]
        res = fn(cmd[1:])

    ###
    ### Commands start here
    ###

    def _boolean_command(self, name, cmd):
        "Parse and verify boolean parameter"
        if len(cmd) != 1 or cmd[0].lower() not in ("on", "off"):
            raise self.Error(name + " expected ON or OFF")
        return cmd[0].lower() == "on"

    # Note that doc text is used for generating help output.

    def command_backup(self, cmd):
        """backup ?DB? FILE: Backup DB (default "main") to FILE

        Copies the contents of the current database to FILE
        overwriting whatever was in FILE.  If you have attached databases
        then you can specify their name instead of the default of "main".

        The backup is done at the page level - SQLite copies the pages
        as is.  There is no round trip through SQL code.
        """
        dbname = "main"
        if len(cmd) == 1:
            fname = cmd[0]
        elif len(cmd) == 2:
            dbname = cmd[0]
            fname = cmd[1]
        else:
            raise self.Error("Backup takes one or two parameters")
        out = apsw.Connection(fname)
        b = out.backup("main", self.db, dbname)
        try:
            while not b.done:
                b.step()
        finally:
            b.finish()
            out.close()

    def command_bail(self, cmd):
        """bail ON|OFF: Stop after hitting an error (default OFF)

        If an error is encountered while processing commands or SQL
        then exit.  (Note this is different than SQLite shell which
        only exits for errors in SQL.)
        """
        self.bail = self._boolean_command("bail", cmd)

    def command_cd(self, cmd):
        """cd ?DIR?: Changes current directory

        If no directory supplied then change to home directory"""
        if len(cmd) > 1:
            raise self.Error("Too many directories")
        d = cmd and cmd[0] or os.path.expanduser("~")
        if not os.path.isdir(d):
            raise self.Error(f"'{ d }' is not a directory")
        os.chdir(d)

    def command_changes(self, cmd):
        """changes ON|OFF: Show changes from last SQL and total changes (default OFF)

        After executing SQL that makes changes, the number of affected
        rows is displayed as well as a running count of all changes.
        """
        self.changes = self._boolean_command("changes", cmd)

    def command_close(self, cmd):
        """close: Closes the current database

        Use .open to open a database, or .connection to switch to another
        connection
        """
        if len(cmd):
            raise self.Error("Unexpected arguments")
        self.db.close()

    def command_connection(self, cmd):
        """connection ?NUMBER?: List connections, or switch active connection

        This covers all connections, not just those started in this
        shell.  Closed connections are not shown.
        """
        self.db_references.add(self.db)
        dbs = []
        for c in apsw.connections():
            try:
                c.filename
            except apsw.ConnectionClosedError:
                continue
            dbs.append(c)

        if len(cmd) == 0:
            co = self.colour
            if self.db not in dbs:
                self.write(self.stdout, co.bold + "(Current connection is closed)" + co.bold_ + "\n")
            for i, c in enumerate(dbs):
                sel = "*" if self.db is c else " "
                self.write(
                    self.stdout,
                    f"{ co.bold}{ sel }{ co.bold_} { co.vnumber }{ i: 2}{ co.vnumber_ } - ({ c.open_vfs }) \"{ co.vstring }{ c.filename }{ co.vstring_ }\"\n"
                )
        elif len(cmd) == 1:
            c = dbs[int(cmd[0])]
            if self.db is not c:
                self._db = c
                self.dbfilename = c.filename
        else:
            raise self.Error("Too many arguments")

    def command_colour(self, cmd=[]):
        """colour SCHEME: Selects a colour scheme

        If using a colour terminal in interactive mode then output is
        automatically coloured to make it more readable.  Use 'off' to
        turn off colour, and no name or 'default' for the default colour
        scheme.
        """
        if len(cmd) > 1:
            raise self.Error("Too many colour schemes")
        c = cmd and cmd[0] or "default"
        if c not in self._colours:
            raise self.Error("No such colour scheme: " + c)
        self.colour_scheme = c
        self._out_colour()

    command_color = command_colour

    def command_databases(self, cmd):
        """databases: Lists names and files of attached databases

        """
        if len(cmd):
            raise self.Error("databases command doesn't take any parameters")
        self.push_output()
        self.header = True
        self.output = self.output_column
        self.truncate = False
        self.widths = [3, 15, 58]
        try:
            self.process_sql("pragma database_list", internal=True)
        finally:
            self.pop_output()

    _dbconfig_ignore = {
        "SQLITE_DBCONFIG_MAINDBNAME", "SQLITE_DBCONFIG_LOOKASIDE", "SQLITE_DBCONFIG_MAX",
        "SQLITE_DBCONFIG_STMT_SCANSTATUS"
    }

    def command_dbconfig(self, cmd):
        """dbconfig ?NAME VALUE?: Show all dbconfig, or set a specific one

        With no arguments lists all settings.  Supply a name and integer value
        to change.  For example:

            .dbconfig enable_fkey 1
        """
        if len(cmd) == 0:
            outputs = {}
            for k in apsw.mapping_db_config:
                if type(k) is not str or k in self._dbconfig_ignore:
                    continue
                pretty = k[len("SQLITE_DBCONFIG_"):].lower()
                outputs[pretty] = self.db.config(getattr(apsw, k), -1)
            w = max(len(k) for k in outputs.keys())
            for k, v in outputs.items():
                self.write(self.stdout, " " * (w - len(k)))
                self.write(self.stdout, k + ":  ")
                self.write_value(v)
                self.write(self.stdout, "\n")
            return
        elif len(cmd) != 2:
            raise self.Error("Expected zero or two parameters")
        key = "SQLITE_DBCONFIG_" + cmd[0].upper()
        if key not in apsw.mapping_db_config:
            raise self.Error(f"Unknown config option { key }")
        v = self.db.config(getattr(apsw, key), int(cmd[1]))
        self.write(self.stdout, cmd[0].lower() + ": ")
        self.write_value(v)
        self.write(self.stdout, "\n")

    def command_dbinfo(self, cmd):
        """dbinfo ?NAME?: Shows summary and file information about the database

        This includes the numbers of tables, indices etc, as well as fields from
        the files as returned by :func:`apsw.ext.dbinfo`.

        NAME defaults to 'main', and can be the attached name of a database.
        """
        if len(cmd) > 1:
            raise self.Error("too many parameters")
        schema = cmd[0] if cmd else "main"

        def total(t):
            return self.db.execute(f"select count(*) from [{ schema }].sqlite_schema where type='{ t }'").get

        outputs = [
            ("number of tables", total("table")),
            ("number of indexes", total("index")),
            ("number of triggers", total("trigger")),
            ("number of views", total("view")),
            ("schema size", int(self.db.execute(f"select total(length(sql)) from [{ schema }].sqlite_schema").get)),
        ]
        for i, info in enumerate(apsw.ext.dbinfo(self.db, schema)):
            if i == 1:
                outputs.append(("journal mode", self.db.pragma("journal_mode")))
            if info:
                outputs.extend(v for v in dataclasses.asdict(info).items())
            else:
                if i == 0:
                    outputs.append(("filename", self.db.filename))
                else:
                    outputs.append(
                        ("filename",
                         self.db.filename_wal if self.db.pragma("journal_mode") == "wal" else self.db.filename_journal))

        w = max(len(k) for k, v in outputs)
        for k, v in outputs:
            self.write(self.stdout, " " * (w - len(k)))
            self.write(self.stdout, k + ":  ")
            self.write_value(v)
            self.write(self.stdout, "\n")

    def command_dump(self, cmd):
        """dump ?TABLE? [TABLE...]: Dumps all or specified tables in SQL text format

        The table name is treated as like pattern so you can use % as
        a wildcard.  You can use dump to make a text based backup of
        the database.  It is also useful for comparing differences or
        making the data available to other databases.  Indices and
        triggers for the table(s) are also dumped.  Finally views
        matching the table pattern name are dumped.

        Note that if you are dumping virtual tables such as used by
        the FTS5 module then they may use other tables to store
        information.  For example if you create a FTS5 table named
        *recipes* then it also creates *recipes_content*,
        *recipes_segdir* etc.  Consequently to dump this example
        correctly use:

           .dump recipes recipes_%

        If the database is empty or no tables/views match then there
        is no output.
        """
        # Simple tables are easy to dump.  More complicated is dealing
        # with virtual tables, foreign keys etc.

        # Lock the database while doing the dump so nothing changes
        # under our feet
        self.process_sql("BEGIN IMMEDIATE", internal=True)

        # Used in comment() - see issue 142
        outputstrtype = str

        # Python 2.3 can end up with nonsense like "en_us" so we fall
        # back to ascii in that case
        outputstrencoding = getattr(self.stdout, "encoding", "ascii")
        try:
            codecs.lookup(outputstrencoding)
        except Exception:
            outputstrencoding = "ascii"

        def unicodify(s):
            if not isinstance(s, outputstrtype):
                # See issue 142 - it may not be in an expected encoding
                return s.decode(outputstrencoding, "replace")
            return s

        try:
            # first pass -see if virtual tables or foreign keys are in
            # use.  If they are we emit pragmas to deal with them, but
            # prefer not to emit them
            v = {"virtuals": False, "foreigns": False}

            def check(name, sql):
                if name.lower().startswith("sqlite_"):
                    return False
                sql = sql.lower()
                if re.match(r"^\s*create\s+virtual\s+.*", sql):
                    v["virtuals"] = True
                # pragma table_info doesn't tell us if foreign keys
                # are involved so we guess if any the various strings are
                # in the sql somewhere
                if re.match(r".*\b(foreign\s*key|references)\b.*", sql):
                    v["foreigns"] = True
                return True

            if len(cmd) == 0:
                cmd = ["%"]

            tables = []
            for pattern in cmd:
                for name, sql in self.db.execute(
                        "SELECT name,sql FROM sqlite_schema "
                        "WHERE sql NOT NULL AND type IN ('table','view') "
                        "AND tbl_name LIKE ?1", (pattern, )):
                    if check(name, sql) and name not in tables:
                        tables.append(name)

            if not tables:
                return

            # will we need to analyze anything later?
            analyze_needed = []
            for stat in self.db.execute(
                    "select name from sqlite_schema where sql not null and type='table' and tbl_name like 'sqlite_stat%'"
            ):
                for name in tables:
                    if len(
                            self.db.execute("select * from " + self._fmt_sql_identifier(stat[0]) + " WHERE tbl=?",
                                            (name, )).fetchall()):
                        if name not in analyze_needed:
                            analyze_needed.append(name)
            analyze_needed.sort()

            def blank():
                self.write(self.stdout, "\n")

            def comment(s):
                s = unicodify(s)
                self.write(self.stdout, textwrap.fill(s, 78, initial_indent="-- ", subsequent_indent="-- ") + "\n")

            pats = ", ".join([(x, "(All)")[x == "%"] for x in cmd])
            comment("SQLite dump (by APSW %s)" % (apsw.apsw_version(), ))
            comment("SQLite version " + apsw.sqlite_lib_version())
            comment("Date: " + unicodify(time.strftime("%c")))
            comment("Tables like: " + pats)
            comment("Database: " + self.db.filename)
            try:
                import getpass
                import socket
                comment("User: %s @ %s" % (unicodify(getpass.getuser()), unicodify(socket.gethostname())))
            except ImportError:
                pass
            blank()

            comment("The values of various per-database settings")
            self.write(self.stdout, "PRAGMA page_size=" + str(self.db.pragma("page_size")) + ";\n")
            comment("PRAGMA encoding='" + self.db.pragma("encoding") + "';\n")
            vac = {0: "NONE", 1: "FULL", 2: "INCREMENTAL"}
            vacvalue = self.db.pragma("auto_vacuum")
            comment("PRAGMA auto_vacuum=" + vac.get(vacvalue, str(vacvalue)) + ";\n")
            comment("PRAGMA max_page_count=" + str(self.db.pragma("max_page_count")) + ";\n")
            blank()

            # different python versions have different requirements
            # about specifying cmp to sort routine so we use this
            # portable workaround with a decorated list instead
            dectables = [(x.lower(), x) for x in tables]
            dectables.sort()
            tables = [y for x, y in dectables]

            virtuals = v["virtuals"]
            foreigns = v["foreigns"]

            if virtuals:
                comment("This pragma is needed to restore virtual tables")
                self.write(self.stdout, "PRAGMA writable_schema=ON;\n")
            if foreigns:
                comment("This pragma turns off checking of foreign keys "
                        "as tables would be inconsistent while restoring.  It was introduced "
                        "in SQLite 3.6.19.")
                self.write(self.stdout, "PRAGMA foreign_keys=OFF;\n")

            if virtuals or foreigns:
                blank()

            self.write(self.stdout, "BEGIN TRANSACTION;\n")
            blank()

            def sqldef(s):
                # return formatted sql watching out for embedded
                # comments at the end forcing trailing ; onto next
                # line https://sqlite.org/src/info/c04a8b8a4f
                if "--" in s.split("\n")[-1]:
                    nl = "\n"
                else:
                    nl = ""
                return s + nl + ";\n"

            # do the table dumping loops
            oldtable = self._output_table
            try:
                self.push_output()
                self.output = self.output_insert
                # Dump the table
                for table in tables:
                    for sql in self.db.execute("SELECT sql FROM sqlite_schema WHERE name=?1 AND type='table'",
                                               (table, )):
                        comment("Table  " + table)
                        # Special treatment for virtual tables - they
                        # get called back on drops and creates and
                        # could thwart us so we have to manipulate
                        # sqlite_schema directly
                        if sql[0].lower().split()[:3] == ["create", "virtual", "table"]:
                            self.write(
                                self.stdout, "DELETE FROM sqlite_schema WHERE name=" + apsw.format_sql_value(table) +
                                " AND type='table';\n")
                            self.write(
                                self.stdout,
                                "INSERT INTO sqlite_schema(type,name,tbl_name,rootpage,sql) VALUES('table',%s,%s,0,%s);\n"
                                % (apsw.format_sql_value(table), apsw.format_sql_value(table),
                                   apsw.format_sql_value(sql[0])))
                        else:
                            self.write(self.stdout, "DROP TABLE IF EXISTS " + self._fmt_sql_identifier(table) + ";\n")
                            self.write(self.stdout, sqldef(sql[0]))
                            self._output_table = self._fmt_sql_identifier(table)
                            self.process_sql("select * from " + self._fmt_sql_identifier(table), internal=True)
                        # Now any indices or triggers
                        first = True
                        for name, sql in self.db.execute(
                                "SELECT name,sql FROM sqlite_schema "
                                "WHERE sql NOT NULL AND type IN ('index', 'trigger') "
                                "AND tbl_name=?1 AND name NOT LIKE 'sqlite_%' "
                                "ORDER BY lower(name)", (table, )):
                            if first:
                                comment("Triggers and indices on  " + table)
                                first = False
                            self.write(self.stdout, sqldef(sql))
                        blank()
                # Views done last.  They have to be done in the same order as they are in sqlite_schema
                # as they could refer to each other
                first = True
                for name, sql in self.db.execute("SELECT name,sql FROM sqlite_schema "
                                                 "WHERE sql NOT NULL AND type='view' "
                                                 "AND name IN ( " + ",".join([apsw.format_sql_value(i)
                                                                              for i in tables]) + ") ORDER BY _ROWID_"):
                    if first:
                        comment("Views")
                        first = False
                    self.write(self.stdout, "DROP VIEW IF EXISTS %s;\n" % (self._fmt_sql_identifier(name), ))
                    self.write(self.stdout, sqldef(sql))
                if not first:
                    blank()

                # sqlite sequence
                # does it exist
                if len(self.db.execute("select * from sqlite_schema where name='sqlite_sequence'").fetchall()):
                    first = True
                    for t in tables:
                        v = self.db.execute("select seq from main.sqlite_sequence where name=?1", (t, )).fetchall()
                        if len(v):
                            assert len(v) == 1
                            if first:
                                comment("For primary key autoincrements the next id "
                                        "to use is stored in sqlite_sequence")
                                first = False
                            self.write(
                                self.stdout,
                                'DELETE FROM main.sqlite_sequence WHERE name=%s;\n' % (apsw.format_sql_value(t), ))
                            self.write(
                                self.stdout, 'INSERT INTO main.sqlite_sequence VALUES (%s, %s);\n' %
                                (apsw.format_sql_value(t), v[0][0]))
                    if not first:
                        blank()
            finally:
                self.pop_output()
                self._output_table = oldtable

            # analyze
            if analyze_needed:
                comment("You had used the analyze command on these tables before.  Rerun for this new data.")
                for n in analyze_needed:
                    self.write(self.stdout, "ANALYZE " + self._fmt_sql_identifier(n) + ";\n")
                blank()

            # header fields
            count = 0
            for name in ("user_version", "application_id"):
                val = self.db.pragma(name)
                if val:
                    if count == 0:
                        comment("Database header")
                    self.write(self.stdout, f"pragma {name}={val};\n")
                    count += 1
            if count:
                blank()

            # Save it all
            self.write(self.stdout, "COMMIT TRANSACTION;\n")

            # cleanup pragmas
            if foreigns:
                blank()
                comment("Restoring foreign key checking back on.  Note that SQLite 3.6.19 is off by default")
                self.write(self.stdout, "PRAGMA foreign_keys=ON;\n")
            if virtuals:
                blank()
                comment("Restoring writable schema back to default")
                self.write(self.stdout, "PRAGMA writable_schema=OFF;\n")
                # schema reread
                blank()
                comment("We need to force SQLite to reread the schema because otherwise it doesn't know that "
                        "the virtual tables we inserted directly into sqlite_schema exist.")
                self.write(self.stdout, "BEGIN;\nCREATE TABLE no_such_table(x,y,z);\nROLLBACK;\n")

        finally:
            self.process_sql("END", internal=True)

    def command_echo(self, cmd):
        """echo ON|OFF: If ON then each SQL statement or command is printed before execution (default OFF)

        The SQL statement or command is sent to error output so that
        it is not intermingled with regular output.
        """
        self.echo = self._boolean_command("echo", cmd)

    def set_encoding(self, enc):
        """Saves *enc* as the default encoding, after verifying that
        it is valid.  You can also include :error to specify error
        handling - eg 'cp437:replace'
        """
        enc = enc.split(":", 1)
        if len(enc) > 1:
            enc, errors = enc
        else:
            enc = enc[0]
            errors = None
        try:
            codecs.lookup(enc)
        except LookupError:
            raise self.Error("No known encoding '%s'" % (enc, ))
        try:
            if errors is not None:
                codecs.lookup_error(errors)
        except LookupError:
            raise self.Error("No known codec error handler '%s'" % (errors, ))
        self.encoding = enc, errors

    def command_encoding(self, cmd):
        """encoding ENCODING: Set the encoding used for new files opened via .output and imports

        SQLite and APSW/Python work internally using Unicode and characters.
        Files however are a sequence of bytes.  An encoding describes
        how to convert between bytes and characters.  The default
        encoding is utf8 and that is generally the best value to use
        when other programs give you a choice.

        You can also specify an error handler.  For example
        `cp437:replace` will use code page 437 and any Unicode
        codepoints not present in cp437 will be replaced (typically
        with something like a question mark).  Other error handlers
        include `ignore`, `strict` (default) and `xmlcharrefreplace`.

        This command affects files opened after setting the encoding
        as well as imports.
        """
        if len(cmd) != 1:
            raise self.Error("Encoding takes one argument")
        self.set_encoding(cmd[0])

    def command_exceptions(self, cmd):
        """exceptions ON|OFF: If ON then detailed tracebacks are shown on exceptions (default OFF)

        Normally when an exception occurs the error string only is
        displayed.  However it is sometimes useful to get a full
        traceback.  An example would be when you are developing
        virtual tables and using the shell to exercise them.  In
        addition to displaying each stack frame, the local variables
        within each frame are also displayed.
        """
        self.exceptions = self._boolean_command("exceptions", cmd)

    def command_exit(self, cmd):
        """exit ?CODE?: Exit this program with optional exit code"""
        if len(cmd) > 1:
            raise self.Error("Too many parameters for exit")
        try:
            c = 0 if not cmd else int(cmd[0])
        except ValueError:
            raise self.Error(f"{ cmd[0] } isn't an exit code")
        sys.exit(c)

    def command_find(self, cmd):
        """find value ?TABLE?: Searches all columns of all tables for a value

        The find command helps you locate data across your database
        for example to find a string or any references to an id.

        You can specify a like pattern to limit the search to a subset
        of tables (eg specifying CUSTOMER% for all tables beginning
        with CUSTOMER).

        The value will be treated as a string and/or integer if
        possible.  If value contains '%' or '_' then it is also treated as
        a like pattern.

        This command can take a long time to execute needing to scan
        all of the relevant tables, rows, and columns.
        """
        if len(cmd) < 1 or len(cmd) > 2:
            raise self.Error("At least one argument required and at most two accepted")
        tablefilter = "%"
        if len(cmd) == 2:
            tablefilter = cmd[1]
        querytemplate = []
        queryparams = []

        def qp():  # binding for current queryparams
            return "?" + str(len(queryparams))

        s = cmd[0]
        if '%' in s or '_' in s:
            queryparams.append(s)
            querytemplate.append("%s LIKE " + qp())
        queryparams.append(s)
        querytemplate.append("%s = " + qp())
        try:
            i = int(s)
            queryparams.append(i)
            querytemplate.append("%s = " + qp())
        except ValueError:
            pass
        querytemplate = " OR ".join(querytemplate)
        for (table, ) in self.db.execute("SELECT name FROM sqlite_schema WHERE type='table' AND name LIKE ?1",
                                         (tablefilter, )):
            t = self._fmt_sql_identifier(table)
            query = "SELECT * from %s WHERE " % (t, )
            colq = []
            for _, column, _, _, _, _ in self.db.execute("pragma table_info(%s)" % (t, )):
                colq.append(querytemplate % ((self._fmt_sql_identifier(column), ) * len(queryparams)))
            query = query + " OR ".join(colq)
            self.process_sql(query, queryparams, internal=True, summary=("Table " + table + "\n", "\n"))

    def command_ftsq(self, cmd):
        """ftsq TABLE query: Issues the query against the named FTS5 table

        The top 20 results are shown.  Text after the table name is used
        exactly as the query - do not extra shell quote it.
        """
        if len(cmd) != 2:
            raise self.Error("Expected a table name and a query")
        query =f"select rowid, snippet({ cmd[0] }, -1, '<<', '>>', '...', 10) as 'snippet' from { cmd[0] }(?) order by rank limit 20"
        self.process_sql(query, (cmd[1], ))

    def command_header(self, cmd):
        """header(s) ON|OFF: Display the column names in output (default OFF)

        """
        self.header = self._boolean_command("header", cmd)

    command_headers = command_header

    _help_info = None

    def command_help(self, cmd):
        """help ?COMMAND?: Shows list of commands and their usage

        If COMMAND is specified then shows detail about that COMMAND.
        ``.help all`` will show detailed help about all commands.
        """
        if not self._help_info:
            # buildup help database
            self._help_info = {}
            for c in dir(self):
                if not c.startswith("command_"):
                    continue
                # help is 3 parts
                # - the syntax string (eg backup ?dbname? filename)
                # - the one liner description (eg saves database to filename)
                # - the multi-liner detailed description
                # We grab this from the doc string for the function in the form
                #   syntax: one liner\nmulti\nliner
                d = getattr(self, c).__doc__
                assert d, c + " command must have documentation"
                c = c[len("command_"):]
                if c in ("headers", "color"): continue
                while d[0] == "\n":
                    d = d[1:]
                parts = d.split("\n", 1)
                firstline = parts[0].strip().split(":", 1)
                assert len(firstline) == 2, c + " command must have usage: description doc"
                if len(parts) == 1 or len(parts[1].strip()) == 0:  # work around textwrap bug
                    multi = ""
                else:
                    multi = textwrap.dedent(parts[1])
                if c == "mode":
                    firstline[1] = firstline[1] + " " + " ".join(self._output_modes)
                    multi = multi + "\n\n" + "\n\n".join(self._output_modes_detail)
                if c == "colour":
                    colours = list(self._colours.keys())
                    colours.sort()
                    firstline[1] = firstline[1] + " from " + ", ".join(colours)
                if len(multi.strip()) == 0:  # All whitespace
                    multi = None
                else:
                    # break into paragraphs
                    lines = multi.strip("\n").split("\n")
                    # make whitespace only lines be empty
                    for i in range(len(lines)):
                        if not lines[i].strip():
                            lines[i] = ""
                    multi = [lines[0]]
                    for l in lines[1:]:
                        if multi[-1] and l and l.lstrip() == l:
                            multi[-1] += " " + l
                        else:
                            multi.append(l)

                self._help_info[c] = ('.' + firstline[0].strip(), firstline[1].strip(), multi)

        self.write(self.stderr, "\n")

        tw = self._terminal_width(32)
        if len(cmd) == 0:
            commands = list(self._help_info.keys())
            commands.sort()
            w = 0
            for command in commands:
                if len(self._help_info[command][0]) > w:
                    w = len(self._help_info[command][0])
            out = []
            for command in commands:
                hi = self._help_info[command]
                # usage string
                out.append(hi[0])
                # space padding (including 2 for between columns)
                out.append(" " * (2 + w - len(hi[0])))
                # usage message wrapped if need be
                out.append(("\n" + " " * (2 + w)).join(textwrap.wrap(hi[1], tw - w - 2)))
                # newline
                out.append("\n")
            self.write(self.stderr, "".join(out))
        else:
            if cmd[0] == "all":
                cmd = list(self._help_info.keys())
                cmd.sort()
            w = 0
            for command in self._help_info:
                if len(self._help_info[command][0]) > w:
                    w = len(self._help_info[command][0])

            for command in cmd:
                command = command.lstrip(".")
                if command == "headers": command = "header"
                if command not in self._help_info:
                    raise self.Error("No such command \"%s\"" % (command, ))
                out = []
                hi = self._help_info[command]
                # usage string
                out.append(hi[0])
                # space padding (2)
                out.append(" " * (2 + w - len(hi[0])))
                # usage message wrapped if need be
                out.append(("\n" + " " * (2 + w)).join(textwrap.wrap(hi[1], tw - w - 2)) + "\n")
                if hi[2]:
                    # newlines
                    out.append("\n")
                    # detailed message
                    for i, para in enumerate(hi[2]):
                        out.append(textwrap.fill(para, tw) + "\n")
                # if not first one then print separator header
                if command != cmd[0]:
                    self.write(self.stderr, "\n" + "=" * tw + "\n")
                self.write(self.stderr, "".join(out))
        self.write(self.stderr, "\n")

    def command_import(self, cmd):
        """import FILE TABLE: Imports separated data from FILE into TABLE

        Reads data from the file into the named table using the
        current separator and encoding.  For example if the separator
        is currently a comma then the file should be CSV (comma
        separated values).

        All values read in are supplied to SQLite as strings.  If you
        want SQLite to treat them as other types then declare your
        columns appropriately.  For example declaring a column 'REAL'
        will result in the values being stored as floating point if
        they can be safely converted.

        Another alternative is to create a temporary table, insert the
        values into that and then use casting.:

          CREATE TEMPORARY TABLE import(a,b,c);
          .import filename import
          CREATE TABLE final AS SELECT cast(a as BLOB), cast(b as INTEGER),
               cast(c as CHAR) from import;
          DROP TABLE import;

        You can also get more sophisticated using the SQL CASE
        operator.  For example this will turn zero length strings into
        null:

          SELECT CASE col WHEN '' THEN null ELSE col END FROM ...
        """
        if len(cmd) != 2:
            raise self.Error("import takes two parameters")

        try:
            final = None
            # start transaction so database can't be changed
            # underneath us
            self.db.execute("BEGIN IMMEDIATE")
            final = "ROLLBACK"

            # how many columns?
            ncols = len(self.db.execute("pragma table_info(" + self._fmt_sql_identifier(cmd[1]) + ")").fetchall())
            if ncols < 1:
                raise self.Error("No such table '%s'" % (cmd[1], ))

            cur = self.db.cursor()
            sql = "insert into %s values(%s)" % (self._fmt_sql_identifier(cmd[1]), ",".join("?" * ncols))

            kwargs = {}
            if self.separator == ",":
                kwargs["dialect"] = "excel"
            elif self.separator == "\t":
                kwargs["dialect"] = "excel-tab"
            else:
                kwargs["quoting"] = csv.QUOTE_NONE
                kwargs["delimiter"] = self.separator
                kwargs["doublequote"] = False
                kwargs["quotechar"] = "\x00"
            row = 1
            for line in self._csvin_wrapper(cmd[0], kwargs):
                if len(line) != ncols:
                    raise self.Error("row %d has %d columns but should have %d" % (row, len(line), ncols))
                try:
                    cur.execute(sql, line)
                except Exception:
                    self.write_error(f"Error inserting row { row }")
                    raise
                row += 1
            self.db.execute("COMMIT")

        except Exception:
            if final:
                self.db.execute(final)
            raise

    def _csvin_wrapper(self, filename, dialect):
        # Returns a csv reader that works around python bugs and uses
        # dialect dict to configure reader
        thefile = codecs.open(filename, "r", self.encoding[0])
        for line in csv.reader(thefile, **dialect.copy()):
            yield line
        thefile.close()
        return

    def command_autoimport(self, cmd):
        """autoimport FILENAME ?TABLE?: Imports filename creating a table and automatically working out separators and data types (alternative to .import command)

        The import command requires that you precisely pre-setup the
        table and schema, and set the data separators (eg commas or
        tabs).  This command figures out the separator and csv dialect
        automatically.  There must be at least two columns and two rows.

        If the table is not specified then the basename of the file
        will be used.

        Additionally the type of the contents of each column is also
        deduced - for example if it is a number or date.  Empty values
        are turned into nulls.  Dates are normalized into YYYY-MM-DD
        format and DateTime are normalized into ISO8601 format to
        allow easy sorting and searching.  4 digit years must be used
        to detect dates.  US (swapped day and month) versus rest of
        the world is also detected providing there is at least one
        value that resolves the ambiguity.

        Care is taken to ensure that columns looking like numbers are
        only treated as numbers if they do not have unnecessary
        leading zeroes or plus signs.  This is to avoid treating phone
        numbers and similar number like strings as integers.

        This command can take quite some time on large files as they
        are effectively imported twice.  The first time is to
        determine the format and the types for each column while the
        second pass actually imports the data.
        """
        if len(cmd) < 1 or len(cmd) > 2:
            raise self.Error("Expected one or two parameters")
        if not os.path.exists(cmd[0]):
            raise self.Error("File \"%s\" does not exist" % (cmd[0], ))
        if len(cmd) == 2:
            tablename = cmd[1]
        else:
            tablename = None
        try:
            final = None
            c = self.db.cursor()
            c.execute("BEGIN IMMEDIATE")
            final = "ROLLBACK"

            if not tablename:
                tablename = os.path.splitext(os.path.basename(cmd[0]))[0]

            if c.execute("pragma table_info(%s)" % (self._fmt_sql_identifier(tablename), )).fetchall():
                raise self.Error("Table \"%s\" already exists" % (tablename, ))

            # The types we support deducing
            def DateUS(v):  # US formatted date with wrong ordering of day and month
                return DateWorld(v, switchdm=True)

            def DateWorld(v, switchdm=False):  # Sensibly formatted date as used anywhere else in the world
                y, m, d = self._getdate(v)
                if switchdm: m, d = d, m
                if m < 1 or m > 12 or d < 1 or d > 31:
                    raise ValueError
                return "%d-%02d-%02d" % (y, m, d)

            def DateTimeUS(v):  # US date and time
                return DateTimeWorld(v, switchdm=True)

            def DateTimeWorld(v, switchdm=False):  # Sensible date and time
                y, m, d, h, M, s = self._getdatetime(v)
                if switchdm: m, d = d, m
                if m < 1 or m > 12 or d < 1 or d > 31 or h < 0 or h > 23 or M < 0 or M > 59 or s < 0 or s > 65:
                    raise ValueError
                return "%d-%02d-%02dT%02d:%02d:%02d" % (y, m, d, h, M, s)

            def Number(v):  # we really don't want phone numbers etc to match
                # Python's float & int constructors allow whitespace which we don't
                if re.search(r"\s", v):
                    raise ValueError
                if v == "0": return 0
                if v[0] == "+":  # idd prefix
                    raise ValueError
                if re.match("^[0-9]+$", v):
                    if v[0] == "0": raise ValueError  # also a phone number
                    return int(v)
                if v[0] == "0" and not v.startswith("0."):  # deceptive not a number
                    raise ValueError
                return float(v)

            # Work out the file format
            formats = [{"dialect": "excel"}, {"dialect": "excel-tab"}]
            seps = ["|", ";", ":"]
            if self.separator not in seps:
                seps.append(self.separator)
            for sep in seps:
                formats.append({"quoting": csv.QUOTE_NONE, "delimiter": sep, "doublequote": False, "quotechar": "\x00"})
            possibles = []
            errors = []
            encodingissue = False
            # format is copy() on every use.  This appears bizarre and
            # unnecessary.  However Python 2.3 and 2.4 somehow manage
            # to empty it if not copied.
            for format in formats:
                ncols = -1
                lines = 0
                try:
                    for line in self._csvin_wrapper(cmd[0], format.copy()):
                        if lines == 0:
                            lines = 1
                            ncols = len(line)
                            # data type guess setup
                            datas = []
                            for i in range(ncols):
                                datas.append([DateUS, DateWorld, DateTimeUS, DateTimeWorld, Number])
                            allblanks = [True] * ncols
                            continue
                        if len(line) != ncols:
                            raise ValueError("Expected %d columns - got %d" % (ncols, len(line)))
                        lines += 1
                        for i in range(ncols):
                            if not line[i]:
                                continue
                            allblanks[i] = False
                            if not datas[i]:
                                continue
                            # remove datas that give ValueError
                            d = []
                            for dd in datas[i]:
                                try:
                                    dd(line[i])
                                    d.append(dd)
                                except ValueError:
                                    pass
                            datas[i] = d
                    if ncols > 1 and lines > 1:
                        # if a particular column was allblank then clear datas for it
                        for i in range(ncols):
                            if allblanks[i]:
                                datas[i] = []
                        possibles.append((format.copy(), ncols, lines, datas))
                except UnicodeDecodeError:
                    encodingissue = True
                except Exception:
                    s = str(sys.exc_info()[1])
                    if s not in errors:
                        errors.append(s)

            if len(possibles) == 0:
                if encodingissue:
                    raise self.Error(
                        "The file is probably not in the current encoding \"%s\" and didn't match a known file format" %
                        (self.encoding[0], ))
                v = "File doesn't appear to match a known type."
                if len(errors):
                    v += "  Errors reported:\n" + "\n".join(["  " + e for e in errors])
                raise self.Error(v)
            if len(possibles) > 1:
                raise self.Error("File matches more than one type!")
            format, ncols, lines, datas = possibles[0]
            fmt = format.get("dialect", None)
            if fmt is None:
                fmt = "(delimited by \"%s\")" % (format["delimiter"], )
            self.write(self.stdout, "Detected Format %s  Columns %d  Rows %d\n" % (fmt, ncols, lines))
            # Header row
            reader = self._csvin_wrapper(cmd[0], format)
            for header in reader:
                break
            # Check schema
            identity = lambda x: x
            for i in range(ncols):
                if len(datas[i]) > 1:
                    raise self.Error("Column #%d \"%s\" has ambiguous data format - %s" %
                                     (i + 1, header[i], ", ".join([d.__name__ for d in datas[i]])))
                if datas[i]:
                    datas[i] = datas[i][0]
                else:
                    datas[i] = identity
            # Make the table
            sql = "CREATE TABLE %s(%s)" % (self._fmt_sql_identifier(tablename), ", ".join(
                [self._fmt_sql_identifier(h) for h in header]))
            c.execute(sql)
            # prep work for each row
            sql = "INSERT INTO %s VALUES(%s)" % (self._fmt_sql_identifier(tablename), ",".join(["?"] * ncols))
            for line in reader:
                vals = []
                for i in range(ncols):
                    l = line[i]
                    if not l:
                        vals.append(None)
                    else:
                        vals.append(datas[i](l))
                c.execute(sql, vals)

            c.execute("COMMIT")
            self.write(self.stdout, "Auto-import into table \"%s\" complete\n" % (tablename, ))
        except Exception:
            if final:
                self.db.execute(final)
            raise

    def _getdate(self, v):
        # Returns a tuple of 3 items y,m,d from string v
        m = re.match(r"^([0-9]+)[^0-9]([0-9]+)[^0-9]([0-9]+)$", v)
        if not m:
            raise ValueError
        y, m, d = int(m.group(1)), int(m.group(2)), int(m.group(3))
        if d > 1000:  # swap order
            y, m, d = d, m, y
        if y < 1000 or y > 9999:
            raise ValueError
        return y, m, d

    def _getdatetime(self, v):
        # must be at least HH:MM
        m = re.match(r"^([0-9]+)[^0-9]([0-9]+)[^0-9]([0-9]+)[^0-9]+([0-9]+)[^0-9]([0-9]+)([^0-9]([0-9]+))?$", v)
        if not m:
            raise ValueError
        items = list(m.group(1, 2, 3, 4, 5, 7))
        for i in range(len(items)):
            if items[i] is None:
                items[i] = 0
        items = [int(i) for i in items]
        if items[2] > 1000:
            items = [items[2], items[1], items[0]] + items[3:]
        if items[0] < 1000 or items[0] > 9999:
            raise ValueError
        return items

    def command_indices(self, cmd):
        """indices TABLE: Lists all indices on table TABLE

        """
        if len(cmd) != 1:
            raise self.Error("indices takes one table name")
        self.push_output()
        self.header = False
        self.output = self.output_list
        try:
            self.process_sql(
                "SELECT name FROM sqlite_schema WHERE type='index' AND tbl_name LIKE ?1 "
                "UNION ALL SELECT name FROM sqlite_temp_schema WHERE type='index' AND tbl_name LIKE "
                "?1 ORDER by name",
                cmd,
                internal=True)
        finally:
            self.pop_output()

    def command_load(self, cmd):
        """load FILE ?ENTRY?: Loads a SQLite extension library

        Note: Extension loading may not be enabled in the SQLite
        library version you are using.

        By default sqlite3_extension_init is called in the library but
        you can specify an alternate entry point.

        If you get an error about the extension not being found you
        may need to explicitly specify the directory.  For example if
        it is in the current directory then use:

          .load ./extension.so
        """
        if len(cmd) < 1 or len(cmd) > 2:
            raise self.Error("load takes one or two parameters")
        try:
            self.db.enable_load_extension(True)
        except Exception:
            raise self.Error("Extension loading is not supported")

        self.db.load_extension(*cmd)

    def log_handler(self, code, message):
        "Called with SQLite log messages when logging is ON"
        code = f"( { code } - { apsw.ext.result_string(code) } ) "
        self.write_error(code + message + "\n")

    def command_log(self, cmd):
        "log ON|OFF: Shows SQLite log messages (default off)"
        setting = self._boolean_command("log", cmd)
        apsw.config(apsw.SQLITE_CONFIG_LOG, self.log_handler if setting else None)

    _output_modes = None

    def command_mode(self, cmd):
        """mode MODE ?OPTIONS?: Sets output mode to one of"""
        if not cmd:
            raise self.Error("Specify an output mode - use .help mode for detailed list")
        w = cmd[0]
        if w == "tabs":
            w = "list"
        if not hasattr(self, "output_" + w):
            raise self.Error("Expected a valid output mode: " + ", ".join(self._output_modes) +
                             "\nUse .help mode for a detailed list")

        m = getattr(self, "output_" + w)

        # argument parsing
        if w == "insert":
            if len(cmd) not in (1, 2):
                raise self.Error("Output mode %s doesn't take parameters" % (cmd[0]))
            table_name = cmd[1] if len(cmd) == 2 else "table"
            self._output_table = self._fmt_sql_identifier(table_name)
            self.output = m
            return

        if w not in {"box", "qbox", "table"}:
            if len(cmd) != 1:
                raise self.Error("Output mode %s doesn't take parameters" % (cmd[0]))
            if cmd[0] == "csv":
                self.separator = ","
            elif cmd[0] == "tabs":
                self.separator = "\t"
            self.truncate = True
            self.output = m
            return

        defaults = {
            "quote": w in {"qbox"},
            "string_sanitize": {
                "box": 0,
                "table": 2,
                "qbox": 1
            }[w],
            "null": "NULL",
            "truncate": {
                "box": 1024,
                "table": 2048,
                "qbox": 4096
            }[w],
            "text_width": 0 if self.interactive else 80,
            "use_unicode": {
                "box": True,
                "table": False,
                "qbox": True
            }[w],
        }

        # argparse unfortunately tries to do too much and really is about program arguments,
        # but it isn't worthwhile re-implementing this
        p = argparse.ArgumentParser(allow_abbrev=False, usage=f".mode { w } [options]", prog="")
        if hasattr(p, "exit_on_error"):
            p.exit_on_error = False
        p.set_defaults(**defaults)
        p.add_argument("--quote", dest="quote", action="store_true", help="Show values in SQL syntax [%(default)s]")
        p.add_argument("--no-quote", dest="quote", action="store_false", help="Show values as strings")
        p.add_argument(
            "--string-sanitize",
            type=int,
            choices=(0, 1, 2),
            help="How much to clean up string characters (0 - none, 1 - medium, 2 - everything) [%(default)s]")
        p.add_argument("--null", help="How to show NULL [%(default)s]")
        p.add_argument("--truncate", type=int, help="How many characters to truncate long output at [%(default)s]")
        p.add_argument("--width",
                       type=int,
                       dest="text_width",
                       help="Maximum width of the table [Screen width if terminal, else 80 chars]")
        p.add_argument("--unicode",
                       action="store_true",
                       dest="use_unicode",
                       help="Use unicode line drawing [%(default)s]")
        p.add_argument("--no-unicode",
                       action="store_true",
                       dest="use_unicode",
                       help="Use ascii line drawing like +=-+ ")
        text = io.StringIO()
        try:
            with contextlib.redirect_stderr(text):
                with contextlib.redirect_stdout(text):
                    self.box_options = vars(p.parse_args(cmd[1:]))
        except (SystemExit, argparse.ArgumentError) as exc:
            if isinstance(exc, argparse.ArgumentError):
                print(exc.message, file=text)
            print("\n\nUse --help for options", file=text)
            raise Shell.Error(text.getvalue()) from None
        self.output = self.output_box

    # needed so command completion and help can use it
    def _calculate_output_modes(self):
        modes = [m[len("output_"):] for m in dir(self) if m.startswith("output_")]
        modes.append("tabs")
        modes.sort()
        self._output_modes = modes

        detail = []

        for m in modes:
            if m in {'tabs', "column", "line"}:
                continue
            d = getattr(self, "output_" + m).__doc__
            assert d, "output mode " + m + " needs doc"
            d = d.replace("\n", " ").strip()
            while "  " in d:
                d = d.replace("  ", " ")
            detail.append(m + ": " + d)
        self._output_modes_detail = detail

    def command_nullvalue(self, cmd):
        """nullvalue STRING: Print STRING in place of null values

        This affects textual output modes like column and list and
        sets how SQL null values are shown.  The default is a zero
        length string.  Insert mode and dumps are not affected by this
        setting.  You can use double quotes to supply a zero length
        string.  For example:

          .nullvalue ""         # the default
          .nullvalue <NULL>     # rather obvious
          .nullvalue " \\t "     # A tab surrounded by spaces
        """
        if len(cmd) != 1:
            raise self.Error("nullvalue takes exactly one parameter")
        self.nullvalue = self.fixup_backslashes(cmd[0])

    def command_open(self, cmd):
        """open ?OPTIONS? ?FILE?: Opens a database connection

        Options are:

        --wipe     Closes any existing connections in this process referring to
                   the same file  and deletes the database file, journals etc
                   before opening

        --vfs VFS  Which vfs to use when opening

        If FILE is omitted then a memory database is opened
        """
        wipe = False
        vfs = None
        dbname = None
        c = cmd
        while c:
            p = c.pop(0)
            if p.startswith("--"):
                if p == "--wipe":
                    wipe = True
                    continue
                if p == "--vfs":
                    vfs = c.pop(0)
                    continue
                raise self.Error("Unknown open param: " + p)
            if dbname is not None:
                raise self.Error("Too many arguments: " + p)
            dbname = p

        if wipe:
            if not dbname:
                raise self.Error("You must specify a filename with --wipe")
            for c in apsw.connections():
                try:
                    if c.filename and os.path.samefile(c.filename, dbname):
                        c.close()
                except (apsw.ConnectionClosedError, FileNotFoundError):
                    pass
            for suffix in "", "-journal", "-wal", "-shm":
                try:
                    os.remove(dbname + suffix)
                except OSError:
                    pass
        self.db_references.add(self.db)
        self.dbfilename = dbname if dbname is not None else ""
        self._db = apsw.Connection(self.dbfilename,
                                   vfs=vfs,
                                   flags=apsw.SQLITE_OPEN_URI | apsw.SQLITE_OPEN_READWRITE
                                   | apsw.SQLITE_OPEN_CREATE)
        self._apply_fts()

    def command_output(self, cmd):
        """output FILENAME: Send output to FILENAME (or stdout)

        If the FILENAME is 'stdout' then output is sent to standard
        output from when the shell was started.  The file is opened
        using the current encoding (change with 'encoding' command).
        """
        # Flush everything
        self.stdout.flush()
        self.stderr.flush()
        if hasattr(self.stdin, "flush"):
            try:
                self.stdin.flush()
            except IOError:  # see issue 117
                pass

        # we will also close stdout but only do so once we have a
        # replacement so that stdout is always valid

        if len(cmd) != 1:
            raise self.Error("You must specify a filename")

        try:
            fname = cmd[0]
            if fname == "stdout":
                old = None
                if self.stdout != self._original_stdout:
                    old = self.stdout
                self.stdout = self._original_stdout
                if old is not None:  # done here in case close raises exception
                    old.close()
                return

            newf = codecs.open(fname, "w", self.encoding[0], self.encoding[1])
            old = None
            if self.stdout != self._original_stdout:
                old = self.stdout
            self.stdout = newf
            if old is not None:
                old.close()
        finally:
            self._out_colour()

    def command_parameter(self, cmd):
        """parameter CMD ...:  Maintain named bindings you can use in your queries.

        Specify a subcommand:

           list            -- shows current bindings
           clear           -- deletes all bindings
           unset NAME      -- deletes named binding
           set NAME VALUE  -- sets binding to VALUE

        The value must be a valid SQL literal or expression.  For example
        `3` will be an integer 3 while '3' will be a string.

        Example::

          .parameter set floor 10.99
          .parameter set text 'Acme''s Glove'
          SELECT * FROM sales WHERE price > $floor AND description != $text;
        """
        if cmd:
            if len(cmd) == 1 and cmd[0] in {"clear", "init"}:
                self.bindings = {}
                return
            if len(cmd) == 1 and cmd[0] == "list":
                if not self.bindings:
                    self.write(self.stdout, "No parameters set\n")
                    return
                w = max(10, max(len(k) for k in self.bindings) + 1)
                for k, v in sorted(self.bindings.items()):
                    self.write(self.stdout, k + " " * (w - len(k)))
                    self.write_value(v)
                    self.write(self.stdout, "\n")
                return
            if len(cmd) == 2 and cmd[0] == "unset":
                try:
                    del self.bindings[cmd[1]]
                    return
                except KeyError:
                    raise self.Error(f"'{ cmd[1] }' is not in parameters")
            if len(cmd) == 3 and cmd[0] == "set":
                try:
                    v = self.db.execute(f"select ({ cmd[2] })").get
                except Exception:
                    raise self.Error(f"Does not appear to be a valid SQLite value: { cmd[2] }")
                self.bindings[cmd[1]] = v
                return
        raise self.Error(".parameter command not understood.  Use .help parameter to get usage")

    def command_print(self, cmd):
        """print STRING: print the literal STRING

        If more than one argument is supplied then they are printed
        space separated.  You can use backslash escapes such as \\n
        and \\t.
        """
        self.write(self.stdout, " ".join([self.fixup_backslashes(i) for i in cmd]) + "\n")

    def command_prompt(self, cmd):
        """prompt MAIN ?CONTINUE?: Changes the prompts for first line and continuation lines

        The default is to print 'sqlite> ' for the main prompt where
        you can enter a dot command or a SQL statement.  If the SQL
        statement is not complete then you are
        prompted for more using the continuation prompt which defaults
        to ' ..> '.  Example:

          .prompt "command> " "more command> "

        You can use backslash escapes such as \\n and \\t.
        """
        if len(cmd) < 1 or len(cmd) > 2:
            raise self.Error("prompt takes one or two arguments")
        self.prompt = self.fixup_backslashes(cmd[0])
        if len(cmd) == 2:
            self.moreprompt = self.fixup_backslashes(cmd[1])

    def command_py(self, cmd):
        """py ?PYTHON?: Starts a python REPL or runs the Python statement provided

        The namespace provided includes ``apsw`` for the module, ``shell`` for this
        shell and ``db`` for the current database.

        Using the .output command does not affect output from this command.  You
        can write to `shell.stdout` and `shell.stderr`.
        """
        self.stdout.flush()
        self.stderr.flush()
        vars = {"shell": self, "apsw": apsw, "db": self.db}
        if cmd:
            assert len(cmd) == 1
            interp = code.InteractiveInterpreter(locals=vars)
            try:
                # we have to make sys.excepthook and sys.__excepthook__ be
                # the same otherwise the traceback includes methods in the
                # code module.  Ubuntu's apport messes with excepthook
                hook = sys.excepthook
                sys.excepthook = sys.__excepthook__
                res = interp.runsource(cmd[0])
            finally:
                sys.excepthook = hook
            if res:
                self.write_error("Incomplete Python statement\n")
        else:
            # this should be locals (plural) but the method is written
            # in singular and works due to being passed as a positional
            # argument
            code.interact(local=vars, exitmsg=self.colour.intro + "Returning to APSW shell" + self.colour.intro_)

    def command_read(self, cmd):
        """read FILENAME: Processes SQL and commands in FILENAME (or Python if FILENAME ends with .py)

        Treats the specified file as input (a mixture or SQL and/or
        dot commands).  If the filename ends in .py then it is treated
        as Python code instead.

        For Python code the symbol 'db' refers to the current database,
        'shell' refers to the instance of the shell and 'apsw' is the
        apsw module.
        """
        if len(cmd) != 1:
            raise self.Error("read takes a single filename")
        if cmd[0].lower().endswith(".py"):
            g = {}
            g.update({'apsw': apsw, 'shell': self, 'db': self.db})
            # compile step is needed to associate name with code
            f = open(cmd[0], "rb")
            try:
                exec(compile(f.read(), cmd[0], 'exec'), g, g)
            finally:
                f.close()
        else:
            f = codecs.open(cmd[0], "r", self.encoding[0])
            try:
                try:
                    self.push_input()
                    self.stdin = f
                    self.interactive = False
                    self.input_line_number = 0
                    while True:
                        line = self.get_complete_line()
                        if line is None:
                            break
                        self.process_complete_line(line)
                except Exception:
                    self._append_input_description()
                    raise

            finally:
                self.pop_input()
                f.close()

    def command_restore(self, cmd):
        """restore ?DB? FILE: Restore database from FILE into DB (default "main")

        Copies the contents of FILE to the current database (default "main").
        The backup is done at the page level - SQLite copies the pages as
        is.  There is no round trip through SQL code.
        """
        dbname = "main"
        if len(cmd) == 1:
            fname = cmd[0]
        elif len(cmd) == 2:
            dbname = cmd[0]
            fname = cmd[1]
        else:
            raise self.Error("Restore takes one or two parameters")
        input = apsw.Connection(fname)
        b = self.db.backup(dbname, input, "main")
        try:
            while not b.done:
                b.step()
        finally:
            b.finish()
            input.close()

    def command_schema(self, cmd):
        """schema ?TABLE? [TABLE...]: Shows SQL for table

        If you give one or more tables then their schema is listed
        (including indices).  If you don't specify any then all
        schemas are listed. TABLE is a like pattern so you can use % for
        wildcards.
        """
        self.push_output()
        self.output = self.output_list
        self.header = False
        try:
            if len(cmd) == 0:
                cmd = ['%']
            for n in cmd:
                self.process_sql(
                    "SELECT sql||';' FROM "
                    "(SELECT sql sql, type type, tbl_name tbl_name, name name "
                    "FROM sqlite_schema UNION ALL "
                    "SELECT sql, type, tbl_name, name FROM sqlite_temp_schema) "
                    "WHERE tbl_name LIKE ?1 AND type!='meta' AND sql NOTNULL AND name NOT LIKE 'sqlite_%' "
                    "ORDER BY substr(type,2,1), name", (n, ),
                    internal=True)
        finally:
            self.pop_output()

    def command_separator(self, cmd):
        """separator STRING: Change separator for output mode and .import

        You can use quotes and backslashes.  For example to set the
        separator to space tab space you can use:

          .separator " \\t "

        The setting is automatically changed when you switch to csv or
        tabs output mode.  You should also set it before doing an
        import (ie , for CSV and \\t for TSV).
        """
        if len(cmd) != 1:
            raise self.Error("separator takes exactly one parameter")
        self.separator = self.fixup_backslashes(cmd[0])

    _shows = ("echo", "headers", "mode", "changes", "nullvalue", "output", "separator", "width", "exceptions",
              "encoding")

    def command_shell(self, cmd):
        """shell CMD ARGS...: Run CMD ARGS in a system shell

        Note that output goes to the process standard output, not
        whatever the shell .output command has configured.
        """
        if len(cmd) == 0:
            raise self.Error("Specify command and arguments to run")
        assert len(cmd) == 1
        res = os.system(cmd[0])
        if res != 0:
            self.write_error(f"Exit code { res }\n")

    def command_show(self, cmd):
        """show: Show the current values for various settings."""
        if len(cmd) > 1:
            raise self.Error("show takes at most one parameter")
        if len(cmd):
            what = cmd[0]
            if what not in self._shows:
                raise self.Error("Unknown show: '%s'" % (what, ))
        else:
            what = None

        outs = []
        for i in self._shows:
            k = i
            if what and i != what:
                continue
            # boolean settings
            if i in ("echo", "headers", "exceptions", "changes"):
                if i == "headers": i = "header"
                v = "off"
                if getattr(self, i):
                    v = "on"
            elif i in ("nullvalue", "separator"):
                v = self._fmt_c_string(getattr(self, i))
            elif i == "mode":
                for v in self._output_modes:
                    if self.output == getattr(self, "output_" + v):
                        break
                else:
                    assert False, "Bug: didn't find output mode"
            elif i == "output":
                if self.stdout is self._original_stdout:
                    v = "stdout"
                else:
                    v = getattr(self.stdout, "name", "<unknown stdout>")
            elif i == "width":
                v = " ".join(["%d" % (i, ) for i in self.widths])
            elif i == "encoding":
                v = self.encoding[0]
                if self.encoding[1]:
                    v += " (Errors " + self.encoding[1] + ")"
            else:
                assert False, "Bug: unknown show handling"
            outs.append((k, v))

        # find width of k column
        l = 0
        for k, v in outs:
            if len(k) > l:
                l = len(k)

        for k, v in outs:
            self.write(self.stderr, "%*.*s: %s\n" % (l, l, k, v))

    def command_tables(self, cmd):
        """tables ?PATTERN?: Lists names of tables matching LIKE pattern

        This also returns views.
        """
        self.push_output()
        self.output = self.output_list
        self.header = False
        try:
            if len(cmd) == 0:
                cmd = ['%']

            # The SQLite shell code filters out sqlite_ prefixes if
            # you specified an argument else leaves them in.  It also
            # has a hand coded output mode that does space separation
            # plus wrapping at 80 columns.
            for n in cmd:
                self.process_sql(
                    "SELECT name FROM sqlite_schema "
                    "WHERE type IN ('table', 'view') AND name NOT LIKE 'sqlite_%' "
                    "AND name like ?1 "
                    "UNION ALL "
                    "SELECT name FROM sqlite_temp_schema "
                    "WHERE type IN ('table', 'view') AND name NOT LIKE 'sqlite_%' "
                    "ORDER BY 1", (n, ),
                    internal=True)
        finally:
            self.pop_output()

    def command_timeout(self, cmd):
        """timeout MS: Try opening locked tables for MS milliseconds

        If a database is locked by another process SQLite will keep
        retrying.  This sets how many thousandths of a second it will
        keep trying for.  If you supply zero or a negative number then
        all busy handlers are disabled.
        """
        if len(cmd) != 1:
            raise self.Error("timeout takes a number")
        try:
            t = int(cmd[0])
        except ValueError:
            raise self.Error("%s is not a number" % (cmd[0], ))
        self.db.set_busy_timeout(t)

    def command_timer(self, cmd):
        """timer ON|OFF: Control printing of time and resource usage after each query

        The values displayed are in seconds when shown as floating
        point or an absolute count.  Only items that have changed
        since starting the query are shown.  On non-Windows platforms
        considerably more information can be shown.  SQLite statistics
        are also included.
        """
        self.timer = self._boolean_command("timer", cmd)

    def command_version(self, cmd):
        "version: Displays SQLite, APSW, and Python version information"
        if cmd:
            raise self.Error("No parameters taken")
        versions = {
            "SQLite": f"{ apsw.sqlite_lib_version() } { apsw.sqlite3_sourceid() }",
            "Python": f"{ sys.version } - { sys.executable }",
            "APSW": apsw.apsw_version(),
            "APSW file": apsw.__file__,
            "Amalgamation": apsw.using_amalgamation,
        }
        maxw = max(len(k) for k in versions)
        for k, v in versions.items():
            self.write(self.stdout, " " * (maxw - len(k)) + f"{ k }  { v}\n")

    def command_vfsname(self, cmd):
        "vfsname: VFS name for database, or attached names"
        dbnames = cmd or ["main"]
        for name in dbnames:
            self.write(self.stdout, (self.db.vfsname(name) or "") + "\n")

    def _format_vfs(self, vfs):
        w = max(len(k) for k in vfs.keys())
        for k, v in vfs.items():
            self.write(self.stdout, " " * (w - len(k)))
            self.write(self.stdout, k + ":  ")
            vout = "0x%x" % v if k.startswith("x") else str(v)
            self.write_value(v)
            self.write(self.stdout, "\n")

    def command_vfsinfo(self, cmd):
        "vfsinfo: Shows detailed information about the VFS for the database"
        if cmd:
            raise self.Error("No parameters taken")
        for vfs in apsw.vfs_details():
            if vfs["zName"] == self.db.open_vfs:
                self._format_vfs(vfs)

    def command_vfslist(self, cmd):
        "vfslist: Shows detailed information about all the VFS available"
        if cmd:
            raise self.Error("No parameters taken")
        for i, vfs in enumerate(apsw.vfs_details()):
            if i:
                self.write(self.stdout, "\n")
            self._format_vfs(vfs)

    def command_width(self, cmd):
        """width NUM NUM ...: Set the column widths for "column" mode

        In "column" output mode, each column is a fixed width with values truncated to
        fit.  Specify new widths using this command.  Use a negative number
        to right justify and zero for default column width.
        """
        if len(cmd) == 0:
            raise self.Error("You need to specify some widths!")
        w = []
        for i in cmd:
            try:
                w.append(int(i))
            except ValueError:
                raise self.Error("'%s' is not a valid number" % (i, ))
        self.widths = w

    def _terminal_width(self, minimum):
        """Works out the terminal width which is used for word wrapping
        some output (eg .help)"""

        w = 80
        try:
            # we don't use shutil version because it can't be passed a
            # file descriptor
            if self.stdout.isatty():
                w = os.get_terminal_size(self.stdout.fileno()).columns
        except Exception:
            pass
        return max(w, minimum)

    def push_output(self):
        """Saves the current output settings onto a stack.  See
        :meth:`pop_output` for more details as to why you would use
        this."""
        o = {}
        for k in "separator", "header", "nullvalue", "output", "widths", "truncate":
            o[k] = getattr(self, k)
        self._output_stack.append(o)

    def pop_output(self):
        """Restores most recently pushed output.  There are many
        output parameters such as nullvalue, mode
        (list/tcl/html/insert etc), column widths, header etc.  If you
        temporarily need to change some settings then
        :meth:`push_output`, change the settings and then pop the old
        ones back.

        A simple example is implementing a command like .dump.  Push
        the current output, change the mode to insert so we get SQL
        inserts printed and then pop to go back to what was there
        before.

        """
        # first item should always be present
        assert len(self._output_stack)
        if len(self._output_stack) == 1:
            o = self._output_stack[0]
        else:
            o = self._output_stack.pop()
        for k, v in o.items():
            setattr(self, k, v)

    def _append_input_description(self):
        """When displaying an error in :meth:`handle_exception` we
        want to give context such as when the commands being executed
        came from a .read command (which in turn could execute another
        .read).
        """
        if self.interactive:
            return
        res = []
        res.append("Line %d" % (self.input_line_number, ))
        res.append(": " + getattr(self.stdin, "name", "<stdin>"))
        self._input_descriptions.append(" ".join(res))

    def fixup_backslashes(self, s):
        """Implements the various backlash sequences in s such as
        turning backslash t into a tab.

        This function is needed because :mod:`shlex` does not do it for us.
        """
        if "\\" not in s: return s
        # See the resolve_backslashes function in SQLite shell source
        res = []
        i = 0
        while i < len(s):
            if s[i] != "\\":
                res.append(s[i])
                i += 1
                continue
            i += 1
            if i >= len(s):
                raise self.Error("Backslash with nothing following")
            c = s[i]
            res.append({"\\": "\\", "r": "\r", "n": "\n", "t": "\t"}.get(c, None))
            i += 1  # advance again
            if res[-1] is None:
                raise self.Error("Unknown backslash sequence \\" + c)
        return "".join(res)

    def write(self, dest, text):
        "Writes text to dest.  dest will typically be one of self.stdout or self.stderr."
        dest.write(text)

    def write_error(self, text):
        "Writes text to self.stderr colouring it"
        self.write(self.stderr, self.colour.error + text + self.colour.error_)

    def write_value(self, value, fmt=apsw.format_sql_value):
        "Writes colourized value to self.stdout converting to text with fmt"
        self.write(self.stdout, self.colour.colour_value(value, fmt(value)))

    _raw_input = input

    def get_line(self, prompt=""):
        """Returns a single line of input (may be incomplete SQL) from self.stdin.

        If EOF is reached then return None.  Do not include trailing
        newline in return.
        """
        self.stdout.flush()
        self.stderr.flush()
        try:
            if self.interactive:
                if self.stdin is sys.stdin:
                    c = self.colour.prompt, self.colour.prompt_
                    if self._using_readline:
                        # these are needed so that readline knows they are non-printing characters
                        c = "\x01" + c[0] + "\x02", "\x01" + c[1] + "\x02",
                    line = self._raw_input(c[0] + prompt + c[1]) + "\n"  # raw_input excludes newline
                else:
                    self.write(self.stdout, prompt)
                    line = self.stdin.readline()  # includes newline unless last line of file doesn't have one
            else:
                line = self.stdin.readline()  # includes newline unless last line of file doesn't have one
            self.input_line_number += 1
        except EOFError:
            return None
        if len(line) == 0:  # always a \n on the end normally so this is EOF
            return None
        if line[-1] == "\n":
            line = line[:-1]
        return line

    def get_complete_line(self):
        """Returns a complete input.

        For dot commands it will be one line.  For SQL statements it
        will be as many as is necessary to have a
        :meth:`~apsw.complete` statement (ie semicolon terminated).
        Returns None on end of file."""
        try:
            self._completion_first = True
            command = self.get_line(self.prompt)
            if command is None:
                return None
            if len(command.strip()) == 0:
                return ""
            if command[0] == "?": command = ".help " + command[1:]
            # incomplete SQL?
            while command[0] != "." and not apsw.complete(command):
                self._completion_first = False
                line = self.get_line(self.moreprompt)
                if line is None:  # unexpected eof
                    raise self.Error("Incomplete SQL (line %d of %s): %s\n" %
                                     (self.input_line_number, getattr(self.stdin, "name", "<stdin>"), command))
                if line in ("go", "/"):
                    break
                command = command + "\n" + line
            return command
        except KeyboardInterrupt:
            self.handle_interrupt()
            return ""

    def handle_interrupt(self):
        """Deal with keyboard interrupt (typically Control-C).  It
        will :meth:`~apsw.Connection.interrupt` the database and print"^C" if interactive."""
        self.db.interrupt()
        if not self.bail and self.interactive:
            self.write(self.stderr, "^C\n")
            return
        raise

    def process_complete_line(self, command):
        """Given some text will call the appropriate method to process
        it (eg :meth:`process_sql` or :meth:`process_command`)"""
        try:
            if len(command.strip()) == 0:
                return
            if command[0] == ".":
                self.process_command(command)
            else:
                self.process_sql(command)
        except KeyboardInterrupt:
            self.handle_interrupt()

    def push_input(self):
        """Saves the current input parameters to a stack.  See :meth:`pop_input`."""
        d = {}
        for i in "interactive", "stdin", "input_line_number":
            d[i] = getattr(self, i)
        self._input_stack.append(d)

    def pop_input(self):
        """Restore most recently pushed input parameters (interactive,
        self.stdin, linenumber etc).  Use this if implementing a
        command like read.  Push the current input, read the file and
        then pop the input to go back to before.
        """
        assert (len(self._input_stack)) > 1
        d = self._input_stack.pop()
        for k, v in d.items():
            setattr(self, k, v)

    def complete(self, token, state):
        """Return a possible completion for :mod:`readline`

        This function is called with state starting at zero to get the
        first completion, then one/two/three etc until you return None.  The best
        implementation is to generate the list when state==0, save it,
        and provide members on each increase.

        The default implementation extracts the current full input
        from readline and then calls :meth:`complete_command` or
        :meth:`complete_sql` as appropriate saving the results for
        subsequent calls.
        """
        if state == 0:
            import readline
            # the whole line
            line = readline.get_line_buffer()
            # beginning and end(+1) of the token in line
            beg = readline.get_begidx()
            end = readline.get_endidx()
            # Are we matching a command?
            try:
                if self._completion_first and line.startswith("."):
                    self.completions = self.complete_command(line, token, beg, end)
                else:
                    self.completions = self.complete_sql(line, token, beg, end)
            except Exception:
                # Readline swallows any exceptions we raise.  We
                # shouldn't be raising any so this is to catch that
                traceback.print_exc()
                raise

        if state > len(self.completions):
            return None
        return self.completions[state]

    _sqlite_keywords = apsw.keywords
    # reserved words need to be quoted.  Only a subset of the keywords are reserved
    # but what the heck
    _sqlite_reserved = _sqlite_keywords
    # add a space after each of them except functions which get parentheses
    _sqlite_keywords = [x + (" ", "(")[x in ("VALUES", "CAST")] for x in _sqlite_keywords]

    _sqlite_special_names = """_ROWID_ OID ROWID sqlite_schema
           SQLITE_SEQUENCE""".split()

    _pragmas_bool = ("yes", "true", "on", "no", "false", "off")
    _pragmas = {
        "analysis_limit=": None,
        "application_id": None,
        "auto_vacuum=": ("NONE", "FULL", "INCREMENTAL"),
        "automatic_index=": _pragmas_bool,
        "busy_timeout=": None,
        "cache_size=": None,
        "case_sensitive_like=": _pragmas_bool,
        "cache_spill=": _pragmas_bool,
        "cell_size_check=": _pragmas_bool,
        "checkpoint_fullfsync=": _pragmas_bool,
        "collation_list": None,
        "compile_options": None,
        "data_version": None,
        "database_list": None,
        "defer_foreign_keys=": _pragmas_bool,
        "encoding=": ('UTF-8', 'UTF-16', 'UTF-16le', 'UTF16-16be'),
        "foreign_key_check": None,
        "foreign_key_list(": None,
        "foreign_keys": _pragmas_bool,
        "freelist_count": None,
        "fullfsync=": _pragmas_bool,
        "function_list": None,
        "hard_heap_limit=": None,
        "ignore_check_constraints": _pragmas_bool,
        "incremental_vacuum(": None,
        "index_info(": None,
        "index_list(": None,
        "index_xinfo(": None,
        "integrity_check": None,
        "journal_mode=": ("DELETE", "TRUNCATE", "PERSIST", "MEMORY", "OFF", "WAL"),
        "journal_size_limit=": None,
        "legacy_alter_table=": _pragmas_bool,
        "legacy_file_format=": _pragmas_bool,
        "locking_mode=": ("NORMAL", "EXCLUSIVE"),
        "max_page_count=": None,
        "mmap_size=": None,
        "module_list": None,
        "optimize(": None,
        "page_count;": None,
        "page_size=": None,
        "pragma_list;": None,
        "query_only=": _pragmas_bool,
        "quick_check": None,
        "read_uncommitted=": _pragmas_bool,
        "recursive_triggers=": _pragmas_bool,
        "reverse_unordered_selects=": _pragmas_bool,
        "schema_version": None,
        "secure_delete=": _pragmas_bool,
        "shrink_memory": None,
        "soft_heap_limit=": None,
        "synchronous=": ("OFF", "NORMAL", "FULL"),
        "table_info(": None,
        "table_list": None,
        "table_xinfo(": None,
        "temp_store=": ("DEFAULT", "FILE", "MEMORY"),
        "threads=": None,
        "trusted_schema": _pragmas_bool,
        "user_version=": None,
        "wal_autocheckpoint=": None,
        "wal_checkpoint": None,
        "writable_schema": _pragmas_bool,
    }

    def _get_prev_tokens(self, line, end):
        "Returns the tokens prior to pos end in the line"
        return re.findall(r'"?\w+"?', line[:end])

    def complete_sql(self, line, token, beg, end):
        """Provide some completions for SQL

        :param line: The current complete input line
        :param token: The word readline is looking for matches
        :param beg: Integer offset of token in line
        :param end: Integer end of token in line
        :return: A list of completions, or an empty list if none
        """
        if self._completion_cache is None:
            cur = self.db.cursor()
            collations = [row[1] for row in cur.execute("pragma collation_list")]
            databases = [row[1] for row in cur.execute("pragma database_list")]
            other = [row[0] for row in cur.execute("pragma module_list")]
            for db in databases:
                if db == "temp":
                    master = "sqlite_temp_schema"
                else:
                    master = "[%s].sqlite_schema" % (db, )
                for row in cur.execute("select * from " + master).fetchall():
                    for col in (1, 2):
                        if row[col] not in other and not row[col].startswith("sqlite_"):
                            other.append(row[col])
                    if row[0] == "table":
                        try:
                            for table in cur.execute("pragma [%s].table_info([%s])" % (
                                    db,
                                    row[1],
                            )).fetchall():
                                if table[1] not in other:
                                    other.append(table[1])
                                for item in table[2].split():
                                    if item not in other:
                                        other.append(item)
                        except apsw.SQLError:
                            # See https://github.com/rogerbinns/apsw/issues/86
                            pass
                functions = {}
                for row in cur.execute("pragma function_list"):
                    name = row[0]
                    narg = row[4]
                    functions[name] = max(narg, functions.get(name, -1))

                def fmtfunc(name, nargs):
                    if nargs == 0:
                        return name + "()"
                    return name + "("

                func_list = [fmtfunc(name, narg) for name, narg in functions.items()]

            self._completion_cache = [
                self._sqlite_keywords, func_list, self._sqlite_special_names, collations, databases, other
            ]
            for i in range(len(self._completion_cache)):
                self._completion_cache[i].sort()

        # be somewhat sensible about pragmas
        if "pragma " in line.lower():
            t = self._get_prev_tokens(line.lower(), end)

            # pragma foo = bar
            if len(t) > 2 and t[-3] == "pragma":
                # t[-2] should be a valid one
                for p in self._pragmas:
                    if p.replace("=", "") == t[-2]:
                        vals = self._pragmas[p]
                        if not vals:
                            return []
                        return [x + ";" for x in vals if x.startswith(token)]
            # at equals?
            if len(t) > 1 and t[-2] == "pragma" and line[:end].replace(" ", "").endswith("="):
                for p in self._pragmas:
                    if p.replace("=", "") == t[-1]:
                        vals = self._pragmas[p]
                        if not vals:
                            return []
                        return vals
            # pragma foo
            if len(t) > 1 and t[-2] == "pragma":
                res = [x for x in self._pragmas.keys() if x.startswith(token)]
                res.sort()
                return res

            # pragma
            if len(t) and t[-1] == "pragma":
                res = list(self._pragmas.keys())
                res.sort()
                return res

        # This is currently not context sensitive (eg it doesn't look
        # to see if last token was 'FROM' and hence next should only
        # be table names.  That is a SMOP like pragmas above
        res = []
        ut = token.upper()
        for corpus in self._completion_cache:
            for word in corpus:
                if word.upper().startswith(ut):
                    # potential match - now match case
                    if word.startswith(token):  # exact
                        if word not in res:
                            res.append(word)
                    elif word.lower().startswith(token):  # lower
                        if word.lower() not in res:
                            res.append(word.lower())
                    elif word.upper().startswith(token):  # upper
                        if word.upper() not in res:
                            res.append(word.upper())
                    else:
                        # match letter by letter otherwise readline mangles what was typed in
                        w = token + word[len(token):]
                        if w not in res:
                            res.append(w)
        return res

    # completion for the dot commands are messy because some take
    # variable numbers of parameters and the meanings of the
    # parameters differ depending on how many there are.  so
    # we make some effort for some of the commands
    _command_params = {
        "bail": bool,
        "changes": bool,
        "echo": bool,
        "exceptions": bool,
        "header": bool,
        "timer": bool,
    }
    _command_params["headers"] = _command_params["header"]

    _builtin_commands = None

    def complete_command(self, line, token, beg, end):
        """Provide some completions for dot commands

        :param line: The current complete input line
        :param token: The word readline is looking for matches
        :param beg: Integer offset of token in line
        :param end: Integer end of token in line
        :return: A list of completions, or an empty list if none
        """
        if not self._builtin_commands:
            self._builtin_commands = [
                "." + x[len("command_"):] for x in dir(self) if x.startswith("command_") and x != "command_headers"
            ]

        t = self._get_prev_tokens(line, end)
        if len(t) <= 1 and token:
            return [x + " " for x in self._builtin_commands if x.startswith(token)]

        completions = []

        if t[0] in {"colour", "color"}:
            completions = list(self._colours.keys())
        elif t[0] in {"mode"}:
            completions = self._output_modes
        elif t[0] == "help":
            completions = [v[1:] for v in self._builtin_commands] + ["all"]
        elif t[0] == "dbconfig":
            completions = [
                v[len("SQLITE_DBCONFIG_"):].lower() for v in apsw.mapping_db_config
                if isinstance(v, str) and v not in self._dbconfig_ignore
            ]
        elif t[0] == "parameter":
            if len(t) == 1 or (len(t) == 2 and token):
                completions = ["clear", "list", "unset ", "set "]
            elif len(t) >= 2 and t[1] == "unset" and (len(t) == 2 or token):
                completions = list(self.bindings.keys())
        elif self._command_params.get(t[0], None) is bool:
            completions = ["on", "off", "ON", "OFF"]
        elif t[0] in self._command_params:
            completions = self._command_params[t[0]]

        return [v for v in sorted(completions) if v.startswith(token) and v != token]


    ### Output helpers
    @dataclasses.dataclass(**({"slots": True, "frozen": True} if sys.version_info >= (3, 10) else {}))
    class Row:
        "Returned by :class:`Shell.PositionRow`"
        is_first: bool
        is_last: bool
        row: apsw.SQLiteValues
        columns: tuple[str, ...]

    class PositionRow:
        "Wraps an iterator so you know if a row is first, last, both, or neither"

        def __init__(self, source):
            self.source = source
            self.rows = []
            self.end = False
            self.index = -1
            try:
                self.columns = tuple(h for h, _ in source.get_description())
            except apsw.ExecutionCompleteError:
                self.columns = None

        def __iter__(self):
            return self

        def __next__(self) -> Shell.Row:
            if self.end:
                if not self.rows:
                    raise StopIteration
                assert len(self.rows) == 1
                row = self.rows.pop(0)
                if self.index == 0:
                    return Shell.Row(is_first=True, is_last=True, row=row, columns=self.columns)
                return Shell.Row(is_first=False, is_last=True, row=row, columns=self.columns)
            try:
                self.rows.append(next(self.source))
            except StopIteration:
                self.end = True
                return next(self)
            self.index += 1
            if self.index == 0:
                return next(self)
            if self.index == 1:
                return Shell.Row(is_first=True, is_last=False, row=self.rows.pop(0), columns=self.columns)
            assert len(self.rows) == 2
            return Shell.Row(is_first=False, is_last=False, row=self.rows.pop(0), columns=self.columns)

    ### Colour support

    def _out_colour(self):
        # Sets up color for output.  Input being interactive doesn't
        # matter.  This method needs to be called on all changes to
        # output.
        if getattr(self.stdout, "isatty", False) and self.stdout.isatty():
            self.colour = self._colours[self.colour_scheme]
        else:
            self.colour = self._colours["off"]

    # This class returns an empty string for all undefined attributes
    # so that it doesn't matter if a colour scheme leaves something
    # out.
    class _colourscheme:

        def __init__(self, **kwargs):
            for k, v in kwargs.items():
                setattr(self, k, v)

        def __nonzero__(self):
            return True

        def __str__(self):
            return "_colourscheme(" + str(vars(self)) + ")"

        def __getattr__(self, k):
            return ""

        def colour_value(self, val, formatted):
            c = self.colour
            if val is None:
                return self.vnull + formatted + self.vnull_
            if isinstance(val, str):
                return self.vstring + formatted + self.vstring_
            if isinstance(val, bytes):
                return self.vblob + formatted + self.vblob_
            # must be a number - we don't distinguish between float/int
            return self.vnumber + formatted + self.vnumber_

    # The colour definitions - the convention is the name to turn
    # something on and the name with an underscore suffix to turn it
    # off
    d = _colourscheme(**dict([(v, "\x1b[" + str(n) + "m") for n, v in {
        0: "reset",
        1: "bold",
        4: "underline",
        22: "bold_",
        24: "underline_",
        7: "inverse",
        27: "inverse_",
        30: "fg_black",
        31: "fg_red",
        32: "fg_green",
        33: "fg_yellow",
        34: "fg_blue",
        35: "fg_magenta",
        36: "fg_cyan",
        37: "fg_white",
        39: "fg_",
        40: "bg_black",
        41: "bg_red",
        42: "bg_green",
        43: "bg_yellow",
        44: "bg_blue",
        45: "bg_magenta",
        46: "bg_cyan",
        47: "bg_white",
        49: "bg_"
    }.items()]))

    _colours = {"off": _colourscheme(colour_value=lambda x, y: y)}

    _colours["default"] = _colourscheme(prompt=d.bold,
                                        prompt_=d.bold_,
                                        error=d.fg_red + d.bold,
                                        error_=d.bold_ + d.fg_,
                                        intro=d.fg_blue + d.bold,
                                        intro_=d.bold_ + d.fg_,
                                        transient=d.fg_green,
                                        transient_=d.fg_,
                                        summary=d.fg_blue + d.bold,
                                        summary_=d.bold_ + d.fg_,
                                        header=d.underline,
                                        header_=d.underline_,
                                        vnull=d.fg_red,
                                        vnull_=d.fg_,
                                        vstring=d.fg_yellow,
                                        vstring_=d.fg_,
                                        vblob=d.fg_blue,
                                        vblob_=d.fg_,
                                        vnumber=d.fg_magenta,
                                        vnumber_=d.fg_)
    # unpollute namespace
    del d
    del _colourscheme
    try:
        del n
        del x
        del v
    except Exception:
        pass


def main() -> None:
    # Docstring must start on second line so dedenting works correctly
    """
    Call this to run the :ref:`interactive shell <shell>`.  It
    automatically passes in sys.argv[1:] and exits Python when done.

    """
    try:
        s = Shell()
        _, _, cmds = s.process_args(sys.argv[1:])
        if len(cmds) == 0:
            s.cmdloop()
    except:
        v = sys.exc_info()[1]
        if isinstance(v, SystemExit):
            raise
        if getattr(v, "_handle_exception_saw_this", False):
            pass
        else:
            # Where did this exception come from?
            traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()
