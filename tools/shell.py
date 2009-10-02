#!/usr/bin/env python

import sys
import apsw
import shlex
import os
import csv
import re
import textwrap

class Shell:
    """Implements the SQLite shell

    You can inherit from this class to embed in your own code and user
    interface.  Internally everything is handled as unicode.
    Conversions only happen at the point of input or output which you
    can override in your own code.

    This version fixes a number of bugs present in the sqlite shell.
    Its control-C handling is also friendlier.

    * http://www.sqlite.org/src/info/eb620916be
    * http://www.sqlite.org/src/info/f12a9eeedc
    """

    class Error(Exception):
        "Class raised on errors"
        pass
    
    def __init__(self, stdin=None, stdout=None, stderr=None, encoding="utf8", display_exceptions=True, args=None):
        """Create instance, set defaults and do argument processing.

        :param stdin: Where to read input from (default sys.stdin)
        :param stdout: Where to send output (default sys.stdout)
        :param stderr: Where to send errors (default sys.stderr)
        :param display_exceptions: If True then exceptions are displayed on stderr,
           They are then re-raised.
        :param args: This should be program arguments only (ie if
           passing in sys.argv do not include sys.argv[0] which is the
           program name.
        """
        self.history_file="~/.sqlite_history"
        self.db=None
        self.dbfilename=None
        self.prompt=    "sqlite> "
        self.moreprompt="    ..> "
        self.separator="|"
        self.bail=False
        self.echo=False
        self.header=False
        self.nullvalue=""
        self.output=self.output_list
        self.widths=[]
        # do we truncate output in list mode?  (explain doesn't, regular does)
        self.truncate=True
        # a stack of previous outputs. turning on explain saves previous, off restores
        self._output_stack=[]
        # save initial (default) output settings
        self.push_output()

        # other stuff
        self.encoding=encoding
        if stdin is None: stdin=sys.stdin
        if stdout is None: stdout=sys.stdout
        if stderr is None: stderr=sys.stderr
        self.stdin=stdin
        self.stdout=stdout
        self.original_stdout=self.stdout
        self.stderr=stderr
        self.interactive=stdin.isatty() and stdout.isatty()
        self.out=self.stdout
        
        if args:
            try:
                self.process_args(args)
            except:
                self.handle_exception(display_exceptions)
                raise

    def process_args(self, args):
        """Process command line options specified in args.  It is safe to
        call this multiple times.  We try to be compatible with SQLite shell
        argument parsing.

        The first non-option is the database file name.  Each
        remaining non-option is treated as a complete input (ie it
        isn't joined with others looking for ;).

        SQLite shell uses single dash in front of options.  We allow
        both single and double dashes.
        """
        # we don't use optparse as we need to use single dashes for
        # options - all hand parsed
        if not args:
            return

        options=True
        havedbname=False
        inits=[]
        sqls=[]

        while args:
            if not args[0].startswith("-"):
                options=False
                if not havedbname:
                    # grab new database
                    if self.db: self.db.close()
                    self.db=None
                    self.dbfilename=args[0]
                    havedbname=True
                else:
                    sqls.append(args[0])
                args=args[1:]
                continue

            # remove initial single or double dash
            args[0]=args[0][1:]
            if args[0].startswith("-"):
                args[0]=args[0][1:]

            if args[0]=="init":
                if len(args)<2:
                    raise self.Error("You need to specify a filename after -init")
                inits.append(args[1])
                args=args[2:]
                contintue

            if args[0]=="header" or args[0]=="noheader":
                self.header=args[0]=="header"
                args=args[1:]
                continue

            if args[0] in ("echo", "bail", "interactive"):
                setattr(self, args[0], True)
                args=args[1:]
                continue

            if args[0]=="batch":
                self.interactive=False
                args=args[1:]
                continue

            if args[0] in ("separator", "nullvalue", "encoding"):
                if len(args)<2:
                    raise self.Error("You need to specify a value after -"+args[0])
                setattr(self, args[0], args[1])
                args=args[2:]
                contintue

            if args[0]=="version":
                self._write(self.stdout, apsw.sqlitelibversion()+"\n")
                # A pretty gnarly thing to do
                sys.exit(0)
            
            # only remaining known args are output modes
            if getattr(self, "output_"+args[0], None):
                self.command_mode(args[:1])
                args=args[1:]
                continue
                
            newargs=self.process_unknown_args(args)
            if newargs is None:
                raise self.Error(usage())
            args=newargs
            
        for f in inits:
            self.read_file_named(f)

        for s in sqls:
            if s.startswith("."):
                self.process_command(s)
            else:
                self.process_sql(s)

    def process_unknown_args(self, args):
        return None

    def usage(self):
        "Returns the usage message"

        msg="""
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
"""
        return msg.lstrip()

    ###
    ### The various output routines.  They are always called with the
    ### header irrespective of the setting allowing for some per query
    ### setup. (see output_column for example).
    ### 
    ###

    if sys.version_info>=(3,0):
        _string_types=(str,)
        _binary_types=(bytes,)
    else:
        _string_types=(str,unicode)
        _binary_types=(buffer,)

    def _fmt_text_col(self, v):
        if v is None:
            return self.nullvalue
        elif isinstance(v, self._string_types):
            return v
        elif isinstance(v, self._binary_types):
            # sqlite gives back raw bytes!
            return "<Binary data>"
        else:
            return "%s" % (v,)
            

    def output_column(self, header, line):
        """Items left aligned in space padded columns

        The SQLite shell has rather bizarre behaviour.  If the width hasn't been
        specified for a column then 10 is used unless the column name (header) is
        longer in which case that is used.
        """
        # as an optimization we calculate self._actualwidths which is
        # reset for each query
        if header:
            # calculate _actualwidths
            widths=self.widths[:len(line)]
            while len(widths)<len(line):
                i=len(widths)
                text=self._fmt_text_col(line[i])
                if len(text)<10:
                    widths.append(10)
                else:
                    widths.append(len(text))
            self._actualwidths=widths
                                        
            if self.header:
                # output the headers
                self.output_column(False, line)
                self.output_column(False, ["-"*widths[i] for i in range(len(widths))])

            return

        if self.truncate:
            cols=["%-*.*s" % (self._actualwidths[i], self._actualwidths[i], self._fmt_text_col(line[i])) for i in range(len(line))]
        else:
            cols=["%-*s" % (self._actualwidths[i],  self._fmt_text_col(line[i])) for i in range(len(line))]
        self._write(self.stdout, " ".join(cols)+"\n")

    def output_csv(self, header, line):
        "Items in csv format using current separator"
        # we use self._csv for the work, setup when header is
        # supplied. _csv is a tuple of a StringIO and the csv.writer
        # instance
        if header:
            if sys.version_info<(3,0):
                import StringIO
                s=StringIO.StringIO()
            else:
                import io
                s=io.StringIO()
            import csv
            writer=csv.writer(s, delimiter=self.separator)
            self._csv=(s, writer)
            if self.header:
                self.output_csv(False, line)
            return
        line=[self._fmt_text_col(l) for l in line]
        self._csv[1].writerow(line)
        t=self._csv[0].getvalue()
        self._csv[0].truncate(0)
        if t.endswith("\r\n"):
            t=t[:-2]
        elif t.endswith("\r") or t.endswith("\n"):
            t=t[:-1]
        self._write(self.stdout, t+"\n")

    def _fmt_sql_identifier(self, v):
        "Return the string quoted if needed"
        nonalnum=re.sub("[A-Za-z_0-9]+", "", v)
        if len(nonalnum)==0:
            return v
        # double quote it unless there are any
        if '"' in nonalnum:
            return "[%s]" % (v,)
        return '"%s"' % (v,)

    def _fmt_sql_value(self, v):
        if v is None:
            return "NULL"
        elif isinstance(v, self._string_types):
            return "'"+v.replace("'", "''")+"'"
        elif isinstance(v, self._binary_types):
            res=["X'"]
            if sys.version_info<(3,0):
                trans=lambda x: ord(x)
            else:
                trans=lambda x: x
            for byte in v:
                res.append("%02X" % (trans(byte),))
            res.append("'")
            return "".join(res)
        else:
            return "%s" % (v,)

    def output_insert(self, header, line):
        if header:
            return
        out="INSERT INTO "+self._fmt_sql_identifier(self._output_table)+" VALUES("+",".join([self._fmt_sql_value(l) for l in line])+");\n"
        self._write(self.stdout, out)

    def output_line(self, header, line):
        if header:
            w=5
            for l in line:
                if len(l)>w:
                    w=len(l)
            self._line_info=(w, line)
            return
        w=self._line_info[0]
        for i in range(len(line)):
            self._write(self.stdout, "%*s = %s\n" % (w, self._line_info[1][i], self._fmt_text_col(line[i])))
        self._write(self.stdout, "\n")

    def output_list(self, header, line):
        "All items on one line with separator"
        if header and not self.header:
            return
        fmt=self._fmt_text_col
        self._write(self.stdout, self.separator.join([fmt(x) for x in line])+"\n")

    def _fmt_html_col(self, v):
        return self._fmt_text_col(v).\
           replace("&", "&amp;"). \
           replace("<", "&gt;"). \
           replace(">", "&lt;")
        

    def output_html(self, header, line):
        "HTML table style"
        if header and not self.header:
            return
        line=[self._fmt_html_col(l) for l in line]
        out=["<TR>"]
        for l in line:
            out.append(("<TD>","<TH>")[header])
            out.append(l)
            out.append(("</TD>\n","</TH>\n")[header])
        out.append("</TR>\n")
        self._write(self.stdout, "".join(out))

    def _fmt_python(self, v):
        if v is None:
            return "None"
        elif isinstance(v, self._string_types):
            # ::TODO:: we need to \u escape stuff
            # if something is entirely in ascii then
            # no need for u prefix
            return repr(v)
        elif isinstance(v, self._binary_types):
            if sys.version_info<(3,0):
                res=["buffer(\""]
                for i in v:
                    if ord(i) in self._printable:
                        res.append(i)
                    else:
                        res.append("\\x%02X" % (ord(i),))
                res.append("\")")
                return "".join(res)
            else:
                res=['b"']
                for i in v:
                    res.append("%02X" % (i,))
                res.append('"')
                return "".join(res)
        else:
            return "%s" % (v,)

    def output_python(self, header, line):
        "Tuples in Python source form for each row"
        if header and not self.header:
            return
        self._write(self.stdout, '('+", ".join([self._fmt_python(l) for l in line])+"),\n")

    def _backslashify(self, s):
        v=['"']
        for c in s:
            if c=="\\":
                v.append("\\\\")
            elif c=="\r":
                v.append("\\r")
            elif c=="\n":
                v.append("\\n")
            elif c=="\t":
                v.append("\\t")
            else:
                # in theory could check for how 'printable' the char is
                v.append(c)
        v.append('"')
        return "".join(v)

    # bytes that are ok in C strings - no need for quoting
    _printable=[ord(x) for x in
                "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789~!@#$%^&*()`_-+={}[]:;,.<>/?|"
                ]

    def _fmt_c_string(self, v):
        if isinstance(v, self._string_types):
            return self._backslashify(v)
        elif v is None:
            return '"'+self.nullvalue+'"'
        elif isinstance(v, self._binary_types):
            res=['"']
            if sys.version_info<(3,0):
                o=lambda x: ord(x)
            else:
                o=lambda x: x
            for c in v:
                if o(c) in self._printable:
                    res.append(o)
                else:
                    res.append("\\x%02X" % (o(c),))
            res.append('"')
            return "".join(res)
        else:
            return '"%s"' % (v,)
    

    def output_tcl(self, header, line):
        "Outputs TCL/C style strings using current separator"
        # In theory you could paste the output into your source ...
        if header and not self.header:
            return
        self._write(self.stdout, self.separator.join([self._fmt_c_string(l) for l in line])+"\n")

        
    def cmdloop(self, intro=None, display_exceptions=True):
        """Runs the main command loop.

        :param intro: Initial text banner to display.  Make sure you newline terminate it.
        :param display_exceptions: If True then when exceptions happen they are displayed else
            they are raised.  If displayed and bail is True then the loop exits.
        """
             
        if intro is None:
            intro=u"""
SQLite version %s (APSW %s)
Enter ".help" for instructions
Enter SQL statements terminated with a ";"
""" % (apsw.sqlitelibversion(), apsw.apswversion())
            intro=intro.lstrip()
        if self.interactive and intro:
            if sys.version_info<(3,0):
                intro=unicode(intro)
            self._write(self.out, intro)

        using_readline=False
        try:
            if self.interactive and self.stdin is sys.stdin:
                import readline
                old_completer=readline.get_completer()
                readline.set_completer(self.complete)
                readline.parse_and_bind("tab: complete")
                using_readline=True
                readline.read_history_file(os.path.expanduser(self.history_file))
        except ImportError:
            pass

        try:
            # Keyboard interrupt handling interrupts any current
            # operations, prints out ^C and resets back to a normal
            # prompt.  With SQLite shell you are screwed if on second
            # or more line of SQL statement.
            exit=False
            while not exit:
                if using_readline:
                    # we drop completion cache because it contains
                    # table and column names which could have changed
                    # with last executed SQL
                    self._completion_cache=None
                try:
                    command=self._getline(self.prompt)
                    if command is None: # eof
                        break
                    #  ignore blank/whitespace only lines at statement/command boundaries
                    if len(command.strip())==0: continue
                    if command[0]=="?": command=".help"
                    # If not a dot command then keep getting more until complete
                    while len(command) and command[0]!="." and not apsw.complete(command):
                        line=self._getline(self.moreprompt)
                        if line is None: # eof
                            self._write(self.stderr, "Incomplete SQL: %s\n" % (command,))
                            exit=True
                            break
                        else:
                            command=command+"\n"+line
                    if exit: break
                    try:
                        # To match sqlite shell we only bail on sql not commands
                        canbail=True # ::TODO:: set this back to false
                        if command[0]==".":
                            exit=self.process_command(command)
                            continue
                        canbail=True
                        self.process_sql(command)
                    except KeyboardInterrupt:
                        self.db.interrupt()
                        raise
                    except:
                        self.handle_exception(display_exceptions)
                        if canbail and self.bail:
                            break
                except KeyboardInterrupt:
                    self._write(self.stdout, "^C\n")
                    self.stdout.flush()
        finally:
            if using_readline:
                readline.set_completer(old_completer)
                readline.set_history_length(256)
                readline.write_history_file(os.path.expanduser(self.history_file))

    def handle_exception(self, display=True):
        "Handles the current exception.  If display is True then it is displayed, else it is raised"
        eval=sys.exc_info()[1] # py2&3 compatible way of doing this
        if not display or isinstance(eval, SystemExit):
            raise
        if isinstance(eval, (self.Error, apsw.Error, ValueError)):
            text=eval.args[0]
        else:
            import traceback
            traceback.print_exc()
            import pdb ; pdb.set_trace()
            pass
        if not text.endswith("\n"):
            text=text+"\n"
        self._write(self.stderr, text)

    def ensure_db(self):
        "The database isn't opened until first use.  This function ensures it is now open"
        if not self.db:
            if not self.dbfilename:
                self.dbfilename=":memory:"
            self.db=apsw.Connection(self.dbfilename)

    def process_sql(self, sql, bindings=None):
        "Processes SQL text consisting of one or more statements"
        self.ensure_db()
        cur=self.db.cursor()
        # we need to know when each new statement is executed
        state={'newsql': True}
        def et(cur, sql, bindings):
            state['newsql']=True
            # print statement if echo is on
            if self.echo:
                # ? should we strip leading and trailing whitespace? backslash quote stuff?
                if bindings:
                    self._write(self.stderr, u"%s [%s]\n" % (sql, bindings))
                else:
                    self._write(self.stderr, sql+"\n")
            return True
        cur.setexectrace(et)
        # processing loop
        for row in cur.execute(sql, bindings):
            if state['newsql']:
                # output a header always
                cols=[h for h,d in cur.getdescription()]
                self.output(True, cols)
                state['newsql']=False
            self.output(False, row)
            
    def process_command(self, cmd):
        """Processes a dot command.
        It is split into parts using the shlex.split function which is roughly the
        same method used by Unix/POSIX shells.
        """
        if self.echo:
            self._write(self.stderr, cmd+"\n")
        cmd=shlex.split(cmd)
        assert cmd[0][0]=="."
        cmd[0]=cmd[0][1:]
        fn=getattr(self, "command_"+cmd[0], None)
        if not fn:
            raise self.Error("Unknown command \"%s\".  Enter \".help\" for help" % (cmd[0],))
        exit=bool(fn(cmd[1:]))
        return exit

    ###
    ### Commands start here
    ###

    # Note that doc text is used for generating help output.

    def command_backup(self, cmd):
        """backup ?DB? FILE: Backup DB (default "main") to FILE

        Copies the contents of the current database to FILE
        overwriting whatever was in FILE.  If you have attached databases
        then you can specify their name instead of the default of "main".

        The backup is done at the page level - SQLite copies the pages
        as is.  There is no round trip through SQL code.
        """
        dbname="main"
        if len(cmd)==1:
            fname=cmd[0]
        elif len(cmd)==2:
            dbname=cmd[0]
            fname=cmd[1]
        else:
            raise self.Error("Backup takes one or two parameters")
        self.ensure_db()
        out=apsw.Connection(fname)
        b=out.backup(dbname, self.db, "main")
        try:
            while not b.done:
                b.step()
        finally:
            b.finish()
            out.close()
        return False

    def command_bail(self, cmd):
        """bail ON|OFF: Stop after hitting a SQL error (default OFF)

        If an error is encountered while processing SQL then exit.
        Errors in dot commands do not cause an exit no matter what the
        bail setting.
        """
        if len(cmd)!=1:
            raise self.Error("bail 'ON' or 'OFF'")
        if cmd[0].lower()=="on":
            self.bail=True
        elif cmd[0].lower()=="off":
            self.bail=False
        else:
            raise self.Error("Expected 'ON' or 'OFF'")
        return False

    def command_echo(self, cmd):
        """echo ON|OFF: If ON then each SQL statement or command is printed before execution (default OFF)

        The SQL statement or command is sent to error output so that
        it is not intermingled with regular output.
        """
        if len(cmd)!=1:
            raise self.Error("echo 'ON' or 'OFF'")
        if cmd[0].lower()=="on":
            self.echo=True
        elif cmd[0].lower()=="off":
            self.echo=False
        else:
            raise self.Error("Expected 'ON' or 'OFF'")
        return False

    def command_exit(self, cmd):
        """exit:Exit this program

        """
        if len(cmd):
            raise self.Error("Exit doesn't take any parameters")
        return True

    def command_explain(self, cmd):
        """explain ON|OFF: Set output mode suitable for explain (default OFF)

        Explain shows the underlying SQLite virtual machine code for a
        statement.  You need to prefix the SQL with explain.  For example:

           explain select * from table;

        This output mode formats the explain output nicely.  If you do
        '.explain OFF' then the output mode and settings in place when
        you did '.explain ON' are restored.
        """
        if len(cmd)>1:
            raise self.Error("explain takes at most one parameter")
        if len(cmd)==0 or cmd[0].lower()=="on":
            self.push_output()
            self.header=True
            self.widths=[4,13,4,4,4,13,2,13]
            self.truncate=False
            self.output=self.output_column
        elif cmd[0].lower()=="off":
            self.pop_output()
        else:
            raise self.Error("Unknown value for explain")

    def command_header(self, cmd):
        """header(s) ON|OFF: Display the column names in output (default OFF)

        """
        if len(cmd)!=1:
            raise self.Error("header takes exactly one parameter")
        if cmd[0].lower()=="on":
            self.header=True
        elif cmd[0].lower()=="off":
            self.header=False
        else:
            raise self.Error("Expected 'ON' or 'OFF'")
        return False

    command_headers=command_header

    _help_info=None
    
    def command_help(self, cmd):
        """help ?COMMAND?: Shows list of commands and their usage.  If COMMAND is specified then shows detail about that COMMAND.  ('.help all' will show detailed help about all commands.)
        """
        if not self._help_info:
            # buildup help database
            self._help_info={}
            for c in dir(self):
                if not c.startswith("command_"):
                    continue
                # help is 3 parts
                # - the syntax string (eg backup ?dbname? filename)
                # - the one liner description (eg saves database to filename)
                # - the multi-liner detailed description
                # We grab this from the doc string for the function in the form
                #   syntax: one liner\nmulti\nliner
                d=getattr(self, c).__doc__
                assert d, c+" command must have documentation"
                c=c[len("command_"):]
                if c=="headers": continue
                while d[0]=="\n":
                    d=d[1:]
                parts=d.split("\n", 1)
                firstline=parts[0].strip().split(":", 1)
                assert len(firstline)==2, c+" command must have usage: description doc"
                if c=="mode":
                    if not self._output_modes:
                        self._cache_output_modes()
                    firstline[1]=firstline[1]+" ".join(self._output_modes)
                multi=textwrap.dedent(parts[1])
                if len(multi.strip())==0: # All whitespace
                    multi=None
                else:
                    multi=multi.strip("\n")
                    # we need to keep \n\n as a newline but turn all others into spaces
                    multi=multi.replace("\n\n", "\x00")
                    multi=multi.replace("\n", " ")
                    multi=multi.replace("\x00", "\n\n")
                    multi=multi.split("\n\n")
                self._help_info[c]=('.'+firstline[0].strip(), firstline[1].strip(), multi)

        self._write(self.stderr, "\n")

        tw=self._terminal_width()
        if tw<32:
            tw=32
        if len(cmd)==0:
            commands=self._help_info.keys()
            commands.sort()
            w=0
            for command in commands:
                if len(self._help_info[command][0])>w:
                    w=len(self._help_info[command][0])
            out=[]
            for command in commands:
                hi=self._help_info[command]
                # usage string
                out.append(hi[0])
                # space padding (including 2 for between columns)
                out.append(" "*(2+w-len(hi[0])))
                # usage message wrapped if need be
                out.append(("\n"+" "*(2+w)).join(textwrap.wrap(hi[1], tw-w-2)))
                # newline
                out.append("\n")
            self._write(self.stderr, "".join(out))
        else:
            if cmd[0]=="all":
                cmd=self._help_info.keys()
                cmd.sort()
            w=0
            for command in self._help_info:
                if len(self._help_info[command][0])>w:
                    w=len(self._help_info[command][0])

            for command in cmd:
                if command=="headers": command="header"
                if command not in self._help_info:
                    raise self.Error("No such command \"%s\"" % (command,))
                out=[]
                hi=self._help_info[command]
                # usage string
                out.append(hi[0])
                # space padding (2)
                out.append(" "*(2+w-len(hi[0])))
                # usage message wrapped if need be
                out.append(("\n"+" "*(2+w)).join(textwrap.wrap(hi[1], tw-w-2))+"\n")
                if hi[2]:
                    # newlines
                    out.append("\n")
                    # detailed message
                    for i,para in enumerate(hi[2]):
                        out.append(textwrap.fill(para, tw)+"\n")
                        if i<len(hi[2])-1:
                            out.append("\n")
                # if not first one then print separator header
                if command!=cmd[0]:
                    self._write(self.stderr, "\n"+"="*tw+"\n")
                self._write(self.stderr, "".join(out))
        self._write(self.stderr, "\n")
        return False
        
    _output_modes=None
    
    def command_mode(self, cmd):
        """mode MODE ?TABLE?: Sets output mode to one of

        """
        if len(cmd) in (1,2):
            w=cmd[0]
            if w=="tabs":
                w="list"
            m=getattr(self, "output_"+w, None)
            if w!="insert":
                if len(cmd)==2:
                    raise self.Error("Output mode %s doesn't take parameters" % (cmd[0]))
            if m:
                self.output=m
                # set some defaults
                self.truncate=True
                if cmd[0]=="csv":
                    self.separator=","
                elif cmd[0]=="tabs":
                    self.separator="\t"
                else:
                    pass
                    #self.separator=self._output_stack[0]["separator"]
                if w=="insert":
                    if len(cmd)==2:
                        self._output_table=cmd[1]
                    else:
                        self._output_table="table"
                return False
        if not self._output_modes:
            self._cache_output_modes()
        raise self.Error("Expected a valid output mode: "+", ".join(self._output_modes))

    # needed so command completion can call it
    def _cache_output_modes(self):
        modes=[m[len("output_"):] for m in dir(self) if m.startswith("output_")]
        modes.append("tabs")
        modes.sort()
        self._output_modes=modes

    def command_restore(self, cmd):
        """restore ?DB? FILE: Restore database from FILE into DB (default "main")
        
        Copies the contents of FILE to the current database (default "main").
        The backup is done at the page level - SQLite copies the pages as
        is.  There is no round trip through SQL code.
        """
        dbname="main"
        if len(cmd)==1:
            fname=cmd[0]
        elif len(cmd)==2:
            dbname=cmd[0]
            fname=cmd[1]
        else:
            raise self.Error("Restore takes one or two parameters")
        self.ensure_db()
        input=apsw.Connection(fname)
        b=self.db.backup(dbname, input, "main")
        try:
            while not b.done:
                b.step()
        finally:
            b.finish()
            input.close()
        return False

    def command_separator(self, cmd):
        """separator STRING: Change separator for output mode and .import

        You can use quotes and backslashes.  For example to set the
        separator to space tab space you can use:

          .separator " \\t "

        The setting is automatically changed when you switch to csv or
        tabs output mode.  You should also set it before doing an
        import (ie , for CSV and \\t for TSV).
        """
        if len(cmd)!=1:
            raise self.Error("separator takes exactly one parameter")
        self.separator=self.fixup_backslashes(cmd[0])
        return False

    _shows=("echo", "explain", "headers", "mode", "nullvalue", "output", "separator", "width")

    def command_show(self, cmd):
        """show: Show the current values for various settings.
        """
        if len(cmd)>1:
            raise self.Error("show takes at most one parameter")
        if len(cmd):
            what=cmd[0]
            if what not in _shows:
                raise self.Error("Unknown show: '%s'" % (what,))
        else:
            what=None

        outs=[]
        for i in self._shows:
            k=i
            if what and i!=what:
                continue
            # boolean settings
            if i in ("echo", "headers"):
                if i=="headers": i="header"
                v="off"
                if getattr(self, i):
                    v="on"
            elif i=="explain":
                # we cheat by looking at truncate setting!
                v="on"
                if self.truncate:
                    v="off"
            elif i in ("nullvalue", "separator"):
                v='"'+self._backslashify(getattr(self, i))+'"'
            elif i=="mode":
                if not self._output_modes:
                    self._cache_output_modes()
                for v in self._output_modes:
                    if self.output==getattr(self, "output_"+v):
                        break
                else:
                    assert False, "Bug: didn't find output mode"
            elif i=="output":
                if self.stdout is self.original_stdout:
                    v="stdout"
                else:
                    v=self.stdout.name
            elif i=="width":
                v=" ".join(["%d"%(i,) for i in self.widths])
            else:
                assert False, "Bug: unknown show handling"
            outs.append( (k,v) )

        # find width of k column
        l=0
        for k,v in outs:
            if len(k)>l:
                l=len(k)

        for k,v in outs:
            self._write(self.stdout, "%*.*s: %s\n" % (l,l, k, v))
            
        return False
            

    def command_width(self, cmd):
        """width NUM NUM ...: Set the column widths for "column" mode

        In "column" output mode, each column is a fixed width with values truncated to
        fit.  The default column width is 10 characters which you can change
        column by column with this command.
        """
        # Code is a bit crazy.  SQLite sets the widths as specified
        # except a zero truncates the widths at that point.  If the
        # new widths are less than old then old ones longer than new
        # ones remain.
        #
        # sqlite> .width 1 2 3 0 4 5 6
        #   width: 1 2 3 
        # sqlite> .width 99
        #   width: 99 2 3 
        #
        # This whole zero behaviour is probably because it doesn't
        # check the numbers are actually valid - just uses atoi
        w=[]
        for i in cmd:
            try:
                n=int(i)
                if n==0:
                    self.widths=w
                    return
                w.append(n)
            except:
                raise self.Error("'%s' is not a valid number" % (i,))
        self.widths=w+self.widths[len(w):]
        return False

    def _terminal_width(self):
        try:
            if sys.platform=="win32":
                import ctypes, struct
                h=ctypes.windll.kernel32.GetStdHandle(-12) # -12 is stderr
                buf=ctypes.create_string_buffer(22)
                if ctypes.windll.kernel32.GetConsoleScreenBufferInfo(h, buf):
                    _,_,_,_,_,left,top,right,bottom,_,_=struct.unpack("hhhhHhhhhhh", buf.raw)
                    return right-left+1
                raise Exception()
            else:
                # posix
                import struct, fcntl, termios
                s=struct.pack('HHHH', 0,0,0,0)
                x=fcntl.ioctl(2, termios.TIOCGWINSZ, s)
                return struct.unpack('HHHH', x)[1]
        except:
            try:
                v=int(os.getenv("COLUMNS"))
                if v<10:
                    return 80
                return v
            except:
                return 80

    def push_output(self):
        o={}
        for k in "separator", "header", "nullvalue", "output", "widths", "truncate":
            o[k]=getattr(self, k)
        self._output_stack.append(o)

    def pop_output(self):
        # first item should always be present
        assert len(self._output_stack)
        if len(self._output_stack)==1:
            o=self._output_stack[0]
        else:
            o=self._output_stack.pop()
        for k,v in o.items():
            setattr(self,k,v)
            

    def fixup_backslashes(self, s):
        """Implements for various backlash sequences in s

        This function is needed because shlex does not do it for us.
        """
        if "\\" not in s: return s
        # See the resolve_backslashes function in SQLite shell source
        res=[]
        i=0
        while i<len(s):
            if s[i]!="\\":
                res.append(s[i])
                i+=1
                continue
            i+=1
            if i>=len(s):
                raise self.Error("Backslash with nothing following")
            c=s[i]
            i+=1 # advance again
            if c=="\\":
                res.append(c)
                continue
            if c=="n":
                res.append("\n")
                continue
            if c=="t":
                res.append("\t")
                continue
            if c=="r":
                res.append("\r")
                continue
            raise self.Error("Unknown backslash sequence \\"+c)
        return "".join(res)
                

    if sys.version_info<(3,0):
        def _write(self, dest, text):
            "Writes text to dest using encoding"
            # ensure text is unicode
            if type(text)!=unicode:
                text=unicode(text)
            encoding=getattr(dest, "encoding", self.encoding)
            if encoding is None: encoding=self.encoding
            dest.write(text.encode(encoding))
    else:
        def _write(self, dest, text):
            "Writes unicode/bytes to dest"
            if type(text) is bytes:
                assert(hasattr(dest, buffer))
                dest.buffer.write(text)
            else:
                dest.write(text)

    def _getline(self, prompt):
        """Returns a single line of input (may be incomplete SQL)

        If EOF is reached then return None.  Do not include trailing
        newline in return.
        """
        self.stdout.flush()
        self.stderr.flush()
        try:
            if self.interactive and self.stdin is sys.stdin:
                line=raw_input(prompt)
            else:
                if self.interactive:
                    self._write(self.stdout, prompt)
                line=self.stdin.readline()
        except EOFError:
            return None
        if len(line)==0:
            return None
        if line[-1]=="\n":
            line=line[:-1]
        return line
        
    def complete(self, token, state):
        """Return a possible completion for readline

        This function is called with state starting at zero to get the
        first completion, then one etc until you return None.  The best
        implementation is to generate the list when state==0, save it,
        and provide members on each increase.
        """
        if state==0:
            import readline
            # the whole line
            line=readline.get_line_buffer()
            # begining and end(+1) of the token in line
            beg=readline.get_begidx()
            end=readline.get_endidx()
            # Are we matching a command?
            if line.startswith("."):
                self.completions=self.complete_command(line, token, beg, end)
            else:
                self.completions=self.complete_sql(line, token, beg, end)

        if state>len(self.completions):
            return None
        return self.completions[state]

    # Taken from http://www.sqlite.org/lang_keywords.html
    _sqlite_keywords="""ABORTADD AFTER ALL ALTER ANALYZE AND AS ASC ATTACH AUTOINCREMENT
           BEFORE BEGIN BETWEEN BY CASCADE CASE CAST CHECK COLLATE COLUMN COMMIT
           CONFLICT CONSTRAINT CREATE CROSS CURRENT_DATE CURRENT_TIME
           CURRENT_TIMESTAMP DATABASE DEFAULT DEFERRABLE DEFERRED DELETE DESC
           DETACH DISTINCT DROP EACH ELSE END ESCAPE EXCEPT EXCLUSIVE EXISTS
           EXPLAIN FAIL FOR FOREIGN FROM FULL GLOB GROUP HAVING IF IGNORE
           IMMEDIATE IN INDEX INDEXED INITIALLY INNER INSERT INSTEAD INTERSECT
           INTO IS ISNULL JOIN KEY LEFT LIKE LIMIT MATCH NATURAL NOT NOTNULL NULL
           OF OFFSET ON OR ORDER OUTER PLAN PRAGMA PRIMARY QUERY RAISE REFERENCES
           REGEXP REINDEX RELEASE RENAME REPLACE RESTRICT RIGHT ROLLBACK ROW
           SAVEPOINT SELECT SET TABLE TEMP TEMPORARY THEN TO TRANSACTION TRIGGER
           UNION UNIQUE UPDATE USING VACUUM VALUES VIEW VIRTUAL WHEN WHERE""".split()

    _sqlite_special_names="""_ROWID_ MAIN OID ROWID SQLITE_MASTER
           SQLITE_SEQUENCE SQLITE_TEMP_MASTER TEMP""".split()

    def complete_sql(self, line, token, beg, end):
        """Provide some completions for SQL"""
        if self._completion_cache is None:
            self.ensure_db()
            cur=self.db.cursor()
            collations=[row[1] for row in cur.execute("pragma collation_list")]
            databases=[row[1] for row in cur.execute("pragma database_list")]
            other=[]
            for db in databases:
                for row in cur.execute("select * from [%s].sqlite_master" % (db,)).fetchall():
                    for col in (1,2):
                        if row[col] not in other and not row[col].startswith("sqlite_"):
                            other.append(row[col])
                    if row[0]=="table":
                        for table in cur.execute("pragma [%s].table_info([%s])" % (db, row[1],)).fetchall():
                            if table[1] not in other:
                                other.append(table[1])
                            for item in table[2].split():
                                if item not in other:
                                    other.append(item)

            self._completion_cache=[self._sqlite_keywords, self._sqlite_special_names, collations, databases, other]
            for i in len(self._completion_cache):
                self._completion_cache[i].sort()

        # This is currently not context sensitive (eg it doesn't look
        # to see if last token was 'FROM' and hence next should only
        # be table names.  That is a SMOP.
        res=[]
        ut=token.upper()
        for corpus in self._completion_cache:
            for word in corpus:
                if word.upper().startswith(ut):
                    # potential match - now match case
                    if word.startswith(token): # exact
                        if word not in res:
                            res.append(word)
                    elif word.lower().startswith(token): # lower
                        if word.lower() not in res:
                            res.append(word.lower())
                    elif word.upper().startswith(token): # upper
                        if word.upper() not in res:
                            res.append(word.upper())
                    else:
                        # match letter by letter otherwise readline mangles what was typed in
                        w=token+word[len(token):]
                        if w not in res:
                            res.append(w)
        return res

    _builtin_commands=None

    def complete_command(self, line, token, beg, end):
        if not self._builtin_commands:
            self._builtin_commands=["."+x[len("command_"):] for x in dir(self) if x.startswith("command_") and x!="command_headers"]
        if beg==0:
            return [x for x in self._builtin_commands if x.startswith(token)]
        return None
        # could in theory work out parameter completions too
        print "\n",`line`,`token`,beg,end
        return None

if __name__=='__main__':
    try:
        s=Shell(args=sys.argv[1:])
        s.cmdloop()
    except:
        print "exception exit", sys.exc_info()
        pass
