#!/usr/bin/env python

import sys
import apsw
import shlex
import os

print "apsw is",apsw.__file__

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
        self.seperator="|"
        self.bail=False
        self.echo=False
        self.header=False
        self.nullvalue=""
        self.output=self.output_list
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
            
            # only remaining args are output modes
            if getattr(self, "output_"+args[0], None):
                self.output=getattr(self, "output_"+args[0])
                args=args[1:]
                continue
                
            newargs=self.process_unknown_args(args)
            if newargs is None:
                raise self.Error(usage())
            args=newargs
            
        for f in inits:
            self.read_file_named(f)

        for s in sqls:
            self.process_sql(s)

    def usage(self):
        "Returns the usage message"

        msg="""
Usage: program [OPTIONS] FILENAME [SQL] [MORESQL...]
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

    def output_list(self, header, line):
        "All items on one line with separator"
        print header, line

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
                        if command[0]==".":
                            exit=self.process_command(command)
                            continue
                        self.process_sql(command)
                    except KeyboardInterrupt:
                        self.db.interrupt()
                        raise
                    except:
                        self.handle_exception(display_exceptions)
                        if self.bail:
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
        if isinstance(eval, (self.Error, apsw.Error)):
            text=eval.args[0]
        else:
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
                # output a header
                if self.header:
                    cols=[h for h,d in cur.getdescription()]
                    self.output(True, cols)
                state['newsql']=False
            self.output(False, row)
            
    def process_command(self, cmd):
        """Processes a dot command.
        It is split into parts using the shlex.split function which is roughly the
        same method used by Unix/POSIX shells.
        """
        cmd=shlex.split(cmd)
        assert cmd[0][0]=="."
        cmd[0]=cmd[0][1:]
        fn=getattr(self, "command_"+cmd[0], None)
        if not fn:
            raise self.Error("Unknown command \"%s\".  Enter \".help\" for help" % (cmd[0],))
        exit=bool(fn(cmd[1:]))
        return exit

    def command_exit(self, cmd):
        if len(cmd):
            raise self.Error("Exit doesn't take any parameters")
        return True

    if sys.version_info<(3,0):
        def _write(self, dest, text):
            "Writes text to dest using encoding"
            # ensure text is unicode
            if type(text)!=unicode:
                text=unicode(text)
            encoding=getattr(dest, "encoding", self.encoding)
            if encoding is None: encoding=self.encoding
            dest.write(text.decode(encoding))
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
        first completion, the one etc until you return None.  The best
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
            if line[:end].startswith("."):
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
                    # http://www.sqlite.org/src/tktview/668fe2263793beea87df571d646a0b8be2ecc4dc
                    if row[0]=="table" and db=="main":
                        for table in cur.execute("pragma table_info([%s])" % (row[1],)).fetchall():
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

if __name__=='__main__':
    try:
        s=Shell(args=sys.argv[1:])
        s.cmdloop()
    except:
        print "exception exit", sys.exc_info()
        pass
