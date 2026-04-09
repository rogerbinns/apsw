#!/usr/bin/env python3

import contextlib
import gc
import io
import json
import os
import pathlib
import random
import re
import shlex
import sys
import tempfile
import textwrap
import unittest
import warnings

import apsw
import apsw.shell

TESTFILEPREFIX = os.environ.get("APSWTESTPREFIX", "")

LOAD_EXTENSION_FILENAME = "./testextension.sqlext"

def del_temp_files():
    for name in (
        "testdb",
        "testdb2",
        "testdb3",
        "testfile",
        "testfile2",
        "testdb2x",
        "test-shell-1",
        "test-shell-1.py",
        "test-shell-in",
        "test-shell-out",
        "test-shell-err",
    ):
        for i in "-shm", "-wal", "-journal", "":
            p =pathlib.Path(TESTFILEPREFIX + name + i)
            if p.exists():
                p.unlink()

def random_integers(howmany):
    for i in range(howmany):
        yield (random.randint(0, 9999999999),)

def suppress_warning(name):
    if hasattr(__builtins__, name):
        warnings.simplefilter("ignore", getattr(__builtins__, name))


class Shell(unittest.TestCase):

    def setUp(self):
        del_temp_files()

    def tearDown(self):
        for c in apsw.connections():
            c.close()
        del_temp_files()

    def assertTablesEqual(self, dbl, left, dbr, right):
        # Ensure tables have the same contents.  Rowids can be
        # different and select gives unordered results so this is
        # quite challenging
        l = dbl.cursor()
        r = dbr.cursor()
        # check same number of rows
        lcount = l.execute("select count(*) from [" + left + "]").get
        rcount = r.execute("select count(*) from [" + right + "]").get
        self.assertEqual(lcount, rcount)
        # check same number and names and order for columns
        lnames = [row[1] for row in l.execute("pragma table_info([" + left + "])")]
        rnames = [row[1] for row in r.execute("pragma table_info([" + left + "])")]
        self.assertEqual(lnames, rnames)
        # read in contents, sort and compare
        lcontents = l.execute("select * from [" + left + "]").fetchall()
        rcontents = r.execute("select * from [" + right + "]").fetchall()
        lcontents.sort(key=lambda x: repr(x))
        rcontents.sort(key=lambda x: repr(x))
        self.assertEqual(lcontents, rcontents)

    def testShell(self, shellclass=None):
        "Check Shell functionality"
        if shellclass is None:
            shellclass = apsw.shell.Shell

        fh = [io.StringIO() for _ in ("in", "out", "err")]
        kwargs = {"stdin": fh[0], "stdout": fh[1], "stderr": fh[2]}

        def reset():
            for i in fh:
                i.truncate(0)
                i.seek(0)

        def isempty(x):
            self.assertEqual(get(x), "")

        def isnotempty(x):
            self.assertNotEqual(len(get(x)), 0)

        def cmd(c):
            assert fh[0].tell() == 0
            fh[0].truncate(0)
            fh[0].seek(0)
            fh[0].write(c)
            fh[0].seek(0)

        def get(x):
            x.seek(0)
            return x.read()

        # Make one and ensure help works
        shellclass(stdin=fh[0], stdout=fh[1], stderr=fh[2], args=["", ".help"])
        self.assertNotIn("Traceback", get(fh[2]))
        reset()

        # Lets give it some harmless sql arguments and do a sanity check
        s = shellclass(args=[TESTFILEPREFIX + "testdb", "create table x(x)", "insert into x values(1)"], **kwargs)
        self.assertTrue(s.db.filename.endswith("testdb"))
        # do a dump and check our table is there with its values
        s.command_dump([])
        self.assertTrue("x(x)" in get(fh[1]))
        self.assertTrue("(1);" in get(fh[1]))

        # empty args
        self.assertEqual((None, [], []), s.process_args(None))

        # input description
        reset()
        pathlib.Path(TESTFILEPREFIX + "test-shell-1").write_text("syntax error", encoding="utf8")
        try:
            shellclass(args=[TESTFILEPREFIX + "testdb", ".read %stest-shell-1" % (TESTFILEPREFIX,)], **kwargs)
        except shellclass.Error:
            self.assertTrue("test-shell-1" in get(fh[2]))
            isempty(fh[1])

        # Check single and double dash behave the same
        reset()
        try:
            shellclass(args=["-init"], **kwargs)
        except shellclass.Error:
            isempty(fh[1])
            self.assertTrue("specify a filename" in get(fh[2]))

        reset()
        s = shellclass(**kwargs)
        try:
            s.process_args(["--init"])
        except shellclass.Error:
            self.assertTrue("specify a filename" in str(sys.exc_info()[1]))

        # various command line options
        # an invalid one
        reset()
        try:
            shellclass(args=["---tripledash"], **kwargs)
        except shellclass.Error:
            isempty(fh[1])
            self.assertTrue("-tripledash" in get(fh[2]))
            self.assertTrue("--tripledash" not in get(fh[2]))

        ###
        ### --init
        ###
        reset()
        pathlib.Path(TESTFILEPREFIX + "test-shell-1").write_text("syntax error", encoding="utf8")
        try:
            shellclass(args=["-init", TESTFILEPREFIX + "test-shell-1"], **kwargs)
        except shellclass.Error:
            # we want to make sure it read the file
            isempty(fh[1])
            self.assertTrue("syntax error" in get(fh[2]))
        reset()
        pathlib.Path(TESTFILEPREFIX + "test-shell-1").write_text("select 3;", encoding="utf8")
        shellclass(args=["-init", TESTFILEPREFIX + "test-shell-1"], **kwargs)
        # we want to make sure it read the file
        isempty(fh[2])
        self.assertTrue("3" in get(fh[1]))

        ###
        ### --header
        ###
        reset()
        s = shellclass(**kwargs)
        s.process_args(["--header"])
        self.assertEqual(s.header, True)
        s.process_args(["--noheader"])
        self.assertEqual(s.header, False)
        s.process_args(["--noheader", "-header", "-noheader", "--header"])
        self.assertEqual(s.header, True)
        s.process_args(["-no-colour", "--nocolor"])
        self.assertEqual(s.colour_scheme, "off")
        # did they actually turn on?
        isempty(fh[1])
        isempty(fh[2])
        s.process_args([TESTFILEPREFIX + "testdb", ".mode column", "select 3"])
        isempty(fh[2])
        self.assertTrue("3" in get(fh[1]))
        self.assertTrue("----" in get(fh[1]))

        ###
        ### --echo, --bail, --interactive
        ###
        reset()
        for v in ("echo", "bail", "interactive"):
            s = shellclass(**kwargs)
            b4 = getattr(s, v)
            s.process_args(["--" + v])
            # setting should have changed
            self.assertNotEqual(b4, getattr(s, v))
            isempty(fh[1])
            isempty(fh[2])

        ###
        ### --batch
        ###
        reset()
        s = shellclass(**kwargs)
        s.interactive = True
        s.process_args(["-batch"])
        self.assertEqual(s.interactive, False)
        isempty(fh[1])
        isempty(fh[2])

        ###
        ### --separator, --nullvalue, --encoding
        ###
        for v, val in ("separator", "\n"), ("nullvalue", "abcdef"), ("encoding", "iso8859-1"):
            reset()
            s = shellclass(args=["--" + v, val], **kwargs)
            # We need the eval because shell processes backslashes in
            # string.  After deliberating that is the right thing to
            # do
            if v == "encoding":
                self.assertEqual((val, None), getattr(s, v))
            else:
                self.assertEqual(val, getattr(s, v))
            isempty(fh[1])
            isempty(fh[2])
            self.assertRaises(shellclass.Error, shellclass, args=["-" + v, val, "--" + v], **kwargs)
            isempty(fh[1])
            self.assertTrue(v in get(fh[2]))

        ###
        ### --version
        ###
        reset()
        self.assertRaises(SystemExit, shellclass, args=["--version"], **kwargs)
        # it writes to stdout
        isempty(fh[2])
        self.assertTrue(apsw.sqlite_lib_version() in get(fh[1]))

        ###
        ### --help
        ###
        reset()
        self.assertRaises(SystemExit, shellclass, args=["--help"], **kwargs)
        # it writes to stderr
        isempty(fh[1])
        self.assertTrue("-version" in get(fh[2]))

        ###
        ### Items that correspond to output mode
        ###
        reset()
        shellclass(
            args=[
                "--python",
                "--column",
                "--python",
                ":memory:",
                "create table x(x)",
                "insert into x values(x'aa')",
                "select * from x;",
            ],
            **kwargs,
        )
        isempty(fh[2])
        self.assertTrue('b"' in get(fh[1]) or "buffer(" in get(fh[1]))

        ###
        ### Is process_unknown_args called as documented?
        ###
        reset()

        class s2(shellclass):
            def process_unknown_args(self, args):
                1 / 0

        self.assertRaises(ZeroDivisionError, s2, args=["--unknown"], **kwargs)
        isempty(fh[1])
        self.assertTrue("division" in get(fh[2]))  # py2 says "integer division", py3 says "int division"

        class s3(shellclass):
            def process_unknown_args(_, args):
                self.assertEqual(args[0:2], ["myoption", "myvalue"])
                return args[2:]

        reset()
        self.assertRaises(s3.Error, s3, args=["--python", "--myoption", "myvalue", "--init"], **kwargs)
        isempty(fh[1])
        self.assertTrue("-init" in get(fh[2]))

        ###
        ### .open
        ####
        reset()
        s = shellclass(**kwargs)
        self.assertTrue(s.db.filename == "")
        for n in "testdb", "testdb2", "testdb3":
            fn = TESTFILEPREFIX + n
            reset()
            cmd(".open " + fn)
            s.cmdloop()
            self.assertTrue(s.db.filename.endswith(fn))
        reset()
        fn = TESTFILEPREFIX + "testdb"
        cmd(".open " + fn)
        cmd("create table foo(x); insert into foo values(2);")
        s.cmdloop()
        for row in s.db.cursor().execute("select * from foo"):
            break
        else:
            self.fail("Table doesn't have any rows")
        reset()
        cmd(".open --wipe " + fn)
        s.cmdloop()
        for row in s.db.cursor().execute("select * from sqlite_schema"):
            self.fail("--wipe didn't wipe file")

        N = "sentinel-chidh-jklhfd"

        class vfstest(apsw.VFS):
            def __init__(self):
                super().__init__(name=N, base="")

        ref = vfstest()
        reset()
        cmd(f".open --vfs {N} {fn}\n.connection\n.close")
        s.cmdloop()
        isempty(fh[2])
        self.assertIn(f"({N})", get(fh[1]))
        del ref

        reset()
        cmd(f".open {fn}\n.connection")
        s.cmdloop()
        # uri should be on by default
        self.assertIn("URI", get(fh[1]).splitlines()[-1])
        reset()
        cmd(f".open --flags READWRITE|CREATE {fn}\n.connection")
        s.cmdloop()
        self.assertNotIn("URI", get(fh[1]).splitlines()[-1])
        reset()
        cmd(f".open --flags readwrite|orange {fn}")
        s.cmdloop()
        isnotempty(fh[2])
        self.assertIn("'SQLITE_OPEN_ORANGE' is not a known open flag", get(fh[2]))

        ###
        ### Some test data
        ###
        reset()
        s = shellclass(**kwargs)
        s.cmdloop()

        def testnasty():
            reset()
            cmd("""create table if not exists nastydata(x,y);
                 insert into nastydata values(x'', 9e999); -- see issue 482 for zero sized blob
                 insert into nastydata values(null,'xxx\\u1234\\uabcdyyy\r\n\t\"this is nasty\u0001stuff!');
                """)
            s.cmdloop()
            isempty(fh[1])
            isempty(fh[2])
            reset()
            cmd(".bail on\n.header OFF\nselect * from nastydata;")
            s.cmdloop()
            isempty(fh[2])
            isnotempty(fh[1])

        ###
        ### Output formats - column
        ###
        reset()
        x = "a" * 20
        cmd(".mode column\n.header ON\nselect '" + x + "';")
        s.cmdloop()
        isempty(fh[2])
        # colwidth should be 2 more
        sep = "-" * (len(x) + 2)  # apostrophes quoting string in column header
        out = get(fh[1]).replace("\n", "")
        self.assertEqual(len(out.split(sep)), 2)
        self.assertEqual(len(out.split(sep)[0]), len(x) + 2)  # plus two apostrophes
        self.assertEqual(len(out.split(sep)[1]), len(x) + 2)  # same
        self.assertTrue("  " in out.split(sep)[1])  # space padding
        # make sure truncation happens
        reset()
        cmd(".width 5\nselect '" + x + "';\n")
        s.cmdloop()
        isempty(fh[2])
        self.assertTrue("a" * 6 not in get(fh[1]))
        # right justification
        reset()
        cmd(".header off\n.width -3 -3\nselect 3,3;\n.width 3 3\nselect 3,3;")
        s.cmdloop()
        isempty(fh[2])
        v = get(fh[1])
        self.assertTrue(v.startswith("  3    3"))
        v = v.split("\n")
        self.assertNotEqual(v[0], v[1])
        self.assertEqual(len(v[0]), len(v[1]))
        # do not output blob as is
        self.assertTrue("\xaa" not in get(fh[1]))
        # undo explain
        reset()
        cmd(".explain OFF\n")
        s.cmdloop()
        testnasty()

        ###
        ### Output formats - csv
        ###
        reset()
        # mode change should reset separator
        cmd(".separator F\n.mode csv\nselect 3,3;\n")
        s.cmdloop()
        isempty(fh[2])
        self.assertTrue("3,3" in get(fh[1]))
        # tab sep
        reset()
        cmd(".separator '\\t'\nselect 3,3;\n")
        s.cmdloop()
        isempty(fh[2])
        self.assertTrue("3\t3" in get(fh[1]))
        # back to comma
        reset()
        cmd(".mode csv\nselect 3,3;\n")
        s.cmdloop()
        isempty(fh[2])
        self.assertTrue("3,3" in get(fh[1]))
        # quoting
        reset()
        cmd('.header ON\nselect 3 as ["one"], 4 as [\t];\n')
        s.cmdloop()
        isempty(fh[2])
        self.assertTrue('"""one""",\t' in get(fh[1]))
        # custom sep
        reset()
        cmd('.separator |\nselect 3 as ["one"], 4 as [\t];\n')
        s.cmdloop()
        isempty(fh[2])
        self.assertTrue("3|4\n" in get(fh[1]))
        self.assertTrue('"one"|\t\n' in get(fh[1]))
        # testnasty() - csv module is pretty much broken

        ###
        ### Output formats - html
        ###
        reset()
        cmd(".mode html\n.header OFF\nselect 3,4;\n")
        s.cmdloop()
        isempty(fh[2])
        # should be no header
        self.assertTrue("<th>" not in get(fh[1]).lower())
        # does it actually work?
        self.assertTrue("<td>3</td>" in get(fh[1]).lower())
        # check quoting works
        reset()
        cmd(".header ON\nselect 3 as [<>&];\n")
        s.cmdloop()
        isempty(fh[2])
        self.assertTrue("<th>&lt;&gt;&amp;</th>" in get(fh[1]).lower())
        # do we output rows?
        self.assertTrue("<tr>" in get(fh[1]).lower())
        self.assertTrue("</tr>" in get(fh[1]).lower())
        testnasty()

        ###
        ### Output formats - insert
        ###
        reset()
        all = "3,3.1,'3.11',null,x'0311'"
        cmd(".mode insert\n.header OFF\nselect " + all + ";\n")
        s.cmdloop()
        isempty(fh[2])
        self.assertTrue(all in get(fh[1]).lower())
        # empty values
        reset()
        all = "0,0.0,'',null,x''"
        cmd("select " + all + ";\n")
        s.cmdloop()
        isempty(fh[2])
        self.assertTrue(all in get(fh[1]).lower())
        # header, separator and nullvalue should make no difference
        save = get(fh[1])
        reset()
        cmd(".header ON\n.separator %\n.nullvalue +\nselect " + all + ";\n")
        s.cmdloop()
        isempty(fh[2])
        self.assertEqual(save, get(fh[1]))
        # check the table name
        self.assertTrue(get(fh[1]).lower().startswith('insert into "table" values'))
        reset()
        cmd(".mode insert funkychicken\nselect " + all + ";\n")
        s.cmdloop()
        isempty(fh[2])
        self.assertTrue(get(fh[1]).lower().startswith("insert into funkychicken values"))
        testnasty()

        ###
        ### Output formats - json
        ###
        reset()
        all = "3,2.2,'string',null,x'0311'"
        cmd(".mode json\n.header ON\n select " + all + ";")
        s.cmdloop()
        isempty(fh[2])
        out = json.loads(get(fh[1]))
        self.assertEqual(out, [{"3": 3, "2.2": 2.2, "'string'": "string", "null": None, "x'0311'": "AxE="}])
        # a regular table
        reset()
        cmd(f"""create table jsontest([int], [float], [string], [null], [blob]);
                insert into jsontest values({all});
                insert into jsontest values({all});
                select * from jsontest;""")
        s.cmdloop()
        isempty(fh[2])
        out = json.loads(get(fh[1]))
        self.assertEqual(out, [{"int": 3, "float": 2.2, "string": "string", "null": None, "blob": "AxE="}] * 2)
        testnasty()

        ###
        ### Output formats - jsonl
        ###
        reset()
        cmd(".mode jsonl\n.header ON\n select " + all + ";")
        s.cmdloop()
        isempty(fh[2])
        v = get(fh[1]).strip()
        out = json.loads(v)
        self.assertEqual(out, {"3": 3, "2.2": 2.2, "'string'": "string", "null": None, "x'0311'": "AxE="})
        reset()
        cmd("select * from jsontest;")
        s.cmdloop()
        isempty(fh[2])
        out = [json.loads(line) for line in get(fh[1]).splitlines()]
        self.assertEqual(out, [{"int": 3, "float": 2.2, "string": "string", "null": None, "blob": "AxE="}] * 2)
        testnasty()

        ###
        ### Output formats - line
        ###
        reset()
        cmd(".header OFF\n.nullvalue *\n.mode line\nselect 3 as a, null as b, 0.0 as c, 'a' as d, x'aa' as e;\n")
        s.cmdloop()
        isempty(fh[2])
        out = get(fh[1]).replace(" ", "")
        self.assertTrue("a=3\n" in out)
        self.assertTrue("b=*\n" in out)
        self.assertTrue("c=0.0\n" in out)
        self.assertTrue("d=a\n" in out)
        self.assertTrue("e=<Binarydata>\n" in out)
        self.assertEqual(7, len(out.split("\n")))  # one for each col plus two trailing newlines
        # header should make no difference
        reset()
        cmd(".header ON\n.nullvalue *\n.mode line\nselect 3 as a, null as b, 0.0 as c, 'a' as d, x'aa' as e;\n")
        s.cmdloop()
        isempty(fh[2])
        self.assertEqual(out, get(fh[1]).replace(" ", ""))
        # wide column name
        reset()
        ln = "kjsfhgjksfdjkgfhkjsdlafgjkhsdkjahfkjdsajfhsdja" * 12
        cmd("select 3 as %s, 3 as %s1;" % (ln, ln))
        s.cmdloop()
        isempty(fh[2])
        self.assertEqual(get(fh[1]), " %s = 3\n%s1 = 3\n\n" % (ln, ln))
        testnasty()

        ###
        ### Output formats - list
        ###
        reset()
        cmd(
            ".header off\n.mode list\n.nullvalue (\n.separator &\nselect 3 as a, null as b, 0.0 as c, 'a' as d, x'aa' as e;\n"
        )
        s.cmdloop()
        isempty(fh[2])
        self.assertEqual(get(fh[1]), "3&(&0.0&a&<Binary data>\n")
        reset()
        # header on
        cmd(
            ".header on\n.mode list\n.nullvalue (\n.separator &\nselect 3 as a, null as b, 0.0 as c, 'a' as d, x'aa' as e;\n"
        )
        s.cmdloop()
        isempty(fh[2])
        self.assertTrue(get(fh[1]).startswith("a&b&c&d&e\n"))
        testnasty()

        ###
        ### Output formats - python
        ###
        reset()
        cmd(".header off\n.mode python\nselect 3 as a, null as b, 0.0 as c, 'a' as d, x'aa44bb' as e;\n")
        s.cmdloop()
        isempty(fh[2])
        v = eval(get(fh[1]))
        self.assertEqual(len(v), 1)  # 1 tuple
        self.assertEqual(v, ((3, None, 0.0, "a", b"\xaa\x44\xbb"),))
        reset()
        cmd(".header on\n.mode python\nselect 3 as a, null as b, 0.0 as c, 'a' as d, x'aa44bb' as e;\n")
        s.cmdloop()
        isempty(fh[2])
        v = eval("(" + get(fh[1]) + ")")  # need parentheses otherwise indent rules apply
        self.assertEqual(len(v), 2)  # headers and row
        self.assertEqual(
            v,
            (
                ("a", "b", "c", "d", "e"),
                (3, None, 0.0, "a", b"\xaa\x44\xbb"),
            ),
        )
        testnasty()

        ###
        ### Output formats - TCL
        ###
        reset()
        cmd(
            ".header off\n.mode tcl\n.separator -\n.nullvalue ?\nselect 3 as a, null as b, 0.0 as c, 'a' as d, x'aa44bb' as e;\n"
        )
        s.cmdloop()
        isempty(fh[2])
        self.assertEqual(get(fh[1]), '"3"-"?"-"0.0"-"a"-"\\xAAD\\xBB"\n')
        reset()
        cmd(".header on\nselect 3 as a, null as b, 0.0 as c, 'a' as d, x'aa44bb' as e;\n")
        s.cmdloop()
        isempty(fh[2])
        self.assertTrue('"a"-"b"-"c"-"d"-"e"' in get(fh[1]))
        testnasty()

        ###
        ### Output formats - box/qbox/table
        ###
        if hasattr(s, "output_box"):
            # ensure others error
            reset()
            cmd(".mode tcl --foo bar")
            self.assertRaises(apsw.shell.Shell.Error, s.cmdloop)
            for mode in "box", "qbox", "table":
                reset()
                cmd(f".mode {mode} --no-unicode --width 65")
                s.cmdloop()
                isempty(fh[1])
                isempty(fh[2])
                testnasty()

            # check help for them
            reset()
            cmd(".bail off\n.mode box --help")
            s.cmdloop()
            isempty(fh[1])
            self.assertIn("Use unicode line drawing", get(fh[2]))
            reset()
            cmd(".mode table --fred 3")
            s.cmdloop()
            isempty(fh[1])
            self.assertIn("fred", get(fh[2]))
            self.assertIn("--help", get(fh[2]))
            reset()
            cmd(".bail on")
            s.cmdloop()

        # What happens if db cannot be opened?
        s.process_args(args=["/"])
        reset()
        cmd("select * from sqlite_schema;\n.bail on\nselect 3;\n")
        self.assertRaises(apsw.CantOpenError, s.cmdloop)
        isempty(fh[1])
        self.assertTrue("unable to open database file" in get(fh[2]))

        # echo testing - multiple statements
        s.process_args([":memory:"])  # back to memory db
        reset()
        cmd(".bail off\n.echo on\nselect 3;\n")
        s.cmdloop()
        self.assertTrue("select 3;\n" in get(fh[2]))
        # multiline
        reset()
        cmd("select 3;select 4;\n")
        s.cmdloop()
        self.assertTrue("select 3;\n" in get(fh[2]))
        self.assertTrue("select 4;\n" in get(fh[2]))
        # multiline with error
        reset()
        cmd("select 3;select error;select 4;\n")
        s.cmdloop()
        # worked line should be present
        self.assertTrue("select 3;\n" in get(fh[2]))
        # as should the error
        self.assertTrue("no such column: error" in get(fh[2]))
        # is timing info output correctly?
        reset()
        timersupported = False
        try:
            cmd(".bail on\n.echo off\n.timer on\n.timer off\n")
            s.cmdloop()
            timersupported = True
        except s.Error:
            pass

        if timersupported:
            reset()
            # create something that should take some time to execute
            s.db.cursor().execute("create table xyz(x); begin;")
            s.db.cursor().executemany("insert into xyz values(?)", random_integers(4000))
            s.db.cursor().execute("end")
            reset()
            # this takes .6 seconds on my machine so we should
            # definitely have non-zero timing information
            cmd(
                ".timer ON\nselect max(x),min(x),max(x+x),min(x-x) from xyz union select x+max(x),x-min(x),3,4 from xyz union select x,x,x,x from xyz union select x,x,x,x from xyz;select 3;\n"
            )
            s.cmdloop()
            isnotempty(fh[1])
            isnotempty(fh[2])
        reset()
        cmd(".bail off\n.timer off")
        s.cmdloop()

        # command handling
        reset()
        cmd(".nonexist 'unclosed")
        s.cmdloop()
        isempty(fh[1])
        self.assertTrue("no closing quotation" in get(fh[2]).lower())
        reset()
        cmd(".notexist       ")
        s.cmdloop()
        isempty(fh[1])
        self.assertTrue('Unknown command "notexist"' in get(fh[2]))

        ###
        ### Commands - backup and restore
        ###

        reset()
        cmd(".backup with too many parameters")
        s.cmdloop()
        isempty(fh[1])
        isnotempty(fh[2])
        reset()
        cmd(".backup ")  # too few
        s.cmdloop()
        isempty(fh[1])
        isnotempty(fh[2])
        reset()
        cmd(".restore with too many parameters")
        s.cmdloop()
        isempty(fh[1])
        isnotempty(fh[2])
        reset()
        cmd(".restore ")  # too few
        s.cmdloop()
        isempty(fh[1])
        isnotempty(fh[2])
        # bogus filenames
        for i in ("/", '"main" /'):
            for c in (".backup ", ".restore "):
                reset()
                cmd(c + i)
                s.cmdloop()
                isempty(fh[1])
                isnotempty(fh[2])

        def randomtable(cur, dbname=None):
            name = list("abcdefghijklmnopqrstuvwxtz")
            random.shuffle(name)
            name = "".join(name)
            fullname = name
            if dbname:
                fullname = dbname + "." + fullname
            cur.execute("begin;create table %s(x)" % (fullname,))
            cur.executemany("insert into %s values(?)" % (fullname,), random_integers(400))
            cur.execute("end")
            return name

        # Straight forward backup.  The gc.collect() is needed because
        # non-gc cursors hanging around will prevent the backup from
        # happening.
        n = randomtable(s.db.cursor())
        contents = s.db.cursor().execute("select * from " + n).fetchall()
        reset()
        cmd(".backup %stestdb2" % (TESTFILEPREFIX,))
        gc.collect()
        s.cmdloop()
        isempty(fh[1])
        isempty(fh[2])
        reset()
        cmd("drop table " + n + ";")
        s.cmdloop()
        isempty(fh[1])
        isempty(fh[2])
        self.assertTrue(os.path.isfile("%stestdb2" % (TESTFILEPREFIX,)))
        reset()
        cmd(".restore %stestdb2" % (TESTFILEPREFIX,))
        gc.collect()
        s.cmdloop()
        isempty(fh[1])
        isempty(fh[2])
        newcontents = s.db.cursor().execute("select * from " + n).fetchall()
        # no guarantee of result order
        contents.sort()
        newcontents.sort()
        self.assertEqual(contents, newcontents)

        # do they pay attention to the dbname
        s.db.cursor().execute("attach ':memory:' as memdb")
        n = randomtable(s.db.cursor(), "memdb")
        contents = s.db.cursor().execute("select * from memdb." + n).fetchall()
        reset()
        gc.collect()
        cmd(".backup memdb %stestdb2" % (TESTFILEPREFIX,))
        s.cmdloop()
        isempty(fh[1])
        isempty(fh[2])
        s.db.cursor().execute("detach memdb; attach ':memory:' as memdb2")
        reset()
        gc.collect()
        cmd(".restore memdb2 %stestdb2" % (TESTFILEPREFIX,))
        s.cmdloop()
        isempty(fh[1])
        isempty(fh[2])
        newcontents = s.db.cursor().execute("select * from memdb2." + n).fetchall()
        # no guarantee of result order
        contents.sort()
        newcontents.sort()
        self.assertEqual(contents, newcontents)

        ###
        ### Commands - bail
        ###
        reset()
        cmd(".bail")
        s.cmdloop()
        isempty(fh[1])
        isnotempty(fh[2])
        reset()
        cmd(".bail on\n.mode list\nselect 3;\nselect error;\nselect 4;\n")
        self.assertRaises(apsw.Error, s.cmdloop)
        self.assertTrue("3" in get(fh[1]))
        self.assertTrue("4" not in get(fh[1]))
        reset()
        cmd(".bail oFf\n.mode list\nselect 3;\nselect error;\nselect 4;\n")
        s.cmdloop()
        self.assertTrue("3" in get(fh[1]))
        self.assertTrue("4" in get(fh[1]))

        ###
        ### Commands - changes
        ###
        reset()
        cmd(".changes on\nselect 3;")
        s.cmdloop()
        isempty(fh[2])
        self.assertNotIn("changes:", get(fh[1]))
        reset()
        cmd("create table testchange(x); insert into testchange values(3);")
        s.cmdloop()
        isempty(fh[2])
        self.assertIn("changes:", get(fh[1]))
        reset()
        cmd(".changes off\ninsert into testchange values(4);")
        s.cmdloop()
        isempty(fh[2])
        self.assertNotIn("changes:", get(fh[1]))

        ###
        ### Commands - cd / connection / close
        ###
        @contextlib.contextmanager
        def chdir(path):
            before = os.getcwd()
            os.chdir(path)
            try:
                yield
            finally:
                os.chdir(before)

        def in_open_dbs(filename):
            count = 0
            for c in apsw.connections():
                try:
                    if not c.filename:
                        continue
                except apsw.ConnectionClosedError:
                    continue
                if os.path.samefile(filename, c.filename):
                    count += 1
            return count

        with tempfile.TemporaryDirectory(prefix="apsw-shell-test-") as tmpd1:
            with chdir("."):
                reset()
                cmd(f".cd {shlex.quote(tmpd1)}")
                s.cmdloop()
                isempty(fh[1])
                isempty(fh[2])
                self.assertTrue(os.path.samefile(tmpd1, os.getcwd()))

            with chdir(tmpd1):
                reset()
                V = "sentinel-jhdgfsfjdskh-1"
                cmd(f".open {shlex.quote(V)}\ncreate table foo(x);\n.open {shlex.quote(V)}")
                s.cmdloop()
                isempty(fh[1])
                isempty(fh[2])
                self.assertEqual(2, in_open_dbs(V))
                reset()
                cmd(f".open --wipe {shlex.quote(V)}")
                s.cmdloop()
                isempty(fh[1])
                isempty(fh[2])
                self.assertEqual(1, in_open_dbs(V))
                reset()
                cmd(f".close")
                s.cmdloop()
                isempty(fh[1])
                isempty(fh[2])
                self.assertEqual(0, in_open_dbs(V))

        ###
        ### Commands - databases
        ###
        reset()
        cmd(".databases foo")
        s.cmdloop()
        isempty(fh[1])
        isnotempty(fh[2])
        # clean things up
        s = shellclass(**kwargs)
        reset()
        cmd(".header oFF\n.databases")
        s.cmdloop()
        isempty(fh[2])
        for i in "main", "name", "file":
            self.assertTrue(i in get(fh[1]))
        reset()
        cmd("attach '%stestdb' as quack;\n.databases" % (TESTFILEPREFIX,))
        s.cmdloop()
        isempty(fh[2])
        for i in "main", "name", "file", "testdb", "quack":
            self.assertTrue(i in get(fh[1]))
        reset()
        cmd("detach quack;")
        s.cmdloop()
        isempty(fh[2])
        for i in "testdb", "quack":
            self.assertTrue(i not in get(fh[1]))

        ###
        ### Command - dbconfig
        ###
        reset()
        cmd(".dbconfig")
        s.cmdloop()
        isempty(fh[2])
        self.assertIn("trigger_eqp:", get(fh[1]))
        reset()
        self.assertFalse(s.db.config(apsw.SQLITE_DBCONFIG_TRIGGER_EQP, -1))
        cmd(".dbconfig trigger_eqp 1")
        s.cmdloop()
        isempty(fh[2])
        self.assertTrue(s.db.config(apsw.SQLITE_DBCONFIG_TRIGGER_EQP, -1))

        ###
        ### Command - dbinfo
        ###
        if hasattr(s, "command_dbinfo"):
            with tempfile.TemporaryDirectory(prefix="apsw-test-shell-dbinfo-") as tmpd:
                reset()
                cmd(f".open {shlex.quote(tmpd)}/newdb")
                s.cmdloop()
                isempty(fh[2])
                reset()
                cmd(
                    textwrap.dedent("""
                    .dbinfo
                    pragma journal_mode=wal;
                    .dbinfo
                    pragma journal_mode=persist;
                    .dbinfo
                    pragma encoding="UTF-16";
                    .dbinfo
                    create table x(y);
                    .dbinfo
                """)
                )
                s.cmdloop()
                isnotempty(fh[1])
                isempty(fh[2])
                reset()
                cmd(".close\n.connection 0")
                s.cmdloop()

        ###
        ### Commands - dump
        ###
        reset()
        cmd("create     table foo(x); create table bar(x);\n.dump foox")
        s.cmdloop()
        isempty(fh[1])
        isempty(fh[2])
        reset()
        cmd(".dump foo")
        s.cmdloop()
        isempty(fh[2])
        for i in "foo", "create table", "begin", "commit":
            self.assertTrue(i in get(fh[1]).lower())
        self.assertTrue("bar" not in get(fh[1]).lower())
        # can we do virtual tables?
        reset()
        if s.db.pragma("module_list") and "fts3" in s.db.pragma("module_list"):
            reset()
            cmd(
                "CREATE virtual TaBlE    fts3     using fts3(colA FRED  , colB JOHN DOE);\n"
                "insert into fts3 values('one', 'two');insert into fts3 values('onee', 'two');\n"
                "insert into fts3 values('one', 'two two two');"
            )
            s.cmdloop()
            isempty(fh[1])
            isempty(fh[2])
            reset()
            cmd(".dump")
            s.cmdloop()
            isempty(fh[2])
            v = get(fh[1])
            for i in "pragma writable_schema", "create virtual table fts3", "cola fred", "colb john doe":
                self.assertTrue(i in v.lower())
        # analyze
        reset()
        cmd(
            "drop table bar;create table bar(x unique,y);create index barf on bar(x,y);create index barff on bar(y);insert into bar values(3,4);\nanalyze;\n.dump bar"
        )
        s.cmdloop()
        isempty(fh[2])
        v = get(fh[1])
        for i in "analyze bar", "create index barf":
            self.assertTrue(i in v.lower())
        self.assertTrue("autoindex" not in v.lower())  # created by sqlite to do unique constraint
        self.assertTrue("sqlite_sequence" not in v.lower())  # not autoincrements
        # repeat but all tables
        reset()
        cmd(".dump")
        s.cmdloop()
        isempty(fh[2])
        v = get(fh[1])
        for i in "analyze bar", "create index barf":
            self.assertTrue(i in v.lower())
        self.assertTrue("autoindex" not in v.lower())  # created by sqlite to do unique constraint
        # foreign keys
        reset()
        cmd("create table xxx(z references bar(x));\n.dump")
        s.cmdloop()
        isempty(fh[2])
        v = get(fh[1])
        for i in "foreign_keys", "references":
            self.assertTrue(i in v.lower())
        # views
        reset()
        cmd("create view noddy as select * from foo;\n.dump noddy")
        s.cmdloop()
        isempty(fh[2])
        v = get(fh[1])
        for i in "drop view", "create view noddy":
            self.assertTrue(i in v.lower())
        # issue82 - view ordering
        reset()
        cmd(
            "create table issue82(x);create view issue82_2 as select * from issue82; create view issue82_1 as select count(*) from issue82_2;\n.dump issue82%"
        )
        s.cmdloop()
        isempty(fh[2])
        v = get(fh[1])
        s.db.cursor().execute("drop table issue82 ; drop view issue82_1 ; drop view issue82_2")
        reset()
        cmd(v)
        s.cmdloop()
        isempty(fh[1])
        isempty(fh[2])
        # autoincrement
        reset()
        cmd(
            "create table abc(x INTEGER PRIMARY KEY AUTOINCREMENT); insert into abc values(null);insert into abc values(null);\n.dump"
        )
        s.cmdloop()
        isempty(fh[2])
        v = get(fh[1])
        for i in "sqlite_sequence", "'abc', 2":
            self.assertTrue(i in v.lower())
        # user version
        self.assertTrue("user_version" not in v)
        reset()
        cmd("pragma user_version=27;\n.dump")
        s.cmdloop()
        isempty(fh[2])
        v = get(fh[1])
        self.assertTrue("pragma user_version=27;" in v)
        s.db.cursor().execute("pragma user_version=0")
        # some nasty stuff
        reset()
        cmd(
            "create table nastydata(x,y); insert into nastydata values(null,'xxx\\u1234\\uabcd\\U00012345yyy\r\n\t\"this is nasty\u0001stuff!');"
            'create table "table"([except] int); create table [](""); create table [using]("&");'
        )
        s.cmdloop()
        isempty(fh[1])
        isempty(fh[2])
        reset()
        cmd(".dump")
        s.cmdloop()
        isempty(fh[2])
        v = get(fh[1])
        self.assertTrue("nasty" in v)
        self.assertTrue("stuff" in v)
        # sanity check the dumps
        reset()
        cmd(v)  # should run just fine
        s.cmdloop()
        isempty(fh[1])
        isempty(fh[2])
        # drop all the tables we made to do another dump and compare with before
        for t in (
            "abc",
            "bar",
            "foo",
            "fts3",
            "xxx",
            "noddy",
            "sqlite_sequence",
            "sqlite_stat1",
            "issue82",
            "issue82_1",
            "issue82_2",
        ):
            reset()
            cmd("drop table %s;drop view %s;" % (t, t))
            s.cmdloop()  # there will be errors which we ignore
        reset()
        cmd(v)
        s.cmdloop()
        isempty(fh[1])
        isempty(fh[2])
        # another dump
        reset()
        cmd(".dump")
        s.cmdloop()
        isempty(fh[2])
        v2 = get(fh[1])
        v = re.sub("-- Date:.*", "", v)
        v2 = re.sub("-- Date:.*", "", v2)
        self.assertEqual(v, v2)
        # clean database
        reset()
        s = shellclass(args=[":memory:"], **kwargs)
        cmd(v)
        s.cmdloop()
        isempty(fh[1])
        isempty(fh[2])
        reset()
        cmd(v2 + "\n.dump")
        s.cmdloop()
        isempty(fh[2])
        v3 = get(fh[1])
        v3 = re.sub("-- Date:.*", "", v3)
        self.assertEqual(v, v3)
        # trailing comments
        reset()
        cmd("""create table xxblah(b -- ff
) -- xx
; create index xxfoo on xxblah(b -- ff
) -- xx
; create view xxbar as select * from xxblah -- ff
;
insert into xxblah values(3);
.dump
""")
        s.cmdloop()
        isempty(fh[2])
        dump = get(fh[1])
        reset()
        cmd("drop table xxblah; drop view xxbar;")
        s.cmdloop()
        isempty(fh[2])
        isempty(fh[1])
        reset()
        cmd(dump)
        s.cmdloop()
        isempty(fh[2])
        isempty(fh[1])
        self.assertEqual(s.db.cursor().execute("select * from xxbar").fetchall(), [(3,)])
        # check index
        reset()
        cmd("drop index xxfoo;")
        s.cmdloop()
        isempty(fh[1])
        isempty(fh[2])

        ###
        ### Command - echo
        ###
        reset()
        cmd(".echo")
        s.cmdloop()
        isempty(fh[1])
        isnotempty(fh[2])
        reset()
        cmd(".echo bananas")
        s.cmdloop()
        isempty(fh[1])
        isnotempty(fh[2])
        reset()
        cmd(".echo on on")
        s.cmdloop()
        isempty(fh[1])
        isnotempty(fh[2])
        reset()
        cmd(".echo off\nselect 3;")
        s.cmdloop()
        self.assertTrue("3" in get(fh[1]))
        self.assertTrue("select 3" not in get(fh[2]))
        reset()
        cmd(".echo on\nselect 3;")
        s.cmdloop()
        self.assertTrue("3" in get(fh[1]))
        self.assertTrue("select 3" in get(fh[2]))
        # more complex testing is done earlier including multiple statements and errors

        ###
        ### Command - encoding
        ###
        suppress_warning("ResourceWarning")
        for i in ".encoding one two", ".encoding", ".encoding utf8 another":
            reset()
            cmd(i)
            s.cmdloop()
            isempty(fh[1])
            isnotempty(fh[2])
        reset()
        cmd(".encoding this-does-not-exist")
        s.cmdloop()
        isempty(fh[1])
        self.assertTrue("no known encoding" in get(fh[2]).lower())
        # use iso8859-1 to make sure data is read correctly - it
        # differs from utf8
        us = "unitestdata \xaa\x89 34"
        pathlib.Path(
            TESTFILEPREFIX + "test-shell-1").write_text(f"insert into enctest values('{us}');\n", encoding="iso8859-1"
        )
        gc.collect()
        reset()
        cmd(
            ".encoding iso8859-1\ncreate table enctest(x);\n.echo on\n.read %stest-shell-1\n.echo off"
            % (TESTFILEPREFIX,)
        )
        s.cmdloop()
        self.assertEqual(s.db.cursor().execute("select * from enctest").fetchall()[0][0], us)
        self.assertTrue(us in get(fh[2]))
        reset()
        pathlib.Path(TESTFILEPREFIX + "test-shell-1").write_text(us + "\n", encoding="iso8859-1")
        cmd("drop table enctest;create table enctest(x);\n.import %stest-shell-1 enctest" % (TESTFILEPREFIX,))
        s.cmdloop()
        isempty(fh[2])
        isempty(fh[1])
        self.assertEqual(s.db.cursor().execute("select * from enctest").fetchall()[0][0], us)
        reset()
        cmd(".output %stest-shell-1\n.mode list\nselect * from enctest;" % (TESTFILEPREFIX,))
        s.cmdloop()
        self.assertEqual(
            pathlib.Path(TESTFILEPREFIX + "test-shell-1").read_bytes().strip(),  # skip eol
            us.encode("iso8859-1"),
        )
        reset()
        cmd(".output stdout\nselect '%s';\n" % (us,))
        s.cmdloop()
        isempty(fh[2])
        self.assertTrue(us in get(fh[1]))

        ### encoding specifying error handling - see issue 108
        reset()
        cmd(".encoding utf8:replace")
        s.cmdloop()
        isempty(fh[1])
        isempty(fh[2])
        # non-existent error
        reset()
        cmd(".encoding cp437:blahblah")
        s.cmdloop()
        isempty(fh[1])
        isnotempty(fh[2])
        self.assertTrue("blahblah" in get(fh[2]))
        # check replace works
        reset()
        us = "\N{BLACK STAR}8\N{WHITE STAR}"
        pathlib.Path(TESTFILEPREFIX + "test-shell-1").write_text(f"insert into enctest values('{us}');", encoding="utf8")
        cmd(
            ".encoding utf8\n.read %stest-shell-1\n.encoding cp437:replace\n.output %stest-shell-1\nselect * from enctest;\n.encoding utf8\n.output stdout"
            % (TESTFILEPREFIX, TESTFILEPREFIX)
        )
        s.cmdloop()
        isempty(fh[2])
        isempty(fh[1])
        self.assertTrue("?8?" in pathlib.Path(TESTFILEPREFIX + "test-shell-1").read_text(encoding="cp437"))

        ###
        ### Command - exceptions
        ###
        reset()
        cmd("syntax error;")
        s.cmdloop()
        isempty(fh[1])
        isnotempty(fh[2])
        self.assertTrue(len(get(fh[2]).split("\n")) < 5)
        reset()
        s.db.create_scalar_function("make_error", lambda: 1 / 0)
        cmd(".exceptions on\nselect make_error();")
        s.cmdloop()
        isempty(fh[1])
        isnotempty(fh[2])
        self.assertTrue(len(get(fh[2]).split("\n")) > 10)
        self.assertTrue("sql = " in get(fh[2]))
        # deliberately leave exceptions on

        ###
        ### Command - exit
        ###
        for i in (".exit",):
            reset()
            cmd(i)
            self.assertRaises(SystemExit, s.cmdloop)
            isempty(fh[1])
            isempty(fh[2])
            reset()
            cmd(i + " jjgflk")
            s.cmdloop()
            isempty(fh[1])
            isnotempty(fh[2])

        ###
        ### Explain auto format
        ###
        reset()
        cmd(".mode tcl\n.header off\nselect 3;")
        s.cmdloop()
        isempty(fh[2])
        self.assertIn('"3"', get(fh[1]))
        reset()
        cmd("explain select 3;")
        s.cmdloop()
        isempty(fh[2])
        self.assertIn("opcode", get(fh[1]))
        reset()
        cmd("explain query plan select 3;")
        s.cmdloop()
        isempty(fh[2])
        self.assertIn("detail", get(fh[1]))
        reset()
        cmd("select 3;")
        s.cmdloop()
        isempty(fh[2])
        self.assertIn('"3"', get(fh[1]))

        ###
        ### Command find
        ###
        reset()
        cmd(".find one two three")
        s.cmdloop()
        isempty(fh[1])
        isnotempty(fh[2])
        reset()
        cmd(
            "create table findtest([x\" x],y); insert into findtest values(3, 'xx3'); insert into findtest values(34, 'abcd');"
        )
        s.cmdloop()
        isempty(fh[1])
        isempty(fh[2])
        reset()
        cmd(".find 3")
        s.cmdloop()
        isempty(fh[2])
        for text, present in (("findtest", True), ("xx3", True), ("34", False)):
            if present:
                self.assertTrue(text in get(fh[1]))
            else:
                self.assertTrue(text not in get(fh[1]))
        reset()
        cmd(".find does-not-exist")
        s.cmdloop()
        isempty(fh[1])
        isempty(fh[2])
        reset()
        cmd(".find ab_d")
        s.cmdloop()
        isempty(fh[2])
        for text, present in (("findtest", True), ("xx3", False), ("34", True)):
            if present:
                self.assertTrue(text in get(fh[1]))
            else:
                self.assertTrue(text not in get(fh[1]))
        reset()
        cmd(".find 3 table-not-exist")
        s.cmdloop()
        isempty(fh[1])
        isempty(fh[2])

        ###
        ### Command help
        ###
        reset()
        cmd(".help\n.help all\n.help import backup")
        s.cmdloop()
        isempty(fh[1])
        for i in ".import", "Reads data from the file":
            self.assertTrue(i in get(fh[2]))
        reset()
        cmd(".help backup notexist import")
        s.cmdloop()
        isempty(fh[1])
        for i in "Copies the contents", "No such command":
            self.assertTrue(i in get(fh[2]))
        # screw up terminal width
        origtw = s._terminal_width

        def tw(minimum):
            return minimum

        s._terminal_width = tw
        reset()
        cmd(".bail on\n.help all\n.bail off")
        s.cmdloop()
        isempty(fh[1])
        isnotempty(fh[2])

        ###
        ### Command - import
        ###
        # check it fundamentally works
        reset()
        cmd(
            ".encoding utf16\ncreate table imptest(x real, y char);\n"
            "insert into imptest values(3.1, 'xabc');\n"
            "insert into imptest values(3.2, 'xabfff\"ffffc');\n"
            ".output %stest-shell-1\n.mode csv\nselect * from imptest;\n"
            ".output stdout" % (TESTFILEPREFIX,)
        )
        s.cmdloop()
        isempty(fh[1])
        isempty(fh[2])
        # make sure encoding took
        self.assertTrue(b"xab" not in pathlib.Path(TESTFILEPREFIX + "test-shell-1").read_bytes())
        data = s.db.cursor().execute("select * from imptest; delete from imptest").fetchall()
        self.assertEqual(2, len(data))
        reset()
        cmd(".import %stest-shell-1 imptest" % (TESTFILEPREFIX,))
        s.cmdloop()
        isempty(fh[1])
        isempty(fh[2])
        newdata = s.db.cursor().execute("select * from imptest; drop table imptest").fetchall()
        data.sort()
        newdata.sort()
        self.assertEqual(data, newdata)
        # error handling
        for i in (
            ".import",
            ".import one",
            ".import one two three",
            ".import nosuchfile nosuchtable",
            ".import nosuchfile sqlite_schema",
        ):
            reset()
            cmd(i)
            s.cmdloop()
            isempty(fh[1])
            isnotempty(fh[2])
        # wrong number of columns
        reset()
        cmd(
            "create table imptest(x,y);\n.mode tabs\n.output %stest-shell-1\nselect 3,4;select 5,6;select 7,8,9;"
            % (TESTFILEPREFIX,)
        )
        s.cmdloop()
        isempty(fh[1])
        isempty(fh[2])
        reset()
        cmd(".output stdout\n.import %stest-shell-1 imptest" % (TESTFILEPREFIX,))
        s.cmdloop()
        isempty(fh[1])
        isnotempty(fh[2])
        reset()
        # check it was done in a transaction and aborted
        self.assertEqual(0, s.db.cursor().execute("select count(*) from imptest").fetchall()[0][0])

        ###
        ### Command - autoimport
        ###

        # errors
        for i in (
            ".autoimport",
            ".autoimport 1 2 3",
            ".autoimport nosuchfile",
            ".autoimport %stest-shell-1 sqlite_schema" % (TESTFILEPREFIX,),
        ):
            reset()
            cmd(i)
            s.cmdloop()
            isempty(fh[1])
            isnotempty(fh[2])

        # check correct detection with each type of separator and that types are not mangled
        c = s.db.cursor()
        for row in (
            ("a,b", "21/1/20", "00"),
            ("  ", "1/1/20", 10),
            ('a"b', "1/1/01", "00"),
            ("+40", "01123", "2010 100 15"),
            ("2010//10//13", "2010/10/13  12", 2),
            ("2010/13/13 12:13", "13/13/2010 12:93", "13/2010/13"),
            ("+3", " 3", 3),
            ("03.03", "03.03.20", "03"),
            (
                (None, 2, 5.5),
                (None, 4, 99),
            ),
        ):
            c.execute("""drop table if exists aitest ; create table aitest("x y", ["], "3d")""")
            if isinstance(row[0], tuple):
                f = c.executemany
            else:
                f = c.execute
            f("insert into aitest values(?,?,?)", row)
            fname = TESTFILEPREFIX + "test-shell-1"
            for sep in "\t", "|", ",", "X":
                reset()
                cmd(
                    '.mode csv\n.headers on\n.output %stest-shell-1\n.separator "%s"\nselect * from aitest;\n.output stdout\n.separator X\ndrop table if exists "test-shell-1";\n.autoimport %stest-shell-1'
                    % (TESTFILEPREFIX, sep, TESTFILEPREFIX)
                )
                s.cmdloop()
                isnotempty(fh[1])
                isempty(fh[2])
                self.assertTablesEqual(s.db, "aitest", s.db, "test-shell-1")

        # Change encoding back to sensible
        reset()
        cmd(".encoding utf8")
        s.cmdloop()

        # Check date detection
        for expect, fmt, sequences in (
            (
                "1999-10-13",
                "%d-%d:%d",
                (
                    (1999, 10, 13),
                    (13, 10, 1999),
                    (10, 13, 1999),
                ),
            ),
            (
                "1999-10-13T12:14:17",
                "%d/%d/%d/%d/%d/%d",
                (
                    (1999, 10, 13, 12, 14, 17),
                    (13, 10, 1999, 12, 14, 17),
                    (10, 13, 1999, 12, 14, 17),
                ),
            ),
            (
                "1999-10-13T12:14:00",
                "%dX%dX%dX%dX%d",
                (
                    (1999, 10, 13, 12, 14),
                    (13, 10, 1999, 12, 14),
                    (10, 13, 1999, 12, 14),
                ),
            ),
        ):
            for seq in sequences:
                pathlib.Path(TESTFILEPREFIX + "test-shell-1").write_text(
                    "a,b\nrow," + (fmt % seq) + "\n", encoding="utf8"
                )
                reset()
                cmd("drop table [test-shell-1];\n.autoimport %stest-shell-1" % (TESTFILEPREFIX,))
                s.cmdloop()
                isempty(fh[2])
                imp = c.execute("select b from [test-shell-1] where a='row'").fetchall()[0][0]
                self.assertEqual(imp, expect)

        # Check diagnostics when unable to import
        for err, content in (
            ("current encoding", b"\x81\x82\x83\tfoo\n\x84\x97\xff\tbar"),
            ("known type", "abcdef\nhiojklmnop\n"),
            ("more than one", "ab,c\tdef\nqr,dd\t\n"),
            ("ambiguous data format", "a,b\n1/1/2001,3\n2001/4/4,4\n"),
        ):
            if isinstance(content, bytes):
                continue
            pathlib.Path(TESTFILEPREFIX + "test-shell-1").write_text(content, encoding="utf8")
            reset()
            cmd("drop table [test-shell-1];\n.autoimport %stest-shell-1" % (TESTFILEPREFIX,))
            s.cmdloop()
            errmsg = get(fh[2])
            self.assertTrue(err in errmsg)

        ###
        ### Command - indices
        ###
        for i in ".indices", ".indices one two":
            reset()
            cmd(i)
            s.cmdloop()
            isempty(fh[1])
            isnotempty(fh[2])
        reset()
        cmd("create table indices(x unique, y unique); create index shouldseethis on indices(x,y);")
        s.cmdloop()
        isempty(fh[1])
        isempty(fh[2])
        reset()
        cmd(".indices indices")
        s.cmdloop()
        isempty(fh[2])
        for i in "shouldseethis", "autoindex":
            self.assertTrue(i in get(fh[1]))

        ###
        ### Command - limit
        ###
        reset()
        cmd(".limit")
        s.cmdloop()
        isempty(fh[2])
        self.assertIn("trigger_depth:", get(fh[1]))
        reset()
        fa = s.db.limit(apsw.SQLITE_LIMIT_VARIABLE_NUMBER)
        cmd(f".limit variable_numbER {fa - 1}")
        s.cmdloop()
        isempty(fh[2])
        self.assertEqual(s.db.limit(apsw.SQLITE_LIMIT_VARIABLE_NUMBER), fa - 1)

        ###
        ### Command - load
        ###
        if hasattr(s.db, "load_rxtension"):
            lf = LOAD_EXTENSION_FILENAME
            for i in ".load", ".load one two three":
                reset()
                cmd(i)
                s.cmdloop()
                isempty(fh[1])
                isnotempty(fh[2])
            reset()
            cmd(".load nosuchfile")
            s.cmdloop()
            isempty(fh[1])
            self.assertTrue("nosuchfile" in get(fh[2]) or "ExtensionLoadingError" in get(fh[2]))
            reset()
            cmd(".mode list\n.load " + lf + " alternate_sqlite3_extension_init\nselect doubleup(2);")
            s.cmdloop()
            isempty(fh[2])
            self.assertTrue("4" in get(fh[1]))
            reset()
            cmd(".mode list\n.load " + lf + "\nselect half(2);")
            s.cmdloop()
            isempty(fh[2])
            self.assertTrue("1" in get(fh[1]))

        ###
        ### Command - log
        ###
        reset()
        cmd(".log on\n+;")
        s.cmdloop()
        self.assertIn("SQLITE_ERROR", get(fh[2]))
        reset()
        cmd(".log off\n+;")
        s.cmdloop()
        self.assertNotIn("SQLITE_ERROR", get(fh[2]))

        ###
        ### Command - mode
        ###
        # already thoroughly tested in code above
        for i in ".mode", ".mode foo more", ".mode invalid":
            reset()
            cmd(i)
            s.cmdloop()
            isempty(fh[1])
            isnotempty(fh[2])

        ###
        ### command nullvalue & separator
        ###
        # already tested in code above
        for i in ".nullvalue", ".nullvalue jkhkl lkjkj", ".separator", ".separator one two":
            reset()
            cmd(i)
            b4 = s.nullvalue, s.separator
            s.cmdloop()
            isempty(fh[1])
            isnotempty(fh[2])
            self.assertEqual(b4, (s.nullvalue, s.separator))

        ###
        ### command output
        ###
        for i in ".output", ".output too many args", ".output " + os.sep:
            reset()
            cmd(i)
            b4 = s.stdout
            s.cmdloop()
            isempty(fh[1])
            isnotempty(fh[2])
            self.assertEqual(b4, s.stdout)

        ###
        ### Command - parameter
        ###
        reset()
        cmd("select :foo;")
        s.cmdloop()
        isempty(fh[1])
        self.assertIn("No binding present for 'foo' -", get(fh[2]))
        for val in ("orange", "banana"):
            reset()
            cmd(f".parameter set foo '{val}'\nselect $foo;")
            s.cmdloop()
            isempty(fh[2])
            self.assertIn(val, get(fh[1]))
        reset()
        cmd(".parameter list")
        s.cmdloop()
        isempty(fh[2])
        self.assertIn("banana", get(fh[1]))
        reset()
        cmd(".parameter unset foo\nselect $foo;")
        s.cmdloop()
        isempty(fh[1])
        self.assertIn("No binding present for 'foo' -", get(fh[2]))
        reset()
        cmd(".parameter set bar 3\n.parameter clear\nselect $bar;")
        s.cmdloop()
        isempty(fh[1])
        self.assertIn("No binding present for 'bar' -", get(fh[2]))

        ###
        ### Command prompt
        ###
        # not much to test until pty testing is working
        for i in ".prompt", ".prompt too many args":
            reset()
            cmd(i)
            b4 = s.prompt, s.moreprompt
            s.cmdloop()
            isempty(fh[1])
            isnotempty(fh[2])
            self.assertEqual(b4, (s.prompt, s.moreprompt))

        ###
        ### Command - py
        ###
        reset()
        cmd(".py None")
        s.cmdloop()
        isempty(fh[1])
        isempty(fh[2])
        reset()
        cmd(".py [")
        s.cmdloop()
        self.assertIn("Incomplete", get(fh[2]))
        # interactive use is tested interactively

        ###
        ### Command read
        ###
        # pretty much thoroughly tested above
        pathlib.Path(TESTFILEPREFIX + "test-shell-1.py").write_text(
            """
assert apsw
assert shell
shell.write(shell.stdout, "hello world\\n")
""",
            encoding="utf8",
        )
        for i in ".read", ".read one two", ".read " + os.sep:
            reset()
            cmd(i)
            s.cmdloop()
            isempty(fh[1])
            isnotempty(fh[2])

        reset()
        cmd(".read %stest-shell-1.py" % (TESTFILEPREFIX,))
        s.cmdloop()
        isempty(fh[2])
        self.assertTrue("hello world" in get(fh[1]))

        # restore tested with backup

        ###
        ### Command - schema
        ###
        # make sure it works
        reset()
        cmd(".schema")
        s.cmdloop()
        isempty(fh[2])
        isnotempty(fh[1])
        reset()
        cmd(
            "create table schematest(x);create index unrelatedname on schematest(x);\n.schema schematest foo notexist foo"
        )
        s.cmdloop()
        isempty(fh[2])
        for i in "schematest", "unrelatedname":
            self.assertTrue(i in get(fh[1]))

        # separator done earlier

        ###
        ### Command - shell
        ###
        reset()
        # this uses the process stdout/err which we can't capture without heroics
        cmd(".shell exit 1")
        s.cmdloop()
        self.assertIn("Exit code", get(fh[2]))
        reset()
        cmd(".shell %s > %s" % ("dir" if sys.platform == "win32" else "ls", os.devnull))
        s.cmdloop()
        # should always work on these platforms
        if sys.platform in {"win32", "linux", "darwin"}:
            self.assertNotIn("Exit code", get(fh[2]))

        ###
        ### Command - show
        ###
        # set all settings to known values
        resetcmd = ".echo off\n.changes off\n.headers off\n.mode list\n.nullvalue ''\n.output stdout\n.separator |\n.width 1 2 3\n.exceptions off"
        reset()
        cmd(resetcmd)
        s.cmdloop()
        isempty(fh[2])
        isempty(fh[1])
        reset()
        cmd(".show")
        s.cmdloop()
        isempty(fh[1])
        isnotempty(fh[2])
        baseline = get(fh[2])
        for i in (
            ".echo on",
            ".changes on",
            ".headers on",
            ".mode column",
            ".nullvalue T",
            ".separator %",
            ".width 8 9 1",
            ".exceptions on",
        ):
            reset()
            cmd(resetcmd)
            s.cmdloop()
            isempty(fh[1])
            if not get(fh[2]).startswith(".echo off"):
                isempty(fh[2])
            reset()
            cmd(i + "\n.show")
            s.cmdloop()
            isempty(fh[1])
            # check size has not changed much
            self.assertTrue(abs(len(get(fh[2])) - len(baseline)) < 14)

        # output
        reset()
        cmd(".output %stest-shell-1\n.show" % (TESTFILEPREFIX,))
        s.cmdloop()
        isempty(fh[1])
        self.assertTrue("output: " + TESTFILEPREFIX + "test-shell-1" in get(fh[2]))
        reset()
        cmd(".output stdout\n.show")
        s.cmdloop()
        isempty(fh[1])
        self.assertTrue("output: stdout" in get(fh[2]))
        self.assertTrue(not os.path.exists("stdout"))
        # errors
        reset()
        cmd(".show one two")
        s.cmdloop()
        isempty(fh[1])
        self.assertTrue("at most one parameter" in get(fh[2]))
        reset()
        cmd(".show notexist")
        s.cmdloop()
        isempty(fh[1])
        self.assertTrue("notexist: " not in get(fh[2]))

        ###
        ### Command tables
        ###
        reset()
        cmd(".tables")
        s.cmdloop()
        isempty(fh[2])
        isnotempty(fh[1])
        reset()
        cmd(
            "create table tabletest(x);create index tabletest1 on tabletest(x);create index noway on tabletest(x);\n.tables tabletest\n.tables"
        )
        s.cmdloop()
        isempty(fh[2])
        self.assertTrue("tabletest" in get(fh[1]))
        self.assertTrue("tabletest1" not in get(fh[1]))
        self.assertTrue("noway" not in get(fh[1]))

        ###
        ### Command timeout
        ###
        for i in (".timeout", ".timeout ksdjfh", ".timeout 6576 78987"):
            reset()
            cmd(i)
            s.cmdloop()
            isempty(fh[1])
            isnotempty(fh[2])
        for i in (".timeout 1000", ".timeout 0", ".timeout -33"):
            reset()
            cmd(i)
            s.cmdloop()
            isempty(fh[1])
            isempty(fh[2])

        # timer is tested earlier

        ###
        ### Command - version
        ###
        reset()
        cmd(".version")
        s.cmdloop()
        isempty(fh[2])
        self.assertIn(apsw.apsw_version(), get(fh[1]))

        ###
        ### Command - vfsname / vfsinfo / vfslist
        ###
        name = s.db.open_vfs
        reset()
        cmd(".vfsname")
        s.cmdloop()
        self.assertEqual(s.db.vfsname("main") or "", get(fh[1]).strip())
        reset()
        cmd(".vfsname temp")
        s.cmdloop()
        self.assertEqual(s.db.vfsname("temp") or "", get(fh[1]).strip())
        reset()
        cmd(".vfsinfo")
        s.cmdloop()
        self.assertIn(name, get(fh[1]))
        reset()
        cmd(".vfslist")
        s.cmdloop()
        self.assertIn(name, get(fh[1]))

        ###
        ### Command width
        ###
        # does it work?
        reset()
        cmd(".width 10 10 10 0")
        s.cmdloop()
        isempty(fh[1])
        isempty(fh[2])

        def getw():
            reset()
            cmd(".show width")
            s.cmdloop()
            isempty(fh[1])
            return [int(x) for x in get(fh[2]).split()[1:]]

        self.assertEqual([10, 10, 10, 0], getw())
        # some errors
        for i in ".width", ".width foo", ".width 1 2 3 seven 3":
            reset()
            cmd(i)
            s.cmdloop()
            isempty(fh[1])
            isnotempty(fh[2])
            self.assertEqual([10, 10, 10, 0], getw())
        for i, r in ("9 0 9", [9, 0, 9]), ("10 -3 10 -3", [10, -3, 10, -3]), ("0", [0]):
            reset()
            cmd(".width " + i)
            s.cmdloop()
            isempty(fh[1])
            isempty(fh[2])
            self.assertEqual(r, getw())

        ###
        ### Unicode output with all output modes
        ###
        colname = "\N{BLACK STAR}8\N{WHITE STAR}"
        val = 'xxx\u1234\uabcdyyy this" is nasty\u0001stuff!'
        noheadermodes = ("insert",)
        # possible ways val can be represented (eg csv doubles up double quotes)
        outputs = (val, val.replace('"', '""'), val.replace('"', "&quot;"), val.replace('"', '\\"'))
        for mode in [x[len("output_") :] for x in dir(shellclass) if x.startswith("output_")]:
            if mode in ("qbox", "table", "box"):
                # apsw.ext.format_query_table already tested elsewhere
                continue
            reset()
            cmd(
                ".separator |\n.width 999\n.encoding utf8\n.header on\n.mode %s\nselect '%s' as '%s';"
                % (mode, val, colname)
            )
            s.cmdloop()
            isempty(fh[2])
            # modes too complicated to construct the correct string
            if mode in ("python", "tcl"):
                continue
            # all others
            if mode not in noheadermodes:
                self.assertIn(colname if "json" not in mode else json.dumps(colname), get(fh[1]))
            cnt = 0
            for o in outputs:
                cnt += (o if "json" not in mode else json.dumps(o)) in get(fh[1])
            self.assertTrue(cnt)

        # clean up files
        for f in fh:
            f.close()

__all__ = ("Shell",)

if __name__ == "__main__":
    unittest.main()
