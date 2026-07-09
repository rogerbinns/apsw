#!/usr/bin/env python3

import inspect
import os
import pathlib
import subprocess
import sys
import tempfile
import textwrap
import unittest

import apsw
import apsw.query


class Query(unittest.TestCase):
    def setUp(self):
        self.db = apsw.Connection("")

    def tearDown(self):
        try:
            del sys.modules["apsw.tests._querytest"]
        except KeyError:
            pass
        try:
            global q
            del q
        except NameError:
            pass

        for c in apsw.connections():
            c.close()

    def testCLI(self):
        "command line interface"

        if os.environ.get("COVERAGE_RUN", ""):
            cov = ["-m", "coverage", "run", "--source", "apsw", "-p"]
        else:
            cov = []

        cli = [sys.executable] + cov + ["-m", "apsw.query"]

        # check we get help
        proc = subprocess.run(cli + ["--help"], capture_output=True)
        self.assertEqual(0, proc.returncode)
        self.assertIn(b"Source is filename", proc.stdout)

        # check file or import is required
        proc = subprocess.run(cli + ["--output", "foo"], capture_output=True)
        self.assertNotEqual(0, proc.returncode)
        self.assertIn(b"is required", proc.stderr)

        # spaces deliberately used
        with tempfile.TemporaryDirectory(prefix="apsw query test", ignore_cleanup_errors=True) as td:
            td = pathlib.Path(td)

            f = td / " source! .sql"
            f.write_text("""-- name: select_1 -> int\nSELECT 1""")

            proc = subprocess.run(cli + ["--file", str(f)], capture_output=True)
            self.assertEqual(0, proc.returncode)
            self.assertIn(b"async ", proc.stdout)
            self.assertIn(b"SELECT 1", proc.stdout)

            outf = pathlib.Path(td) / "quack.howdy"
            proc = subprocess.run(cli + ["--file", str(f), "--output", str(outf)], capture_output=True)
            self.assertEqual(0, proc.returncode)
            self.assertEqual(b"", proc.stdout)
            self.assertEqual(b"", proc.stderr)

            self.assertIn("SELECT 1", outf.read_text())

            proc = subprocess.run(cli + ["--import", "apsw.tests._querytest"], capture_output=True)
            self.assertEqual(0, proc.returncode)
            self.assertIn(b"async ", proc.stdout)
            self.assertIn(b"SELECT 2", proc.stdout)

            proc = subprocess.run(
                cli + ["--import", "apsw.tests._querytest", "--output", str(outf)], capture_output=True
            )
            self.assertEqual(0, proc.returncode)
            self.assertEqual(b"", proc.stdout)
            self.assertEqual(b"", proc.stderr)

            self.assertIn("SELECT 2", outf.read_text())

            proc = subprocess.run(cli + ["--import", "apsw.query"], capture_output=True)
            self.assertNotEqual(0, proc.returncode)
            self.assertEqual(b"", proc.stdout)
            self.assertIn(b"was not imported", proc.stderr)

    def testGeneral(self):
        # see comment in atestGeneral

        source = inspect.getsource(self.atestGeneral).splitlines()

        while "TEST CODE STARTS HERE" not in source[0]:
            source.pop(0)

        adjusted = textwrap.dedent("\n".join(source))

        for sub, repl in (
            ("apytest", "pytest"),
            ("await ", ""),
            ("async ", ""),
        ):
            adjusted = adjusted.replace(sub, repl)

        exec(adjusted, globals=globals(), locals=locals())

    async def atestGeneral(self):
        # this code is evaluated in the sync text version with all the
        # async / await omitted to avoid having duplicate code

        self.db = await apsw.Connection.as_async("")

        # TEST CODE STARTS HERE

        with apsw.query.import_hook():
            import apsw.tests._querytest as q

        self.assertEqual(q.__doc__, 'a"b\\n\nd')

        self.assertEqual(3, await q.apytest(2))

        self.assertEqual((3, 4), await q.no_bind(self.db))
        self.assertEqual((b"abc", None), await q.binding(self.db, b"abc", None))
        y = "a local"
        self.assertEqual((3.3, y), await q.binding_locals(self.db, 3.3))

        with self.assertRaises(KeyError):
            del y
            await q.binding_locals(self.db, 3)

        x = await q.level1(self.db)
        self.assertEqual(x.__class__.__name__, "ns_level1")
        self.assertEqual(x.kwargs, {"one": 1, "T W O": 2})
        y = await q.level2(self.db)
        self.assertEqual(y.__class__.__name__, "ns_level2")
        self.assertEqual(y.kwargs, {"3": 3, "": 4})
        z = await q.level3(self.db)
        self.assertEqual(z.__class__.__name__, "ns_level3")
        self.assertEqual(z.kwargs, {"select": 5, "class": 6})

        with self.assertRaises(apsw.query.RowExpected):
            await q.res_zero(self.db)

        self.assertIsNone(await q.res_zero_opt(self.db))

        self.assertEqual("abcdef", await q.res_zero_literal(self.db))

        self.assertIs(q.ns_level1.ns_level2.ns_level3, await q.res_zero_nested(self.db))

        with self.assertRaises(apsw.query.TooManyRows):
            await q.too_many(self.db)

        l = []
        async for row in await q.no_ret(self.db):
            l.append(row)

        self.assertEqual(l, await q.list_ret(self.db))

        with self.assertRaises(apsw.query.TooManyRows):
            await q.none_rows(self.db)

        self.assertIsNone(await q.none(self.db))

        v = await q.change_count(self.db)
        self.assertEqual(3, v)
        self.assertIsSubclass(type(v), int)

        iter1 = q.iter1(self.db)
        self.assertNotIsInstance(iter1, apsw.Cursor)
        rows = []
        async for row in iter1:
            rows.append(row)
        self.assertEqual(2, len(rows))
        self.assertIsInstance(rows[0], q.ns_level1.ns_level2.ns_level3)
        self.assertIsInstance(rows[1], q.ns_level1.ns_level2.ns_level3)

        self.assertEqual(rows[0].kwargs, {"three": 3, "four": 4})
        self.assertEqual(rows[1].kwargs, {"one": "one", "two": 3.3})

        iter2 = q.iter2(self.db)
        self.assertNotIsInstance(iter2, apsw.Cursor)
        rows = []
        async for row in iter2:
            rows.append(row)
        self.assertEqual(2, len(rows))
        self.assertIsInstance(rows[0], tuple)
        self.assertIsInstance(rows[1], tuple)

        self.assertEqual(rows, [(3, 4), ('one', 3.3)])

    def testGeneralAsync(self):
        try:
            import asyncio
        except ImportError:
            return
        asyncio.run(self.atestGeneral(), debug=True)

    def testStuff(self):
        # import hook
        # import when source is a zip file
        # template errors
        # args
        # return types
        return


__all__ = ("Query",)

if __name__ == "__main__":
    unittest.main()
