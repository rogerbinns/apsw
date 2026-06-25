#!/usr/bin/env python3

import unittest
import os
import pathlib
import subprocess
import sys
import tempfile

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
        with apsw.query.import_hook():
            import apsw.tests._querytest as q

        self.assertEqual(3, q.pytest(2))

        self.assertEqual((3, 4), q.no_bind(self.db))
        self.assertEqual((b"abc", None), q.binding(self.db, b"abc", None))
        y = "a local"
        self.assertEqual((3.3, y), q.binding_locals(self.db, 3.3))

        with self.assertRaises(KeyError):
            del y
            q.binding_locals(self.db, 3)

        x = q.level1(self.db)
        self.assertEqual(x.__class__.__name__, "ns_level1")
        self.assertEqual(x.kwargs, {"one": 1, "T W O": 2})
        y = q.level2(self.db)
        self.assertEqual(y.__class__.__name__, "ns_level2")
        self.assertEqual(y.kwargs, {"3": 3, "": 4})
        z = q.level3(self.db)
        self.assertEqual(z.__class__.__name__, "ns_level3")
        self.assertEqual(z.kwargs, {"select": 5, "class": 6})

        with self.assertRaises(apsw.query.RowExpected):
            q.res_zero(self.db)

        self.assertIsNone(q.res_zero_opt(self.db))

        self.assertEqual("abcdef", q.res_zero_literal(self.db))

        self.assertIs(q.ns_level1.ns_level2.ns_level3, q.res_zero_nested(self.db))

        with self.assertRaises(apsw.query.TooManyRows):
            q.too_many(self.db)

    async def atestGeneral(self):
        # same as above, but async

        self.db = await apsw.Connection.as_async("")

        with apsw.query.import_hook():
            import apsw.tests._querytest as q

        self.assertEqual(3, await q.apytest(2))

        self.assertEqual((3, 4), await q.no_bind(self.db))
        self.assertEqual((b"abc", None), await q.binding(self.db, b"abc", None))
        y = "a local"
        self.assertEqual((3.3, y), await q.binding_locals(self.db, 3.3))

        with self.assertRaises(KeyError):
            del y
            await q.binding_locals(self.db, 3)

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
