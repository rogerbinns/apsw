#!/usr/bin/env python3

import inspect
import os
import pathlib
import subprocess
import sys
import tempfile
import textwrap
from typing import Callable
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

    async def atestResultTypes(self):
        "result type annotation effects"

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

        self.assertEqual(rows, [(3, 4), ("one", 3.3)])

    async def atestParams(self):
        "parameter handling"

        self.db = await apsw.Connection.as_async("")

        # TEST CODE STARTS HERE

        with apsw.query.import_hook():
            import apsw.tests._querytest as q

        self.assertEqual(3, await q.p_binding(self.db, 3))
        self.assertEqual("a'\\\"\03", await q.p_binding(self.db))
        self.assertEqual("Orange[Red]", param_type(q.p_binding, "one"))

        # check the !conversions
        class Conv:
            def __init__(self, v):
                self.v = v

            def __repr__(self):
                # unicode in here to check ascii conversion
                return f"repr \u1234\u5678 {self.v}"

            def __str__(self):
                return f"str {self.v}"

        with self.assertRaisesRegex(TypeError, ".*binding.*"):
            await q.p_binding(self.db, Conv(7))

        xyz = Conv("'\0=\"")

        self.assertEqual(["repr ሴ噸 '\x00=\"", "str '\x00=\"", "repr \\u1234\\u5678 '\x00=\""], await q.p_conv(self.db))

        # check params are done normally - too many names and values
        with self.assertRaisesRegex(TypeError, ".*multiple values.*"):
            await q.p_binding(self.db, 3, one=4)

        with self.assertRaises(TypeError):
            await q.p_binding(self.db, 3, 4, 5)

        # id
        self.assertEqual([{"a'b": 3, "B": 4}, {"b": 3, "a'b": 4}], await q.p_id(self.db, "a'b"))
        self.assertEqual([{'a""b': 3, "B": 4}, {"b": 3, 'a""b': 4}], await q.p_id(self.db, 'a""b'))

        with self.assertRaisesRegex(ValueError, ".*zero byte.*"):
            await q.p_id(self.db, "a\0b")

        # seqid
        name = "'\"\\  ["
        self.assertEqual(
            {"tbl_name": name, "type": "table"}, (await q.p_seqid(self.db, name, ("type", "tbl_name"))).kwargs
        )

        with self.assertRaisesRegex(ValueError, ".*zero byte.*"):
            await q.p_seqid(self.db, name, ("type", "tbl\0_name"))

        # eval
        a = 1
        b = 2
        self.assertEqual(6, await getattr(await q.p_eval(self.db), "get"))

        self.assertEqual(5, await q.p_evalfn(self.db, "seven"))

        self.assertIn("iter", await q.p_eval_seq(self.db))

        self.assertEqual(
            {"tbl_name": name, "type": "table"}, (await q.p_eval_seqid(self.db, name)).kwargs
        )

        # literal
        self.assertEqual("abcdef", await q.p_literal(self.db, "'ab'||'cd'||'ef'"))
        self.assertEqual("abcdef?", await q.p_eval_literal(self.db, "'ab'||'cd'||'ef'"))


    def testAsync(self):
        "async testing of all the things"
        try:
            import asyncio
        except ImportError:
            return

        for name in dir(self):
            if name.startswith("atest"):
                with self.subTest(name=f"{name} (async)", desc=getattr(self, name).__doc__):
                    self.setUp()
                    asyncio.run(getattr(self, name)(), debug=True)
                    self.tearDown()

    def testSync(self):
        "sync testing of all the things"
        for name in dir(self):
            if name.startswith("atest"):
                with self.subTest(name=f"{name} (sync)", desc=getattr(self, name).__doc__):
                    self.setUp()

                    source = inspect.getsource(getattr(self, name)).splitlines()

                    while "TEST CODE STARTS HERE" not in source[0]:
                        source.pop(0)

                    adjusted = textwrap.dedent("\n".join(source))

                    for sub, repl in (
                        ("apytest", "pytest"),
                        ("await ", ""),
                        ("async ", ""),
                    ):
                        adjusted = adjusted.replace(sub, repl)

                    exec(adjusted, globals=globals(), locals={"self": self})

                    self.tearDown()

    def testStuff(self):
        # import hook
        # import when source is a zip file
        # template errors
        # args
        # return types
        return


def param_type(func: Callable, param_name: str) -> str:
    # returns the text annotation of a function parameter
    return inspect.signature(func).parameters[param_name].annotation


__all__ = ("Query",)

if __name__ == "__main__":
    unittest.main()
