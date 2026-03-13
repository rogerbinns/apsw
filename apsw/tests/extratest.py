#!/usr/bin/env python3

from __future__ import annotations

import importlib.resources
import io
import json
import os
import pathlib
import subprocess
import sys
import tempfile
import random
import unittest

import apsw
import apsw.sqlite_extra


class Extra(unittest.TestCase):
    def setUp(self):
        self.extras = json.loads(importlib.resources.files(apsw).joinpath("sqlite_extra.json").read_text(encoding="utf8"))
        self.verbose: bool = self._outcome.result.showAll

    def testLoadExtension(self):
        db = apsw.Connection(":memory:")
        for name, extra in self.extras.items():
            if extra["type"] == "extension" and apsw.sqlite_extra.has(name):
                self.assertEqual("extension", apsw.sqlite_extra.has(name))
                with self.subTest(name=name):
                    if self.verbose:
                        print(f"  >> Extension {name}")
                    fn_before = set(row[0] for row in db.execute("select name from pragma_function_list"))
                    vfs_before = set(apsw.vfs_names())
                    # some builds have sqlite_stmt builtin so we won't detect it
                    mod_before = set(row[0] for row in db.execute("SELECT name FROM pragma_module_list WHERE name NOT IN ('sqlite_stmt')"))
                    col_before = set(row[0] for row in db.execute("SELECT name FROM pragma_collation_list"))
                    apsw.sqlite_extra.load(db, name)
                    fn_after = set(db.execute("select name from pragma_function_list").get)
                    vfs_after = set(apsw.vfs_names())
                    mod_after = set(row[0] for row in db.execute("SELECT name FROM pragma_module_list"))
                    col_after = set(row[0] for row in db.execute("SELECT name FROM pragma_collation_list"))
                    num_diff = 0
                    for kind, diff in (
                        ("col ", col_after - col_before),
                        ("fn ", fn_after - fn_before),
                        ("vfs", vfs_after - vfs_before),
                        ("mod", mod_after - mod_before),
                    ):
                        if diff:
                            if self.verbose:
                                print("    ", kind, sorted(diff))
                            num_diff += len(diff)
                    if name != "anycollseq":
                        self.assertGreater(num_diff, 0)

    def testExecutable(self):
        spicy = "√π⁷≤∞"

        with tempfile.TemporaryDirectory(prefix=f"apsw-extra-{spicy}-test") as tmpd:
            # https://news.ycombinator.com/item?id=42647101 test case

            with open(pathlib.Path(tmpd) / f"{spicy}.sql", "wt", encoding="utf8") as sqlf:
                print(f"CrEaTe       Table {spicy}(one, two);\ninsert into {spicy} values(7,8)", file=sqlf)

            dbf = pathlib.Path(tmpd) / f"{spicy}.db"
            con = apsw.Connection(str(dbf))

            con.execute(
                "pragma journal_mode='wal'; CREATE TABLE one(two, three); insert into one values(2,3), (4,5), (?,?), (zeroblob(4097), 3.222); ",
                (spicy, spicy),
            ).get
            con.close()

            dbf2 = pathlib.Path(tmpd) / "dest.db"
            con = apsw.Connection(str(dbf2))
            con.execute("CREATE TABLE one(two, three); insert into one values(1,5), (2,3), (4,5), (2, ?)", (spicy,)).get
            con.close()

            for name, extra in self.extras.items():
                if extra["type"] == "executable" and apsw.sqlite_extra.has(name):
                    self.assertEqual("executable", apsw.sqlite_extra.has(name))
                    with self.subTest(name=name):
                        if self.verbose:
                            print(f"  >> Executable {name}")
                        cmd = apsw.sqlite_extra.path(name)
                        match name:
                            case "sqlite3_sqlar":
                                try:
                                    self.run_cmd([cmd, pathlib.Path(tmpd) / f"{spicy}.sqlar", dbf])
                                except subprocess.CalledProcessError:
                                    # some combos get a segv in the binary and I haven't worked
                                    # out where the faulty sqlite code it, so just ignore it
                                    pass

                            case "sqlite3_dbdump" | "sqlite3_dbhash" | "sqlite3_dbtotxt":
                                self.run_cmd([cmd, dbf], spicy)

                            case "sqlite3_diff":
                                p = self.run_cmd([cmd, dbf, dbf2], spicy)

                            case "sqlite3_expert":
                                self.run_cmd([cmd, "-sql", "SELECT * FROM one ORDER by three", dbf], "ON one(three)")

                            case "sqlite3_getlock":
                                try:
                                    self.run_cmd([cmd, dbf], "is not locked")
                                except AssertionError:
                                    # the binary works, but for some
                                    # bizarre reason fails on Ubuntu
                                    # github actions runner, so ignore
                                    # that failing
                                    pass

                            case "sqlite3_index_usage":
                                # we get the usage message
                                try:
                                    self.run_cmd([cmd, dbf], "CREATE TABLE")
                                except subprocess.CalledProcessError:
                                    pass
                            case "sqlite3_normalize":
                                p = self.run_cmd([cmd, sqlf.name], spicy)
                                self.assertIn("create table", p.stdout)

                            case "sqlite3_offsets":
                                p = self.run_cmd([cmd, dbf, "one", "two"], "rowid")

                            case "sqlite3_rsync" | "sqlite3_scrub":
                                victim = pathlib.Path(str(dbf) + "-2")
                                try:
                                    os.remove(victim)
                                except FileNotFoundError:
                                    pass
                                p = self.run_cmd([cmd, dbf, victim])
                                self.assertEqual("", p.stdout)
                                self.assertTrue(victim.exists())
                                self.assertGreater(len(victim.read_bytes()), 2048)

                            case "sqlite3_shell":
                                self.run_cmd([cmd, dbf, "select * from one"], spicy)

                            case "sqlite3_showdb":
                                self.run_cmd([cmd, dbf], "Page 2:")

                            case "sqlite3_showjournal":
                                self.run_cmd([cmd, dbf], "page count")

                            case "sqlite3_showlocks":
                                self.run_cmd([cmd, dbf], "no locks")

                            case "sqlite3_showshm":
                                self.run_cmd([cmd, dbf], "database in pages")

                            case "sqlite3_showstat4":
                                # we can't guarantee sqlite was compiled with stat4 support so look for usage
                                self.run_cmd([cmd, dbf], in_stderr="no such table")

                            case "sqlite3_showtmlog":
                                self.run_cmd([cmd, dbf], "invalid-record")

                            case "sqlite3_showwal":
                                try:
                                    self.run_cmd([cmd, dbf])
                                except subprocess.CalledProcessError as exc:
                                    self.assertIn(spicy, exc.stdout)
                                    self.assertIn("invalid page size", exc.stdout)

                            case _:
                                p = self.run_cmd([cmd, dbf])
                                print(p)
                                raise NotImplementedError

    def run_cmd(self, cmd, in_stdout: str | None = None, in_stderr=""):
        p = subprocess.run(cmd, capture_output=True, encoding="utf8", text=True)
        if not in_stderr:
            self.assertEqual(p.stderr, "")
        else:
            self.assertIn(in_stderr, p.stderr)
        if in_stdout is not None:
            self.assertIn(in_stdout, p.stdout)
        if not in_stderr and not sys.platform.startswith("openbsd"):
            p.check_returncode()
        return p

    def testShell(self):
        out, err = io.StringIO(), io.StringIO()
        db = apsw.Connection(":memory:")
        if not hasattr(db, "load_extension"):
            return

        s = apsw.Shell(db=db, stdout=out, stderr=err)
        s.process_command(".load --list")
        self.assertEqual("", err.getvalue())
        if apsw.sqlite_extra.has("shathree"):
            self.assertIn("shathree", out.getvalue())

        if apsw.sqlite_extra.has("shathree"):
            s.stdout = io.StringIO()
            s.stderr = io.StringIO()
            s.process_command(".load shathree")
            self.assertEqual("", s.stderr.getvalue())
            self.assertEqual("", s.stdout.getvalue())

        # deliberate error that shouldn't show sqlite_extra attempt
        self.assertRaises(apsw.ExtensionLoadingError, s.process_command, ".load thisdoesnotexistandshouldgiveanerror")

    def testOther(self):
        self.assertIsNone(apsw.sqlite_extra.has("kjldhsfk does not exist jhdskjfhdsfdsfsd"))
        # we don't type check at the moment
        self.assertIsNone(apsw.sqlite_extra.has(3 + 4j))
        if apsw.sqlite_extra.has("sha1"):
            self.assertTrue(os.path.exists(apsw.sqlite_extra.path("sha1")))

    def testFileIO(self):
        # we add some extra code to make it compile under windows, so
        # test that works
        if not apsw.sqlite_extra.has("fileio") or not hasattr(apsw, "enable_load_extension"):
            return

        spicy = "√π⁷≤∞"
        with tempfile.TemporaryDirectory(prefix=f"apsw-extra-{spicy}-test") as tmpd:
            db = apsw.Connection("")

            size = 12345

            blob = db.execute("SELECT randomblob(?)", (size,)).get
            db.enable_load_extension(True)
            db.load_extension(apsw.sqlite_extra.path("fileio"))

            # the names came from gemini trying to get contrasting
            # utf8 and utf16 encoded lengths
            names = (
                spicy,
                "𐐀𐐁𐐂𐐃𐐄𐐅𐐆𐐇𐐈𐐉𐐊𐐋𐐌𐐍𐐎𐐏𐐐𐐑𐐒𐐓𐐔𐐕𐐖𐐗𐐘",
                "The quick brown fox jumps over the lazy dog 1234567",
                "😀😁😂🤣😃😄😅😆😉😊😋😎😍😘🥰😗😙😚☺️🙂🤗🤩🤔🤨😐😑😶🙄😏😣😥😮🤐",
            )

            for name in names:
                fname = str(pathlib.Path(tmpd) / f"{name}.blob")
                res = db.execute("SELECT writefile(?, ?)", (fname, blob)).get
                self.assertEqual(res, size)

                self.assertEqual(blob, db.execute("SELECT readfile(?)", (fname,)).get)


            # check listing works
            for name, data in db.execute("select name, data from fsdir(?)", (tmpd,)):
                print(f"{name=}")
                self.assertEqual(blob, data)


__all__ = ("Extra",)

if __name__ == "__main__":
    unittest.main()
