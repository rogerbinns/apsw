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

            proc=subprocess.run(cli+["--file", str(f)], capture_output=True)
            self.assertEqual(0, proc.returncode)
            self.assertIn(b"async ", proc.stdout)
            self.assertIn(b"SELECT 1", proc.stdout)



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
