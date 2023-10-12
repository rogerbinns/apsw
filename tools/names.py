#!/usr/bin/env python3

import json
import pathlib
import subprocess
import sys

renames = json.load(pathlib.Path(__file__).with_name("renames.json").open())


def check_old():
    names: set[str] = set()
    for mod in renames.values():
        names.update(mod.values())

    pattern = "(" + "|".join(names) + ")"

    excludes = [
        f":!{n}" for n in ("tools/renames.json", "apsw/tests.py", "apsw/__init__.pyi", "Makefile", "MANIFEST.in",
                           "src/apsw.docstrings")
    ]

    sys.exit(subprocess.run(["git", "--no-pager", "grep", "-E", "-w", pattern, "--"] + excludes).returncode)


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description="Various maintenance operations for renamed symbols")
    sub = parser.add_subparsers()

    p = sub.add_parser("check-old", help="Check use of old names")
    p.set_defaults(func=check_old)

    options = parser.parse_args()
    if not hasattr(options, "func"):
        parser.error("You must specify a subcommand")
    options.func()
