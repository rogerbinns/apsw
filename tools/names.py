#!/usr/bin/env python3

import json
import pathlib
import subprocess
import sys
import re
import unittest
import types

renames = json.loads(pathlib.Path(__file__).with_name("renames.json").read_text())


def check_old():
    names: set[str] = set()
    for mod in renames.values():
        names.update(mod.values())

    pattern = "(" + "|".join(names) + ")"

    excludes = [
        f":!{n}"
        for n in (
            "tools/renames.json",
            "apsw/tests/__main__.py",
            "apsw/__init__.pyi",
            "Makefile",
            "MANIFEST.in",
            "src/apsw.docstrings",
        )
    ]

    sys.exit(subprocess.run(["git", "--no-pager", "grep", "-E", "-w", pattern, "--"] + excludes).returncode)


def rst_gen():
    # we need to know if something is an attribute or a function
    import apsw

    def get_link(klass: str, name: str) -> str:
        h = apsw if klass == "apsw" else getattr(apsw, klass)
        return "meth" if callable(getattr(h, name)) else "attr"

    print("""
.. list-table::
    :header-rows: 1
    :widths: auto

    * - Class
      - Name
      - Old name
""")
    for klass, members in sorted(renames.items(), key=lambda x: x[0].lower()):
        if klass == "apsw":
            kl = ":mod:`apsw`"
        else:
            kl = f":class:`{ klass }`"
        for new, old in sorted(members.items()):
            print(f"""\
    * - { kl }
      - :{ get_link(klass, new) }:`{ klass }.{ new }`
      - :index:`{ old } <single: { old }; { klass }.{ new }>`""")
            kl = ""


def run_tests():
    test_file_name = pathlib.Path("apsw/tests/__main__.py")
    source = test_file_name.read_text()

    subs = {}

    for _, members in renames.items():
        for new, old in members.items():
            if new in subs:
                assert subs[new] == old
            subs[new] = old

    def repl(mo):
        return subs[mo.group(0)]

    old_source = re.sub("\\b(" + "|".join(subs.keys()) + ")\\b", repl, source)

    for sub, repl in (
        ("from .ftstests import *", "from apsw.tests.ftstests import *"),
        ("from .sessiontests import *", "from apsw.tests.sessiontests import *"),
    ):
        old_source = old_source.replace(sub, repl)

    module = types.ModuleType("tests")
    vars(module)["__file__"] = str(test_file_name)

    exec(compile(old_source, "apsw/tests/__main__.py", "exec"), vars(module))

    module.setup()

    unittest.TextTestRunner().run(unittest.defaultTestLoader.loadTestsFromModule(module))


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Various maintenance operations for renamed symbols")
    sub = parser.add_subparsers()

    p = sub.add_parser("check-old", help="Check use of old names")
    p.set_defaults(func=check_old)

    p = sub.add_parser("rst-gen", help="Generate documentation")
    p.set_defaults(func=rst_gen)

    p = sub.add_parser("run-tests", help="Run tests converting them to use old names")
    p.set_defaults(func=run_tests)

    options = parser.parse_args()
    if not hasattr(options, "func"):
        parser.error("You must specify a subcommand")
    options.func()
