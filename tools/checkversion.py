#!/usr/bin/env python

# This is to prevent a repeat of issue #559 where minimum version
# numbers weren't updated for a release.
#
# The minimum version exists in the following places:
#
# apsw.c in a #if
# apsw.c in #error message after the #if
# doc/about.rst in a paragraph
# tools/megatest.py in SQLITEVERS tuple
#
# we want to ensure they all have the same value

import re
import pathlib
import sys


def convert_to_number(s):
    s = [int(part) for part in s.split(".")]

    assert s[0] == 3
    assert len(s) in {2, 3}

    if len(s) == 2:
        s += [0]

    return s[0] * 1000000 + s[1] * 1000 + s[2]


def as_string(n):
    return f"{n // 1000000}.{(n // 1000) % 1000}.{n % 1000}"


vers = {}

mo = re.search(r"^#if SQLITE_VERSION_NUMBER < ([0-9]+)$", pathlib.Path("src/apsw.c").read_text(), re.MULTILINE)
vers["apsw.c #if"] = int(mo.group(1))

mo = re.search(
    r"^#error Your SQLite version is too old.  It must be at least ([0-9\.]+)$",
    pathlib.Path("src/apsw.c").read_text(),
    re.MULTILINE,
)
vers["apsw.c #error"] = convert_to_number(mo.group(1))

mo = re.search(
    r"^Use with `SQLite <https://sqlite.org/>`__ ([0-9\.]+) or later",
    pathlib.Path("doc/about.rst").read_text(),
    re.MULTILINE,
)
vers["about.rst text"] = convert_to_number(mo.group(1))

import megatest

vers["megatest SQLITEVERS"] = convert_to_number(min(megatest.SQLITEVERS))


if len(set(vers.values())) == 1:
    sys.exit(0)

maxk = 2 + max(len(k) for k in vers)

print("Minimum SQLite versions do not agree\n")
for k, v in sorted(vers.items()):
    print(f"{k:{maxk}} {as_string(v)}  {v}")
sys.exit(1)
