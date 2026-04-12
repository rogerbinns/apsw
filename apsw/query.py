#!/usr/bin/env python3

from __future__ import annotations

import ast
import pathlib
import re
import textwrap

"""
Provides Python access to SQLite queries in a separate file or string

this should end up in the rst doc inseat of here

from file, text, import, resource


-- python:

   everything in following /* */ (on lines by themselves is copied verbatim).
   use to introduce types like :code:`from my mymod import mytype`


-- name:

    names query, -- comments become docstring, everything up to EOF or next -- name: / python:

"""


def py_from_file(filename: str | pathlib.Path) -> str:
    "Returns the Python code corresponding to the named file"
    return py_from_text(pathlib.Path(filename).read_text())


def py_from_text(text: str) -> str:
    "Returns the Python code corresponding to text containing queries"
    res: list[str] = []

    for block, value, comments, body in _sections(text):
        match block:
            case "python":
                res.append(f"# {value}")
                res.extend(f"# {line}" for line in comments.splitlines())
                res.append("")
                res.extend(body.splitlines())
            case "name":
                from pprint import pprint
                pprint(_parse_name(value))
            case _:
                raise ValueError(f"Don't know how to handle {block}: {value}")
    return "\n".join(res) + "\n"


def _parse_name(text: str):
    res = {}
    # we use ast to do all the work by pretending it is a function
    # definition.  It may not have any parameters listed, so add empty
    if "(" not in text:
        text += "()"
    parsed = ast.parse("def " + text + ": pass")
    fn = parsed.body[0]
    res["name"] = fn.name
    res["args"] = []
    for a, default in zip(fn.args.args, [None] * (len(fn.args.args) - len(fn.args.defaults)) + fn.args.defaults):
        res["args"].append(
            {
                "name": a.arg,
                "annotation": ast.unparse(a.annotation) if a.annotation else None,
                "default": ast.unparse(default) if default else None,
            }
        )
    res["return_type"] = ast.unparse(fn.returns) if fn.returns else None
    return res


def _sections(text: str):
    title = None
    comments: list[str] = []
    body: list[str] = []

    def part():
        nonlocal title, comments, body
        if title:
            # remove /* and */ from python body
            if title[0] == "python":
                body = [line for line in body if line.strip() not in {"/*", "*/"}]

            # remove leading and trailing blank lines from comments
            # and body
            for lines in comments, body:
                while lines and not lines[0].strip():
                    del lines[0]
                while lines and not lines[-1].strip():
                    del lines[-1]

            yield title[0], title[1], textwrap.dedent("\n".join(comments) + "\n"), "\n".join(body) + "\n"
        title = None
        comments = []
        body = []

    for line in text.splitlines():
        # handle blanks
        if not line.strip():
            # no section yet
            if not title:
                continue
            # body hasn't started
            if not body:
                comments.append(line)
                continue
        mo = re.match(r"^--\s*(?P<type>\w+)\s*:\s*(?P<value>.*?)\s*$", line)
        if mo:
            yield from part()
            title = mo.group("type"), mo.group("value")
            continue
        if not body and line.startswith("--"):
            comments.append(line[2:])
            continue
        body.append(line)
    yield from part()


if __name__ == "__main__":
    import argparse
    import sys

    parser = argparse.ArgumentParser(description="Outputs the Python for a query source")

    parser.add_argument("-o", "--output", help="Output filename [stdout]")

    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--file", help="Source is filename")
    group.add_argument(
        "--import",
        metavar="MODULENAME",
        help="Source is a .sql file corresponding to named module.  ie there is a .sql file where normally there would be a .py file",
    )
    group.add_argument(
        "--resource",
        metavar=("MODULENAME", "FILENAME"),
        nargs=2,
        help="Uses importlib.resources given the module name and the filename relative to the module",
    )

    options = parser.parse_args()

    def output():
        if options.output:
            return open(options.output, "wt")
        else:
            return sys.stdout

    if options.file:
        res = py_from_file(options.file)
        o = output()
        try:
            o.write(res)
        finally:
            o.close()
    else:
        sys.exit("not implemented yet")
