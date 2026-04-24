#!/usr/bin/env python3

from __future__ import annotations

import ast
import collections.abc
import importlib.abc
import importlib.resources
import importlib.util
import pathlib
import re
import sys
import textwrap
from string import Formatter
from types import ModuleType
from typing import Any, assert_never

"""
Provides Python access to SQLite queries in a separate file or string

See https://rogerbinns.github.io/apsw/query.html for details
"""


# ::TODO:: figure out a way that queries can be bound to a connection
# or cursor (including contextvar of one).  eg both of these should be
# possible:
#
#   con = apsw.Connection()
#   # by default have to provide connection | cursor as first param
#   print(example.fractal(con, width=120))
#
#   # bind all methods to a connection | cursor
#   example.bind(con)
#   # now don't need to supply connection | cursor
#   print(example.fractal(width=120))
#
#   # another alternative - add the methods to a connection | cursor
#   example.bind(cursor)
#   print(cursor.fractal(width=120))


class changes(int):
    "Indicates the number of rows deleted, inserted, and updated are returned"

    pass


class TooManyRows(Exception):
    """More than one row was returned by the SQL"""

    pass


class RowExpected(Exception):
    """A row was was expected but not returned by the SQL"""

    pass


class ChainMapRO:
    """Read-only chainmap for execute bindings

    This only implements enough to be useful for bindings.
    """

    def __init__(self):
        self.maps: list[collections.abc.Mapping[str, Any]] = []

    def __getitem__(self, key: Any) -> Any:
        for map in self.maps:
            try:
                return map[key]
            except KeyError:
                pass
        raise KeyError(f"{key!r} in query but not in bindings. Does it need to be a parameter or local variable?")

    def items(self):
        # Called when displaying locals
        seen = set()
        for map in self.maps:
            for k, v in map.items():
                if k not in seen:
                    seen.add(k)
                    yield (k, v)


# ChainMapRO doesn't implement the full abc such as iter and len but
# they aren't used so we don't care!  That is why we register rather
# thank inherit
collections.abc.Mapping.register(ChainMapRO)

# only need the parse method
_template_parse = Formatter().parse


def _validate_template_string(string: str) -> bool:
    """Checks if it is f-string style using {}, colons etc

    Returns False if not a template, True otherwise.  If a template
    then it is checked for validity and raises an exception if not.
    """

    for i, (literal, field, spec, conversion) in enumerate(_template_parse(string)):
        if i == 0 and field is None and spec is None and conversion is None:
            return False
        match conversion:
            case "r" | "s" | "a" | None:
                pass
            case _:
                raise ValueError(f"Unknown conversion !{conversion}")
        if spec is not None:
            s = set(spec.split(":"))
            if "eval" in s:
                s.discard("eval")
            if s == {"id"} or s == {"seq"} or s == {"id", "seq"} or s == set():
                pass
            else:
                raise ValueError(f"Spec :{spec} not understood")
    return True


def template_expand(template: str, vars: ChainMapRO) -> str:
    "Expands template to SQL, using and updating vars"
    res: list[str] = []
    bindings = {}

    def add_binding(v: Any):
        name = f"_id_{id(v)}"
        res.append(f"${name}")
        bindings[name] = v

    for literal, field, spec, conversion in _template_parse(template):
        if literal:
            res.append(literal)
        if field is None:
            continue
        match conversion:
            case None:
                conv = lambda x: x
            case "r":
                conv = repr
            case "s":
                conv = str
            case "a":
                conv = ascii
            case _:
                raise ValueError(f"Unknown {conversion=}")
        if not spec:
            add_binding(conv(vars[field]))
        else:
            spec = set(spec.split(":"))

            if "eval" in spec:
                spec.remove("eval")
                # ::TODO:: does this need __builtins__ for middle param so
                # eg len() works
                try:
                    value = eval(field, None, vars)
                except Exception as exc:
                    # Python 3.10 doesn't have add_note
                    getattr(exc, "add_note", lambda x: None)(f"Evaluating: {field}")
                    raise
            else:
                value = vars[field]

            if spec == {"id"}:
                value = conv(value)
                res.append('"' + value.replace('"', '""') + '"')
            elif spec == {"seq", "id"}:
                for i, v in enumerate(values):
                    if i:
                        res.append(", ")
                    res.append('"' + conv(value).replace('"', '""') + '"')
            elif spec == {"seq"}:
                for i, v in enumerate(values):
                    if i:
                        res.append(", ")
                    add_binding(conv(v))
            elif spec == set():
                add_binding(conv(value))
            else:
                raise ValueError(f"Unknown specs {spec}")

    if bindings:
        vars.maps.insert(0, bindings)
    return "".join(res)


def _typed_results(node: ast.AST | None, is_async: bool) -> str:
    # given a return annotation, provide the code

    a = "async " if is_async else ""

    # I originally tried to use match but was outsmarted
    if node is None:
        return "        return cursor.execute(sql, vals)\n"

    if isinstance(node, ast.Constant) and node.value is None and node.kind is None:
        return f"""\
        {a}for _ in cursor.execute(sql, vals):
            pass
        return None"""

    if isinstance(node, ast.Name) and node.id == "changes":
        return f"""\
        changes_start = cursor.connection.total_changes()
        {a}for _ in cursor.execute(sql, vals):
            pass
        return cursor.connection.total_changes() - changes_start"""

    if (
        isinstance(node, ast.BinOp)
        and isinstance(node.op, ast.BitOr)
        and isinstance(node.left, ast.Name)
        and isinstance(node.right, (ast.Constant, ast.Name))
    ):
        # one row - return left
        # zero - return right
        # more - raise exception
        l = ast.unparse(node.left)
        r = ast.unparse(node.right)
        return f"""\
        retval = _NotSet
        {a}for row in cursor.execute(sql, vals):
            if retval is not _NotSet:
                raise TooManyRows
            desc = cursor.get_description()
            retval = {l}(row[0]) if len(desc) == 1 else {l}(**dict(zip((d[0] for d in desc), row)))
        return {r} if retval is _NotSet else retval
"""

    if isinstance(node, ast.Name):
        r = ast.unparse(node)
        return f"""\
        retval = _NotSet
        {a}for row in cursor.execute(sql, vals):
            if retval is not _NotSet:
                raise TooManyRows
            desc = cursor.get_description()
            retval = {r}(row[0]) if len(desc) == 1 else {r}(**dict(zip((d[0] for d in desc), row)))
        if retval is _NotSet:
            raise RowExpected
        return retval
    """

    if isinstance(node, ast.List) and len(node.elts) == 1 and isinstance(node.elts[0], ast.Name):
        r = ast.unparse(node.elts[0])
        return f"""\
        {a}for row in cursor.execute(sql, vals):
            desc = cursor.get_description()
            yield {r}(row[0]) if len(desc) == 1 else {r}(**dict(zip((d[0] for d in desc), row)))"""

    raise ValueError(f"Return not understood {ast.unparse(node)!r} ")


def _signature_for(meta: dict, is_async: bool, is_sync: bool) -> str:
    "Call signature based on parsed name and async mode"

    # the method gets declared 3 times - once sync, once async,
    # and once with the union of both
    assert (is_async or is_sync) or (is_async and is_sync)

    sig = f"(executor: "
    take: list[str] = []
    if is_async:
        take.extend('"apsw.AsyncConnection" "apsw.AsyncCursor"'.split())
    if is_sync:
        take.extend("apsw.Connection apsw.Cursor".split())
    sig += " | ".join(take)

    if meta["args"]:
        for i, (name, details) in enumerate(meta["args"].items()):
            if meta["kw_only_pos"] == i:
                sig += ", *"
            sig += f", {name}"
            match (details["annotation"], details["default"]):
                case (None, _):
                    sig += f': "apsw.SQLiteValue" = {details['default']}'
                case (_, None):
                    sig += f": {details['annotation']}"
                case (None, None):
                    sig += ': "apsw.SQLiteValue"'
                case _:
                    sig += f": ({details['annotation']}) = {details['default']}"
    sig += ") -> "

    if meta["return_type"] is None:
        match (is_async, is_sync):
            case (True, True):
                sig += '"apsw.AsyncCursor" | apsw.Cursor'
            case (True, False):
                sig += '"apsw.AsyncCursor"'
            case (False, True):
                sig += "apsw.Cursor"
            case _:
                assert_never((False, False))
    else:
        # _typed_results does validation of return type so
        # this just yolos it

        node = meta["return_type"]

        if isinstance(node, ast.List) and len(node.elts) == 1:
            inner = ast.unparse(node.elts[0])
            match (is_async, is_sync):
                case (True, True):
                    sig += f"Iterator[{inner}] | AsyncIterator[{inner}]"
                case (True, False):
                    sig += f"AsyncIterator[{inner}]"
                case (False, True):
                    sig += f"Iterator[{inner}]"
                case _:
                    assert_never((False, False))

        else:
            if isinstance(node, ast.BinOp):
                return_type = f"{ast.unparse(node.left)} | Literal[{ast.unparse(node.right)}]"
            else:
                return_type = ast.unparse(node)

            match (is_async, is_sync):
                case (True, True):
                    sig += f"Awaitable[{return_type}] | {return_type}"
                case (True, False):
                    sig += f"Awaitable[{return_type}]"
                case (False, True):
                    sig += f"{return_type}"
                case _:
                    assert_never((False, False))

    return sig


def py_from_file(filename: str | pathlib.Path, encoding: str = "utf8") -> str:
    "Returns the Python code corresponding to the named file"
    return _make_py_from_text(pathlib.Path(filename).read_text(encoding=encoding))


def py_from_text(text: str) -> str:
    "Returns the Python code corresponding to text containing queries"

    return _make_py_from_text(text)


def py_from_resource(anchor: importlib.resources.Anchor, name: str, encoding="utf8") -> str:
    """Uses :mod:`importlib.resources` to find locate named file

    The anchor should either be a module, or the name of a module.
    The name is a file relative to the anchor, and should use ``/`` as
    the directory separator on all platforms.

    This lets you keep your SQL files alongside your code, and will
    correctly handle wheels and other formats.

    Setuptools has `data file support
    <https://setuptools.pypa.io/en/latest/userguide/datafiles.html>`__
    with other packaging tools providing something similar.
    """

    files = importlib.resources.files(anchor)

    text = files.joinpath(name).read_text(encoding=encoding)

    return _make_py_from_text(text)


def install_import_hook():
    """You can use this to allow directly importing .sql files as modules

    An import hook will be installed, if not already installed.  It is
    ok to call this function multiple times.

    You can import :code:`.sql` files as though they were native
    Python.  In the following example import, if ``my_queries.sql``
    exists at the location where you would normally have
    `my_queries.py`` then it automatically works.

        import some_package.my_queries as queries

    """
    for inst in sys.meta_path:
        if isinstance(inst, _Import_Hook):
            return
    sys.meta_path.append(_Import_Hook())

class _Import_Hook(importlib.abc.MetaPathFinder):

    def find_spec(self, fullname: str, path: collections.abc.Sequence[str] | None, target: ModuleType | None = None):
        name = fullname.split(".")[-1]
        search_dirs = path if path else sys.path

        for entry in search_dirs:
            base_path = pathlib.Path(entry) / name
            sql_path = base_path.with_suffix(".sql")

            if sql_path.exists():
                return importlib.util.spec_from_loader(fullname, _SQLSourceLoader(sql_path), origin=str(sql_path))

        return None


class _SQLSourceLoader(importlib.abc.SourceLoader):
    # This only handles one file at a time which is why all the
    # parameters are ignored
    def __init__(self, path: pathlib.Path):
        self.path = path

    def get_filename(self, fullname: str):
        return str(self.path)

    def get_data(self, path: str):
        return _make_py_from_text(self.path.read_text(encoding="utf8")).encode("utf8")


def _make_py_from_text(text: str) -> str:
    "Internal routine that converts SQL file to Python code"
    res: list[str] = []

    res.extend(
        """\

# This code was generated by apsw.query from ::TODO:: fill this in
# and also use $SOURCE_DATE_EPOCH for reproducible build

# do not evaluate annotations at import time (default in Python 3.14+)
from __future__ import annotations

# some of the imports may not be used hence the noqa marking

import sys # noqa:
from typing import overload
from collections.abc import  Awaitable, Iterator, AsyncIterator # noqa:

import apsw
from apsw.query import ChainMapRO, template_expand, changes, TooManyRows, RowExpected #  noqa:

_NotSet = object()
"Sentinel for an unset value"

""".splitlines()
    )

    # ::TODO:: if first block is python and it starts with a docstring
    # then it needs to be put first

    for block, value, comments, body in _sections(text):
        match block:
            case "python":
                res.append(f"# {value}")
                res.extend(f"# {line}" for line in comments.splitlines())
                res.append("")
                res.extend(body.splitlines())
                res.append("")
                res.append("")
            case "name":
                meta = _parse_name(value)
                try:
                    is_template = _validate_template_string(body)
                except Exception as exc:
                    # Another py 3.10 compat
                    getattr(exc, "add_note", lambda x: None)(f"""In query '{meta["name"]}'""")
                    raise
                res.append(f"""\
async def _async_{meta["name"]}(cursor: "apsw.AsyncCursor", sql: str, vals: ChainMapRO):
    try:

{_typed_results(meta["return_type"], True)}
    except Exception as exc:
        # py 3.10 doesn't have add_note
        getattr(exc, 'add_note', lambda x: None)("In query named {meta["name"]!r}")
        raise

@overload
def {meta["name"]}{_signature_for(meta, True, False)}:
     ...
@overload
def {meta["name"]}{_signature_for(meta, False, True)}:
    ...
def {meta["name"]}{_signature_for(meta, True, True)}:
{"\n".join(_triple_quote(comments, "    ")) if comments.strip() else "    # Add SQL comments after name: for a docstring\n"}
    cursor : apsw.Cursor | "apsw.AsyncCursor" = executor.cursor() if isinstance(executor, apsw.Connection) else executor
    vals = ChainMapRO()
""")
                if meta["args"]:
                    res.append("    vals.maps.append({")
                    for name in meta["args"]:
                        res.append(f'        "{name}": {name},')
                    res.append("    })")
                if meta["locals"]:
                    res.append("""    vals.maps.append(sys._getframe(1).f_locals)""")
                res.append("    try:")
                if _validate_template_string(body):
                    res.append(f"        sql = template_expand({meta['name']}.template, vals)")
                else:
                    res.append(f"        sql = {meta['name']}.sql")
                res.append("        if cursor.connection.is_async:")
                res.append(f"            return _async_{meta['name']}(cursor, sql, vals)")
                res.extend(_typed_results(meta["return_type"], False).splitlines())
                res.append("    except Exception as exc:")
                res.append("        # py 3.10 doesn't have add_note")
                res.append(f"""        getattr(exc, 'add_note', lambda x: None)("In query named {meta["name"]!r}")""")
                res.append("        raise")

                res.append("")
                res.append(f"{meta['name']}.{'template' if is_template else 'sql'} = \\")
                res.extend(_triple_quote(body))
                res.append("")

                res.append("")
            case _:
                raise ValueError(f"Unknown section {block}")

    # ::TODO:: add __all__=("each", "name")

    return "\n".join(res) + "\n"


def _triple_quote(text: str, indent: str = "") -> list[str]:
    res = []
    triple = '"""'
    text = [line.replace(triple, '"""') for line in text.splitlines()]
    for i, line in enumerate(text):
        res.append(f"{indent}{triple if i == 0 else ''}{line}{triple if i == len(text) - 1 else ''}")
    return res


def _parse_name(text: str):
    "parse query name similar to it being a function definition"
    res = {}
    res["locals"] = False

    # we use ast to do all the work by pretending it is a function
    # definition.  It may not have any parameters listed, so add empty
    # ones if necessary.
    parse_as = f"def {text}: pass"
    try:
        ast.parse(parse_as)
    except SyntaxError as exc:
        # note offset/end_offset are 1 based not zero.  If no args are provided then
        # locals is om
        res["locals"] = True

        # -> without preceding ()
        if exc.text[exc.offset - 1 : exc.end_offset - 1] == "->" and exc.msg == "expected '('":
            parse_as = exc.text[: exc.offset - 1] + "()" + exc.text[exc.offset - 1 :]
        # no ()
        elif exc.text[exc.offset - 1 : exc.end_offset - 1] == ":" and exc.msg == "expected '('":
            parse_as = exc.text[: exc.offset - 1] + "()" + exc.text[exc.offset - 1 :]
        else:
            raise ValueError(f"Unable to parse {text!r}: {exc.args[0]}") from None

    parsed = ast.parse(parse_as)
    fn = parsed.body[0]
    res["name"] = fn.name
    res["args"] = {}
    res["kw_only_pos"] = len(fn.args.args) if fn.args.kwonlyargs else None
    if fn.args.kwarg:
        if fn.args.kwarg.arg != "locals" or fn.args.kwarg.annotation:
            raise ValueError(
                f"**locals without annotation to indicate using caller locals must be used.\nSaw {ast.unparse(fn.args.kwarg)!r} in {text!r}"
            )
        res["locals"] = True

    for a, default in zip(
        fn.args.args + fn.args.kwonlyargs,
        ([None] * (len(fn.args.args) - len(fn.args.defaults))) + fn.args.defaults + fn.args.kw_defaults,
    ):
        assert isinstance(a, ast.arg)
        if a.arg in res["args"]:
            raise ValueError(f"Argument {a.arg!r} appears more than once in {text!r}")
        res["args"][a.arg] = {
            "annotation": ast.unparse(a.annotation) if a.annotation else None,
            "default": ast.unparse(default) if default else None,
        }

    res["return_type"] = fn.returns

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
        dest="import_",
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
    elif options.resource:
        res = py_from_resource(options.resource[0], options.resource[1])
        o = output()
        try:
            o.write(res)
        finally:
            o.close()
    elif options.import_:
        install_import_hook()
        mod = importlib.import_module(options.import_)
        if not isinstance(getattr(mod, "__loader__", None), _SQLSourceLoader):
            sys.exit(f"{options.import_!r} was not imported from a SQL file")
        res = py_from_file(mod.__loader__.path)
        o = output()
        try:
            o.write(res)
        finally:
            o.close()
    else:
        sys.exit("not implemented yet")
