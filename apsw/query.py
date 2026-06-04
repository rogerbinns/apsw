#!/usr/bin/env python3

from __future__ import annotations

import ast
import collections.abc
import importlib.abc
import importlib.util
import pathlib
import re
import sys
import textwrap
from string import Formatter
from types import ModuleType
from typing import Any

"""
Provides Pythonic interface to SQL in a file

See :doc:`query` for details
"""


# ::TODO::
#
# Add a type - eg ExecuteSequence - and if one of the params is that type
# then do executemany instead of execute
#
# Figure out a way of indicating transaction - ie the SQL should be run wrapped
# in with/savepoint.  It shouldn't be automatic for every SQL
#
# If return type is Any then don't do any conversion


class changes(int):
    "Indicates the number of rows deleted, inserted, and updated are returned"

    pass


class TooManyRows(Exception):
    """More than one row was returned by the SQL, but at most one was expected"""

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
        exc = KeyError(key)
        getattr(exc, "add_note", lambda x: None)(f"{key!r} in SQL template  but not in bindings. Does it need to be eval, a parameter. or local variable?")
        raise exc

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


def bind_sql(sql: str):
    "Binds `sql` to a function"

    # It is done this way so the SQL comes before the function
    # definition which means sphinx links to the SQL decorator making
    # for better documentation

    def decorator(func: collections.abc.Callable) -> collections.abc.Callable:
        func.sql = sql
        return func

    return decorator


# only need the parse method
_template_parse = Formatter().parse


def _is_template_string(string: str) -> bool:
    """Checks if it is f-string style using {}, colon etc

    Returns False if not a template, True if it should be processed as
    a template.
    """

    for i, (literal, field, spec, conversion) in enumerate(_template_parse(string)):
        if i == 0 and field is None and spec is None and conversion is None:
            return False
    return True


def template_expand(template: str, vars: ChainMapRO) -> str:
    "Expands template to SQL, using and updating vars"
    res: list[str] = []
    bindings = {}

    def add_binding(v: Any):
        # Use incrementing numbers for the bindings as this is
        # friendly to the statement cache
        name = f"_binding_{len(bindings)}"
        res.append(f"${name}")
        bindings[name] = v

    for literal, field, spec, conversion in _template_parse(template):
        if literal:
            res.append(literal)
        if field is None:
            assert spec is None and conversion is None
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
                raise ValueError(f"Unknown !{conversion=}")
        if not spec:
            add_binding(conv(vars[field]))
        else:
            spec = [s.strip() for s in spec.split("|")]

            if spec and spec[0] == "eval":
                spec.pop(0)
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

            match spec:
                case ["id"]:
                    value = conv(value)
                    res.append('"' + value.replace('"', '""') + '"')

                case ["seq", "id"]:
                    for i, v in enumerate(values):
                        if i:
                            res.append(", ")
                        res.append('"' + conv(value).replace('"', '""') + '"')

                case ["seq"]:
                    for i, v in enumerate(value):
                        if i:
                            res.append(", ")
                        add_binding(conv(v))

                case ["literal"]:
                    res.append(conv(v))

                case []:
                    add_binding(conv(value))

                case _:
                    raise ValueError(f"Unknown {spec=}")

    if bindings:
        vars.maps.insert(0, bindings)
    return "".join(res)


def _unwrap(node: ast.AST, name: str) -> str | None:
    # If a name[foo], return str(inside) - "foo" in this example - else None
    if isinstance(node, ast.Subscript) and isinstance(node.value, ast.Name) and node.value.id == name:
        return ast.unparse(node.slice)
    return None

def _retval_for(a_type:ast.AST | str) -> str:
    # how a row is converted to the type
    t = ast.unparse(a_type) if not isinstance(a_type, str) else a_type
    if t == "Any":
            return  "row[0] if len(desc) == 1 else row"
    return f"{t}(row[0]) if len(desc) == 1 else {t}(**dict(zip((d[0] for d in desc), row, strict=True)))"

def _gen_function(meta: dict[str, Any]) -> str:
    "Generates the code for one query"
    res: list[str] = []
    comments: str | None = meta["comments"]

    async_sig = "(executor: apsw.AsyncCursor | apsw.AsyncConnection"
    sync_sig = "(executor: apsw.Cursor | apsw.Connection"
    both_sig = "(executor: apsw.Cursor | apsw.Connection | apsw.AsyncCursor | apsw.AsyncConnection"

    sig = ""

    if meta["args"]:
        for i, (name, details) in enumerate(meta["args"].items()):
            if meta["kw_only_pos"] == i:
                sig += ", *"
            sig += f", {name}"
            match (details["annotation"], details["default"]):
                case (None, _):
                    sig += f": apsw.Binding = {details['default']}"
                case (_, None):
                    sig += f": {details['annotation']}"
                case (None, None):
                    sig += ": apsw.Binding"
                case _:
                    sig += f": {details['annotation']} = {details['default']}"
    sig += ") -> "

    async_sig += sig
    sync_sig += sig
    both_sig += sig

    node = meta["return_type"]

    if node is None:
        async_sig += "apsw.AsyncCursor"
        sync_sig += "apsw.Cursor"
        both_sig += "apsw.AsyncCursor | apsw.Cursor"

        inner = """
    async def async_inner() -> apsw.AsyncCursor:
        return await cursor.execute(sql, vals)

    def sync_inner() -> apsw.Cursor:
        return cursor.execute(sql, vals)
"""

    elif isinstance(node, ast.Constant) and node.value is None and node.kind is None:
        async_sig += "Awaitable[None]"
        sync_sig += "None"
        both_sig += "None | Awaitable[None]"

        inner = """
    async def async_inner() -> None:
        async for _ in await cursor.execute(sql, vals):
            pass

    def sync_inner() -> None:
        for _ in cursor.execute(sql, vals):
            pass
"""

    elif isinstance(node, ast.Name) and node.id == "changes":
        async_sig += "Awaitable[changes]"
        sync_sig += "changes"
        both_sig += "changes | Awaitable[changes]"

        inner = """
    async def async_inner() -> changes:
        count = cursor.connection.total_changes()
        async for _ in await cursor.execute(sql, vals):
            pass
        return cursor.connection.total_changes() - count

    def sync_inner() -> changes:
        count = cursor.connection.total_changes()
        for _ in cursor.execute(sql, vals):
            pass
        return cursor.connection.total_changes() - count
"""

    elif (
        isinstance(node, ast.BinOp)
        and isinstance(node.op, ast.BitOr)
        and isinstance(node.left, (ast.Name, ast.Attribute))
        and isinstance(node.right, (ast.Constant, ast.Name, ast.Subscript))
    ):
        # one row - return left
        # zero - return right
        # more - raise exception
        l = ast.unparse(node.left)
        r = ast.unparse(node.right)

        async_sig += f"Awaitable[{l} | {r}]"
        sync_sig += f"{l} | {r}"
        both_sig += f"Awaitable[{l} | {r}] | {l} | {r}"



        inner = f"""
    async def async_inner() -> {l} | {r}:
        retval = _NotSet
        async for row in await cursor.execute(sql, vals):
            if retval is not _NotSet:
                raise TooManyRows
            desc = cursor.get_description()
            retval = {_retval_for(node.left)}
        return ({_unwrap(node.right, "Literal") or r}) if retval is _NotSet else retval

    def sync_inner() -> {l} | Literal[{r}]:
        retval = _NotSet
        for row in cursor.execute(sql, vals):
            if retval is not _NotSet:
                raise TooManyRows
            desc = cursor.get_description()
            retval = {_retval_for(node.left)}
        return ({_unwrap(node.right, "Literal") or r}) if retval is _NotSet else retval
"""

    elif isinstance(node, (ast.Name, ast.Attribute)):
        r = ast.unparse(node)
        async_sig += f"Awaitable[{r}]"
        sync_sig += r
        both_sig += f"Awaitable[{r}] | {r}"

        inner = f"""
    async def async_inner() -> {r}:
        retval = _NotSet
        async for row in await cursor.execute(sql, vals):
            if retval is not _NotSet:
                raise TooManyRows
            desc = cursor.get_description()
            retval = {_retval_for(node)}
        if retval is _NotSet:
            raise RowExpected
        return retval

    def sync_inner() -> {r}:
        retval = _NotSet
        for row in cursor.execute(sql, vals):
            if retval is not _NotSet:
                raise TooManyRows
            desc = cursor.get_description()
            retval = {_retval_for(node)}
        if retval is _NotSet:
            raise RowExpected
        return retval
"""

    elif (i := _unwrap(node, "Iterator")) is not None:
        async_sig += f"AsyncIterator[{i}]"
        sync_sig += f"Iterator[{i}]"
        both_sig += f"AsyncIterator[{i}] | Iterator[{i}]"

        inner = f"""
    async def async_inner() -> AsyncIterator[{i}]:
        async for row in await cursor.execute(sql, vals):
            desc = cursor.get_description()
            yield {_retval_for(i)}

    def sync_inner() -> Iterator[{i}]:
        for row in cursor.execute(sql, vals):
            desc = cursor.get_description()
            yield {_retval_for(i)}
"""

    elif (l := _unwrap(node, "list")) is not None:
        async_sig += f"list[{l}]"
        sync_sig += f"list[{l}]"
        both_sig += f"list[{l}]"

        inner = f"""
    async def async_inner() -> list[{l}]:
        res = []
        async for row in await cursor.execute(sql, vals):
            desc = cursor.get_description()
            res.append({_retval_for(l)})
        return res

    def sync_inner() -> list[{l}]:
        res = []
        for row in cursor.execute(sql, vals):
            desc = cursor.get_description()
            res.append({l}(row[0]) if len(desc) == 1 else {l}(**dict(zip((d[0] for d in desc), row, strict=True))))
        return res
"""

    else:
        raise ValueError(f"Return not understood {ast.unparse(node)!r} ")

    res.append(f"""
@overload
def {meta["name"]}{async_sig}:
    ...
@overload
def {meta["name"]}{sync_sig}:
    ...
@bind_sql({"\n".join(_triple_quote(meta["sql"]))})
def {meta["name"]}{both_sig}:
{"\n".join(_triple_quote(comments, "    ")) if comments else "    # Add -- SQL comments after name: line for a docstring\n"}
    cursor: apsw.Cursor | apsw.AsyncCursor = executor.cursor() if isinstance(executor, apsw.Connection) else executor
    vals = ChainMapRO()
    sql = {meta["name"]}.sql
""")
    if meta["args"]:
        res.append("    vals.maps.append({")
        for name in meta["args"]:
            res.append(f'        "{name}": {name},')
        res.append("    })")
    if meta["locals"]:
        res.append("""    vals.maps.append(sys._getframe(1).f_locals) # type: ignore[attr-defined]  # noqa: SLF001""")
    if meta["is_template"]:
        res.append("    sql = template_expand(sql, vals)")
    res.append(inner)
    res.append("""    return async_inner() if cursor.connection.is_async else sync_inner()

""")

    return "\n".join(res)


class import_hook:
    """Use this to allow directly importing ``.sql`` files as Python modules

    An import hook will be installed, if not already installed.  It is
    ok to call this function multiple times.  The hook is appended to
    :data:`sys.meta_path` which means it only takes effect if there
    isn't already a matching :code:`.py` file.

    The resulting module is :class:`lazy loaded
    <importlib.util.LazyLoader>` meaning the SQL won't be read and Python
    generated until first access.

    **Advanced**:  You can instead use as a context manager
    (:code:`with`) in which case the hook is uninstalled on exiting the
    context.
    """

    def __init__(self):
        self._hook = _Import_Hook()

        for inst in sys.meta_path:
            if isinstance(inst, _Import_Hook) and not inst.context_owned:
                return
        sys.meta_path.append(self._hook)

    def __enter__(self):
        self._hook.context_owned = True
        if self._hook not in sys.meta_path:
            sys.meta_path.append(self._hook)
        return self

    def __exit__(self, exc_type, exc_value, exc_tb):
        try:
            sys.meta_path.remove(self._hook)
        except ValueError:
            pass
        return False


class _Import_Hook(importlib.abc.MetaPathFinder):
    "apsw.query hook allowing importing .sql files as corresponding Python code"

    def __init__(self):
        # if owned by import_hook as a context manger
        self.context_owned = False

    def find_spec(self, fullname: str, path: collections.abc.Sequence[str] | None, target: ModuleType | None = None):
        # ::TODO:: this currently only works with filesystem and needs
        # to be adjusted if there are zip files etc.  That involves
        # invoking the corresponding finders to check for the .sql
        # entry as well as saving the source to pass to
        # _SQLSourceLoader
        name = fullname.split(".")[-1]
        search_dirs = path if path else sys.path

        for entry in search_dirs:
            base_path = pathlib.Path(entry) / name
            sql_path = base_path.with_suffix(".sql")

            if sql_path.exists():
                spec = importlib.util.spec_from_loader(fullname, _SQLSourceLoader(sql_path), origin=str(sql_path))
                spec.loader = importlib.util.LazyLoader(spec.loader)
                return spec


class _SQLSourceLoader(importlib.abc.SourceLoader):
    # This only handles one file at a time which is why all the
    # parameters are ignored
    def __init__(self, path: pathlib.Path):
        self.path = path

    def get_filename(self, fullname: str):
        return f"<apsw.query source {str(self.path)!r}>.py"

    def get_source(self, fullname: str):
        return _make_py_from_text(self.path.read_text(encoding="utf8"))

    def get_data(self, path: str):
        return self.get_source(path).encode("utf8")


def _make_py_from_text(text: str) -> str:
    "Internal routine that converts SQL file to Python code"

    res: list[str] = []

    all_names: set[str] = set()

    unused_import = "# type: ignore[unused-import]  # pyright: ignore[reportUnusedImport]"

    res.extend(
        [
            """\
# This code was generated by apsw.query from SQL source
#
# Some of the lines end up too long - that is ok
#
# ruff: noqa: E501
# flake8: noqa: E501
# pylint: disable=line-too-long

""",
            f"""
# do not evaluate annotations at import time (default in Python 3.14+)
from __future__ import annotations

# some of the imports may not be used hence the noqa markings

import sys  {unused_import}
from typing import overload, Literal  {unused_import}
from collections.abc import  Awaitable, Iterator, AsyncIterator  {unused_import}

import apsw
from apsw.query import bind_sql, ChainMapRO, template_expand, changes, TooManyRows, RowExpected  {unused_import}

_NotSet = object()
"Sentinel for an unset value"

""",
        ]
    )

    for block_num, (lineno, block, value, comments, body) in enumerate(_sections(text)):
        match block:
            case "python":
                res.append(f"# {value}")
                res.extend(f"# {line}" for line in comments.splitlines())
                res.append("")
                try:
                    parsed = ast.parse(body)
                except BaseException as exc:
                    #  Another py 3.10 compat
                    getattr(exc, "add_note", lambda x: None)(f"""In Python section starting line {lineno}""")
                    raise
                # figure out if there is a docstring and make that
                # first in the top block so it precedes the from
                # __future__ stuff
                if (
                    block_num == 0
                    and parsed.body
                    and isinstance(parsed.body[0], ast.Expr)
                    and isinstance(parsed.body[0].value, ast.Constant)
                    and isinstance(parsed.body[0].value.value, str)
                ):
                    cutoff = parsed.body[0].end_lineno
                    res.insert(1, "\n".join(body.splitlines()[:cutoff]) + "\n")
                    body = "\n".join(body.splitlines()[cutoff:]) + "\n"
                res.append(body)
                res.append("\n")
            case "name":
                try:
                    meta = {}
                    meta = _parse_name(value)
                    if meta["name"] in all_names:
                        raise ValueError(f"Duplicate {meta['name']!r}")
                    all_names.add(meta["name"])
                    meta["is_template"] = _is_template_string(body)
                    meta["comments"] = comments if comments.strip() else None
                    meta["sql"] = body
                    res.append(_gen_function(meta))
                except BaseException as exc:
                    #  Another py 3.10 compat
                    getattr(exc, "add_note", lambda x: None)(
                        f"""In section starting line {lineno}""" + (f" name {meta['name']!r}" if meta else "")
                    )
                    raise

            case _:
                raise ValueError(f"Unknown section {block}")

    res.append("__all__ = (" + ", ".join(repr(n) for n in sorted(all_names)) + ", )")

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
    line_start = 0

    def part():
        nonlocal title, comments, body, line_start
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

            yield line_start, title[0], title[1], textwrap.dedent("\n".join(comments) + "\n"), "\n".join(body) + "\n"
        title = None
        comments = []
        body = []
        line_start = -1

    for lineno, line in enumerate(text.splitlines(), 1):
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
            line_start = lineno
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
    elif options.import_:
        import_hook()
        mod = importlib.import_module(options.import_)
        if not isinstance(getattr(mod, "__loader__", None), _SQLSourceLoader):
            sys.exit(f"{options.import_!r} was not imported from a SQL file")
        res = mod.__loader__.get_source(None)
        o = output()
        try:
            o.write(res)
        finally:
            o.close()
