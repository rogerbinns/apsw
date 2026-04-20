#!/usr/bin/env python3

from __future__ import annotations

import ast
import pathlib
import re
import textwrap
import collections.abc
from string import Formatter
from typing import Final, Any

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


class ChainMapRO:
    """Internal read-only chainmap for execute bindings

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
        raise KeyError(str(key))

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


class NotSetType:
    __slots__ = ()

    def __repr__(self) -> str:
        return "NotSet"


NotSet: Final = NotSetType()
"Used to indicate a parameter value was not supplied, so locals can be used instead"

# only need the parse method
_template_parse = Formatter().parse


def validate_template_string(string: str) -> bool:
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
            else:
                raise ValueError(f"Unknown specs {spec}")

    if bindings:
        vars.maps.insert(0, bindings)
    return "".join(res)


def _typed_results(node: ast.AST) -> str:
    # given a return annotation, provide the code

    # I originally tried to use match but was outsmarted
    if isinstance(node, ast.Constant) and node.value is None and node.kind is None:
        return """\
for _ in cursor.execute(sql, vals):
    pass
return None"""

    if isinstance(node, ast.Name) and node.id == "changes":
        return """\
changes_start = cursor.connection.total_changes()
for _ in cursor.execute(sql, vals):
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
retval = NotSet
for row in cursor.execute(sql, vals):
    if retval is not NotSet:
        raise TooManyRows
    desc = cursor.get_description()
    retval = {l}(row[0]) if len(desc) == 1 else {l}(**dict(zip((d[0] for d in desc), row)))
return {r} if retval is NotSet else retval
"""

    if isinstance(node, ast.Name):
        r = ast.unparse(node)
        return f"""\
retval = NotSet
for row in cursor.execute(sql, vals):
    if retval is not NotSet:
        raise TooManyRows
    desc = cursor.get_description()
    retval = {r}(row[0]) if len(desc) == 1 else {r}(**dict(zip((d[0] for d in desc), row)))
if retval is NotSet:
    raise RowExpected
return retval
    """

    if isinstance(node, ast.List) and len(node.elts) == 1 and isinstance(node.elts[0], ast.Name):
        r = ast.unparse(node.elts[0])
        return f"""\
for row in cursor.execute(sql, vals):
    desc = cursor.get_description()
    yield {r}(row[0]) if len(desc) == 1 else {r}(**dict(zip((d[0] for d in desc), row)))"""

    raise ValueError(f"Return not understood {ast.unparse(node)!r} ")


def py_from_file(filename: str | pathlib.Path) -> str:
    "Returns the Python code corresponding to the named file"
    return py_from_text(pathlib.Path(filename).read_text())


def py_from_text(text: str) -> str:
    "Returns the Python code corresponding to text containing queries"
    res: list[str] = []

    res.extend(
        """\

# This code was generated by apsw.query from ::TODO:: fill this in
# and also use $SOURCE_DATE_EPOCH for reproducible build

# do not evaluate annotations at import time (default in Python 3.14+)
from __future__ import annotations

import sys
import apsw
from apsw.query import ChainMapRO, NotSet, NotSetType, template_expand

""".splitlines()
    )

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
                    is_template = validate_template_string(body)
                except Exception as exc:
                    # Another py 3.10 compat
                    getattr(exc, "add_note", lambda x: None)(f"""In query '{meta["name"]}'""")
                    raise
                res.append(f"class {meta['name']}:")
                if comments.strip():
                    res.extend(_triple_quote(comments, "    "))
                    res.append("")

                res.append(f"    {'template' if is_template else 'sql'} = \\")
                res.extend(_triple_quote(body))

                # ::TODO:: if last arg is "locals", no type or default, then caller
                # locals are also used.  Think through better annotation.
                # only

                # ::TODO:: reject *args and handle **kwargs

                # this is going to need an overload or similar to handle async connection
                signature = "(cls, executor: apsw.Connection | apsw.Cursor"
                if meta["args"]:
                    # ::TODO:: only add * if locals is in use?
                    signature += ", *"
                    for name, details in meta["args"].items():
                        signature += f", {name}"
                        match (details["annotation"], details["default"]):
                            case (None, _):
                                signature += f" = {details['default']}"
                            case (_, None):
                                signature += f": ({details['annotation']}) | NotSetType = NotSet"
                            case (None, None):
                                signature += "apsw.SQLiteValue | NotSetType = NotSet"
                            case _:
                                signature += f"({details['annotation']}) = {details['default']}"
                signature += ") -> "
                signature += "apsw.Cursor" if meta["return_type"] is None else ast.unparse(meta["return_type"])
                res.append("    @classmethod")
                res.append(f"    def __call__{signature}:")
                res.append("        vals = ChainMapRO()")
                if meta["args"]:
                    res.append("        vals.maps.append({")
                    for name in meta["args"]:
                        res.append(f'            "{name}": {name},')
                    res.append("        })")
                res.extend(
                    """\
        vals.maps.append(sys._getframe(1).f_locals)
        if executor.is_async: # use copy as at call time
            vals.maps[1] = vals.maps[-1].copy()
""".splitlines()
                )
                res.append("        try:")
                if validate_template_string(body):
                    res.append("            sql = template_expand(cls.template, vals)")
                else:
                    res.append("            sql = cls.sql")

                if meta["return_type"] is None:
                    res.append("            return executor.execute(sql, vals)")
                else:
                    res.append(
                        "            cursor = executor.cursor() if isinstance(executor, apsw.Connection) else executor"
                    )
                    res.extend(("            " + line) for line in _typed_results(meta["return_type"]).splitlines())
                res.append("        except Exception as exc:")
                res.append("            # py 3.10 doesn't have add_note")
                res.append(
                    f"""            getattr(exc, 'add_note', lambda x: None)("In query named '{meta["name"]}'")"""
                )
                res.append("            raise")

                res.append("")
                res.append(f"{meta['name']} = {meta['name']}()")

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
    res = {}
    # we use ast to do all the work by pretending it is a function
    # definition.  It may not have any parameters listed, so add empty
    # ones if necessary. ::TODO:: decide if **locals should be default
    # params
    parse_as = f"def {text}: pass"
    try:
        ast.parse(parse_as)
    except SyntaxError as exc:
        # note offset/end_offset are 1 based not zero

        # -> without preceding ()
        if exc.text[exc.offset - 1 : exc.end_offset - 1] == "->" and exc.msg == "expected '('":
            parse_as = exc.text[: exc.offset - 1] + "()" + exc.text[exc.offset - 1 :]
        elif exc.text[exc.offset - 1 : exc.end_offset - 1] == ":" and exc.msg == "expected '('":
            parse_as = exc.text[: exc.offset - 1] + "()" + exc.text[exc.offset - 1 :]
        else:
            raise ValueError(f"Unable to parse {text!r}")

    parsed = ast.parse(parse_as)
    fn = parsed.body[0]
    res["name"] = fn.name
    res["args"] = {}
    for a, default in zip(fn.args.args, [None] * (len(fn.args.args) - len(fn.args.defaults)) + fn.args.defaults):
        assert a.arg not in res["args"]
        res["args"][a.arg] = {
            "annotation": ast.unparse(a.annotation) if a.annotation else None,
            "default": ast.unparse(default) if default else None,
        }

    res["return_type"] = fn.returns

    import pprint

    pprint.pprint(res)

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
