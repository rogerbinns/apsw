#!/usr/bin/env/python3

import ast, re
from typing import Any


def process(module: ast.Module, source: str) -> list[tuple[str, str, str]]:
    res = []
    body = module.body
    i = 0

    while i < len(body):
        if isinstance(body[i], ast.If):
            if not isinstance(body[i].body[0], ast.ClassDef):
                i += 1
                continue
            klass = body[i].body[0]
            code = ast.unparse(klass).split("\n")
            code[0] = code[0].replace("class ", "").replace("(", "\\(")
            res.append((klass.name, "\n".join(code), f"{ klass.name } Protocol"))
            i += 1
            continue
        if not isinstance(body[i], ast.Assign):
            i += 1
            continue
        name = ast.unparse(body[i].targets)
        value = ast.get_source_segment(source, body[i].value)
        i += 1
        assert isinstance(body[i], ast.Expr) and isinstance(
            body[i].value, ast.Constant
        ), f"Expecting constant at line { body[i].lineno }"
        descr = body[i].value.value
        assert isinstance(descr, str)
        i += 1
        res.append((name, value, descr))
    return res


# stuff defined in standard library
std_typing = {
    "Union",
    "Callable",
    "Tuple",
    "Dict",
    "List",
    "Optional",
    "Any",
    "Sequence",
    "Iterable",
    "Mapping",
    "Protocol",
}
std_other = {"None", "int", "float", "bytes", "str", "dict", "tuple", "bool", "list"}

# from apsw
apsw_mod = {"zeroblob", "Cursor", "Connection", "FTS5ExtensionApi"}


def sub(m: re.Match) -> str:
    # we have to add a backslash quoted zero width space on the end, otherwise docutils
    # sees our replacement merging with the next token and claiming an error.  If a regular
    # space is used then the output has weird spaces everywhere
    sp = "\\\u200b"
    text: str = m.group("name")
    if text in std_typing:
        return f":class:`~typing.{ text }`{sp}"
    if text in std_other:
        if text in {"int", "bool", "float"}:
            return f":class:`{ text }`{sp}"
        if text == "None":
            return f":class:`{ text }`{sp}"
        return f":class:`{ text }`{sp}"
    return f":class:`{ text }`{sp}"


def nomunge(pattern: str, replacement: Any, value: str) -> str:
    # re causes problems with Mapping so quick hack
    hack = "abc.Mapping" in value
    if hack:
        value = value.replace("abc.Mapping", "Xabc.XMapping")
    value = re.sub(pattern, replacement, value)
    if hack:
        value = value.replace("Xabc.XMapping", "abc.Mapping")
    return value


def output(doc: list[tuple[str, str, str]]) -> str:
    in_doc: set[str] = set()
    # build a mapping of known names
    for name, _, _ in doc:
        in_doc.add(name)
    in_doc.update(apsw_mod)
    pattern = r"\b(?P<name>" + "|".join(std_other.union(std_typing.union(in_doc))) + r")\b"
    res = ""
    for name, value, descr in doc:
        value = nomunge(pattern, sub, value)
        # I can't find a way of making *:class:`foo` work - the *
        # makes the :class: not be understood, even with a zero width
        # space.  So force a real space
        value = value.replace("*", "* ")
        descr = nomunge(pattern, sub, descr)
        # easiest to fix in post ...
        descr = descr.replace(
            ":meth:`:class:`FTS5ExtensionApi`\​.query_phrase`", ":meth:`FTS5ExtensionApi.query_phrase`"
        )
        res += f"""
.. class:: { name }

{ valuefmt(value, indent="    | ") }

{ valuefmt(descr, indent="    ") }

"""
    return res


def valuefmt(value: str, indent: str) -> str:
    return indent + f"\n{ indent }".join(value.split("\n"))


if __name__ == "__main__":
    source = open("src/apswtypes.py").read()
    parsed = ast.parse(source, type_comments=True)
    assert isinstance(parsed, ast.Module)
    with open("doc/typing.rstgen", "wt") as f:
        ours = process(parsed, source)
        print(output(ours), file=f)
