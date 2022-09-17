#!/usr/bin/env/python3

import ast, re
from typing import List, Tuple


def process(module: ast.Module, source: str) -> List[Tuple[str, str, str]]:
    res = []
    body = module.body
    i = 0
    while i < len(body):
        if not isinstance(body[i], ast.Assign):
            i += 1
            continue
        name = ast.unparse(body[i].targets)
        value = ast.get_source_segment(source, body[i].value)
        i += 1
        assert isinstance(body[i], ast.Expr) and isinstance(
            body[i].value, ast.Constant), f"Expecting constant at line { body[i].lineno }"
        descr = body[i].value.value
        assert isinstance(descr, str)
        i += 1
        res.append((name, value, descr))
    return res


# stuff defined in standard library
std_typing = {"Union", "Callable", "Tuple", "Dict", "List", "Optional", "Any", "Sequence"}
std_other = {"None", "int", "float", "bytes", "str", "dict", "tuple"}

# from apsw
apswmod = {"zeroblob", "Cursor", "Connection"}


def sub(m: re.Match) -> str:
    text = m.group(0)
    if text in std_typing:
        return f"`{ text } <https://docs.python.org/3/library/typing.html#typing.{ text }>`__ "
    if text in std_other:
        return f"`{ text } <https://docs.python.org/3/library/stdtypes.html#{ text }>`__"
    return f":class:`{ text }`"


def output(doc: List[Tuple[str, str, str]]) -> str:
    indoc: set[str] = set()
    # build a mapping of known names
    for name, _, _ in doc:
        indoc.add(name)
    indoc.update(apswmod)
    pattern = "\\b(" + "|".join(std_other.union(std_typing.union(indoc))) + ")\\b"

    res = ""
    for name, value, descr in doc:
        value = re.sub(pattern, sub, value)
        descr = re.sub(pattern, sub, descr)
        res += f"""
.. class:: { name }

{ valuefmt(value, indent="    | ") }

{ valuefmt(descr, indent="    ") }

"""
    return res


def valuefmt(value: str, indent: str) -> str:
    return indent + f"\n{ indent }".join(value.split("\n"))


if __name__ == '__main__':
    source = open("src/types.py").read()
    parsed = ast.parse(source, type_comments=True)
    assert isinstance(parsed, ast.Module)
    with open("doc/typing.rstgen", "wt") as f:
        ours = process(parsed, source)
        print(output(ours), file=f)
