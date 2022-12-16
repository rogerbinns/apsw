#!/usr/bin/env/python3

import ast, re
from typing import List, Tuple, Any


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
std_typing = {"Union", "Callable", "Tuple", "Dict", "List", "Optional", "Any", "Sequence", "Mapping"}
std_other = {"None", "int", "float", "bytes", "str", "dict", "tuple", "bool"}

# from apsw
apsw_mod = {"zeroblob", "Cursor", "Connection"}


def sub(m: re.Match) -> str:
    text = m.group("name")
    if text in std_typing:
        return f"`{ text } <https://docs.python.org/3/library/typing.html#typing.{ text }>`__ "
    if text in std_other:
        return f"`{ text } <https://docs.python.org/3/library/stdtypes.html#{ text }>`__"
    return f" :class:`{ text }`"

def nomunge(pattern: str, replacement: Any, value: str) -> str:
    # re causes problems with Mapping so quick hack
    hack = "abc.Mapping" in value
    if hack:
        value = value.replace("abc.Mapping", "Xabc.XMapping")
    value = re.sub(pattern, replacement, value)
    if hack:
        value = value.replace("Xabc.XMapping", "abc.Mapping")
    return value

def output(doc: List[Tuple[str, str, str]]) -> str:
    in_doc: set[str] = set()
    # build a mapping of known names
    for name, _, _ in doc:
        in_doc.add(name)
    in_doc.update(apsw_mod)
    pattern = r"\b(?P<name>" + "|".join(std_other.union(std_typing.union(in_doc))) + ")\\b"

    res = ""
    for name, value, descr in doc:
        value = nomunge(pattern, sub, value)
        descr = nomunge(pattern, sub, descr)
        res += f"""
.. class:: { name }

{ valuefmt(value, indent="    | ") }

{ valuefmt(descr, indent="    ") }

"""
    return res


def valuefmt(value: str, indent: str) -> str:
    return indent + f"\n{ indent }".join(value.split("\n"))


if __name__ == '__main__':
    source = open("src/apswtypes.py").read()
    parsed = ast.parse(source, type_comments=True)
    assert isinstance(parsed, ast.Module)
    with open("doc/typing.rstgen", "wt") as f:
        ours = process(parsed, source)
        print(output(ours), file=f)
