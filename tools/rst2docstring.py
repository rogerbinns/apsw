#!/usr/bin/env python3
"""
This implements functionality similar to Argument Clinic.

* Docstrings are available as C symbols, and then used
* The syntax for __text_signature is used to give a partial type
  signature
* Argument parsing is automated

Argument parsing is done differently.  Generated code is placed in the
original source so normal tools can see it, but argument clinic then
uses checksums to detect modifications.  We use the simpler approach
of replacing the generated section if it differs, so git will tell if
it was modified.
"""

import sys
import os
import io
import textwrap
import glob

from typing import Union, List

# symbols to skip because we can't apply docstrings
skip = {
    "apsw.compile_options",
    "apsw.connection_hooks",
    "apsw.keywords",
    "apsw.main",
    "apsw.using_amalgamation",
}

skipseen = set()


def process_file(name: str) -> list:
    "Read one rst file and extract docstrings"
    items = []
    current: List[str] = []

    def do_current():
        nonlocal current
        if current:
            while not current[-1].strip():
                current.pop()
            item = classify(current)
            if item:
                items.append(item)
        current = []

    for line in open(name):
        if line.startswith(".. "):
            do_current()
            kind = line.split()[1]
            if kind.endswith("::"):
                current.append(line)
                continue
        if current:
            current.append(line)

    do_current()
    return items


def classify(doc: list) -> Union[dict, None]:
    "Process docstring and ignore or update details"
    line = doc[0]
    assert line.startswith(".. ")
    kind = line.split()[1]
    assert kind.endswith("::")
    kind = kind.rstrip(":")
    if kind in {"index", "currentmodule", "code-block", "note", "seealso", "module", "data"}:
        return None

    assert kind in ("class", "method", "attribute"), f"unknown kind { kind } in { line }"
    rest = line.split("::", 1)[1].strip()
    if "(" in rest:
        name, signature = rest.split("(", 1)
        signature = "(" + signature
    else:
        name, signature = rest, ""
    name = name.strip()
    signature = signature.strip()

    if kind == "class":
        name += ".__init__"
    elif "." not in name:
        name = "apsw." + name

    doc = doc[1:]
    while doc and not doc[0].strip():
        doc = doc[1:]

    if not doc:
        return None
    if name in skip:
        skipseen.add(name)
        return None
    # These aren't real classes
    if name.split(".")[0] in {"VTCursor", "VTModule", "VTTable"}:
        return None

    doc = [f"{ line }\n" for line in textwrap.dedent("".join(doc)).strip().split("\n")]

    symbol = make_symbol(name)
    return {
        "name": name,
        "symbol": symbol,
        "signature_original": signature,
        "signature": analyze_signature(signature) if signature else [],
        "doc": doc
    }


def make_symbol(n: str) -> str:
    "Returns C symbol name"
    n = n[0].upper() + n[1:]
    n = n.replace(".", "_").replace("__", "_").replace("__", "_")
    return n.rstrip("_")


def cppsafe(lines: List[str], eol: str) -> str:
    def backslash(l):
        return l.replace('"', '\\"').replace("\n", "\\n")

    res = "\n".join(f'''"{ backslash(line) }"{ eol }''' for line in lines)
    res = res.strip().strip("\\").strip()
    return res


def fixup(item: dict, eol: str) -> str:
    "Return docstring lines after making safe for C"
    lines = item["doc"]
    if item["signature"]:
        # cpython can't handle the arg or return type info
        sig = simple_signature(item["signature"])
        func = item["name"].split(".")[1]
        lines = [f'''{ func }{ sig }\n--\n\n{ item["name"] }{ item["signature_original"] }\n\n'''] + lines

    return cppsafe(lines, eol)


def simple_signature(signature: List[dict]) -> str:
    "Return signature simple enough to be accepted for __text_signature__"
    res = ["$self"]
    for param in signature:
        if param["name"] == "return":
            continue
        res.append(param["name"])
    res.append("/")
    return "(" + ",".join(res) + ")"


def analyze_signature(s: str) -> List[dict]:
    "parse signature returning info about each item"
    res = []
    if "->" in s:
        s, rettype = [ss.strip() for ss in s.split("->", 1)]
    else:
        rettype = "None"
    res.append({"name": "return", "type": rettype})

    assert s[0] == "(" and s[-1] == ")"

    # we want to split on commas, but a param could be:  Union[Dict[A,B],X]
    nest_start = "[("
    nest_end = "])"

    pos = 1
    nesting = 0
    name = ""
    after_name = ""
    skip_to_next = False

    def add_param():
        nonlocal name, after_name
        p = {"name": name}
        after_name = [a.strip() for a in after_name.strip().lstrip(":").split("=", 1)]
        if len(after_name) > 1:
            after_name, default = after_name
        else:
            after_name, default = after_name[0], None
        p["type"] = after_name
        p["default"] = default
        res.append(p)
        name = after_name = ""

    for pos in range(1, len(s) - 1):
        c = s[pos]
        if c in nest_start or nesting:
            after_name += c
            if c in nest_start:
                nesting += 1
                continue
            if c not in nest_end:
                continue
            nesting -= 1
            continue

        if c == ',':
            assert name
            add_param()
            skip_to_next = False
            continue

        if skip_to_next:
            after_name += c
            continue

        if name and not (name + c).isidentifier() and not (name[0] == "*" and (name[1:] + c).isidentifier()):
            skip_to_next = True
            after_name = ""
            continue

        if c.isidentifier() or (not name and c in "/*"):
            name += c

    if name:
        add_param()

    return res


def check_and_update(symbol: str, code: str):
    for fn in glob.glob("src/*.c"):
        orig = open(fn, "r").read()
        if symbol not in orig:
            continue
        return check_and_update_file(fn, symbol, code)
    else:
        raise ValueError(f"Failed to find code with { symbol }")


def check_and_update_file(filename: str, symbol: str, code: str):
    lines = open(filename, "r").read().split("\n")
    insection = False
    for lineno, line in enumerate(lines):
        if line.strip() == "{":
            lineopen = lineno
        elif line.strip() == "}" and insection:
            lineclose = lineno
            break
        elif symbol in line:
            insection = True
    else:
        raise ValueError(f"{ symbol } not found in { filename }")

    lines = lines[:lineopen] + code.split("\n") + lines[lineclose + 1:]

    new = "\n".join(lines)
    if not new.endswith("\n"):
        new += "\n"
    replace_if_different(filename, new)


def replace_if_different(filename: str, contents: str) -> None:
    if not os.path.exists(filename) or open(filename).read() != contents:
        print(f"{ 'Creating' if not os.path.exists(filename) else 'Updating' } { filename }")
        open(filename, "w").write(contents)


# only "int" shows up in signature (python type) but could correspond to
# different c types such as 64 bit int
int_overrides = {"apsw.softheaplimit": {"limit": "int64"}}


def do_argparse(item):
    for param in item["signature"]:
        if not param["type"].strip():
            sys.exit(f"{ item['name'] } param { param } has no type from { item['signature_original'] }")
    res = [f"#define { item['symbol'] }_CHECK do {{ \\"]

    fstr = ""
    optional = False
    argnames = []

    for param in item["signature"]:
        if param["name"] == "return":
            continue
        if item["name"] in int_overrides and param["name"] in int_overrides[item["name"]]:
            assert param["type"] == "int"
            param["type"] = int_overrides[item["name"]][param["name"]]
        if param["type"] == "str":
            type = "const char *"
            kind = "s"
        elif param["type"] == "bool":
            type = "int"
            kind = "b"
        elif param["type"] == "int":
            type = "int"
            kind = "i"
        elif param["type"] == "int64":
            type = "long long"
            kind = "L"
        else:
            assert False, f"Don't know how to handle type for { item ['name'] } param { param }"

        res.append(f"  assert(__builtin_types_compatible_p(typeof({ param['name'] }), { type })); \\")

        if not optional and param["default"]:
            fstr += "|"
            optional = True

        fstr += kind
        argnames.append(param["name"])

    res.append("} while(0)\n")

    code = f"""\
  {{
    static char *kwlist[] = {{{ ", ".join('"' + a + '"' for a in argnames) }, NULL}};
    { item['symbol'] }_CHECK;
    if (!PyArg_ParseTupleAndKeywords(args, kwds, "{ fstr }:" { item['symbol'] }_USAGE, kwlist, { ", ".join("&" + a for a in argnames) }))
      return NULL;
  }}"""

    usage = f"{ item['name'] }{ item['signature_original'] }".replace('"', '\\"')
    res.insert(0, f"""#define { item['symbol'] }_USAGE "{ usage }"\n""")

    check_and_update(f"{ item['symbol'] }_CHECK", code)

    res = "\n".join(res) + "\n"
    return res


if __name__ == '__main__':
    items = []
    for fname in sys.argv[2:]:
        items.extend(process_file(fname))

    allcode = "\n".join(open(fn).read() for fn in glob.glob("src/*.c"))

    missing = []

    out = io.StringIO()
    print("""/* This file is generated by rst2docstring */

#ifndef __GNUC__
#define __builtin_types_compatible_p(x,y) (1)
#endif
""",
          file=out)
    method, mid, eol, end = "#define ", " ", " \\", ""
    for item in sorted(items, key=lambda x: x["symbol"]):
        print(f"""{ method } { item["symbol"] }_DOC{ mid }{ fixup( item, eol) } { end }\n""", file=out)
        if f"{ item['symbol'] }_CHECK" in allcode:
            print(do_argparse(item), file=out)
        else:
            if any(param["name"] != "return"
                   for param in item["signature"]) and not any(param["name"].startswith("*")
                                                               for param in item["signature"]):
                if item["name"] not in {"apsw.format_sql_value"}:
                    missing.append(item["name"])

    outval = out.getvalue()
    replace_if_different(sys.argv[1], outval)

    symbols = sorted([item["symbol"] for item in items])

    if skip != skipseen:
        print("in skip, but not seen\n")
        for s in skip:
            if s not in skipseen:
                print("  ", s)
        print()

    if any(s not in allcode for s in symbols):
        print("Unreferenced doc\n")
        for s in symbols:
            if s not in code:
                print("  ", s)
        sys.exit(2)

    if missing:
        print("Not argparse checked", missing)