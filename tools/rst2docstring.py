#!/usr/bin/env python3
"""
This implements functionality similar to Argument Clinic.

* Docstrings are available as C symbols, and then used
* The syntax for __text_signature is used to give a partial type
  signature
* Argument parsing is automated
* Type stubs are generated

Argument parsing is done differently.  Generated code is placed in the
original source so normal tools can see it, but argument clinic then
uses checksums to detect modifications.  We use the simpler approach
of replacing the generated section if it differs, so git will tell if
it was modified.

Beware that this code evolved and was not intelligently designed
"""

import sys
import os
import io
import textwrap
import glob
import inspect

from typing import Union, List

# symbols to skip because we can't apply docstrings (PyModule_AddObject doesn't take docstring)
docstrings_skip = {
    "apsw.compile_options",
    "apsw.connection_hooks",
    "apsw.keywords",
    "apsw.main",
    "apsw.using_amalgamation",
    "apsw.SQLITE_VERSION_NUMBER",
}


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
    if kind in {"index", "currentmodule", "code-block", "note", "seealso", "module", "literalinclude"}:
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

    if name == "main":  # from shell, so get its spec
        import apsw
        import warnings
        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            # these inspect methods are deprecated, but the signature based ones
            # mess up formatting - eg saying '<class TextIO>' instead of 'TextIO'
            signature = inspect.formatargspec(*inspect.getfullargspec(apsw.main))

    if kind == "class":
        name += ".__init__"
        if not signature:
            # this happens for the classes that can't be directly instantiated
            signature = "() -> None"
    elif "." not in name:
        name = "apsw." + name

    doc = doc[1:]
    while doc and not doc[0].strip():
        doc = doc[1:]

    if not doc:
        return None
    # These aren't real classes
    if name.split(".")[0] in {"VTCursor", "VTModule", "VTTable"}:
        return None

    doc = [f"{ line }\n" for line in textwrap.dedent("".join(doc)).strip().split("\n")]

    symbol = make_symbol(name)
    return {
        "kind": kind,
        "name": name,
        "symbol": symbol,
        "signature_original": signature,
        "signature": analyze_signature(signature) if signature else [],
        "doc": doc,
        "skip_docstring": name in docstrings_skip
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
        p = param["name"]
        if param["default"]:
            p += (f"={ param['default'] }")
        res.append(p)
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


# Python 'int' can be different C sizes (int32, int64 etc) so we override with more
# specific types here
type_overrides = {
    "apsw.softheaplimit": {
        "limit": "int64"
    },
    "Blob.readinto": {
        "buffer": "PyObject",
        "offset": "int64",
        "length": "int64"
    },
    "Blob.reopen": {
        "rowid": "int64"
    },
    "Connection.blobopen": {
        "rowid": "int64"
    },
    "Connection.filecontrol": {
        "pointer": "pointer"
    },
    "Connection.set_last_insert_rowid": {
        "rowid": "int64"
    },
    "Cursor.execute": {
        "statements": "strtype"
    },
    "Cursor.executemany": {
        "statements": "strtype",
        "sequenceofbindings": "Sequence"
    },
    "URIFilename.uri_int": {
        "default": "int64",
    },
    "VFSFile.__init__": {
        "filename": "PyObject",
        "flags": "List[int,int]"
    },
    "VFSFile.xFileControl": {
        "ptr": "pointer"
    },
    "VFSFile.xRead": {
        "offset": "int64"
    },
    "VFSFile.xTruncate": {
        "newsize": "int64"
    },
    "VFSFile.xWrite": {
        "offset": "int64"
    },
    "VFS.xDlClose": {
        "handle": "pointer"
    },
    "VFS.xDlSym": {
        "handle": "pointer"
    },
    "VFS.xSetSystemCall": {
        "pointer": "pointer"
    },
    "VFS.xOpen": {
        "flags": "List[int,int]"
    }
}


def callable_erasure(f, token="Callable"):
    "Removes nested square brackets after token"
    if token not in f:
        return f
    res = ""
    rest = f
    while token in rest:
        idx = rest.index(token)
        res += rest[:idx + len(token)]
        rest = rest[idx + len(token):]

        c = rest[0]
        if c == ']':  # no type to erase
            continue
        assert c == '[', f"expected [ at '{ rest }' processing '{ f }'"
        nesting = 1
        rest = rest[1:]
        while nesting:
            c = rest[0]
            rest = rest[1:]
            if c == '[':
                nesting += 1
            elif c == ']':
                nesting -= 1
                if not nesting:
                    break

    res += rest
    return res


def do_argparse(item):
    for param in item["signature"]:
        try:
            param["type"] = type_overrides[item['name']][param["name"]]
        except KeyError:
            pass
        if not param["type"]:
            sys.exit(f"{ item['name'] } param { param } has no type from { item['signature_original'] }")
    res = [f"#define { item['symbol'] }_CHECK do {{ \\"]

    fstr = ""
    optional = False
    # names of python level keywords
    kwlist = []
    # what is passed at C level
    parse_args = []

    for param in item["signature"]:
        if param["name"] == "return":
            continue
        pname = param["name"]
        if pname in {"default"}:
            pname += "_"
        args = ["&" + pname]
        default_check = None
        if param["type"] == "str":
            type = "const char *"
            kind = "s"
            if param["default"]:
                if param["default"] == "None":
                    default_check = f"{ pname } == 0"
                else:
                    breakpoint()
                    pass
        elif param["type"] == "Optional[str]":
            type = "const char *"
            kind = "z"
            if param["default"]:
                if param["default"] == "None":
                    default_check = f"{ pname } == 0"
                else:
                    breakpoint()
                    pass
        elif param["type"] == "bool":
            type = "int"
            kind = "O&"
            args = ["argcheck_bool"] + args
            if param["default"]:
                assert param["default"] in {"True", "False"}
                dval = int(param["default"] == "True")
                default_check = f"{ pname } == { dval }"
        elif param["type"] == "int":
            type = "int"
            kind = "i"
            if param["default"]:
                try:
                    val = int(param['default'])
                except ValueError:
                    val = param['default'].replace("apsw.", "")
                default_check = f"{ pname } == ({ val })"
        elif param["type"] == "int64":
            type = "long long"
            kind = "L"
            if param["default"]:
                default_check = f"{ pname } == { int(param['default']) }L"
        elif param["type"] == "pointer":
            type = "void *"
            kind = "O&"
            args = ["argcheck_pointer"] + args
            if param["default"]:
                breakpoint()
                pass
        elif param["type"] in {"PyObject", "Any"}:
            type = "PyObject *"
            kind = "O"
            if param["default"]:
                breakpoint()
                pass
        elif param["type"] == "bytes":
            type = "Py_buffer"
            kind = "y*"
            if param["default"]:
                breakpoint()
                pass
        elif callable_erasure(param["type"]) in {
                "Optional[Callable]", "Optional[RowTracer]", "Optional[ExecTracer]", "Optional[ScalarProtocol]",
                "Optional[AggregateFactory]"
        }:
            # the above are all callables and we don't check beyond that
            type = "PyObject *"
            kind = "O&"
            args = ["argcheck_Optional_Callable"] + args
            if param["default"]:
                breakpoint()
                pass
        elif param["type"] == "Optional[Union[str,URIFilename]]":
            type = "PyObject *"
            kind = "O&"
            args = ["argcheck_Optional_str_URIFilename"] + args
            if param["default"]:
                breakpoint()
                pass
        elif param["type"] == "Optional[Bindings]":
            type = "PyObject *"
            kind = "O&"
            args = ["argcheck_Optional_Bindings"] + args
            if param["default"]:
                if param["default"] == "None":
                    default_check = f"{ pname } == NULL"
                else:
                    breakpoint()
                pass
        elif callable_erasure(param["type"]) == "Callable":
            type = "PyObject *"
            kind = "O&"
            args = ["argcheck_Callable"] + args
            if param["default"]:
                breakpoint()
                pass
        elif param["type"] == "Sequence":
            # note that we can't check for sequence because anything
            # that PySequence_Fast accepts is ok which includes sets,
            # iterators, generators etc and I can't test for all of
            # them
            type = "PyObject *"
            kind = "O"
            if param["default"]:
                breakpoint()
                pass
        elif param["type"] == "Connection":
            type = "Connection *"
            kind = "O!"
            args = ["&ConnectionType"] + args
            if param["default"]:
                breakpoint()
                pass
        elif param["type"] == "strtype":
            type = "PyObject *"
            kind = "O!"
            args = ["&PyUnicode_Type"] + args
            if param["default"]:
                breakpoint()
                pass
        elif param["type"] == "List[int,int]":
            type = "PyObject *"
            kind = "O&"
            args = ["argcheck_List_int_int"] + args
            if param["default"]:
                breakpoint()
                pass
        else:
            assert False, f"Don't know how to handle type for { item ['name'] } param { param }"

        kwlist.append(pname)
        res.append(f"  assert(__builtin_types_compatible_p(typeof({ pname }), { type })); \\")
        if default_check:
            res.append(f"  assert({ default_check }); \\")

        if not optional and param["default"]:
            fstr += "|"
            optional = True

        fstr += kind
        parse_args.extend(args)

    res.append("} while(0)\n")

    code = f"""\
  {{
    static char *kwlist[] = {{{ ", ".join(f'"{ a }"' for a in kwlist) }, NULL}};
    { item['symbol'] }_CHECK;
    if (!PyArg_ParseTupleAndKeywords(args, kwds, "{ fstr }:" { item['symbol'] }_USAGE, kwlist, { ", ".join(parse_args) }))
      return { "NULL" if not item['symbol'].endswith("_init") else -1 };
  }}"""

    usage = f"{ item['name'] }{ item['signature_original'] }".replace('"', '\\"')
    res.insert(0, f"""#define { item['symbol'] }_USAGE "{ usage }"\n""")

    check_and_update(f"{ item['symbol'] }_CHECK", code)

    res = "\n".join(res) + "\n"
    return res


def is_sequence(s):
    return isinstance(s, (list, tuple))


def get_class_signature(klass: str, items: List[dict]) -> str:
    for item in items:
        if item["kind"] == "class" and item["name"] == f"{ klass }.__init__":
            sig = item["signature_original"]
            if not sig:
                return "(self)"
            assert sig[0] == "("
            if sig != "()":
                return "(self, " + sig[1:]
            return "(self)"
    raise KeyError(f"class { klass } not found")


def generate_typestubs(items):
    try:
        import apsw
    except ImportError:
        print("Skipping type stub updates because can't import apsw")
        return

    out = io.StringIO()
    print("""# This file is generated by rst2docstring""", file=out)
    with open("src/types.py", "rt") as f:
        print(f.read(), file=out)

    lastclass = ""

    for item in sorted(items, key=lambda x: x["symbol"]):
        if item["kind"] == "class":
            # these end up in an unhelpful place in the sort order
            continue
        klass, name = item['name'].split(".", 1)
        signature = item["signature_original"]
        if klass == "apsw":
            name = item["name"][len("apsw."):]
            if item["kind"] == "method":
                assert signature.startswith("(")
                print(f"def { name }{ signature }: ...", file=out)
            else:
                assert item["kind"] == "attribute"
                print(f"{ name }: { attribute_type(item) }", file=out)
        else:
            if klass != lastclass:
                lastclass = klass
                klass_signature = get_class_signature(klass, items)
                print(f"\nclass { klass }:", file=out)
                print(f"    def __init__{ klass_signature }: ...", file=out)

            if item["kind"] == "method":
                for find, replace in (
                    ("apsw.", ""),  # some constants
                    ("List[int,int]", "List[int]"),  # can't see how to type a 2 item list
                ):
                    signature = signature.replace(find, replace)
                if not signature.startswith("(self"):
                    signature = "(self" + (", " if signature[1] != ")" else "") + signature[1:]
                print(f"    def { name }{ signature }: ...", file=out)
            else:
                assert item["kind"] == "attribute"
                print(f"    { name }: { attribute_type(item) }", file=out)

    # constants
    print("\n", file=out)
    for n in dir(apsw):
        if not n.startswith("SQLITE_") or n == "SQLITE_VERSION_NUMBER":
            continue
        assert isinstance(getattr(apsw, n), int)
        print(f"{ n }: int", file=out)

    # mappings
    print("\n", file=out)
    for n in dir(apsw):
        if not n.startswith("mapping_"):
            continue
        print(f"{ n }: Dict[Union[str,int],Union[int,str]]", file=out)

    # exceptions
    print("\n", file=out)
    print("class Error(Exception): ...", file=out)
    for n in dir(apsw):
        if n != "ExecTraceAbort" and (not n.endswith("Error") or n == "Error"):
            continue
        print(f"class { n }(Error): ...", file=out)

    replace_if_different("apsw/__init__.pyi", out.getvalue())


def attribute_type(item: dict) -> str:
    # docstring will start with :type: type
    doc = "\n".join(item["doc"]).strip().split("\n")[0]
    assert doc.startswith(":type:"), f"Expected :type: for doc in { item }"
    return doc[len(":type:"):].strip()


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
        if item["skip_docstring"]:
            continue
        print(f"""{ method } { item["symbol"] }_DOC{ mid }{ fixup( item, eol) } { end }\n""", file=out)
        if f"{ item['symbol'] }_CHECK" in allcode:
            print(do_argparse(item), file=out)
        else:
            if any(param["name"] != "return"
                   for param in item["signature"]) and not any(param["name"].startswith("*")
                                                               for param in item["signature"]):
                if item["name"] not in {
                        "apsw.format_sql_value",
                        "VFSFile.excepthook",
                        "Cursor.__next__",
                        "Cursor.__iter__",
                        "VFS.excepthook",
                }:
                    missing.append(item["name"])

    outval = out.getvalue()
    replace_if_different(sys.argv[1], outval)

    symbols = sorted([item["symbol"] for item in items if not item["skip_docstring"]])

    if any(s not in allcode for s in symbols):
        print("Unreferenced doc\n")
        for s in symbols:
            if s not in allcode:
                print("  ", s)
        sys.exit(2)

    if missing:
        print("Not argparse checked", missing)

    generate_typestubs(items)