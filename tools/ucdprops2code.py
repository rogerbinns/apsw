#!/usr/bin/env python3

# Generates code from unicode properties db


import sys
import itertools
import pathlib
import re

from typing import Any

try:
    batched = itertools.batched
except AttributeError:
    # Copied from https://docs.python.org/3/library/itertools.html#itertools.batched
    def batched(iterable, n):
        # batched('ABCDEFG', 3) --> ABC DEF G
        if n < 1:
            raise ValueError("n must be at least one")
        it = iter(iterable)
        while batch := tuple(itertools.islice(it, n)):
            yield batch


def is_one_value(vals):
    return len(vals) == 1 and isinstance(vals[0], int)


def is_one_range(vals):
    return len(vals) == 1 and not isinstance(vals[0], int)


def all_vals(vals):
    for row in vals:
        if isinstance(row, int):
            yield row
        else:
            yield from range(row[0], row[1])


def fmt(v: int) -> str:
    # format values the same as in the text source for easy grepping
    return "0x%04X" % v


# We do Python code for testing and development
def generate_python() -> str:
    out: list[str] = []
    out.append(f'unicode_version = "{ ucd_version }"')
    out.append("")

    for top in props:
        names = []
        for name, vals in sorted(props[top].items()):
            names.append(name)
            if is_one_value(vals):
                out.append(f"def is_{top}_{name}(c: int) -> bool:")
                out.append(f"   return c == { fmt(vals[0])}")
            elif is_one_range(vals):
                out.append(f"def is_{top}_{name}(c: int) -> bool:")
                out.append(f"   return { fmt(vals[0][0]) } <= c <= { fmt(vals[0][1]) }")
            else:
                vals = list(all_vals(vals))
                out.append(f"# { len(vals):,} codepoints")
                out.append(f"_{ top }_{ name }_members = {{")
                for row in batched(vals, 10):
                    out.append("    " + ", ".join(fmt(v) for v in row) + ",")
                out.append("}")
                out.append("")
                out.append(f"def is_{top}_{name}(c: int) -> bool:")
                out.append(f"   return c in _{ top }_{ name }_members")
            out.append("")
            out.append("")
        out.append(f"def all_{ top }_flags(c: int) -> tuple[str, ...]:")
        out.append("    return tuple(name for name, is_set in (")
        for name in names:
            out.append("        " + f'("{ name }", is_{ top }_{ name }(c)),')
        out.append("    ) if is_set)")
        out.append("")
        out.append("")

    return "\n".join(out) + "\n"


props = {
    "grapheme": {},
    "word": {},
    "sentence": {},
}

ucd_version = None


def extract_version(filename: str, source: str):
    global ucd_version
    if filename == "emoji-data.txt":
        for line in source.splitlines():
            if line.startswith("# Used with Emoji Version "):
                mo = re.match(r".*Version (?P<version>[^\s]+)\s.*", line)
                break
        else:
            raise ValueError("No matching version line found")
    else:
        mo = re.match(r"# [^-]+-(?P<version>.*)\.txt", source.splitlines()[0])
    # we only care about major.minor
    version = ".".join(mo.group("version").split(".")[:2])
    if ucd_version is None:
        ucd_version = version
    elif ucd_version != version:
        sys.exit(f"Already saw {ucd_version=} but {filename=} is {version=}")


def parse_source_lines(source: str):
    for line in source.splitlines():
        if not line.strip() or line.startswith("#"):
            continue
        line = line[: line.index("#")]
        vals, prop = line.split(";")
        prop = prop.strip()
        vals = vals.strip().split("..")
        if len(vals) == 1:
            yield int(vals[0], 16), None, prop
        else:
            yield int(vals[0], 16), int(vals[1], 16), prop


def populate(source: str, dest: dict[str, Any]):
    for start, end, prop in parse_source_lines(source):
        try:
            accumulate = dest[prop]
        except KeyError:
            accumulate = dest[prop] = []

        if end is None:
            accumulate.append(start)
        else:
            accumulate.append((start, end))


def extract_prop(source: str, dest: dict[str, Any], name: str):
    assert name not in dest
    accumulate = dest[name] = []

    for start, end, prop in parse_source_lines(source):
        if prop == name:
            if end is None:
                accumulate.append(start)
            else:
                accumulate.append((start, end))

    assert len(accumulate) > 0


def read_props(data_dir: str):
    if data_dir:
        url = pathlib.Path(data_dir) / "emoji-data.txt"
    else:
        url = "https://www.unicode.org/Public/UCD/latest/ucd/emoji/emoji-data.txt"

    print("Reading", url)
    if isinstance(url, str):
        source = urllib.request.urlopen(url).read().decode("utf8")
    else:
        source = url.read_text("utf8")

    extract_version("emoji-data.txt", source)
    extract_prop(source, props["grapheme"], "Extended_Pictographic")

    for top in "Grapheme", "Word", "Sentence":
        if data_dir:
            url = pathlib.Path(data_dir) / f"{ top }BreakProperty.txt"
        else:
            url = f"https://www.unicode.org/Public/UCD/latest/ucd/auxiliary/{ top }BreakProperty.txt"
        print("Reading", url)
        if isinstance(url, str):
            source = urllib.request.urlopen(url).read().decode("utf8")
        else:
            source = url.read_text("utf8")
        extract_version(f"{ top }BreakProperty.txt", source)
        populate(source, props[top.lower()])


# ::TODO:: some emoji modifiers like U+1F3FF {EMOJI MODIFIER FITZPATRICK TYPE-6 (Sk Symbol modifier)}
# are not marked as extend and should be.  seems to be the tables from unicode that are wrong!

py_code_header = f"""\
# Generated by { sys.argv[0] } - Do not edit

"""

if __name__ == "__main__":
    import argparse
    import urllib.request

    p = argparse.ArgumentParser(description="Generate code from Unicode properties")
    p.add_argument(
        "--data-dir",
        help="Directory containing local copies of the relevant unicode database files.  If "
        "not supplied the latest files are read from https://www.unicode.org/Public/UCD/latest/ucd/",
    )
    p.add_argument("out_py", type=argparse.FileType("w", encoding="utf8"), help="File to write python code to")

    options = p.parse_args()

    read_props(options.data_dir)

    py_code = generate_python()
    options.out_py.write(py_code_header)
    options.out_py.write(py_code)
    options.out_py.close()
