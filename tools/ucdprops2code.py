#!/usr/bin/env python3

# Generates code from unicode properties db


import sys
import os
import itertools
import pathlib
import re
import pprint
import collections
import math

from typing import Any, Iterable

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
            yield from range(row[0], row[1] + 1)


def fmt(v: int) -> str:
    # format values the same as in the text source for easy grepping
    return f"{v:04X}"


def augiter(content: Iterable):
    "yields is_first, is_last, item from content"
    all_items = tuple(content)
    last_index = len(all_items) - 1
    for i, item in enumerate(all_items):
        is_first = i == 0
        is_last = i == last_index
        yield is_first, is_last, item


def fmt_cat(enum_name: str, cat: str | tuple[str, ...]):
    if isinstance(cat, str):
        return f"{ enum_name }.{ cat }"
    return " | ".join(f"{ enum_name }.{ c }" for c in cat)


def fmt_cat_c(enum_name: str, cat: str | tuple[str, ...]):
    if isinstance(cat, str):
        return f"{ enum_name }_{ cat }"
    return "(" + " | ".join(f"{ enum_name }_{ c }" for c in cat) + ")"


def bsearch(enum_name: str, indent: int, items: list, n: int):
    # n is if tests at same level.  2 means binary search, 3 is trinary etc
    indent_ = "    " * indent

    if len(items) > n:
        # break into smaller
        step = math.ceil(len(items) / n)
        chunks = list(range(0, len(items), step))
        for is_first, is_last, begin in augiter(chunks):
            test = None
            if not is_last:
                test = items[chunks[1 + chunks.index(begin)]][0]
            if is_first:
                yield f"{ indent_ }if (c < 0x{ test:04X})"
                yield f"{ indent_ }{{"
            elif is_last:
                yield indent_ + "else"
                yield indent_ + "{"
            else:
                yield f"{ indent_ }else if (c < 0x{ test:04X})"
                yield indent_ + "{"
            yield from bsearch(enum_name, indent + 1, items[begin : begin + step], n)
            yield indent_ + "}"
    else:
        for is_first, is_last, (start, end, cat) in augiter(items):
            if start == end:
                test = f"c == 0x{ start:04X}"
            else:
                test = f"(c >= 0x{ start:04X}) && (c <= 0x{ end:04X})"
            if not is_last:
                yield f"{ indent_ }if ({ test })"
                yield f"{ indent_ }  return { fmt_cat_c(enum_name, cat) };"
            else:
                yield f"{ indent_ }/* { test } */"
                yield f"{ indent_ }return { fmt_cat_c(enum_name, cat) };"


def generate_c() -> str:
    out: list[str] = []

    out.append(f'static const char *unicode_version = "{ ucd_version }";')
    out.append("")
    out.extend(generate_c_table("grapheme", "GC", grapheme_ranges))
    out.append("")
    out.extend(generate_c_table("word", "WC", word_ranges))
    out.append("")
    out.extend(generate_c_table("sentence", "SC", sentence_ranges))
    out.append("")
    out.extend(generate_c_table("category", "Category", category_ranges))
    out.append("")

    return "\n".join(out) + "\n"


def comment(language, text):
    text = text.splitlines()
    if len(text) == 1:
        text = text[0]
        indent = text[: len(text) - len(text.lstrip())]
        text = text[len(indent) :]
        if language == "python":
            yield f"{ indent }# { text}"
        else:
            yield f"{ indent }/* { text } */"
        return
    if language == "python":
        indent = 999
        for line in text:
            indent = min(indent, len(line) - len(line.lstrip()))
        for line in text:
            yield f"{ indent * ' '}# { line[indent:] }"
    else:
        yield "/*"
        for line in text:
            yield f" { line}"
        yield "*/"


def category_enum(language: str, name="Category"):
    assert language in {"c", "python"}

    all_cats = set()
    for _, _, cat in category_ranges:
        if isinstance(cat, str):
            all_cats.add(cat)
        else:
            all_cats.update(cat)

    cats = set()
    cats_members = {}
    for cat in unicode_categories.values():
        v = cat.split()
        v[1] = f"{v[0]}_{v[1]}"
        cats.add(v[0])
        if v[0] not in cats_members:
            cats_members[v[0]] = []
        cats_members[v[0]].append(v[1])
    cat_vals = {}
    if language == "python":
        yield f"class {name}(enum.IntFlag):"
    yield from comment(language, "Major category values - mutually exclusive")
    for i, cat in enumerate(sorted(cats)):
        if language == "python":
            yield f"    { cat } = 2**{ i }"
        else:
            yield f"#define Category_{ cat } (1u << { i })"
        cat_vals[cat] = i

    max_used = len(cats)

    py_comment = """\
    Minor category values - note: their values overlap so tests must include equals")
    To test for a minor, you must do like:"
        if codepoint & Letter_Upper == Letter_Upper ..."
"""

    c_comment = """\
   Minor category values - note: their values overlap so tests must include equals")
   To test for a minor, you must do like:"
       if ( (codepoint & Category_Letter_Upper) == Category_Letter_Upper) ..."
"""

    yield from comment(language, py_comment if language == "python" else c_comment)

    for cat, members in sorted(cats_members.items()):
        for i, member in enumerate(sorted(members), len(cats)):
            if language == "python":
                yield f"    { member } = 2**{ i } | 2**{ cat_vals[cat] }"
            else:
                yield f"#define Category_{ member }  ( (1u << { i }) | (1u << { cat_vals[cat] }))"
        max_used = max(max_used, i)

    # the rest
    ignore = cats.copy()
    for minors in cats_members.values():
        ignore.update(minors)
    yield from comment(language, "    Remaining non-category convenience flags")
    for cat in sorted(all_cats):
        if cat not in ignore:
            max_used += 1
            if language == "python":
                yield f"    { cat } = 2**{ max_used}"
            else:
                yield f"#define Category_{ cat } (1u << { max_used})"

    if language == "c" and False:  # Not used at the moment
        yield ""
        yield "/* deliberately leaves out the major category values */"
        yield "#define ALL_CATEGORY_VALUES \\"
        for cat in sorted(all_cats):
            if cat not in cat_vals:
                yield f"    X({cat}) \\"
        yield ""


def generate_python_table(name, enum_name, ranges):
    yield f"# { name }"
    yield ""
    if name == "category":
        assert ranges is category_ranges
        yield from category_enum("python")
    else:
        yield f"class { enum_name }(enum.IntFlag):"
        all_cats = set()
        for _, _, cat in ranges:
            if isinstance(cat, str):
                all_cats.add(cat)
            else:
                all_cats.update(cat)
        for i, cat in enumerate(sorted(all_cats)):
            yield f"    { cat } =  2**{ i }"
    yield ""
    yield f"# Codepoints by { name } category"
    yield "#"
    for k, v in stats[name].most_common():
        if not isinstance(k, str):
            k = " | ".join(k)
        yield f"# {v: 10,} { k }"
    others = sys.maxunicode + 1 - stats[name].total()
    yield f"# {others:10,} (other)"
    yield ""
    yield f"# {len(ranges):,} ranges"

    yield ""
    yield ""

    # make a copy because we modify it
    ranges = ranges[:]
    # first codepoint NOT in table
    table_limit = options.table_limit

    if table_limit:
        yield f"{ name}_fast_lookup = ["
        line = ""
        for cp in range(table_limit):
            if cp % 16 == 0:
                if line:
                    yield line.rstrip()
                    line = ""
                yield f"    # { cp:04X} - {min(table_limit,cp+16)-1:04X}"
            cat = fmt_cat(enum_name, ranges[0][2])
            if len(line) + len(cat) > 116:
                yield line.rstrip()
                line = ""
            if not line:
                line = "    "
            line += f"{cat}, "
            if cp >= ranges[0][1]:
                ranges.pop(0)

        if line:
            yield line.rstrip()
        yield "]"
        yield ""
        ranges[0][0] = table_limit

    yield f"def { name }_category(c: int) -> { enum_name }:"
    yield '    "Returns category corresponding to codepoint"'
    yield ""
    if table_limit:
        yield f"    if c < 0x{ table_limit:04X}:"
        yield f"        return { name}_fast_lookup[c]"
        yield ""

    yield from bsearch(enum_name, 1, ranges, 2)


def generate_c_table(name, enum_name, ranges):
    yield f"/* { name } */"
    yield ""
    if name == "category":
        assert ranges is category_ranges
        yield from category_enum("c")
    else:
        all_cats = set()
        for _, _, cat in ranges:
            if isinstance(cat, str):
                all_cats.add(cat)
            else:
                all_cats.update(cat)
        for i, cat in enumerate(sorted(all_cats)):
            yield f"#define { enum_name }_{ cat } (1u <<  { i })"
        yield ""
        yield f"#define ALL_{ enum_name.upper() }_VALUES \\"
        for cat in sorted(all_cats):
            yield f"     X({enum_name}_{ cat }) \\"
        yield ""
        yield ""
    yield ""
    yield f"/* Codepoints by { name } category"
    yield ""
    for k, v in stats[name].most_common():
        if not isinstance(k, str):
            k = " | ".join(k)
        yield f"  {v: 10,} { k }"
    others = sys.maxunicode + 1 - stats[name].total()
    yield f"  {others:10,} (other)"
    yield ""
    yield f"  {len(ranges):,} ranges"
    yield ""
    yield "*/"
    yield ""
    yield ""

    # make a copy because we modify it
    ranges = ranges[:]
    # first codepoint NOT in table
    table_limit = options.table_limit

    if table_limit:
        yield f"static unsigned int { name}_fast_lookup[] = {{"
        line = ""
        for cp in range(table_limit):
            if cp % 16 == 0:
                if line:
                    yield line.rstrip()
                    line = ""
                yield f"    /* { cp:04X} - {min(table_limit,cp+16)-1:04X} */"
            cat = fmt_cat_c(enum_name, ranges[0][2])
            if len(line) + len(cat) > 116:
                yield line.rstrip()
                line = ""
            if not line:
                line = "    "
            line += f"{cat}, "
            if cp >= ranges[0][1]:
                ranges.pop(0)

        if line:
            yield line.rstrip()
        yield "};"
        yield ""
        ranges[0][0] = table_limit

    yield "static unsigned int"
    yield f"{ name }_category(Py_UCS4 c)"
    yield "{"
    yield "   /* Returns category corresponding to codepoint */"
    yield ""
    if options.table_limit:
        yield f"    if (c < 0x{ table_limit:04X})"
        yield f"        return { name}_fast_lookup[c];"
        yield ""

    yield from bsearch(enum_name, 1, ranges, 2)
    yield "}"
    yield ""


props = {
    "grapheme": {},
    "word": {},
    "sentence": {},
    "category": {},
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
        vals, prop = line.split(";", 1)
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


def extract_prop(source: str, dest: dict[str, Any], prop_name: str, name: str | None = None):
    if name is None:
        name = prop_name
    assert name not in dest
    accumulate = dest[name] = []

    for start, end, prop in parse_source_lines(source):
        if prop == prop_name:
            if end is None:
                accumulate.append(start)
            else:
                accumulate.append((start, end))

    assert len(accumulate) > 0


unicode_categories = {
    "Lu": "Letter Uppercase",
    "Ll": "Letter Lowercase",
    "Lt": "Letter Titlecase",
    "Lm": "Letter Modifier",
    "Lo": "Letter Other",
    "Mn": "Mark NonSpacing",
    "Mc": "Mark SpacingCombining",
    "Me": "Mark Enclosing",
    "Nd": "Number DecimalDigit",
    "Nl": "Number Letter",
    "No": "Number Other",
    "Pc": "Punctuation Connector",
    "Pd": "Punctuation Dash",
    "Ps": "Punctuation Open",
    "Pe": "Punctuation Close",
    "Pi": "Punctuation InitialQuote",
    "Pf": "Punctuation FinalQuote",
    "Po": "Punctuation Other",
    "Sm": "Symbol Math",
    "Sc": "Symbol Currency",
    "Sk": "Symbol Modifier",
    "So": "Symbol Other",
    "Zs": "Separator Space",
    "Zl": "Separator Line",
    "Zp": "Separator Paragraph",
    "Cc": "Other Control",
    "Cf": "Other Format",
    "Cs": "Other Surrogate",
    "Co": "Other PrivateUse",
    "Cn": "Other NotAssigned",
}


def extract_categories(source: str, dest: dict[str, Any]):
    for v in unicode_categories.values():
        v = v.split()
        assert len(v) == 2
        v[1] = f"{v[0]}_{v[1]}"
        dest[v[0]] = []
        dest[v[1]] = []

    for start, end, cat in parse_source_lines(source):
        v = unicode_categories[cat].split()
        v[1] = f"{v[0]}_{v[1]}"
        if end is None:
            dest[v[0]].append(start)
            dest[v[1]].append(start)
        else:
            dest[v[0]].append((start, end))
            dest[v[1]].append((start, end))


def extract_width(source: str, dest: dict[str, Any]):
    dest["Wide"] = []
    for start, end, width in parse_source_lines(source):
        if width in {"F", "W"}:
            if end is None:
                dest["Wide"].append(start)
            else:
                dest["Wide"].append((start, end))


def read_props(data_dir: str):
    def get_source(url: str) -> str:
        base = url.split("/")[-1]
        if data_dir:
            url = pathlib.Path(data_dir) / base

        print("Reading", url)
        if isinstance(url, str):
            source = urllib.request.urlopen(url).read().decode("utf8")
        else:
            source = url.read_text("utf8")

        return source

    source = get_source("https://www.unicode.org/Public/UCD/latest/ucd/extracted/DerivedGeneralCategory.txt")
    extract_version("DerivedGeneralCategory.txt", source)
    extract_categories(source, props["category"])

    source = get_source("https://www.unicode.org/Public/UCD/latest/ucd/EastAsianWidth.txt")
    extract_version("EastAsianWidth.txt", source)
    extract_width(source, props["category"])

    source = get_source("https://www.unicode.org/Public/UCD/latest/ucd/emoji/emoji-data.txt")

    extract_version("emoji-data.txt", source)
    extract_prop(source, props["grapheme"], "Extended_Pictographic")
    extract_prop(source, props["word"], "Extended_Pictographic")
    extract_prop(source, props["category"], "Extended_Pictographic")

    source = get_source("https://www.unicode.org/Public/UCD/latest/ucd/DerivedCoreProperties.txt")

    extract_version("DerivedCoreProperties.txt", source)
    extract_prop(source, props["grapheme"], "InCB; Linker", "InCB_Linker")
    extract_prop(source, props["grapheme"], "InCB; Consonant", "InCB_Consonant")
    extract_prop(source, props["grapheme"], "InCB; Extend", "InCB_Extend")

    for top in "Grapheme", "Word", "Sentence":
        source = get_source(f"https://www.unicode.org/Public/UCD/latest/ucd/auxiliary/{ top }BreakProperty.txt")
        extract_version(f"{ top }BreakProperty.txt", source)
        populate(source, props[top.lower()])
        if top == "Grapheme":
            extract_prop(source, props["category"], "Regional_Indicator")


grapheme_ranges = []


stats = {}


def generate_ranges(name, source, dest):
    all_cp = {}
    # somewhat messy because the same codepoint can be
    # in multiple categories
    for category, vals in source.items():
        for val in all_vals(vals):
            if val in all_cp:
                existing = all_cp[val]
                # sets aren't hashable so we keep things as a sorted
                # tuple, and do this dance to update them
                cat = tuple(sorted((set(existing) if isinstance(existing, tuple) else {existing}) | {category}))
            else:
                cat = category
            all_cp[val] = cat

    print(f"{name} categories and members")
    by_cat = collections.Counter()
    for v in all_cp.values():
        by_cat[v] += 1
    pprint.pprint(by_cat)
    stats[name.lower()] = by_cat

    last = None

    for cp in range(0, sys.maxunicode + 1):
        cat = all_cp.get(cp, "Other")
        if cat != last:
            dest.append([cp, cp, cat])
        else:
            dest[-1][1] = cp
        last = cat


def generate_grapheme_ranges():
    generate_ranges("Grapheme", props["grapheme"], grapheme_ranges)


word_ranges = []


def generate_word_ranges():
    generate_ranges("Word", props["word"], word_ranges)


sentence_ranges = []


def generate_sentence_ranges():
    generate_ranges("sentence", props["sentence"], sentence_ranges)


category_ranges = []


def generate_category_ranges():
    generate_ranges("category", props["category"], category_ranges)


def replace_if_different(filename: str, contents: str) -> None:
    if not os.path.exists(filename) or pathlib.Path(filename).read_text() != contents:
        print(f"{ 'Creating' if not os.path.exists(filename) else 'Updating' } { filename }")
        pathlib.Path(filename).write_text(contents)


def get_tr29_section():
    res = []
    res.append(f'unicode_version = "{ ucd_version }"')
    res.append('"""The `Unicode version <https://www.unicode.org/versions/enumeratedversions.html>`__')
    res.append('that the rules and data tables implement"""')
    res.append("")
    res.append("")
    res.extend(category_enum("python", name="_Category"))
    res.append("")
    res.append("")
    return "\n".join(res)


py_code_header = f"""\
# Generated by { sys.argv[0] } - Do not edit

"""

c_code_header = f"""\
/*  Generated by { sys.argv[0] } - Do not edit */

"""


if __name__ == "__main__":
    import argparse
    import urllib.request

    p = argparse.ArgumentParser(description="Generate code from Unicode properties")
    p.add_argument(
        "--table-limit", type=int, default=256, help="First codepoint value not part of fast lookup table [%(default)s]"
    )
    p.add_argument(
        "--data-dir",
        help="Directory containing local copies of the relevant unicode database files.  If "
        "not supplied the latest files are read from https://www.unicode.org/Public/UCD/latest/ucd/",
    )
    p.add_argument(
        "out_file", type=argparse.FileType("w", encoding="utf8"), help="File to write code to with .c extension"
    )

    options = p.parse_args()

    read_props(options.data_dir)

    generate_grapheme_ranges()
    generate_word_ranges()
    generate_sentence_ranges()
    generate_category_ranges()

    assert options.out_file.name.endswith(".c")
    c_code = generate_c()
    options.out_file.write(c_code_header)
    options.out_file.write(c_code)
    options.out_file.close()

    lines = []
    in_replacement = False
    for line in pathlib.Path("apsw/tr29.py").read_text().splitlines():
        if line == "### BEGIN UNICODE UPDATE SECTION ###":
            in_replacement = True
            lines.append(line)
            continue
        if in_replacement and line == "### END UNICODE UPDATE SECTION ###":
            in_replacement = False
            lines.append(get_tr29_section())
            lines.append(line)
            continue
        if not in_replacement:
            lines.append(line)

    lines = "\n".join(lines) + "\n"
    replace_if_different("apsw/tr29.py", lines)
