#!/usr/bin/env python3

# Generates code from unicode properties db
# It has evolved over time and was not intelligently designed
# in advance


import sys
import os
import itertools
import pathlib
import re
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
    if enum_name == "age":
        if cat == "NULL":
            return "NULL"
        return f'"{cat}"'

    if enum_name != "strip":
        if isinstance(cat, str):
            return f"{ enum_name }_{ cat }"
        return "(" + " | ".join(f"{ enum_name }_{ c }" for c in cat) + ")"

    if cat == 0:
        return "0"

    cat = list(cat)

    prefix = f"STRIP_MAXCHAR_{ cat.pop(0) } | "

    if len(cat) == 1:
        if 0 <= cat[0] <= 30:
            return f"{prefix}{cat[0]}"
        return f"{prefix}0x{cat[0]:04X}"
    assert len(cat) == 2
    return f"({prefix}0x{cat[0]:04X} | (0x{cat[1]:04X}ull << 21))"


def bsearch(enum_name: str, indent: int, items: list, n: int):
    # n is if tests at same level.  2 means binary search, 3 is trinary etc
    indent_ = "  " * indent

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
    out.extend(generate_casefold_expansion(props["casefold"]))
    out.append("")
    out.extend(generate_c_table("line", "LB", line_ranges))
    out.append("")
    out.extend(generate_line_hard_breaks())
    out.append("")
    out.extend(generate_c_table("strip", "strip", strip_ranges))
    out.append("")
    out.extend(generate_strip_special_handling())
    out.append("")
    out.extend(generate_c_table("age", "age", age_ranges))
    out.append("")
    out.extend(ucdnames.generate_names_code(DerivedName_txt_contents))
    out.append("")

    return "\n".join(out) + "\n"


def generate_casefold_expansion(src) -> list[str]:
    res: list[str] = []

    def add_line(l):
        l = l + " " * (119 - len(l)) + "\\"
        res.append(l)

    res.append(f"/* {len(src):,} codepoints have casefold")
    res.append("")

    # group by maxchar, expansion =  codepoints
    groups = {}
    for codepoint, repl in src.items():
        key = (max(next_pyuni_maxval(r) for r in repl), len(repl))
        try:
            groups[key].append(codepoint)
        except KeyError:
            groups[key] = [codepoint]

    res.append("    count  maxchar  expansion")
    for (maxchar, expansion), codepoints in sorted(groups.items()):
        res.append(f"   {len(codepoints): 6,} {maxchar: 8}      {expansion}")

    res.append("")
    res.append("*/")
    res.append("")

    add_line("#define CASEFOLD_EXPANSION")
    indent = "  "
    for (maxchar, expansion), codepoints in sorted(groups.items()):
        for codepoint in codepoints:
            if codepoint > ord("Z"):
                add_line(f"{indent}case 0x{ codepoint:04X}:")
        add_line(f"{indent*2}changed = 1;")
        if expansion > 1:
            add_line(f"{indent*2}expansion += { expansion -1 };")
        add_line(f"{indent*2}maxchar |= CASEFOLD_MAXCHAR_{ maxchar};")
        add_line(f"{indent*2}break;")
    res[-1] = res[-1].rstrip("\\").rstrip()
    res.append("")

    add_line("#define CASEFOLD_WRITE")
    for codepoint, replacement in sorted(src.items()):
        if codepoint > ord("Z"):
            add_line(f"{indent}case 0x{ codepoint:04X}:")
            for r in replacement[:-1]:
                add_line(f"{indent*2}WRITE_DEST(0x{r:04X});")
            add_line(f"{indent*2}dest_char = 0x{replacement[-1]:04X};")
            add_line(f"{indent*2}break;")

    res[-1] = res[-1].rstrip("\\").rstrip()
    res.append("")

    return res


def generate_strip_special_handling():
    res: list[str] = []

    def add_line(l):
        l = l + " " * (119 - len(l)) + "\\"
        res.append(l)

    res.append(f"/* The {len(strip_special_handling)} codepoints that expand to 3+ codepoints */")
    res.append("")

    add_line("#define STRIP_WRITE")
    for codepoint, expansion in sorted(strip_special_handling.items()):
        assert len(expansion) >= 3
        add_line(f"case 0x{codepoint:04X}:")
        for c in expansion:
            add_line(f"    WRITE_DEST(0x{c:04X});")
        add_line("    break;")
    res[-1] = res[-1].rstrip("\\").rstrip()
    res.append("")

    return res


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
    assert language == "python"

    all_cats = set()
    for _, _, cat in category_ranges:
        if isinstance(cat, str):
            all_cats.add(cat)
        else:
            all_cats.update(cat)

    yield f"class {name}:"
    for i, cat in enumerate(sorted(all_cats)):
        yield f"    { cat } = 2**{ i }"


def generate_c_table(name, enum_name, ranges):
    # we can use 32 bit values for all tables except line/LB
    ret_type = "unsigned int" if enum_name not in {"LB", "strip", "Category"} else "unsigned long long"
    int_suffix = "u" if enum_name not in {"LB", "strip", "Category"} else "ull"
    if enum_name == "age":
        ret_type = "const char *"

    yield f"/* { name } */"
    yield ""
    if name in {"strip", "age"}:
        pass
    else:
        all_cats = set()
        for _, _, cat in ranges:
            if isinstance(cat, str):
                all_cats.add(cat)
            else:
                all_cats.update(cat)
        for i, cat in enumerate(sorted(all_cats)):
            yield f"#define { enum_name }_{ cat } (1{int_suffix} << {i})"
        yield ""
        yield f"#define ALL_{ enum_name.upper() }_VALUES \\"
        for cat in sorted(all_cats):
            l = f"  X({enum_name}_{ cat })"
            l += " " * (119 - len(l)) + "\\"
            yield l
        yield ""
        yield ""
    yield ""
    yield f"/* Codepoints by { name } category"
    yield ""
    for k, v in stats[name].most_common():
        if not isinstance(k, str):
            k = " | ".join(k)
        yield f"  {v: 10,} { k }"
    yield ""
    yield f"  {len(ranges):,} ranges"
    yield ""
    yield "*/"
    yield ""
    yield ""
    if name == "strip":
        yield "/* increases the max char value - bits 50+ above 2 packed 21 bit values */"
        yield "#define STRIP_MAXCHAR_127 (1ull << 50)"
        yield "#define STRIP_MAXCHAR_255 (1ull << 51)"
        yield "#define STRIP_MAXCHAR_65535 (1ull << 52)"
        yield "#define STRIP_MAXCHAR_1114111 (1ull << 53)"
        yield "#define STRIP_MAXCHAR_MASK (STRIP_MAXCHAR_127 | STRIP_MAXCHAR_255 | STRIP_MAXCHAR_65535 | STRIP_MAXCHAR_1114111)"
        yield ""

    # make a copy because we modify it
    ranges = ranges[:]
    # first codepoint NOT in table
    table_limit = options.table_limit
    if enum_name == "age":  # not worth fast lookup
        table_limit = 0

    if table_limit:
        yield f"static {ret_type} { name}_fast_lookup[] = {{"
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

    yield f"static {ret_type}"
    yield f"{ name }_category(Py_UCS4 c)"
    yield "{"
    if enum_name != "strip":
        yield "  /* Returns category corresponding to codepoint */"
    else:
        yield "  /* Returns value corresponding to codepoint "
        yield ""
        yield "     A value of zero means skip/omit the codepoint"
        yield "     One means it stays unchanged"
        yield "     We pack 2 replacement codepoints as 21 bit values (lowest first)"
        yield "     3 through 30 means it turns into that many codepoints (about 300, handled separately)"
        yield ""
        yield "     We also have to keep track of the maxchar bucket because CPython compiled with assertions"
        yield "     fails if a larger value than was need was used.  This is STRIP_MAXCHAR_*"
        yield "  */"
    yield ""
    if table_limit:
        yield f"  if (c < 0x{ table_limit:04X})"
        yield f"    return { name}_fast_lookup[c];"
        yield ""

    yield from bsearch(enum_name, 1, ranges, 2)
    yield "}"
    yield ""


props = {
    "grapheme": {},
    "word": {},
    "sentence": {},
    "line": {},
    "category": {},
    "casefold": {},
    "strip": {},
    "age": {},
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
    # we only care about major.minor - emoji data doesn't even have patch
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
    global codepoint_to_category  # we also fill this out
    for v in unicode_categories.keys():
        dest[v] = []

    for start, end, cat in parse_source_lines(source):
        if end is None:
            dest[cat].append(start)
            codepoint_to_category[start] = cat
        else:
            dest[cat].append((start, end))
            for i in range(start, end + 1):
                codepoint_to_category[i] = cat


east_asian_widths_FWH = set()

# FW only
wide_codepoints = set()


def extract_width(source: str):
    for start, end, width in parse_source_lines(source):
        # See line rules LB30 for why this is here
        if width in {"F", "H", "W"}:
            for cp in range(start, 1 + (end if end is not None else start)):
                east_asian_widths_FWH.add(cp)

        if width in {"F", "W"}:
            for cp in range(start, 1 + (end if end is not None else start)):
                wide_codepoints.add(cp)


def extract_casefold(source: str, dest: dict[int, list]):
    for line in source.splitlines():
        if not line.strip() or line.startswith("#"):
            continue
        line = line[: line.index("#")].strip()
        codepoint, kind, repl, blank = line.split(";")
        assert not blank
        if kind.strip() in {"S", "T"}:
            continue
        repl = tuple(int(r, 16) for r in repl.split())
        codepoint = int(codepoint, 16)
        dest[codepoint] = repl
        if codepoint <= 127:
            assert len(repl) == 1
            assert "A" <= chr(codepoint) <= "Z"
            assert repl[0] == codepoint + 32


# codepoints in these categories are removed
strip_categories = {
    "Lm",  # Letter modifier"
    "Mn",  # Mark non-spacing
    "Mc",  # Mark spacing combining
    "Me",  # Mark enclosing
    "Pc",  # Punctuation connector
    "Pd",  # Punctuation dash
    "Pe",  # Punctuation close
    "Po",  # Punctuation open
    "Pc",  # Punctuation close
    "Pi",  # Punctuation initial quote
    "Pf",  # Punctuation final quote
    "Po",  # Punctuation other
    "Ps",  # Punctuation open
    "Sk",  # Symbol modifier
    "Zs",  # Separator space
    "Zl",  # Separator line
    "Zp",  # Separator paragraph
    "Cc",  # Other control
    "Cf",  # Other format
    "Cs",  # Other surrogate
    "Co",  # Other private use
    "Cn",  # Other not assigned
}


# we have to know the maxchar value
strip_maxchar: dict[int, int] = {}


def next_pyuni_maxval(val: int) -> int:
    # https://docs.python.org/3/c-api/unicode.html#c.PyUnicode_New
    for size in 127, 255, 65535, 1114111:
        if val <= size:
            return size
    raise Exception("Can't get here")


unified_ideograph: dict[int, int] = {}


def extract_unified_ideograph(source: str):
    for line in source.splitlines():
        if not line.strip() or line.startswith("#"):
            continue
        codepoints, equiv = [l.strip() for l in line.split("#")[0].split(";")]
        equiv = int(equiv, 16)
        if ".." in codepoints:
            low, high = codepoints.split("..")
            codepoints = list(range(int(low, 16), 1 + int(high, 16)))
        else:
            codepoints = [int(codepoints, 16)]
        for codepoint in codepoints:
            unified_ideograph[codepoint] = equiv


def extract_strip(source: str, dest):
    # see the comment generated in generate_c_table which explains
    # how the information is represented.  generate_strip_ranges
    # takes this output to do that
    dest[0] = []
    dest[1] = []
    start_seq = None
    for line in source.splitlines():
        codepoint, name, category, combining_class, bidi, decomp, *_ = line.split(";")
        codepoint = int(codepoint, 16)

        if name.endswith(", First>"):
            start_seq = codepoint
            continue
        elif name.endswith(", Last>"):
            seq = (start_seq, codepoint)
            start_seq = None
        else:
            seq = (codepoint, codepoint)

        for codepoint in range(seq[0], 1 + seq[1]):
            # double check the data matches
            assert category == codepoint_to_category[codepoint]

            if category in strip_categories:
                dest[0].append(codepoint)
                continue

            if not decomp:
                if codepoint in unified_ideograph:
                    decomp = f"{unified_ideograph[codepoint]:X}"

            if not decomp:
                dest[1].append(codepoint)
                continue

            decomp = decomp.split()
            if decomp[0][0] == "<":
                decomp = decomp[1:]

            # unhexify
            decomp = [int(d, 16) for d in decomp]

            # filter
            decomp = [d for d in decomp if codepoint_to_category[d] not in strip_categories]
            if len(decomp) == 0:
                dest[0].append(codepoint)
                continue

            if len(decomp) == 1:
                if decomp[0] == codepoint:
                    # this shouldn't happen - decomposing to include self
                    raise Exception("this can't happen")
                if decomp[0] not in dest:
                    dest[decomp[0]] = []
                dest[decomp[0]].append(codepoint)
                continue

            decomp = tuple(decomp)
            if decomp in dest:
                dest[decomp].append(codepoint)
            else:
                dest[decomp] = [codepoint]


def read_props(data_dir: str):
    def get_source(url: str) -> str:
        parts = url.split("/")
        if data_dir:
            candidates = (
                pathlib.Path(data_dir) / parts[-1],
                pathlib.Path(data_dir) / parts[-2] / parts[-1],
            )
            for url in candidates:
                if url.exists():
                    break
            else:
                sys.exit(f"Failed to find file in data dir.  Looked for\n{candidates}")

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
    extract_width(source)

    source = get_source("https://www.unicode.org/Public/UCD/latest/ucd/CaseFolding.txt")
    extract_version("CaseFolding.txt", source)
    extract_casefold(source, props["casefold"])

    source = get_source("https://www.unicode.org/Public/UCD/latest/ucd/emoji/emoji-data.txt")
    extract_version("emoji-data.txt", source)
    extract_prop(source, props["grapheme"], "Extended_Pictographic")
    extract_prop(source, props["word"], "Extended_Pictographic")
    extract_prop(source, props["category"], "Extended_Pictographic")
    extract_prop(source, props["line"], "Extended_Pictographic")

    source = get_source("https://www.unicode.org/Public/UCD/latest/ucd/DerivedCoreProperties.txt")
    extract_version("DerivedCoreProperties.txt", source)
    extract_prop(source, props["grapheme"], "InCB; Linker", "InCB_Linker")
    extract_prop(source, props["grapheme"], "InCB; Consonant", "InCB_Consonant")
    extract_prop(source, props["grapheme"], "InCB; Extend", "InCB_Extend")

    source = get_source("https://www.unicode.org/Public/UCD/latest/ucd/EquivalentUnifiedIdeograph.txt")
    extract_version("EquivalentUnifiedIdeograph.txt", source)
    extract_unified_ideograph(source)

    source = get_source("https://www.unicode.org/Public/UCD/latest/ucd/UnicodeData.txt")
    # it has no version
    extract_strip(source, props["strip"])

    source = get_source("https://www.unicode.org/Public/UCD/latest/ucd/DerivedAge.txt")
    populate(source, props["age"])

    for top in "Grapheme", "Word", "Sentence":
        source = get_source(f"https://www.unicode.org/Public/UCD/latest/ucd/auxiliary/{ top }BreakProperty.txt")
        extract_version(f"{ top }BreakProperty.txt", source)
        populate(source, props[top.lower()])
        if top == "Grapheme":
            extract_prop(source, props["category"], "Regional_Indicator")

    source = get_source("https://www.unicode.org/Public/UCD/latest/ucd/LineBreak.txt")
    extract_version("LineBreak.txt", source)
    populate(source, props["line"])

    global DerivedName_txt_contents
    source = get_source("https://www.unicode.org/Public/UCD/latest/ucd/extracted/DerivedName.txt")
    extract_version("DerivedName.txt", source)
    DerivedName_txt_contents = source


codepoint_to_category = {}

grapheme_ranges = []


stats = {}


def generate_ranges(name, source, dest, other_name="Other", tailor=None):
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

    by_cat = collections.Counter()
    stats[name.lower()] = by_cat

    last = None

    for cp in range(0, sys.maxunicode + 1):
        cat = all_cp.get(cp, other_name)
        if tailor:
            cat = tailor(cp, cat)
        by_cat[cat] += 1
        if cat != last:
            dest.append([cp, cp, cat])
        else:
            dest[-1][1] = cp
        last = cat


age_ranges = []


def generate_age_ranges():
    generate_ranges("Age", props["age"], age_ranges, other_name="NULL")


def generate_grapheme_ranges():
    generate_ranges("Grapheme", props["grapheme"], grapheme_ranges)


word_ranges = []


def generate_word_ranges():
    generate_ranges("Word", props["word"], word_ranges)


sentence_ranges = []


def generate_sentence_ranges():
    generate_ranges("sentence", props["sentence"], sentence_ranges)


category_ranges = []


# we need this for better width values
grapheme_prop_by_codepoint = {}


def grapheme_prop(codepoint: int) -> str:
    # build this
    if not grapheme_prop_by_codepoint:
        for tag, ranges in props["grapheme"].items():
            for item in ranges:
                if isinstance(item, int):
                    item = (item, item)
                for cp in range(item[0], item[1] + 1):
                    grapheme_prop_by_codepoint[cp] = tag
    return grapheme_prop_by_codepoint.get(codepoint, "Other")


# these were found by running python -m apsw.unicode width-check
# against many terminals and finding out how they differed from
# what this returned without the overrides, also looking at
# what the C and Python wcswidths gave.
#
# Tested terminals were alacritty, kitty, konsole, tmux,
# gnome-terminal, gnome-console, st
width_overrides = {
    # They all agree on these
    # HALFWIDTH KATAKANA VOICED SOUND MARK
    0xFF9E: 1,
    # HALFWIDTH KATAKANA SEMI-VOICED SOUND MARK
    0xFF9F: 1,
}

# The terminals get widths wrong for codepoints added in 15.1
# but we won't fix that
#    2FFC - 2FFF
#    31EF

# Hangul JUNGSEONG where all except ~3 are rendered as
# width 1 or 2 depending on terminal and have zero width
# according this library, and Py/C wcswidth.  When part
# of other Hangul they are indeed zero.
#   D780 - D7FB


def category_width(codepoint: int, cat: str | tuple[str]):
    def add_cat(c: str):
        nonlocal cat
        if isinstance(cat, tuple):
            cat = tuple(list(cat) + [c])
        else:
            cat = (cat, c)

    if codepoint in width_overrides:
        w = width_overrides[codepoint]
        if w == -1:
            add_cat("WIDTH_INVALID")
        elif w == 0:
            add_cat("WIDTH_ZERO")
        elif w == 1:
            pass
        elif w == 2:
            add_cat("WIDTH_TWO")
        else:
            raise Exception(f"unexpected width { codepoint=} {w=}")

        return cat

    GC = grapheme_prop(codepoint)

    if GC == "SpacingMark":
        # These are usually category Mc but it is usually the case
        # that the codepoint they combine with then ends up two wide
        # so we treat them as width 1
        return cat

    # can't be represented so minus one gets returned
    if codepoint_to_category[codepoint] in {
        "Cc",  # Other control
        "Cs",  # Other surrogates
    }:
        # wcswidth (Python) gives 1 for surrogates, but Python refuses to
        # output UTF8 text containing them, so we also treat them as
        # invalid just as wcswidth (C) does
        add_cat("WIDTH_INVALID")
        return cat

    # Always zero no matter what the wide codepoints say
    if codepoint_to_category[codepoint] in {
        "Mn",  # Mark NonSpacing
        "Me",  # Mark enclosing
        "Mc",  # Mark spacing combining
        "Cf",  # Other format
    }:
        add_cat("WIDTH_ZERO")
        return cat

    if GC in {
        "V",  # Hangul
        "T",  # Hangul
        "Extend",  # don't really adjust the width of the codepoint they extend
    }:
        add_cat("WIDTH_ZERO")
        return cat

    # Not Assigned are 2 in terminals, -1 in C wcswidth, 2 in py wcswidth
    if codepoint_to_category[codepoint] in {
        "Cn",  # Other Not Assigned
    }:
        # There are blocks named CJK Ideograph Extension B, First and
        # a corresponding Last, with Extensions like B, C, ... H, I.
        # Codepoints 20000 - 2EE5D  that have Cn Not Assigned as their
        # category but technically are letter other.  East Asian Widths
        # have most as W.  Example doc for one of the blocks
        # https://en.wikipedia.org/wiki/CJK_Unified_Ideographs_Extension_C
        # Terminals are 2 wide, C wcswidth is -1 and py wcswidth is 2.
        # We'll treat them no different than other not assigned to get
        # consistent wrapping.

        # we mark as invalid because they really are unknown and text_wrap
        # will replace with a placeholder which is a sure thing width
        add_cat("WIDTH_INVALID")
        return cat

    # now use wide codepoints
    if codepoint in wide_codepoints:
        add_cat("WIDTH_TWO")

    return cat


def generate_category_ranges():
    generate_ranges("category", props["category"], category_ranges, tailor=category_width)


line_ranges = []

line_hard_breaks = []


def generate_line_hard_breaks():
    yield "#define ALL_LINE_HARD_BREAKS \\"
    for _, is_last, v in augiter(line_hard_breaks):
        yield f"  X(0x{v:04X}) " + ("\\" if not is_last else "")
    yield ""


def generate_line_ranges():
    generate_ranges("line", props["line"], line_ranges, "XX", line_resolve_classes)


def line_resolve_classes(codepoint: int, cat: str | tuple[str]):
    # the category should always have been a tuple, not str or tuple
    # but it isn't worth fixing so these are the workarounds
    def has_cat(c: str) -> bool:
        return cat == c if isinstance(cat, str) else c in cat

    def replace_cat(match: str, replacement: str):
        nonlocal cat
        if not has_cat(match):
            return
        if isinstance(cat, str):
            cat = replacement
        else:
            newcat = []
            for c in cat:
                newcat.append(c if c != match else replacement)
            cat = tuple(newcat)

    if any(has_cat(c) for c in ("BK", "CR", "LF", "NL")):
        line_hard_breaks.append(codepoint)

    # this is to do the mapping in 6.1 Resolve line breaking classes
    # https://www.unicode.org/reports/tr14/#LB1
    for c in {"AI", "SG", "XX"}:
        replace_cat(c, "AL")
    if has_cat("SA"):
        if codepoint_to_category[codepoint] in {"Mn", "Mc"}:
            replace_cat("SA", "CM")
        else:
            replace_cat("SA", "AL")
    replace_cat("CJ", "NS")

    def add_cat(c: str):
        nonlocal cat
        if isinstance(cat, tuple):
            cat = tuple(list(cat) + [c])
        else:
            cat = (cat, c)

    if codepoint_to_category[codepoint] == "Pi":
        add_cat("Punctuation_Initial_Quote")
    if codepoint_to_category[codepoint] == "Pf":
        add_cat("Punctuation_Final_Quote")
    if codepoint_to_category[codepoint] == "Cn":
        add_cat("Other_NotAssigned")

    if codepoint in east_asian_widths_FWH:
        add_cat("EastAsianWidth_FWH")

    # DOTTED CIRCLE U+25CC is in the rules
    if codepoint == 0x25CC:
        add_cat("DOTTED_CIRCLE")
    # as well as HYPHEN U+2010
    if codepoint == 0x2010:
        add_cat("HYPHEN")

    return cat


strip_ranges = []

strip_stats = collections.Counter()


def generate_strip_ranges():
    generate_ranges("strip", props["strip"], strip_ranges, 0, tailor=strip_tailor)
    stats["strip"] = strip_stats


strip_special_handling = {}


def strip_tailor(codepoint, cat):
    if cat == 0:
        strip_stats["(stripped)"] += 1
        return cat

    if cat == 1:
        strip_stats["self"] += 1
        return (next_pyuni_maxval(codepoint), 1)

    cat = [cat] if isinstance(cat, int) else list(cat)

    strip_stats[f"length {len(cat)}"] += 1

    maxval = max(next_pyuni_maxval(c) for c in cat)

    if len(cat) >= 3:
        strip_special_handling[codepoint] = cat
        strip_stats["Special handling because 3+ codepoints"] += 1
        return (maxval, len(cat))

    return tuple([maxval] + cat)


def replace_if_different(filename: str, contents: str) -> None:
    if not os.path.exists(filename) or pathlib.Path(filename).read_text() != contents:
        print(f"{ 'Creating' if not os.path.exists(filename) else 'Updating' } { filename }")
        pathlib.Path(filename).write_text(contents)


def get_unicode_section():
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

# yes it is hacky
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import ucdnames

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
    generate_line_ranges()
    generate_strip_ranges()
    generate_age_ranges()

    assert options.out_file.name.endswith(".c")
    c_code = generate_c()
    options.out_file.write(c_code_header)
    options.out_file.write(c_code)
    options.out_file.close()

    lines = []
    in_replacement = False
    for line in pathlib.Path("apsw/unicode.py").read_text().splitlines():
        if line == "### BEGIN UNICODE UPDATE SECTION ###":
            in_replacement = True
            lines.append(line)
            continue
        if in_replacement and line == "### END UNICODE UPDATE SECTION ###":
            in_replacement = False
            lines.append(get_unicode_section())
            lines.append(line)
            continue
        if not in_replacement:
            lines.append(line)

    lines = "\n".join(lines) + "\n"
    replace_if_different("apsw/unicode.py", lines)
