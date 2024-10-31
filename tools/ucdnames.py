#!/usr/bin/env python3

from __future__ import annotations

import collections
import sys
import os


def check_numbered(codepoint: int, name: str, numbered: list[tuple[str, str]]) -> bool:
    # check if codepoint are in numbered and verify name match
    # return True of codepoint covered by numbered range
    for cprange, pattern in numbered:
        start, end = [int(x, 16) for x in cprange.split("..")]
        if start <= codepoint <= end:
            expected = pattern.rsplit("-", 1)[0]
            got = name.rsplit("-", 1)[0]
            assert expected == got, f"{codepoint=:04X} {name=} {cprange=} {pattern=}"
            return True
    return False


def generate_names_code(source: str, show_status: bool = False) -> list[str]:
    # needs DerivedName.txt source

    if not show_status:
        print("Generating codepoint names, takes a while ...", flush=True, end=" ")

    # these have numeric suffix, although the number
    # varies (hex of codepoint, counter) and will
    # be generated from if statement, not name tables
    numbered = [
        ("18800..18AFF", "TANGUT COMPONENT-*"),
        ("FE00..FE0F", "VARIATION SELECTOR-*"),
        ("E0100..E01EF", "VARIATION SELECTOR-*"),
    ]

    # codepoint to name
    mapping: dict[int, str] = {}

    # parse data
    for line in source.splitlines():
        if not line.lstrip() or line.startswith("#"):
            continue

        codepoint, name = (p.strip() for p in line.split(";"))

        if name.endswith("-*"):
            # 18cff in unicode 16.0
            if ".." not in codepoint:
                codepoint = f"{codepoint}..{codepoint}"
            numbered.append((codepoint, name))
            continue

        cpint = int(codepoint, 16)

        if check_numbered(cpint, name, numbered):
            continue

        # hangul syllable
        if 0xAC00 <= cpint <= 0xD7A3:
            continue

        mapping[cpint] = name

    # only 38 different chars are used for names.  we find substrings to use
    # with the other bytes
    letters: set[str] = set()
    for name in mapping.values():
        letters.update(name)
    assert len(letters) == 38, f"{sorted(letters)=} {len(letters)=}"

    # substitutions - byte to substring
    subs: dict[str, str | None] = {}

    for i in range(0, 256):
        subs[chr(i)] = chr(i) if chr(i) in letters else None

    # \0 doesn't occur in text
    original_text = "\0".join(mapping.values())

    # the order substitutions are applied because they can incorporate each other
    subs_order: list[str] = []

    while any(v is None for v in subs.values()):
        text = original_text

        for s in subs_order:
            text = text.replace(subs[s], s)

        substrings: collections.Counter[str] = collections.Counter()

        for s in text.split("\0"):
            for n in range(2, len(s) + 1):
                for pos in range(0, len(s) - n + 1):
                    substrings[s[pos : pos + n]] += 1

        best_savings, best_substring = -1, ""

        for substring, count in substrings.most_common():
            # can it possibly improve?
            if len(substring) * count < best_savings:
                continue
            savings = len(text) - len(text.replace(substring, " "))
            # we want better savings, or equal savings with longer fragment
            if savings > best_savings or (savings == best_savings and len(substring) > len(best_substring)):
                best_savings = savings
                best_substring = substring

        remaining = len(subs) - sum((0 if v is None else 1) for v in subs.values())
        if show_status:
            count = text.count(best_substring)
            print(f"{best_savings=} {best_substring=} {count=}  remaining slots = {remaining}")
        else:
            print(f"{int((256-remaining)/256*100):3}%\x08\x08\x08\x08", end="", flush=True)

        for s in subs:
            if subs[s] is None:
                subs[s] = best_substring
                subs_order.append(s)
                break
        else:
            raise Exception("unreachable")

        text = original_text
        for s in subs_order:
            text = text.replace(subs[s], s)

    if show_status:
        print(f"original size = {len(original_text):,}   substring replaced = {len(text):,}")

    MYNAME = os.path.basename(__file__)

    code: list[str] = []
    code.append("/* This section until corresponding end comment is generated from")
    code.append(f"   DerivedName.txt by {MYNAME} */")
    code.append("")
    code.append("")

    code.append("/* This array is 256 entries long where each entry is")
    code.append("   a byte indicating length and then the replacement text */")
    code.append("static const char * const name_subs[] = {")

    for text in subs.values():
        for s in reversed(subs_order):
            text = text.replace(s, subs[s])
        code.append(f'  "\\x{len(text):02x}" "{text}",')

    code.append("};")

    code.append("")
    code.append("")

    # codepoint to offset in packed bytes
    offsets: dict[int, int] = {}
    packed_bytes = ""

    for codepoint in sorted(mapping):
        offsets[codepoint] = len(packed_bytes)
        text = mapping[codepoint]
        for s in subs_order:
            text = text.replace(subs[s], s)
        text = chr(len(text)) + text
        packed_bytes += text

    code.append("/* The main table of names */")
    code.append("static const unsigned char name_table[] = {")
    code.append("   ")
    for c in packed_bytes:
        if len(code[-1]) > 116:
            code.append("   ")
        code[-1] += f" {ord(c)},"
    code.append("};")

    code.append("")
    code.append("")

    codepoint_ranges = []
    current = []
    for codepoint in sorted(mapping):
        if not current:
            current.append(codepoint)
            continue
        if (
            codepoint != current[-1] + 1
            or
            # this only affects 3 blocks avoiding making them huge
            offsets[codepoint] - offsets[current[0]] > 7800
        ):
            codepoint_ranges.append((current[0], current[-1]))
            current = [codepoint]
            continue
        current.append(codepoint)

    if current:
        codepoint_ranges.append((current[0], current[-1]))

    # forward declatation because we are included first
    code.append("static PyObject *name_expand(const unsigned char *name, unsigned skip);")
    code.append("static PyObject *regular_codepoint_to_name(Py_UCS4 codepoint)")
    code.append("{")
    code.append("   /* This could be done as a binary search, but is not a performance")
    code.append("      function so we don't worry */")
    code.append("")
    range_total = sum(1 + end - start for start, end in codepoint_ranges)
    code.append(f"   /* {len(codepoint_ranges)} ranges covering {range_total:,} codepoints */")
    for start, end in codepoint_ranges:
        code.append(f"   if(codepoint >= 0x{ start:04X} && codepoint <= 0x{ end:04X})")
        code.append(f"       return name_expand(name_table + {offsets[start]}, codepoint - 0x{start:04X});")
    code.append("    Py_RETURN_NONE;")
    code.append("}")

    code.append("")
    code.append("")

    # tags
    tag_range_start = 0xE0001
    tag_range_end = 0xE007F
    table = [mapping.pop(codepoint, "") for codepoint in range(tag_range_start, tag_range_end + 1)]
    code.append("/* name mapping for inline tags which appear near the end of the range */")
    code.append(f"#define TAG_RANGE_START 0x{tag_range_start:X}")
    code.append(f"#define TAG_RANGE_END   0x{tag_range_end:X}")
    code.append("")
    code.append("static const unsigned char tag_range_names[] = {")
    code.append("   ")
    for text in table:
        for s in subs_order:
            text = text.replace(subs[s], s)
        text = chr(len(text)) + text
        for c in text:
            if len(code[-1]) > 116:
                code.append("   ")
            code[-1] += f" {ord(c)},"
    code.append("};")

    code.append("")
    code.append("")

    numbered_total = 0
    for cprange, _ in numbered:
        start, end = [int(x, 16) for x in cprange.split("..")]
        numbered_total += 1 + end - start
    code.append(f"/* This handles the {len(numbered)} numbered suffix ranges")
    code.append(f"   covering { numbered_total:,} codepoints */")
    code2: list[str] = []
    code2.append("#define NAME_RANGES(codepoint)")
    # ranges where it is hex of the codepoint (most of them)
    hex_id = ("%04X", "")
    for cprange, pattern in sorted(numbered, key=lambda x: int(x[0].split("..")[0], 16)):
        number_format, adjust = {
            "3400..4DBF": hex_id,
            "4E00..9FFF": hex_id,
            "F900..FA6D": hex_id,
            "FA70..FAD9": hex_id,
            "FE00..FE0F": ("%d", " - 0xFE00 + 1"),
            "13460..143FA": hex_id,
            "17000..187F7": hex_id,
            "18800..18AFF": ("%03d", " - 0x18800 + 1"),
            "18B00..18CD5": hex_id,
            "18CFF..18CFF": hex_id,
            "18D00..18D08": hex_id,
            "1B170..1B2FB": hex_id,
            "20000..2A6DF": hex_id,
            "2A700..2B739": hex_id,
            "2B740..2B81D": hex_id,
            "2B820..2CEA1": hex_id,
            "2CEB0..2EBE0": hex_id,
            "2EBF0..2EE5D": hex_id,
            "2F800..2FA1D": hex_id,
            "30000..3134A": hex_id,
            "31350..323AF": hex_id,
            "E0100..E01EF": ("%d", " - 0xE0100 + 17"),
        }[cprange]
        start, end = [int(x, 16) for x in cprange.split("..")]
        code2.append(f"  if(codepoint >= 0x{start:04X} && codepoint <= 0x{end:04X})")
        assert pattern.endswith("-*")
        if (number_format, adjust) == hex_id:
            # py <=3.11 doesn't have %X (upper case) so we have to implement
            code2.append(f'    return name_with_hex_suffix("{pattern[:-1]}", codepoint);')
        else:
            code2.append(f'    return PyUnicode_FromFormat("{pattern[:-1]}{number_format}", codepoint{adjust});')

    max_len = max(len(c) for c in code2) + 1
    for c in code2[:-1]:
        code.append(c + " " * (max_len - len(c)) + "\\")
    code.append(code2[-1])
    code.append("")

    code.append(f"/* End of code from {MYNAME} */")

    # sanity check on tables we read
    max_cp_with_name = max(mapping)
    assert max_cp_with_name < 0xE0001

    if not show_status:
        print(" done")

    return code


if __name__ == "__main__":
    c = generate_names_code(open(sys.argv[1], "rt").read(), True)
    open("dbnames.c", "wt").write("\n".join(c))
