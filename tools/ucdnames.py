#!/usr/bin/env python3

from __future__ import annotations

import collections
import sys


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


def generate_names_code(filename: str) -> str:
    # needs DerivedName.txt source

    # these have numeric suffix, although the number
    # varies (hex of codepoint, counter) and will
    # be generated from if statement, not name tables
    numbered = [
        ("18800..18AFF", "TANGUT COMPONENT-*"),
        ("FE00..FE0F", "VARIATION SELECTOR-*"),
        # unicode 16
        ("E0100..E01EF", "VARIATION SELECTOR-*"),
    ]

    # codepoint to name
    mapping: dict[int, str] = {}

    # parse data
    print("Reading", filename)
    for line in open(filename, "rt"):
        if not line.lstrip() or line.startswith("#"):
            continue

        codepoint, name = (p.strip() for p in line.split(";"))

        if name.endswith("-*"):
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
    letters = set()
    for name in mapping.values():
        letters.update(name)

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

        count = text.count(best_substring)
        print(
            f"{best_savings=} {best_substring=} {count=}  remaining slots =",
            len(subs) - sum((0 if v is None else 1) for v in subs.values()),
        )
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

    print(f"original size = {len(original_text):,}   substring replaced = {len(text):,}")

    code: list[str] = []

    code.append("/* This array is 256 entries long where each entry is")
    code.append("   a byte indicating length and then the replacement text */")
    code.append("static const char *name_subs[]= {")

    for sub, text in subs.items():
        for s in reversed(subs_order):
            text = text.replace(s, subs[s])
        code.append(f'  "\\x{len(text):02x}" "{text}",')

    code.append("};")

    return "\n".join(code)


c = generate_names_code(sys.argv[1])

print(c)

open("dbnames.h", "wt").write(c)
