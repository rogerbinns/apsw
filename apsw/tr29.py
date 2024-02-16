#!/usr/bin/env python3

"""
An implementation of Unicode Text Segmentation
primarily intended for text search

https://www.unicode.org/reports/tr29/
"""


from __future__ import annotations

from typing import Callable, Any

# This module is expected to be C in the future, so pretend these methods
# are present in this module
from _tr29db import *


class TextIterator:
    def __init__(self, text: str, offset: int, catfunc: Callable, end_marker: Any):
        self.text = text
        self.start = offset
        self.end = len(text)  # we allow pointing to one item beyond end
        self.pos = offset  # index we are currently examining but have not accepted yet
        self.catfunc = catfunc
        self.end_marker = end_marker
        self.accepted = 0  # bitmask of accepted properties
        if offset < 0 or offset > self.end:
            raise ValueError(f"{offset=} is out of bounds 0 - { self.end }")
        if self.pos == self.end:
            self.char = self.lookahead = self.end_marker
        else:
            self.char = self.lookahead = self.catfunc(ord(self.text[self.pos]))

    def end_of_text(self) -> bool:
        return self.pos >= self.end

    def start_of_text(self) -> bool:
        return self.pos == self.start

    def has_accepted(self, cat) -> bool:
        return bool(self.accepted & cat)

    def peek(self, count: int):
        # 0 corresponds to current char, 1 to lookahead, -1 to behind current char etc
        offset = self.pos - 1 + count
        assert offset >= self.start and offset <= self.end
        if offset == self.end:
            return self.end_marker
        return self.catfunc(ord(self.text[offset]))

    def advance(self) -> tuple:
        "Returns tuple of current char and lookahead props"
        if self.end_of_text():
            raise ValueError("Trying to advance beyond end of text")
        if self.pos != self.start:
            self.accepted |= self.char
        self.char = self.lookahead
        self.pos += 1
        self.lookahead = self.catfunc(ord(self.text[self.pos])) if self.pos < self.end else self.end_marker
        return self.char, self.lookahead


def grapheme_next_break(text: str, offset: int = 0) -> int:
    """Returns end of Grapheme cluster /  User Perceived Character

    For example regional indicators are in pairs, and a base codepoint
    can be combined with zero or more additional codepoints providing
    diacritics, marks, and variations.  Break points are defined in
    the `TR29 spec
    <https://www.unicode.org/reports/tr29/#Grapheme_Cluster_Boundary_Rules>`__.

    :param text: The text to examine
    :param offset: The first codepoint to examine

    :returns:  Index of first codepoint not part of the grapheme cluster
        starting at offset. You should extract ``text[offset:span]``

    """

    it = TextIterator(text, offset, grapheme_category, GC.EOT)

    # GB1 implicit

    # GB2
    if it.end_of_text():
        return it.pos

    while not it.end_of_text():
        char, lookahead = it.advance()

        # GB3
        if char & GC.CR and lookahead & GC.LF:
            return it.pos + 1

        # GB4
        if char & (GC.Control | GC.CR | GC.LF):
            # break before if any chars are accepted
            if it.accepted:
                return it.pos - 1
            break

        # GB6
        if char & GC.L and lookahead & (GC.L | GC.V | GC.LV | GC.LVT):
            continue

        # GB7
        if char & (GC.LV | GC.V) and lookahead & (GC.V | GC.T):
            continue

        # GB8
        if char & (GC.LVT | GC.T) and lookahead & GC.T:
            continue

        # GB9 (InCB Extend and Linker chars are also marked extend)
        if lookahead & (GC.Extend | GC.InCB_Linker | GC.InCB_Extend | GC.ZWJ):
            continue

        # GB9a
        if lookahead & GC.SpacingMark:
            continue

        # GB9b
        if char & GC.Prepend:
            continue

        # GB9c
        if lookahead & GC.InCB_Consonant and it.has_accepted(GC.InCB_Consonant) and does_gb9c_apply(it):
            continue

        # GB11
        if (
            lookahead & GC.Extended_Pictographic
            and char & GC.ZWJ
            and it.has_accepted(GC.Extended_Pictographic)
            and does_gb11_apply(it)
        ):
            continue

        # GB12
        if char & GC.Regional_Indicator and lookahead & GC.Regional_Indicator:
            char, lookahead = it.advance()
            # re-apply GB9
            if lookahead & (GC.Extend | GC.ZWJ | GC.InCB_Extend):
                continue
            break

        # GB999
        break

    return it.pos


def does_gb9c_apply(it: TextIterator) -> bool:
    bare_linker_seen = False
    i = 0
    while True:
        cp = it.peek(i)
        i -= 1
        if cp & GC.InCB_Consonant:
            return bare_linker_seen
        if cp & GC.InCB_Linker:
            bare_linker_seen = True
            continue
        if cp & (GC.InCB_Extend | GC.ZWJ):
            continue
        return False


def does_gb11_apply(it: TextIterator) -> bool:
    # we are sitting at ZWJ and looking back
    # should only see Extend (zero or more) then
    # extended_pictographic
    assert it.char & GC.ZWJ
    i = -1
    while True:
        cp = it.peek(i)
        i -= 1
        if cp & (GC.Extend | GC.InCB_Extend):
            continue
        return cp & GC.Extended_Pictographic


def grapheme_next(text: str, offset: int = 0) -> tuple[int, int]:
    "Returns span of next grapheme cluster"
    start = offset
    end = grapheme_next_break(text, offset=offset)
    return start, end


def grapheme_iter(text: str, offset: int = 0):
    "Generator providing start, end, text of each grapheme cluster"
    while offset < len(text):
        start, end = grapheme_next(text, offset)
        yield (start, end, text[start:end])
        offset = end


def word_next_break(text: str, offset: int = 0) -> int:
    """Returns end of next word or non-word

    Finds the next break point according to the `TR29 spec
    <https://www.unicode.org/reports/tr29/#Word_Boundary_Rules>`__.
    Note that the segment returned may be a word, or a non-word.
    Use :func:`word_next` to get words.

    :param text: The text to examine
    :param offset: The first codepoint to examine

    :returns:  Next break point
    """

    # From spec
    AHLetter = WC.ALetter | WC.Hebrew_Letter
    MidNumLetQ = WC.MidNumLet | WC.Single_Quote

    it = TextIterator(text, offset, word_category, GC.EOT)

    # WB1 implicit

    # WB2
    if it.end_of_text():
        return it.pos

    while not it.end_of_text():
        char, lookahead = it.advance()

        # WB3
        if char == WC.CR and lookahead == WC.LF:
            continue

        # WB3a/b
        if char & (WC.Newline | WC.CR | WC.LF):
            # break before if any chars are accepted
            if it.accepted:
                return it.pos - 1
            # break after
            break

        # WB3c
        if char == WC.ZWJ and lookahead == WC.Extended_Pictographic:
            continue

        # WB3d
        if char == WC.WSegSpace and lookahead == WC.WSegSpace:
            continue

        # WB4
        ...

        # WB5
        if char & AHLetter and lookahead & AHLetter:
            continue

        # WB6
        if char & AHLetter and lookahead & (WC.MidLetter | MidNumLetQ):
            one_more = it.peek(2)
            if one_more & AHLetter:
                it.advance()
                continue

        # WB7
        if it.accepted and char & (WC.MidLetter & MidNumLetQ):
            last = it.peek(-1)
            if last & AHLetter and lookahead & AHLetter:
                continue

        # WB7a
        if char == WC.Hebrew_Letter and lookahead == WC.Single_Quote:
            continue

        # WB7b
        if char == WC.Hebrew_Letter and lookahead == WC.Double_Quote and it.peek(2) == WC.Hebrew_Letter:
            continue

        # WB7c
        if it.accepted & WC.Hebrew_Letter and char == WC.Double_Quote and lookahead == WC.Hebrew_Letter and it.peek(-1) == WC.Hebrew_Letter:
            continue

        # WB8
        if char == WC.Numeric and lookahead == WC.Numeric:
            continue

        # WB9
        if char & AHLetter and lookahead == WC.Numeric:
            continue

        # WB10
        if char == WC.Numeric and lookahead & AHLetter:
            continue

        # WB11
        if it.accepted and char & (WC.MidNum | MidNumLetQ) and lookahead == WC.Numeric and it.peek(-1) == WC.Numeric:
            continue

        # WB12
        if char == WC.Numeric and lookahead & (WC.MidNum | MidNumLetQ) and it.peek(2) == WC.Numeric:
            continue

        # WB13
        if char == WC.Katakana and lookahead == WC.Katakana:
            continue

        # WB13a
        if char & (AHLetter | WC.Numeric | WC.Katakana | WC.ExtendNumLet) and lookahead == WC.ExtendNumLet:
            continue

        # WB13b
        if char == WC.ExtendNumLet and lookahead & (AHLetter | WC.Numeric | WC.Katakana):
            continue

        # WB15/16
        if char == WC.Regional_Indicator and lookahead == WC.Regional_Indicator:
            char, lookahead = it.advance()
            # re-apply WB4
            if lookahead & (WC.Extend | WC.ZWJ | WC.Format):
                continue
            break

        # WB999
        break

    return it.pos


def word_next(text: str, offset: int = 0) -> tuple[int, int]:
    """Returns span of next word

    Words are determined by there being at least one of
    * letter
    * numeric
    """

    while offset < len(text):
        end = word_next_break(text, offset=offset)
        for c in text[offset:end]:
            if word_category(c) is WC.ALetter or word_category(c) is WC.Numeric:
                return offset, end
        offset = end
    return offset, offset


def word_iter(text: str, offset: int = 0):
    "Generator providing start, end, text of each word"
    while offset < len(text):
        start, end = word_next(text, offset)
        yield (start, end, text[start:end])
        offset = end


if __name__ == "__main__":
    import argparse
    import unicodedata
    import os
    import textwrap
    import sys
    import apsw.fts
    import apsw.ext

    width = 80
    if sys.stdout.isatty():
        width = os.get_terminal_size(sys.stdout.fileno()).columns

    # ::TODO:: benchmark to work out best bsearch parameter

    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(required=True)
    p = subparsers.add_parser("breaktest", help="Run Unicode test file")
    p.set_defaults(function="breaktest")
    p.add_argument("--fail-fast", default=False, action="store_true", help="Exit on first test failure")
    p.add_argument("test", choices=("grapheme", "word", "sentence"), help="What to test")
    # ::TODO:: auto download file if not provided
    p.add_argument(
        "file",
        help="break test text file.  They can be downloaded from https://www.unicode.org/Public/UCD/latest/ucd/auxiliary/",
        type=argparse.FileType("rt", encoding="utf8"),
    )

    p = subparsers.add_parser("show", help="Run against provided text")
    p.set_defaults(function="show")

    p.add_argument(
        "--show", default="grapheme", choices=("grapheme", "word", "sentence"), help="What to show [%(default)s]"
    )
    p.add_argument("--text-file", type=argparse.FileType("rt", encoding="utf8"))
    p.add_argument("--width", default=width, help="Output width [%(default)s]", type=int)
    p.add_argument("text", nargs="*", help="Text to segment unless --text-file used")

    p = subparsers.add_parser("codepoint", help="Show infornation about codepoints")
    p.add_argument("text", nargs="+", help="If a hex constant then use that value, otherwise treat as text")
    p.set_defaults(function="codepoint")

    options = parser.parse_args()

    def codepoint_details(c: str) -> str:
        try:
            name = unicodedata.name(c)
        except ValueError:
            name = "<NO NAME>"
        cat = unicodedata.category(c)
        name += f" ({ cat } { apsw.fts.unicode_categories[cat] })"
        tr29_cat = tr29_cat_func(ord(c)).name
        return "{U+" + ("%04X" % ord(c)) + f" {name} : { tr29_cat }" + "}"

    if options.function == "show":
        if not options.text_file and not options.text:
            p.error("You must specify at least --text-file or text arguments")

        text = ""
        if options.text:
            text += " ".join(options.text)
        if options.text_file:
            if text:
                text += " "
            text += options.text_file.read()

        next_func = globals()[f"{ options.show }_next"]
        tr29_cat_func = globals()[f"{ options.show }_category"]

        counter = 0
        offset = 0
        while offset < len(text):
            begin, end = next_func(text, offset)
            print(f"#{ counter } offset { offset } span { begin }-{ end } codepoints { end - begin }")
            codepoints = []
            for i in range(begin, end):
                codepoints.append(codepoint_details(text[i]))
            print("\n".join(textwrap.wrap(" ".join(codepoints), width=options.width)))
            offset = end

    elif options.function == "breaktest":
        next_break_func = globals()[f"{ options.test }_next_break"]
        tr29_cat_func = globals()[f"{ options.test }_category"]
        ok = "÷"
        not_ok = "\u00d7"
        fails: list[str] = []
        for line_num, line in enumerate(options.file, 1):
            orig_line = line
            if not line.strip() or line.startswith("#"):
                continue
            line = line.split("#")[0].strip().split()
            assert line[0] == ok, f"Line { line_num } doesn't start with { ok }!"
            assert line[-1] == ok, f"Line { line_num } doesn't end with { ok }!"
            line = line[1:]
            text = ""
            breaks = []
            while line:
                c = line.pop(0)
                if c == not_ok:
                    continue
                if c == ok:
                    breaks.append(len(text))
                    continue
                text += chr(int(c, 16))

            def add_failinfo():
                fails.append(orig_line.strip())
                codepoints = []
                for c in text:
                    codepoints.append(codepoint_details(c))
                fails.append(" ".join(codepoints))
                fails.append("")

            offset = 0
            seen: list[int] = []
            lf = len(fails)
            while offset < len(text):
                try:
                    span = next_break_func(text, offset)
                except:
                    apsw.ext.print_augmented_traceback(*sys.exc_info())
                    raise
                if span not in breaks:
                    fails.append(
                        f"Line { line_num } got unexpected break at { span } - expected are { breaks }.  Seen { seen }"
                    )
                    add_failinfo()
                    break
                seen.append(span)
                offset = span
            if options.fail_fast and fails:
                break
            if len(fails) != lf:
                continue
            if set(seen) != set(breaks):
                fails.append(f"Line { line_num } got breaks at { seen } expected at { breaks }")
                add_failinfo()
            if options.fail_fast and fails:
                break

        if fails:
            print(f"{ len(fails)//4 } tests failed:", file=sys.stderr)
            for fail in fails:
                print(fail, file=sys.stderr)
            sys.exit(2)

    elif options.function == "codepoint":
        codepoints = []
        for t in options.text:
            try:
                codepoints.append(int(t, 16))
            except ValueError:
                codepoints.extend(ord(c) for c in t)

        def uniname(cp):
            try:
                return unicodedata.name(chr(cp))
            except ValueError:
                return "<NO NAME>"

        def deets(cp):
            cat = unicodedata.category(chr(cp))
            return f"{ uniname(cp) } category { cat }: { apsw.fts.unicode_categories[cat] }"

        for i, cp in enumerate(codepoints):
            print(f"#{ i } U+{ cp:04X} - { chr(cp) }")
            print(f"unicodedata: { deets(cp) }")
            normalized = []
            for form in "NFD", "NFKD":
                if chr(cp) != unicodedata.normalize(form, chr(cp)):
                    normalized.append((form, unicodedata.normalize(form, chr(cp))))
            for norm, val in normalized:
                val = ", ".join(f"U+{ ord(v):04X} {uniname(ord(v))}" for v in val)
                print(f"{ norm }: { val }")
            print(
                f"TR29 grapheme: { grapheme_category(cp).name }   word: { word_category(cp).name }   sentence: { sentence_category(cp).name }"
            )
            print()
