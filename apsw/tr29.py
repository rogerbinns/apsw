#!/usr/bin/env python3

"""
`Unicode Technical Report #29
<https://www.unicode.org/reports/tr29/>`__ rules for finding user
perceived characters (grapheme clusters), words, and sentences from
Unicode text. Useful for full text search.  Stays :data:`up to date
<unicode_version>` with Unicode specifications and tables, and
includes some useful table lookup methods.

Multiple code points can combine into what is rendered as one
character, for example a base character and combining accents, writing
systems where consonants with vowels added around them, and emoji
sequences to adjust how they are shown.  Different languages have
different ways of separating words and sentences.  This Unicode
standard and rules implemented here deal with that complexity.
"""

from __future__ import annotations

from typing import Callable, Any

import enum

### BEGIN UNICODE UPDATE SECTION ###
unicode_version = "15.1"
"""The `Unicode version <https://www.unicode.org/versions/enumeratedversions.html>`__
that the rules and data tables implement"""

class _Category(enum.IntFlag):
    # Major category values - mutually exclusive
    Letter = 2 ** 0
    Mark = 2 ** 1
    Number = 2 ** 2
    Other = 2 ** 3
    Punctuation = 2 ** 4
    Separator = 2 ** 5
    Symbol = 2 ** 6
#   Minor category values - note: their values overlap so tests must include equals")
#   To test for a minor, you must do like:"
#       if codepoint & Letter_Upper == Letter_Upper ..."
    Letter_Lowercase = 2 ** 7 | 2 ** 0
    Letter_Modifier = 2 ** 8 | 2 ** 0
    Letter_Other = 2 ** 9 | 2 ** 0
    Letter_Titlecase = 2 ** 10 | 2 ** 0
    Letter_Uppercase = 2 ** 11 | 2 ** 0
    Mark_Enclosing = 2 ** 7 | 2 ** 1
    Mark_NonSpacing = 2 ** 8 | 2 ** 1
    Mark_SpacingCombining = 2 ** 9 | 2 ** 1
    Number_DecimalDigit = 2 ** 7 | 2 ** 2
    Number_Letter = 2 ** 8 | 2 ** 2
    Number_Other = 2 ** 9 | 2 ** 2
    Other_Control = 2 ** 7 | 2 ** 3
    Other_Format = 2 ** 8 | 2 ** 3
    Other_NotAssigned = 2 ** 9 | 2 ** 3
    Other_PrivateUse = 2 ** 10 | 2 ** 3
    Other_Surrogate = 2 ** 11 | 2 ** 3
    Punctuation_Close = 2 ** 7 | 2 ** 4
    Punctuation_Connector = 2 ** 8 | 2 ** 4
    Punctuation_Dash = 2 ** 9 | 2 ** 4
    Punctuation_FinalQuote = 2 ** 10 | 2 ** 4
    Punctuation_InitialQuote = 2 ** 11 | 2 ** 4
    Punctuation_Open = 2 ** 12 | 2 ** 4
    Punctuation_Other = 2 ** 13 | 2 ** 4
    Separator_Line = 2 ** 7 | 2 ** 5
    Separator_Paragraph = 2 ** 8 | 2 ** 5
    Separator_Space = 2 ** 9 | 2 ** 5
    Symbol_Currency = 2 ** 7 | 2 ** 6
    Symbol_Math = 2 ** 8 | 2 ** 6
    Symbol_Modifier = 2 ** 9 | 2 ** 6
    Symbol_Other = 2 ** 10 | 2 ** 6
    # Remaining non-category convenience flags
    Extended_Pictographic = 2 ** 14
    Regional_Indicator = 2 ** 15

### END UNICODE UPDATE SECTION ###

from . import _tr29py as _tr29
# from . import _tr29c as _tr29


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
    return _tr29.grapheme_next_break(text, offset)


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


def grapheme_length(text: str, offset: int = 0) -> int:
    "Returns number of grapheme clusters in the text.  Unicode aware version of len"
    # ::TODO:: convert to C
    count = 0
    while offset < len(text):
        offset = grapheme_next_break(text, offset)
        # ::TODO:: off by 1?
        count += 1
    return count


def grapheme_range(text: str, start: int = 0, end: int = -1) -> str:
    "Like text[str:end] but in grapheme cluster units"
    # ::TODO:: convert to C
    return text[start:end]


def grapheme_width(text: str, offset: int = 0) -> int:
    "Returns number of grapheme clusters in the text, counting wide ones as two"
    # ::TODO:: convert to C
    count = 0
    for start, end in grapheme_iter(text, offset):
        count += 2 if any(unicode_category(ord(text[i])) & Category.Wide for i in range(start, end)) else 1
    return count


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
    return _tr29.word_next_break(text, offset)


def word_next(
    text: str, offset: int = 0, *, letter=True, number=True, emoji=False, regional_indicator=False
) -> tuple[int, int]:
    """Returns span of next word

    A segment is considered a word based on the codepoints it contains and their category:

    * letter
    * number
    * emoji (Extended_Pictographic in Unicode specs)
    * regional indicator - two character sequence for flags like 🇧🇷🇨🇦
    """

    mask = 0
    if letter:
        mask |= _Category.Letter
    if number:
        mask |= _Category.Number
    if emoji:
        mask |= _Category.Extended_Pictographic
    if regional_indicator:
        mask |= _Category.Regional_Indicator

    while offset < len(text):
        end = word_next_break(text, offset=offset)
        for pos in range(offset, end):
            if _unicode_category(ord(text[pos])) & mask:
                return offset, end
        offset = end
    return offset, offset


def word_iter(text: str, offset: int = 0, *, letter=True, number=True, emoji=False, regional_indicator=False):
    "Generator providing start, end, text of each word"
    while offset < len(text):
        start, end = word_next(
            text, offset, letter=letter, number=number, emoji=emoji, regional_indicator=regional_indicator
        )
        yield (start, end, text[start:end])
        offset = end


def sentence_next_break(test: str, offset: int = 0) -> int:
    """Returns end of sentence location.

    Finds the next break point according to the `TR29 spec
    <https://www.unicode.org/reports/tr29/#Sentence_Boundary_Rules>`__.
    Note that the segment returned includes trailing white space.

    :param text: The text to examine
    :param offset: The first codepoint to examine

    :returns:  Next break point
    """
    return _tr29.sentence_next_break(text, offset)


def sentence_next(text: str, offset: int = 0) -> tuple[int, int]:
    """Returns span of next sentence"""
    while offset < len(text):
        end = sentence_next_break(text, offset=offset)
        return offset, end
    return offset, offset


def sentence_iter(text: str, offset: int = 0):
    "Generator providing start,end, text of each sentence"
    while offset < len(text):
        start, end = sentence_next(text, offset)
        yield (start, end, text[start:end].strip())
        offset = end


_unicode_category = _tr29.category_category


def unicode_category(codepoint: int) -> str:
    """Returns the `general category <https://en.wikipedia.org/wiki/Unicode_character_property#General_Category>`__ - eg ``Lu`` for Letter Uppercase

    See :data:`apsw.fts.unicode_categories` for descriptions mapping"""
    cat = _unicode_category(codepoint)
    if cat & _Category.Letter:
        if cat & _Category.Letter_Lowercase == _Category.Letter_Lowercase:
            return "Ll"
        if cat & _Category.Letter_Modifier == _Category.Letter_Modifier:
            return "Lm"
        if cat & _Category.Letter_Other == _Category.Letter_Other:
            return "Lo"
        if cat & _Category.Letter_Titlecase == _Category.Letter_Titlecase:
            return "Lt"
        if cat & _Category.Letter_Uppercase == _Category.Letter_Uppercase:
            return "Lu"
    if cat & _Category.Mark:
        if cat & _Category.Mark_Enclosing == _Category.Mark_Enclosing:
            return "Me"
        if cat & _Category.Mark_NonSpacing == _Category.Mark_NonSpacing:
            return "Mn"
        if cat & _Category.Mark_SpacingCombining == _Category.Mark_SpacingCombining:
            return "Mc"
    if cat & _Category.Number:
        if cat & _Category.Number_DecimalDigit == _Category.Number_DecimalDigit:
            return "Nd"
        if cat & _Category.Number_Letter == _Category.Number_Letter:
            return "Nl"
        if cat & _Category.Number_Other == _Category.Number_Other:
            return "No"
    if cat & _Category.Other:
        if cat & _Category.Other_Control == _Category.Other_Control:
            return "Cc"
        if cat & _Category.Other_Format == _Category.Other_Format:
            return "Cf"
        if cat & _Category.Other_NotAssigned == _Category.Other_NotAssigned:
            return "Cn"
        if cat & _Category.Other_PrivateUse == _Category.Other_PrivateUse:
            return "Co"
        if cat & _Category.Other_Surrogate == _Category.Other_Surrogate:
            return "Cs"
    if cat & _Category.Punctuation:
        if cat & _Category.Punctuation_Close == _Category.Punctuation_Close:
            return "Pe"
        if cat & _Category.Punctuation_Connector == _Category.Punctuation_Connector:
            return "Pc"
        if cat & _Category.Punctuation_Dash == _Category.Punctuation_Dash:
            return "Pd"
        if cat & _Category.Punctuation_FinalQuote == _Category.Punctuation_FinalQuote:
            return "Pf"
        if cat & _Category.Punctuation_InitialQuote == _Category.Punctuation_InitialQuote:
            return "Pi"
        if cat & _Category.Punctuation_Open == _Category.Punctuation_Open:
            return "Ps"
        if cat & _Category.Punctuation_Other == _Category.Punctuation_Other:
            return "Po"
    if cat & _Category.Separator:
        if cat & _Category.Separator_Line == _Category.Separator_Line:
            return "Zl"
        if cat & _Category.Separator_Paragraph == _Category.Separator_Paragraph:
            return "Zp"
        if cat & _Category.Separator_Space == _Category.Separator_Space:
            return "Zs"
    if cat & _Category.Symbol:
        if cat & _Category.Symbol_Currency == _Category.Symbol_Currency:
            return "Sc"
        if cat & _Category.Symbol_Math == _Category.Symbol_Math:
            return "Sm"
        if cat & _Category.Symbol_Modifier == _Category.Symbol_Modifier:
            return "Sk"
        if cat & _Category.Symbol_Other == _Category.Symbol_Other:
            return "So"

    raise Exception("Unreachable")


def unicode_is_extended_pictographic(codepoint: int) -> bool:
    "Returns True if the codepoint has the extended pictographic property (Emoji and similar)"
    return bool(_unicode_category(codepoint) & _Category.Extended_Pictographic)


def unicode_is_regional_indicator(codepoint: int) -> bool:
    "Returns True if the codepoint is one of the 26 `regional indicators <https://en.wikipedia.org/wiki/Regional_indicator_symbol>`__ used in pairs to represent country flags"
    return bool(_unicode_category(codepoint) & _Category.Regional_Indicator)

def text_wrap(text: str, width=70, *, initial_indent='', subsequent_indent='', max_lines=None, placeholder=' [...] ')-> str:
    "Like :func:`textwrap.wrap` but Unicode grapheme cluster and words aware"
    raise NotImplementedError()

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

    # ::TODO:: benchmark?

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-cc",
        "--compact-codepoints",
        dest="compact_codepoints",
        action="store_true",
        default=False,
        help="Only show hex codepoint values, not full details",
    )

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
    p.add_argument("show", choices=("grapheme", "word", "sentence"), help="What to show [%(default)s]")
    p.add_argument("--text-file", type=argparse.FileType("rt", encoding="utf8"))
    p.add_argument("--width", default=width, help="Output width [%(default)s]", type=int)
    p.add_argument(
        "--categories",
        default="letter,number",
        help="For word, which segments are included comma separated.  Choose from letter, number, emoji, regional_indicator. [%(default)s]",
    )
    p.add_argument("text", nargs="*", help="Text to segment unless --text-file used")

    p = subparsers.add_parser("codepoint", help="Show information about codepoints")
    p.add_argument("text", nargs="+", help="If a hex constant then use that value, otherwise treat as text")
    p.set_defaults(function="codepoint")

    options = parser.parse_args()

    def codepoint_details(kind, c: str, counter=None) -> str:
        if options.compact_codepoints:
            return f"U+{ord(c):04x}"
        name = unicodedata.name(c, "<NO NAME>")
        cat = unicode_category(ord(c))
        counter = f"#{counter}:" if counter is not None else ""
        name += f" ({ cat } { apsw.fts.unicode_categories[cat] })"
        tr29_cat = " | ".join(_tr29.category_name(kind, ord(c)))
        return "{" + f"{counter}U+" + ("%04X" % ord(c)) + f" {name} : { tr29_cat }" + "}"

    if options.function == "show":
        if not options.text_file and not options.text:
            p.error("You must specify at least --text-file or text arguments")

        params = {"letter": False, "number": False, "emoji": False, "regional_indicator": False}
        for c in options.categories.split(","):
            c = c.strip()
            if c not in params:
                p.error(f"Unknown word category '{c}'")
            params[c] = True

        if options.show != "word":
            params = {}

        text = ""
        if options.text_file:
            text += options.text_file.read()
        if options.text:
            if text:
                text += " "
            text += " ".join(options.text)

        next_func = globals()[f"{ options.show }_next"]

        counter = 0
        offset = 0
        while offset < len(text):
            begin, end = next_func(text, offset, **params)
            print(
                f"#{ counter } offset { offset } span { begin }-{ end } codepoints { end - begin } value: { text[begin:end] }"
            )
            codepoints = []
            for i in range(begin, end):
                codepoints.append(codepoint_details(options.show, text[i]))
            print("\n".join(textwrap.wrap(" ".join(codepoints), width=options.width)))
            offset = end
            counter += 1

    elif options.function == "breaktest":
        next_break_func = globals()[f"{ options.test }_next_break"]

        ok = "÷"
        not_ok = "\u00d7"
        passed: int = 0
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
                for counter, c in enumerate(text):
                    codepoints.append(codepoint_details(options.test, c, counter))
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
            passed += 1

        if fails:
            print(f"{ len(fails)//4 } tests failed, {passed:,} passed:", file=sys.stderr)
            for fail in fails:
                print(fail, file=sys.stderr)
            sys.exit(2)
        else:
            print(f"{passed:,} passed")

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
            cat = unicode_category(cp)
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
                f"TR29 grapheme: { ' | '.join(_tr29.category_name('grapheme', cp)) }   "
                f"word: { ' | '.join(_tr29.category_name('word', cp )) }   "
                f"sentence: { ' | '.join(_tr29.category_name('sentence', cp)) }"
            )
            print()
