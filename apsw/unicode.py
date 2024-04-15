#!/usr/bin/env python3

"""
Up to date Unicode aware methods and lookups

This module helps with :doc:`textsearch` and general Unicode,
addressing the following:

* The standand library :mod:`unicodedata` has limited information
  available (eg if a character is an emoji), and is only updated to
  new `Unicode versions
  <https://www.unicode.org/versions/enumeratedversions.html>`__ on a
  new Python version.

* Multiple consecutive codepoints can combine into a single user
  perceived character (grapheme cluster), such as combining accents,
  vowels and marks in some writing systems, variant selectors, joiners
  and linkers, etc.  That means you can't use indexes into
  :class:`str` safely without potentially breaking them.

* The standard library provides no help in splitting text into
  grapheme clusters, words, and sentences.

* Text processing is performance sensitive - FTS5 easily handles
  hundreds of megabytes to gigabytes of text, and so should this
  module.  It also affects the latency of each query as that is
  tokenized, and results can highlight words and sentences.

See :data:`unicode_version` for the implemented version.

Unicode lookups

   * Category information :func:`category`
   * Is an emoji or similar :func:`is_extended_pictographic`
   * Flag characters :func:`is_regional_indicator`

Case folding, accent removal

   * :func:`casefold` is used to do case insensitive comparisons.
   * :func:`strip` is used to remove accents, marks, punctuation,
     joiners etc

Helpers

These are aware of grapheme cluster boundaries which Python's builtin
string operations are not.  The text width functions take into account
how wide the text is when displayed on most terminals.

    * :func:`grapheme_length` to get the number of grapheme clusters
      in a string
    * :func:`grapheme_substr` to get substrings
    * :func:`grapheme_startswith` and :func:`grapheme_endswith`
    * :func:`grapheme_find` to find a substring
    * :func:`split_lines` to split text into lines using all the
      Unicode hard line break codepoints
    * :func:`text_width` to count how wide the text is
    * :func:`expand_tabs` to expand tabs using text width
    * :func:`text_width_substr` to extract substrings based text width
    * :func:`text_wrap` to wrap paragraphs using Unicode words, line
      breaking, and text width
    * :func:`guess_paragraphs` to establish paragraph boundaries

Grapheme cluster, word, and sentence splitting

    `Unicode Technical Report #29
    <https://www.unicode.org/reports/tr29/>`__ rules for finding
    grapheme clusters, words, and sentences are implemented.  Tr29
    specifies break points which can be found via
    :func:`grapheme_next_break`, :func:`word_next_break`, and
    :func:`sentence_next_break`.

    Building on those are iterators providing optional offsets and the
    text.  This is used for tokenization (getting character and word
    boundaries correct), and for result highlighting (showing
    words/sentences before and after match).

Line break splitting

    `Unicode Technical Report #14
    <https://www.unicode.org/reports/tr14/>`__ rules for finding where
    text cam be broken and resumed on the next line.  Tr14 specifies
    break points which can be found via :func:`line_break_next_break`.

    Building on those are iterators providing optional offsets and the
    text.  This is used for :func:`text_wrap`.

Size

    Using the `ICU <https://icu.unicode.org/>`__ `extension
    <https://pypi.org/project/PyICU/>`__ is 5MB of code that then
    links to shared libraries containing another 5MB of code, and 30MB
    of data.  This module is under 500KB, 5 to 50% faster, and has no
    dependencies.  (ICU includes numerous extra customisations,
    formatting, locale helpers etc.)

Performance

    There some pure Python alternatives, with less functionality.
    They take 5 to 15 times more CPU time to process the same text.
    Use `python3 -m apsw.unicode benchmark --help`.

"""

from __future__ import annotations

from typing import Generator

import re

### BEGIN UNICODE UPDATE SECTION ###
unicode_version = "15.1"
"""The `Unicode version <https://www.unicode.org/versions/enumeratedversions.html>`__
that the rules and data tables implement"""


class _Category:
    # Major category values - mutually exclusive
    Letter = 2**0
    Mark = 2**1
    Number = 2**2
    Other = 2**3
    Punctuation = 2**4
    Separator = 2**5
    Symbol = 2**6
    # Minor category values - note: their values overlap so tests must include equals")
    # To test for a minor, you must do like:"
    #     if codepoint & Letter_Upper == Letter_Upper ..."
    Letter_Lowercase = 2**7 | 2**0
    Letter_Modifier = 2**8 | 2**0
    Letter_Other = 2**9 | 2**0
    Letter_Titlecase = 2**10 | 2**0
    Letter_Uppercase = 2**11 | 2**0
    Mark_Enclosing = 2**7 | 2**1
    Mark_NonSpacing = 2**8 | 2**1
    Mark_SpacingCombining = 2**9 | 2**1
    Number_DecimalDigit = 2**7 | 2**2
    Number_Letter = 2**8 | 2**2
    Number_Other = 2**9 | 2**2
    Other_Control = 2**7 | 2**3
    Other_Format = 2**8 | 2**3
    Other_NotAssigned = 2**9 | 2**3
    Other_PrivateUse = 2**10 | 2**3
    Other_Surrogate = 2**11 | 2**3
    Punctuation_Close = 2**7 | 2**4
    Punctuation_Connector = 2**8 | 2**4
    Punctuation_Dash = 2**9 | 2**4
    Punctuation_FinalQuote = 2**10 | 2**4
    Punctuation_InitialQuote = 2**11 | 2**4
    Punctuation_Open = 2**12 | 2**4
    Punctuation_Other = 2**13 | 2**4
    Separator_Line = 2**7 | 2**5
    Separator_Paragraph = 2**8 | 2**5
    Separator_Space = 2**9 | 2**5
    Symbol_Currency = 2**7 | 2**6
    Symbol_Math = 2**8 | 2**6
    Symbol_Modifier = 2**9 | 2**6
    Symbol_Other = 2**10 | 2**6
    # Remaining non-category convenience flags
    Extended_Pictographic = 2**14
    Regional_Indicator = 2**15
    WIDTH_INVALID = 2**16
    WIDTH_TWO = 2**17
    WIDTH_ZERO = 2**18


### END UNICODE UPDATE SECTION ###

from . import _unicode

assert unicode_version == _unicode.unicode_version
_unicode_category = _unicode.category_category


def category(codepoint: int | str) -> str:
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


def is_extended_pictographic(text: str) -> bool:
    "Returns True if any of the text has the extended pictographic property (Emoji and similar)"
    return _unicode.has_category(text, 0, len(text), _Category.Extended_Pictographic)


def is_regional_indicator(text: str) -> bool:
    "Returns True if any of the text is one of the 26 `regional indicators <https://en.wikipedia.org/wiki/Regional_indicator_symbol>`__ used in pairs to represent country flags"
    return _unicode.has_category(text, 0, len(text), _Category.Regional_Indicator)


def casefold(text: str) -> str:
    """Returns the text for equality comparison without case distinction

    Case folding maps text to a canonical form where case differences
    are removed allowing case insensitive comparison.  Unlike upper,
    lower, and title case, the result is not intended to be displayed
    to people.
    """
    return _unicode.casefold(text)


def strip(text: str) -> str:
    """Returns the text for less exact comparison with accents, punctuation, marks etc removed

    It will strip diacritics leaving the underlying characters so ``áççéñțś`` becomes ``accents``,
    punctuation so ``e.g.`` becomes ``eg`` and ``don't`` becomes ``dont``,  marks so ``देवनागरी``
    becomes ``दवनगर``, as well as all spacing, formatting, `variation selectors
    <https://en.wikipedia.org/wiki/Variation_Selectors_%28Unicode_block%29>`__ and similar codepoints.

    Codepoints are also converted to their compatibility representation.  For example
    the single codepoint Roman numeral ``Ⅲ`` becomes ``III`` (three separate regular upper case `I`),
    and ``🄷🄴🄻🄻🄾`` becomes ``HELLO``.

    The resulting text should not be shown to people, and is intended for doing relaxed equality
    comparisons, at the expense of false positives when the accents, marks, punctuation etc were
    intended.

    You should do :func:`case folding <casefold>` after this.

    Emoji are preserved but variation selectors, `fitzpatrick <https://en.wikipedia.org/wiki/Emoji#Skin_color>`__
    and `joiners <https://en.wikipedia.org/wiki/Zero-width_joiner>`__ are stripped.

    `Regional indicators <https://en.wikipedia.org/wiki/Regional_indicator_symbol>`__ are preserved.
    """
    return _unicode.strip(text)


def split_lines(text: str, offset: int = 0) -> Generator[str, None, None]:
    """Each line, using hard line break rules

    This is a generator yielding a line at a time.  The end of line
    yielded will not include the hard line break characters.
    """
    lt = len(text)
    while offset < lt:
        end = _unicode.line_next_hard_break(text, offset)
        for hard in range(-1, offset - end - 1, -1):
            if ord(text[end + hard]) not in _unicode.hard_breaks:
                yield text[offset : end + hard + 1]
                break
        else:
            # it was entirely hard break chars
            yield ""
        offset = end


def expand_tabs(text: str, tabsize: int = 8) -> str:
    """Turns tabs into spaces aligning on tabsize boundaries, similar to :meth:`str.expandtabs`

    This is aware of grapheme clusters and text width.
    """
    if "\t" not in text:
        return text

    res: list[str] = []
    for line in line_break_iter(text):
        if "\t" not in line:
            res.append(line)
            continue
        clusters: list[str] = []
        pos: int = 0
        for gr in grapheme_iter(line):
            if gr != "\t":
                clusters.append(gr)
                pos += text_width(gr)
            else:
                incr = tabsize - (pos % tabsize)
                clusters.append(" " * incr)
                pos += incr

        res.append("".join(clusters))

    return "".join(res)


def grapheme_length(text: str, offset: int = 0) -> int:
    "Returns number of grapheme clusters in the text.  Unicode aware version of len"
    return _unicode.grapheme_length(text, offset)


def grapheme_substr(text: str, start: int | None = None, stop: int | None = None) -> str:
    """Like text[str:end] but in grapheme cluster units

    start and end can be negative to index from the end, or outside
    the bounds of the text but are never an invalid combination (you
    get empty string returned).

    To get a particular grapheme cluster make stop one more than start.
    For example to get the 3rd last grapheme cluster::

        grapheme_substr(text, -3, -3 + 1)
    """
    return _unicode.grapheme_substr(text, start, stop)


def grapheme_endswith(text: str, substring: str) -> bool:
    "Returns True if `text` ends with `substring` being aware of grapheme cluster boundaries"
    # match str.endswith
    if len(substring) == 0:
        return True

    if text.endswith(substring):
        # it must end with the same codepoints, but also has to start at
        # a grapheme cluster boundary
        expected = len(text) - len(substring)
        boundary = 0
        while boundary < expected:
            boundary = _unicode.grapheme_next_break(text, boundary)
        return boundary == expected

    return False


def grapheme_startswith(text: str, substring: str) -> bool:
    "Returns True if `text` starts with `substring` being aware of grapheme cluster boundaries"
    # match str.startswith
    if len(substring) == 0:
        return True

    if text.startswith(substring):
        # it must start with the same codepoints, but also has to end at
        # a grapheme cluster boundary
        expected = len(substring)
        boundary = 0
        while boundary < expected:
            boundary = _unicode.grapheme_next_break(text, boundary)
        return boundary == expected

    return False


def grapheme_find(text: str, substring: str, start: int = 0, end: int | None = None) -> int:
    """Returns the offset in text where substring can be found, being aware of grapheme clusters.
    The beginning and end of the substring have to be at a grapheme cluster boundary.

    :param start: Where in text to start the search (default beginning)
    :param end: Where to stop the search exclusive (default remaining text)
    :returns: offset into text, or -1 if not found or substring is zero length
    """
    # C version is 7.5X faster than Python version
    return _unicode.grapheme_find(text, substring, start, end if end is not None else len(text))


def text_width(text: str, offset: int = 0) -> int:
    """Returns how many columns the text would be if displayed in a terminal

    You should :func:`split_lines` first and then operate on each line
    separately.

    If the `text` contains new lines, control characters, and similar
    unrepresentable codepoints then minus 1 is returned.

    Terminals aren't entirely consistent with each other, and Unicode
    has many kinds of codepoints, and combinations.  Consequently this
    is right the vast majority of the time, but not always.

    Note that web browsers do variable widths even in monospaced
    sections like <pre> so they won't always agree with the terminal
    either.
    """
    # Some benchmarks in seconds running on 45MB of the UNDR
    # 8.34  wcwidth Python module
    # 8.14  The implementation of this in Python
    # 0.20  Calling libc wcswidth via ctypes
    # 0.14  The Python converted to C
    return _unicode.text_width(text, offset)


def text_width_substr(text: str, offset: int, width: int) -> tuple[int, str]:
    """Extracts substring width or less wide being aware of grapheme cluster boundaries

    :returns: A tuple of how wide the substring is, and the substring"""
    width_so_far = 0
    accepted = offset
    for _, end, grapheme in grapheme_iter_with_offsets(text, offset):
        seg_width = text_width(grapheme)
        if seg_width < 0:
            raise ValueError(f"text contains invalid codepoints {grapheme=}")
        if width_so_far + seg_width <= width:
            width_so_far += seg_width
            accepted = end
        else:
            break
        if width_so_far == width:
            break
    return width_so_far, text[offset:accepted]


def guess_paragraphs(text: str, tabsize: int = 8) -> str:
    """Given text that contains paragraphs containing newlines, guesses where the paragraphs end

    .. code-block:: output

        If you have text like this, where paragraphs have newlines in
        them, then each line gets wrapped separately by text_wrap.
        This function tries to guess where the paragraphs end.

        Blank lines like above are definite.
          Indented lines that continue preserving the indent
          are considered the same paragraph, and a change of indent
          (in or out) is a new paragraph.
        So this will be a new paragraph.

         * Punctuation/numbers at the start of line
           followed by indented text are considered the same
           paragraph
        2. So this ia new paragraph, while
           this line is part of the line above

        3. Optional numbers followed by punctuation then space
        - are considered new paragraphs

    """
    # regex to match what looks like an (optionally numbered) list
    # item
    list_item_re = r"^(?P<indent>\s*[0-9+=,\.*-]+\s+).*"

    # what we turn definite end of paragraph into
    parasep = "\u2029"

    # Force unicode end of line, form feed, next line to parasep
    text = text.replace("\u2028", parasep)
    text = text.replace("\u000d", parasep)
    text = text.replace("\u0085", parasep)

    # tabify
    text = expand_tabs(text, tabsize)

    # Fix Windows EOL
    text = text.replace("\r\n", "\n")

    # Any stray CR become parasep
    text = text.replace("\r", parasep)

    # Two newlines is definite
    text = text.replace("\n\n", parasep + parasep)

    paragraphs: list[str] = []

    def append_paragraph(p: list[str]) -> None:
        # appends the list of strings as a paragraph
        # but we have to strip any indent from second and
        # succeeding line
        not_first = [line.lstrip(" ") for line in p[1:]]
        paragraphs.append(" ".join([p[0]] + not_first))

    # each segment is one or more paragraphs
    for segment in text.split(parasep):
        if "\n" not in segment:
            paragraphs.append(segment)
            continue
        para: list[str] = []

        for line in segment.split("\n"):
            if not para:
                # this is definitely a new paragraph
                para.append(line)
                continue

            # optional spaces, followed by digits|punctuation followed by space
            # is considered a new paragraph as a list item.
            if re.match(list_item_re, line):
                if para:
                    append_paragraph(para)
                para = [line]
                continue

            # Does indent match previous line
            if len(line) - len(line.lstrip(" ")) == len(para[-1]) - len(para[-1].lstrip(" ")):
                para.append(line)
                continue

            # Does indent match previous line as a list item indent?
            mo = re.match(list_item_re, para[-1])
            if mo:
                if len(mo.group("indent")) == len(line) - len(line.lstrip(" ")):
                    para.append(line)
                    continue

            # new paragraph
            append_paragraph(para)
            para = [line]
            continue

        if para:
            append_paragraph(para)

    # turn back into newline as the expected delimiter
    return "\n".join(paragraphs) + "\n"


def text_wrap(
    text: str,
    width: int = 70,
    *,
    tabsize: int = 8,
    hyphen: str = "-",
    combine_space: bool = True,
    invalid: str = "?",
) -> Generator[str, None, None]:
    """Similar to :func:`textwrap.wrap` but Unicode grapheme cluster and line break aware

    .. note::

       Newlines in the text are treated as end of paragraph.  If your text has paragraphs
       with newlines in them, then :func:`guess_paragraphs` can help.

    :param text: string to process
    :param width: width of yielded lines, if rendered using a monospace font such as to a terminal
    :param tabsize: Tab stop spacing as tabs are expanded
    :param hyphen: Used to show a segment was broken because it was wider than `width`
    :param combine_space: Leading space on each (indent) is always preserved.  Other spaces where
          multiple occur are combined into one space.
    :param invalid: If invalid codepoints are encountered such as control characters and surrogates
          then they are replaced with this.
    """
    hyphen_width = text_width(hyphen)
    if hyphen_width + 1 >= width:
        raise ValueError(f"hyphen is too wide { hyphen_width } to allow content for width { width }")

    text = expand_tabs(text, tabsize)

    for line in split_lines(text):
        accumulated: list[str] = []
        line_width = 0
        indent = None
        space = False
        for segment in line_break_iter(line):
            if indent is None:
                indent = " " * (len(segment) - len(segment.lstrip(" "))) if segment[0] == " " else ""
                if len(indent) >= width - hyphen_width:
                    # make space for double width char if indent wider than width
                    indent = indent[: max(0, width - hyphen_width - 2)]
                accumulated = [indent]
                line_width = len(indent)
                if line_width:
                    if len(indent) != len(segment):  # there was spaces and text
                        segment = segment[line_width:]
                    else:
                        continue

            if combine_space:
                new_segment = segment.rstrip(" ")
                new_space = new_segment != segment
                # we want to prepend a space if the previous segment
                # ended in space
                segment = (" " if space else "") + new_segment
                space = new_space

            seg_width = text_width(segment)
            if seg_width < 0:
                # There was something invalid in there
                segment = "".join((c if text_width(c) >= 0 else invalid) for c in segment)
                seg_width = text_width(segment)

            while line_width + seg_width > width:
                if len(accumulated) == 1:  # only indent present
                    if combine_space and segment[0] == " ":
                        # we added a space, but don't need it on new line
                        segment = segment[1:]
                    # hyphenate too long
                    desired = width - hyphen_width - line_width
                    seg_width, substr = text_width_substr(segment, 0, desired)
                    if seg_width == 0:
                        # the first grapheme cluster is wider than desired which is
                        # 1 or 2 depending on indent
                        assert desired in {1, 2}
                        # we will display '*' instead for that first grapheme cluster
                        segment = grapheme_substr(segment, 1)
                        substr = "*" * desired
                    else:
                        segment = segment[len(substr) :]
                        if desired - seg_width:  # did we get less than asked for?
                            substr += " " * (desired - seg_width)
                    yield indent + substr + hyphen
                    accumulated = [indent]
                    line_width = len(indent)
                    seg_width = text_width(segment)
                    continue
                yield "".join(accumulated) + " " * (width - line_width)
                if combine_space and segment[0] == " ":
                    # we added a space, but don't need it on new line
                    segment = segment[1:]
                    seg_width -= 1
                accumulated = [indent]
                line_width = len(indent)
                continue
            accumulated.append(segment)
            line_width += seg_width
        if len(accumulated) == 1:
            # only indent
            yield " " * width
        else:
            yield "".join(accumulated) + " " * (width - line_width)


def version_added(codepoint: int | str) -> str | None:
    "Returns the unicode version the codepoint was added"
    return _unicode.version_added(codepoint)


version_dates = {
    # Extracted from https://www.unicode.org/history/publicationdates.html
    "15.1": (2023, 9, 12),
    "15.0": (2022, 9, 13),
    "14.0": (2021, 9, 14),
    "13.0": (2020, 3, 10),
    "12.1": (2019, 5, 7),
    "12.0": (2019, 3, 5),
    "11.0": (2018, 6, 5),
    "10.0": (2017, 6, 20),
    "9.0": (2016, 6, 21),
    "8.0": (2015, 6, 17),
    "7.0": (2014, 6, 16),
    "6.3": (2013, 9, 30),
    "6.2": (2012, 9, 26),
    "6.1": (2012, 1, 31),
    "6.0": (2010, 10, 11),
    "5.2": (2009, 10, 1),
    "5.1": (2008, 4, 4),
    "5.0": (2006, 7, 14),
    "4.1": (2005, 3, 31),
    # These releases have no day, so we use the first of the month
    "4.0": (2003, 4, 1),
    "3.2": (2002, 3, 1),
    "3.1": (2001, 3, 1),
    "3.0": (1999, 9, 1),
    "2.1": (1998, 5, 1),
    "2.0": (1996, 7, 1),
    "1.1": (1993, 6, 1),
    "1.0": (1991, 10, 1),
}
"""Release date (year, month, day) for each unicode version
intended for use with :meth:`version_added`"""


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
    return _unicode.grapheme_next_break(text, offset)


def grapheme_next(text: str, offset: int = 0) -> tuple[int, int]:
    "Returns span of next grapheme cluster"
    end = grapheme_next_break(text, offset)
    return offset, end


def grapheme_iter(text: str, offset: int = 0) -> Generator[str, None, None]:
    "Generator providing text of each grapheme cluster"
    lt = len(text)
    meth = _unicode.grapheme_next_break
    start = offset
    while offset < lt:
        offset = meth(text, offset)
        yield text[start:offset]
        start = offset


def grapheme_iter_with_offsets(text: str, offset: int = 0) -> Generator[tuple[int, int, str], None, None]:
    "Generator providing start, end, text of each grapheme cluster"
    lt = len(text)
    meth = _unicode.grapheme_next_break
    start = offset
    while offset < lt:
        offset = meth(text, offset)
        yield (start, offset, text[start:offset])
        start = offset


def word_next_break(text: str, offset: int = 0) -> int:
    """Returns end of next word or non-word

    Finds the next break point according to the `TR29 spec
    <https://www.unicode.org/reports/tr29/#Word_Boundary_Rules>`__.
    Note that the segment returned may be a word, or a non-word
    (spaces, punctuation etc).  Use :func:`word_next` to get words.

    :param text: The text to examine
    :param offset: The first codepoint to examine

    :returns:  Next break point
    """
    return _unicode.word_next_break(text, offset)


def _word_cats_to_mask(letter, number, emoji, regional_indicator):
    mask = 0
    if letter:
        mask |= _Category.Letter
    if number:
        mask |= _Category.Number
    if emoji:
        mask |= _Category.Extended_Pictographic
    if regional_indicator:
        mask |= _Category.Regional_Indicator
    return mask


def word_next(
    text: str,
    offset: int = 0,
    *,
    letter: bool = True,
    number: bool = True,
    emoji: bool = False,
    regional_indicator: bool = False,
) -> tuple[int, int]:
    """Returns span of next word

    A segment is considered a word based on the codepoints it contains and their category.
    Use the keyword arguments to include or exclude as needed:

    * letter
    * number
    * emoji (Extended_Pictographic in Unicode specs)
    * regional indicator - two character sequence for flags like 🇧🇷🇨🇦
    """

    mask = _word_cats_to_mask(letter, number, emoji, regional_indicator)
    lt = len(text)
    meth = _unicode.word_next_break
    catcheck = _unicode.has_category

    while offset < lt:
        end = meth(text, offset)
        if catcheck(text, offset, end, mask):
            return offset, end
        offset = end
    return offset, offset


def word_iter(
    text: str,
    offset: int = 0,
    *,
    letter: bool = True,
    number: bool = True,
    emoji: bool = False,
    regional_indicator: bool = False,
) -> Generator[str, None, None]:
    "Generator providing text of each word"

    mask = _word_cats_to_mask(letter, number, emoji, regional_indicator)
    lt = len(text)
    meth = _unicode.word_next_break
    catcheck = _unicode.has_category

    while offset < lt:
        end = meth(text, offset)
        if catcheck(text, offset, end, mask):
            yield text[offset:end]
        offset = end


def word_iter_with_offsets(
    text: str,
    offset: int = 0,
    *,
    letter: bool = True,
    number: bool = True,
    emoji: bool = False,
    regional_indicator: bool = False,
) -> Generator[str, None, None]:
    "Generator providing start, end, text of each word"

    mask = _word_cats_to_mask(letter, number, emoji, regional_indicator)
    lt = len(text)
    meth = _unicode.word_next_break
    catcheck = _unicode.has_category

    while offset < lt:
        end = meth(text, offset)
        if catcheck(text, offset, end, mask):
            yield (offset, end, text[offset:end])
        offset = end


def sentence_next_break(text: str, offset: int = 0) -> int:
    """Returns end of sentence location.

    Finds the next break point according to the `TR29 spec
    <https://www.unicode.org/reports/tr29/#Sentence_Boundary_Rules>`__.
    Note that the segment returned includes leading and trailing white space.

    :param text: The text to examine
    :param offset: The first codepoint to examine

    :returns:  Next break point
    """
    return _unicode.sentence_next_break(text, offset)


def sentence_next(text: str, offset: int = 0) -> tuple[int, int]:
    """Returns span of next sentence"""
    lt = len(text)
    meth = _unicode.sentence_next_break

    while offset < lt:
        end = meth(text, offset=offset)
        return offset, end
    return offset, offset


def sentence_iter(text: str, offset: int = 0) -> Generator[str, None, None]:
    "Generator providing text of each sentence"
    lt = len(text)
    meth = _unicode.sentence_next_break

    while offset < lt:
        end = meth(text, offset)
        yield text[offset:end]
        offset = end


def sentence_iter_with_offsets(text: str, offset: int = 0) -> Generator[tuple[int, int, str], None, None]:
    "Generator providing start, end, text of each sentence"
    lt = len(text)
    meth = _unicode.sentence_next_break

    while offset < lt:
        end = meth(text, offset)
        yield (offset, end, text[offset:end])
        offset = end


def line_break_next_break(text: str, offset: int = 0) -> int:
    """Returns next opportunity to break a line

    Finds the next break point according to the `TR14 spec
    <https://www.unicode.org/reports/tr14/#LB1>`__.

    :param text: The text to examine
    :param offset: The first codepoint to examine

    :returns:  Next break point
    """
    return _unicode.line_next_break(text, offset)


def line_break_next(text: str, offset: int = 0) -> tuple[int, int]:
    """Returns span of next line"""
    lt = len(text)
    meth = _unicode.line_next_break

    while offset < lt:
        end = meth(text, offset=offset)
        return offset, end
    return offset, offset


def line_break_iter(text: str, offset: int = 0) -> Generator[str, None, None]:
    "Generator providing text of each line"
    lt = len(text)
    meth = _unicode.line_next_break

    while offset < lt:
        end = meth(text, offset)
        yield text[offset:end]
        offset = end


def line_break_iter_with_offsets(text: str, offset: int = 0) -> Generator[tuple[int, int, str], None, None]:
    "Generator providing start, end, text of each line"
    lt = len(text)
    meth = _unicode.line_next_break

    while offset < lt:
        end = meth(text, offset)
        yield (offset, end, text[offset:end])
        offset = end


if __name__ == "__main__":
    import argparse
    import unicodedata
    import os
    import sys
    import atexit
    import apsw.fts

    width = 80
    if sys.stdout.isatty():
        width = os.get_terminal_size(sys.stdout.fileno()).columns

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
    p.add_argument("-v", default=False, action="store_true", dest="verbose", help="Show each line as it is tested")
    p.add_argument("--fail-fast", default=False, action="store_true", help="Exit on first test failure")
    p.add_argument(
        "--fail-codepoints-separator",
        default=" ",
        help="What to separate the list pf codepoints with on failure.  Useful for long test strings [%(default)s]",
    )
    p.add_argument("test", choices=("grapheme", "word", "sentence", "line_break"), help="What to test")
    # ::TODO:: auto download file if not provided
    p.add_argument(
        "file",
        help="break test text file.  They can be downloaded from https://www.unicode.org/Public/UCD/latest/ucd/auxiliary/",
        type=argparse.FileType("rt", encoding="utf8"),
    )

    p = subparsers.add_parser("show", help="Run against provided text")
    p.set_defaults(function="show")
    p.add_argument("show", choices=("grapheme", "word", "sentence", "line_break"), help="What to show [%(default)s]")
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

    p = subparsers.add_parser(
        "benchmark",
        help="Measure how long segmentation takes to iterate each segment",
    )
    p.set_defaults(function="benchmark")
    p.add_argument(
        "--size",
        type=float,
        default=50,
        help="How many million characters (codepoints) of text to use [%(default)s]",
    )
    p.add_argument("--seed", type=int, default=0, help="Random seed to use [%(default)s]")
    p.add_argument(
        "--others",
        help="A comma separated list of other packages to also benchmark.  Use 'all' to get all available ones.  Supported are grapheme, uniseg, pyicu",
    )
    p.add_argument(
        "text_file",
        type=argparse.FileType("rt", encoding="utf8"),
        help="""Text source to use.

        The provided text will be repeatedly duplicated and shuffled then
        appended until the sized amount of text is available.

        A suggested source of text is to download the Universal Declaration of
        Human Rights Corpus from https://www.nltk.org/nltk_data/ and
        concatenate all the files together.  It contains the same text
        in most written languages.
        """,
    )

    p = subparsers.add_parser(
        "textwrap",
        help="""Wrap text to fit the specified number of columns.  Each line output
will be padded with spaces to the width.""",
    )
    p.set_defaults(function="textwrap")
    p.add_argument(
        "--measurement",
        default="apsw.unicode",
        choices=["wcswidth-c", "wcswidth-py"],
        help="""Instead of using the builtin function for measuring how wide text is
use the C library function wcswidth, or use the wcwidth Python package wcswidth function""",
    )
    p.add_argument(
        "--invalid",
        default="?",
        help="Replacement for invalid codepoints such as control characters and surrogates [%(default)s]",
    )

    p.add_argument(
        "--width",
        type=int,
        default=width,
        help="How many columns to wrap to [%(default)s]",
    )
    p.add_argument("--tabsize", type=int, default=8, help="Tabstop size [%(default)s]")
    p.add_argument("--hyphen", default="-", help="Text to use when a segment is longer that width [%(default)s]")
    p.add_argument(
        "--no-combine-space",
        dest="combine_space",
        default=True,
        action="store_false",
        help="Disable combining multiple spaces into one.  Note that leading indents are always preserved",
    )
    p.add_argument(
        "--start", default="", help="Text output at the beginning of each line.  It counts against the width"
    )
    p.add_argument("--end", default="", help="Text output at the end of each line.  It counts against the width")
    p.add_argument(
        "--use-stdlib",
        default=False,
        action="store_true",
        help="Uses the system textwrap library instead.  hyphen is ignored and start/end are applied by this code.",
    )
    p.add_argument(
        "--guess-paragraphs",
        default=False,
        action="store_true",
        help="Guess if newlines in text are the same paragraphs.  See the doc for apsw.unicode.guess_paragraphs for details",
    )
    p.add_argument(
        "text_file",
        type=argparse.FileType("rt", encoding="utf8"),
        help="""Text source to use encoded in UTF8. Newlines are considered to delimit each paragraph, so consider --guess-paragraphs.
        Use a name of a single dash to read from standard input.""",
    )

    p = subparsers.add_parser(
        "breaktestgen",
        help="""Extracts data strings to be added to test suite""",
    )
    p.set_defaults(function="breaktestgen")
    p.add_argument("grapheme", type=argparse.FileType("rt", encoding="utf8"), help="Grapheme break test file")
    p.add_argument("word", type=argparse.FileType("rt", encoding="utf8"), help="Word break test file")
    p.add_argument("sentence", type=argparse.FileType("rt", encoding="utf8"), help="Sentence break test file")
    p.add_argument("line_break", type=argparse.FileType("rt", encoding="utf8"), help="Line break test file")

    options = parser.parse_args()

    def codepoint_details(kind, c: str, counter=None) -> str:
        if options.compact_codepoints:
            return f"U+{ord(c):04x}"
        name = unicodedata.name(c, "<NO NAME>")
        cat = category(ord(c))
        counter = f"#{counter}:" if counter is not None else ""
        name += f" ({ cat } { apsw.fts.unicode_categories[cat] })"
        uni_cat = " | ".join(_unicode.category_name(kind, ord(c)))
        return "{" + f"{counter}U+" + ("%04X" % ord(c)) + f" {name} : { uni_cat }" + "}"

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
            print("\n".join(text_wrap(" ".join(codepoints), width=options.width)))
            offset = end
            counter += 1

    elif options.function == "textwrap":
        # stop debug interpreter whining about file not being closed
        atexit.register(lambda: options.text_file.close())
        if options.measurement == "wcswidth-c":
            import ctypes
            import ctypes.util

            libc = ctypes.cdll.LoadLibrary(ctypes.util.find_library("c"))  # C library on Unix/Linux platforms
            if not hasattr(libc, "wcswidth"):
                sys.exit("C library does not have wcswidth function")

            libc.wcswidth.argtypes = [ctypes.c_wchar_p, ctypes.c_size_t]
            libc.wcswidth.restype = ctypes.c_int

            def text_width(text, offset=0):
                return libc.wcswidth(text[offset:], len(text) * 10)

        elif options.measurement == "wcswidth-py":
            import wcwidth

            def text_width(text, offset=0):
                return wcwidth.wcswidth(text[offset:])

        width = options.width
        width = width - text_width(options.start) - text_width(options.end)

        text = options.text_file.read()
        if options.guess_paragraphs:
            text = guess_paragraphs(text)

        if options.use_stdlib:
            import textwrap

            for line in textwrap.wrap(
                text,
                width,
                tabsize=options.tabsize,
                drop_whitespace=options.combine_space,
                replace_whitespace=False,
            ):
                for line in line.splitlines():
                    padding = max(0, width - text_width(line))
                    print(f"{options.start}{line}{' ' * padding}{options.end}")

        else:
            for line in text_wrap(
                text,
                width,
                tabsize=options.tabsize,
                hyphen=options.hyphen,
                combine_space=options.combine_space,
                invalid=options.invalid,
            ):
                print(f"{options.start}{line}{options.end}")

    elif options.function == "breaktest":
        # stop debug interpreter whining about file not being closed
        atexit.register(lambda: options.file.close())

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
            if options.verbose:
                print(f"{ line_num }: { orig_line.rstrip() }")
            expect = not_ok if options.test == "line_break" else ok
            assert line[0] == expect, f"Line { line_num } doesn't start with { expect }!"
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
                fails.append(options.fail_codepoints_separator.join(codepoints))
                fails.append("")

            offset = 0
            seen: list[int] = []
            lf = len(fails)
            while offset < len(text):
                span = next_break_func(text, offset)
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
            return unicodedata.name(chr(cp), "<NO NAME>")

        def deets(cp):
            cat = category(cp)
            return f"{ uniname(cp) } { cat }: { apsw.fts.unicode_categories[cat] }"

        for i, cp in enumerate(codepoints):
            print(f"#{ i } U+{ cp:04X} - { chr(cp) }")
            print(f"Name: { deets(cp) }")

            mangled = []
            for mangle in casefold(chr(cp)), strip(chr(cp)):
                if not mangle:
                    mangled.append("(nothing)")
                else:
                    mangled.append(", ".join(f"U+{ ord(v):04X} {uniname(ord(v))}" for v in mangle))
            print(f"casefold: { mangled[0] }   stripped: { mangled[1] }")
            print(
                f"Width: { text_width(chr(cp)) }  "
                f"TR29 grapheme: { ' | '.join(_unicode.category_name('grapheme', cp)) }   "
                f"word: { ' | '.join(_unicode.category_name('word', cp )) }   "
                f"sentence: { ' | '.join(_unicode.category_name('sentence', cp)) }  "
                f"TR14 line break: { ' | '.join(_unicode.category_name('line_break', cp)) }"
            )
            print()

    elif options.function == "benchmark":
        import random
        import time

        random.seed(options.seed)

        base_text = options.text_file.read()
        text = base_text

        # these are the non-ascii codepoints used in the various break tests
        interesting = "".join(
            chr(int(x, 16))
            for x in """0085 00A0 00AD 01BB 0300 0308 034F 0378 05D0 0600 062D 0631
                0644 0645 0646 064A 064E 0650 0651 0661 0671 06DD 070F 0710 0712 0717
                0718 0719 071D 0721 072A 072B 072C 0900 0903 0904 0915 0924 092F 093C
                094D 0A03 0D4E 1100 1160 11A8 200D 2018 2019 201C 201D 2060 231A 2701
                3002 3031 5B57 5B83 AC00 AC01 1F1E6 1F1E7 1F1E8 1F1E9 1F3FF 1F476
                1F6D1""".split()
        )

        # make interesting be 0.1% of base text
        base_text += interesting * int(len(interesting) / (len(base_text) * 0.001))

        tests: list[Any] = [
            (
                "apsw.unicode",
                unicode_version,
                (
                    ("grapheme", grapheme_iter),
                    ("word", word_iter),
                    ("sentence", sentence_iter),
                    ("line", line_break_iter),
                ),
            )
        ]

        if options.others:
            if options.others == "all":
                ok = []
                try:
                    import uniseg

                    ok.append("uniseg")
                except ImportError:
                    pass
                try:
                    import grapheme

                    ok.append("grapheme")
                except ImportError:
                    pass
                try:
                    import icu

                    ok.append("pyicu")
                except ImportError:
                    pass
                if ok:
                    options.others = ",".join(ok)
                else:
                    options.others = None

        if options.others:
            for package in options.others.split(","):
                package = package.strip()
                if package == "grapheme":
                    import grapheme
                    import grapheme.finder

                    tests.append(
                        ("grapheme", grapheme.UNICODE_VERSION, (("grapheme", grapheme.finder.GraphemeIterator),))
                    )
                elif package == "uniseg":
                    import uniseg
                    import uniseg.graphemecluster
                    import uniseg.wordbreak
                    import uniseg.sentencebreak
                    import uniseg.linebreak

                    # note that uniseg words doesn't determine which
                    # segments are words or not so you just get all
                    # segments

                    tests.append(
                        (
                            "uniseg",
                            uniseg.unidata_version,
                            (
                                ("grapheme", uniseg.graphemecluster.grapheme_clusters),
                                ("word", uniseg.wordbreak.words),
                                ("sentence", uniseg.sentencebreak.sentences),
                                ("line", uniseg.linebreak.line_break_units),
                            ),
                        )
                    )
                elif package == "pyicu":
                    import icu
                    import functools

                    # api only returns breakpoints, so make it match
                    # the others.  It also does its own utf16 based
                    # strings so there is some conversion overhead
                    def icu_iterate(kind, text):
                        icu_it = getattr(icu.BreakIterator, f"create{kind}Instance")(icu.Locale.getEnglish())
                        icu_str = icu.UnicodeString(text)
                        icu_it.setText(icu_str)
                        offset = 0
                        for pos in icu_it:
                            yield str(icu_str[offset:pos])
                            offset = pos

                    tests.append(
                        (
                            "pyicu",
                            icu.UNICODE_VERSION,
                            (
                                ("grapheme", functools.partial(icu_iterate, "Character")),
                                ("word", functools.partial(icu_iterate, "Word")),
                                ("sentence", functools.partial(icu_iterate, "Sentence")),
                                ("line", functools.partial(icu_iterate, "Line")),
                            ),
                        )
                    )

                else:
                    sys.exit(f"Unknown third party package to benchmark '{package}'")

        print(f"Expanding text to { options.size } million chars ...", end="", flush=True)
        while len(text) < options.size * 1_000_000:
            text += "".join(random.sample(base_text, len(base_text)))
        text = text[: int(options.size * 1_000_000)]

        print("\nResults in codepoints per second processed, returning each segment.  Higher is faster.")

        for name, version, parts in tests:
            print(f"\nBenchmarking {name:20s} unicode version { version }")

            for kind, func in parts:
                print(f"{kind:>8}", end=" ", flush=True)
                count = 0
                offset = 0
                start = time.process_time_ns()
                exc = None
                try:
                    for _ in func(text):
                        count += 1
                except Exception as exc2:
                    exc = exc2
                end = time.process_time_ns()
                if exc is not None:
                    print(f"       EXCEPTION {exc!r}")
                else:
                    seconds = (end - start) / 1e9
                    print(f"codepoints per second: { int(len(text)/seconds): 12,d}    segments: {count: 11,d}")

    elif options.function == "breaktestgen":
        # char used to mark ok and not in the files
        ok = "÷"
        not_ok = "\u00d7"

        def get_strings(fh):
            for line in fh:
                if not line.strip() or line.startswith("#"):
                    continue
                line = line.split("#")[0].strip().split()

                line.pop(0)  # remove initial marker
                line.pop(-1)  # and final

                text = ""
                while line:
                    c = line.pop(0)
                    if c == not_ok:
                        continue
                    elif c == ok:
                        text += c
                    else:
                        text += chr(int(c, 16))
                        assert text[-1] != "÷"
                yield text

        def fmt(text):
            res = ""
            for c in text:
                if category(c) in {"Lu", "Ll", "Nd", "Nl", "Pd", "Sm", "Sc", "So", "Zs"}:
                    res += c
                else:
                    c = ord(c)
                    if c <= 0xFFFF:
                        res += f"\\u{c:04X}"
                    else:
                        res += f"\\U{c:08X}"
            return '"' + res + '"'

        for name in ("grapheme", "word", "sentence", "line_break"):
            lines = list(get_strings(getattr(options, name)))

            lines.sort(key=lambda l: len(l))

            print(f'"{name}":')
            print("(")
            # we always take the shorted and longest
            print(fmt(lines.pop(0)), ",")
            print(fmt(lines.pop(-1)), ",")
            # and 20 of the rest
            for offset in range(len(lines) // 20, len(lines), len(lines) // 20):
                print(fmt(lines[offset]), ",")
            print("),")
