#!/usr/bin/env python3

"""
An implementation of Unicode Text Segmentation
primarily intended for text search

https://www.unicode.org/reports/tr29/
"""


from __future__ import annotations

# This module is expected to be C in the future, so pretend these methods
# are present in this module
from _tr29db import *


def grapheme_next_length(text: str, offset: int = 0) -> int:
    """Returns how long a User Perceived Character is

    For example regional indicators are in pairs, and a base codepoint
    can be combined with zero or more additional codepoints providing
    diacritics, marks, and variations.

    :param text: The text to examine
    :param offset: The first codepoint to examine

    :returns:  How many codepoints make up one user perceived
      character.  You should extract ``text[offset:offset+len]``

    """
    lt = len(text)
    if offset < 0 or offset > lt:
        raise ValueError(f"{offset=} is out of bounds 0 - { lt }")

    # At end?
    if offset == lt:
        return 0

    # Only one char?
    if offset + 1 == lt:
        return 1

    # rules are based on lookahead so we use pos to indicate where we are looking
    char = ord(text[offset])
    lookahead = ord(text[offset + 1])

    # GB3
    if is_grapheme_CR(char) and is_grapheme_LF(lookahead):
        return 2

    # GB4/5
    if is_grapheme_Control(char) or is_grapheme_CR(char) or is_grapheme_LF(char):
        return 1

    # State machine for the rest
    pos = offset
    while pos < lt:
        # Do lookahead
        char = ord(text[pos])
        pos += 1
        try:
            lookahead = ord(text[pos])
        except IndexError:
            return pos - offset

        # GB9B
        if is_grapheme_Prepend(char):
            continue

        # GB9a/11
        if is_grapheme_ZWJ(lookahead) or is_grapheme_Extend(lookahead) or is_grapheme_SpacingMark(lookahead):
            pos += 1
            continue

        # GB12/13
        if is_grapheme_Regional_Indicator(char) and is_grapheme_Regional_Indicator(lookahead):
            # suck up the pair then repeat GB9/11
            pos += 1
            try:
                lookahead = ord(text[pos])
            except IndexError:
                return pos - offset
            if not (
                is_grapheme_ZWJ(lookahead)
                or is_grapheme_Extend(lookahead)
                or is_grapheme_Prepend(lookahead)
                or is_grapheme_SpacingMark(lookahead)
            ):
                return pos - offset
            continue

        # GB6
        if is_grapheme_L(char) and (
            is_grapheme_L(lookahead)
            or is_grapheme_V(lookahead)
            or is_grapheme_LV(lookahead)
            or is_grapheme_LVT(lookahead)
        ):
            continue

        # GB7
        if (is_grapheme_LV(char) or is_grapheme_V(char)) and (is_grapheme_V(lookahead) or is_grapheme_T(lookahead)):
            continue

        # GB8
        if (is_grapheme_LVT(char) or is_grapheme_T(char)) and is_grapheme_T(lookahead):
            continue

        # GB999
        return pos - offset

    return pos - offset


