#!/usr/bin/env python3

from __future__ import annotations

"""
An implementation of Unicode Text Segmentation
primarily intended for text search

https://www.unicode.org/reports/tr29/
"""

def upc_len(text: str, offset: int = 0) -> int:
    """Returns how long a User Perceived Character is

    For example regional indicators are in pairs, and a base codepoint
    can be combined with zero or more additional codepoints providing
    diacritics, marks, and variations.

    :param text: The text to examine
    :param offset: The first codepoint to examine

    :returns:  How many codepoints make up one user perceived
      character.  You should extract ``text[offset:offset+len]``

    .. note::

       This does a reasonably good job, but does not do the entire
       algorithm due to needing additional tables.
    """
    # ::TODO:: implement this
    return 1