

"This does the break calculations which were developed in Python.  This module will be replaced by one written in C"

from __future__ import annotations

from . _tr29db import *


class TextIterator:
    state_fields = "pos", "accepted", "char", "lookahead"

    def __init__(self, text: str, offset: int, catfunc: Callable):
        self.saved = None
        self.text = text
        self.start = offset
        self.end = len(text)  # we allow pointing to one item beyond end
        self.pos = offset  # index we are currently examining but have not accepted yet
        self.catfunc = catfunc
        self.accepted = 0  # bitmask of accepted properties
        if offset < 0 or offset > self.end:
            raise ValueError(f"{offset=} is out of bounds 0 - { self.end }")
        if self.pos == self.end:
            self.char = self.lookahead = 0
        else:
            self.char = 0
            self.lookahead = self.catfunc(ord(self.text[self.pos]))

    def end_of_text(self) -> bool:
        return self.pos >= self.end

    def start_of_text(self) -> bool:
        return self.pos == self.start

    def begin(self):
        "used for speculative lookahead"
        assert self.saved is None
        self.saved = {k: getattr(self, k) for k in TextIterator.state_fields}

    def commit(self):
        "lookahead worked"
        self.saved = None

    def rollback(self):
        "undo lookahead"
        assert self.saved is not None
        for k, v in self.saved.items():
            setattr(self, k, v)
        self.saved = None
        return self.char, self.lookahead

    def advance(self) -> tuple[int, int]:
        "Returns tuple of current char and lookahead props"
        if self.end_of_text():
            raise ValueError("Trying to advance beyond end of text")
        if self.pos != self.start:
            self.accepted |= self.char
        self.char = self.lookahead
        self.pos += 1
        self.lookahead = self.catfunc(ord(self.text[self.pos])) if self.pos < self.end else 0
        return self.char, self.lookahead

    def absorb(self, match: int, extend: int = 0):
        """Advances while lookahead matches, keeping self.char, also taking zero or more extend following each match

        Used for various Extend matches"""
        if self.lookahead & match:
            char = self.char
            while self.lookahead & match:
                _, lookahead = self.advance()
                while lookahead & extend:
                    _, lookahead = self.advance()
            self.char = char
        return self.char, self.lookahead


def grapheme_next_break(text: str, offset: int = 0) -> int:

    it = TextIterator(text, offset, grapheme_category)

    # GB1 implicit

    # GB2
    if it.end_of_text():
        return it.pos

    while not it.end_of_text():
        assert it.saved is None, "Incomplete lookahead"
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

        # GB9a
        if lookahead & GC.SpacingMark:
            continue

        # GB9b
        if char & GC.Prepend:
            continue

        # GB9c
        if char & GC.InCB_Consonant and lookahead & (GC.InCB_Extend | GC.InCB_Linker):
            it.begin()
            seen_linker = lookahead & GC.InCB_Linker
            char, lookahead = it.advance()
            while lookahead & (GC.InCB_Extend | GC.InCB_Linker):
                seen_linker = seen_linker or lookahead & GC.InCB_Linker
                char, lookahead = it.advance()
            if seen_linker and lookahead & GC.InCB_Consonant:
                it.commit()
                continue
            char, lookahead = it.rollback()

        # GB11
        if char & GC.Extended_Pictographic and lookahead & (GC.Extend | GC.ZWJ):
            it.begin()
            while lookahead & GC.Extend:
                char, lookahead = it.advance()
            if lookahead & GC.ZWJ:
                char, lookahead = it.advance()
                if lookahead & GC.Extended_Pictographic:
                    it.commit()
                    continue
            char, lookahead = it.rollback()

        # GB9 - has to be after GB9c and GB11 because all InCB_Linker and InCB_Extend
        # are also extend
        if lookahead & (GC.Extend | GC.ZWJ):
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

def word_next_break(text: str, offset: int = 0) -> int:

    # From spec
    AHLetter = WC.ALetter | WC.Hebrew_Letter
    MidNumLetQ = WC.MidNumLet | WC.Single_Quote

    it = TextIterator(text, offset, word_category)

    # WB1 implicit

    # WB2
    if it.end_of_text():
        return it.pos

    while not it.end_of_text():
        char, lookahead = it.advance()

        # WB3
        if char & WC.CR and lookahead & WC.LF:
            it.advance()
            break

        # WB3a/b
        if char & (WC.Newline | WC.CR | WC.LF):
            # break before if any chars are accepted
            if it.accepted:
                return it.pos - 1
            # break after
            break

        # WB3c
        if char & WC.ZWJ and lookahead & WC.Extended_Pictographic:
            continue

        if lookahead & WC.ZWJ:
            it.begin()
            char, lookahead = it.advance()
            if lookahead & WC.Extended_Pictographic:
                it.advance()
                it.commit()
                continue
            char, lookahead = it.rollback()

        # WB3d
        if char == WC.WSegSpace and lookahead & WC.WSegSpace:
            continue

        # WB4
        if lookahead & (WC.Extend | WC.ZWJ | WC.Format):
            action = None
            while lookahead & (WC.Extend | WC.ZWJ | WC.Format):
                if lookahead & WC.ZWJ:
                    # Re-apply wb3c
                    it.begin()
                    _, lookahead = it.advance()
                    if lookahead & WC.Extended_Pictographic:
                        action = "continue"
                        it.commit()
                        break
                    else:
                        it.rollback()
                _, lookahead = it.advance()
            if action == "continue":
                continue
            assert action is None
            it.char = char  # ignore the extending chars

        # WB5
        if char & AHLetter and lookahead & AHLetter:
            continue

        # WB6/7
        if char & AHLetter and lookahead & (WC.MidLetter | MidNumLetQ):
            it.begin()
            char, lookahead = it.advance()
            char, lookahead = it.absorb(WC.Extend | WC.Format | WC.ZWJ)
            if lookahead & AHLetter:
                it.commit()
                continue
            char, lookahead = it.rollback()

        # WB7a
        if char & WC.Hebrew_Letter and lookahead & WC.Single_Quote:
            continue

        # WB7b/c
        if char & WC.Hebrew_Letter and lookahead & WC.Double_Quote:
            it.begin()
            char, lookahead = it.advance()
            if lookahead & WC.Hebrew_Letter:
                it.commit()
                continue
            char, lookahead = it.rollback()

        # WB8
        if char & WC.Numeric and lookahead & WC.Numeric:
            continue

        # WB9
        if char & AHLetter and lookahead & WC.Numeric:
            continue

        # WB10
        if char & WC.Numeric and lookahead & AHLetter:
            continue

        # WB11/12
        if char & WC.Numeric and lookahead & (WC.MidNum | MidNumLetQ):
            it.begin()
            char, lookahead = it.advance()
            char, lookahead = it.absorb(WC.Extend | WC.Format | WC.ZWJ)
            if lookahead & WC.Numeric:
                it.commit()
                continue
            char, lookahead = it.rollback()

        # WB13
        if char & WC.Katakana and lookahead & WC.Katakana:
            continue

        # WB13a
        if char & (AHLetter | WC.Numeric | WC.Katakana | WC.ExtendNumLet) and lookahead & WC.ExtendNumLet:
            continue

        # WB13b
        if char & WC.ExtendNumLet and lookahead & (AHLetter | WC.Numeric | WC.Katakana):
            continue

        # WB15/16
        if char & WC.Regional_Indicator and lookahead & WC.Regional_Indicator:
            char, lookahead = it.advance()
            it.absorb(WC.Extend | WC.ZWJ | WC.Format)
            break

        # WB999
        break

    return it.pos

def sentence_next_break(text: str, offset: int = 0) -> int:
    # From spec
    ParaSep = SC.Sep | SC.CR | SC.LF
    SATerm = SC.STerm | SC.ATerm

    it = TextIterator(text, offset, sentence_category)

    # SB1 implicit

    # SB2
    if it.end_of_text():
        return it.pos

    while not it.end_of_text():
        char, lookahead = it.advance()

        # SB3
        if char & SC.CR and lookahead & SC.LF:
            it.advance()
            break

        # SB4
        if char & ParaSep:
            break

        # SB5
        char, lookahead = it.absorb(SC.Format | SC.Extend)

        # SB6
        if char & SC.ATerm and lookahead & SC.Numeric:
            continue

        # SB7
        if char & (SC.Upper | SC.Lower) and lookahead & SC.ATerm:
            it.begin()
            it.advance()
            char, lookahead = it.absorb(SC.Format | SC.Extend)
            if lookahead & SC.Upper:
                it.commit()
                continue
            char, lookahead = it.rollback()

        # SB8
        if char & SC.ATerm:
            it.begin()
            it.absorb(SC.Close, SC.Format | SC.Extend)
            it.absorb(SC.Sp, SC.Format | SC.Extend)
            it.absorb(0xFFFFFFFF ^ SC.OLetter ^ SC.Upper ^ SC.Lower ^ ParaSep ^ SATerm)
            _, lookahead = it.absorb(SC.Format | SC.Extend)
            if lookahead & SC.Lower:
                it.absorb(SC.Format | SC.Extend)
                it.commit()
                continue
            char, lookahead = it.rollback()

        # SB8a
        if char & SATerm:
            it.begin()
            it.absorb(SC.Close, SC.Format | SC.Extend)
            _, lookahead = it.absorb(SC.Sp, SC.Format | SC.Extend)
            if lookahead & (SC.SContinue | SATerm):
                it.advance()
                it.absorb(SC.Format | SC.Extend)
                it.commit()
                continue
            char, lookahead = it.rollback()

        # SB9 / SB10 / SB11
        if char & SATerm:
            # This will result in a break with the rules to absorb
            # zero or more close then space, and one optional ParaSep
            it.absorb(SC.Close, SC.Format | SC.Extend)
            _, lookahead = it.absorb(SC.Sp, SC.Format | SC.Extend)
            if lookahead & ParaSep:
                # Process parasep in SB3/4 above
                continue
            break

        # SB999
        continue

    return it.pos

def category_name(which: str, codepoint: int) -> tuple(str):
    "Returns category names codepoint corresponds to"
    if which not in {"grapheme", "word", "sentence"}:
        raise ValueError("Parameter which should be one of grapheme, word, sentence")
    cats = []
    enum_class = {"grapheme": GC, "word": WC, "sentence": SC}[which]
    val = (grapheme_category if which == "grapheme" else word_category if which == "word" else sentence_category)(codepoint)
    for name, value in enum_class.__members__.items():
        if val & value:
            cats.append(name)

    return tuple(sorted(cats))
