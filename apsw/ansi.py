#!/usr/bin/env python3

"""
:mod:`apsw.ansi` - ANSI text graphic rendition handling

This handles text wrapping when there are ANSI terminal escape
sequences that change appearance such as foreground and background
colours, underlines etc.

The escape sequences are replaced by a zero width marker, layout done,
and then the sequence effects reinstated across the lines.

:meta private:
"""

from __future__ import annotations

import apsw.unicode

from typing import Literal, Any, Iterator

import dataclasses
import re

# Not supporting
#
# 10-20: font
# 21: double underline
# 26: proportional spacing

# This was picked by random.randint(0xe000, 0xf8ff).  ANSI graphics
# rendition sequences are replaced by this private use codepoint for
# doing text wrapping / line breaking.  It has been special cased as
# zero width, with PUA codepoints treated as class AL.  When
# outputting the resulting text, the codepoint is replaced by the
# original ANSI sequences.  PyUnicode strings are stored using the
# largest codepoint value in the string, so this range was picked to
# keep them to 2 bytes.
PUA_ANSI_CODEPOINT = chr(0xF25B)


@dataclasses.dataclass
class State:
    # these are boolean settings (on or off)
    italic: bool = False
    underline: bool = False
    inverse: bool = False
    conceal: bool = False
    strikethru: bool = False

    # these have a value
    bold: Literal[0] | Literal[1] | Literal[-1] = 0
    blink: Literal[0] | Literal[1] | Literal[2] = 0
    # encoded as what to output
    foreground: str | None = None
    background: str | None = None

    def __bool__(self):
        "Returns ``True`` if any state is set"
        return any(getattr(self, field.name) for field in dataclasses.fields(self))


StartState = State()

value_mapping: dict[int, tuple[str, Any]] = {
    1: ("bold", 1),
    2: ("bold", -1),
    3: ("italic", True),
    4: ("underline", True),
    5: ("blink", 1),
    6: ("blink", 2),
    7: ("inverse", True),
    8: ("conceal", True),
    9: ("strikethru", True),
    22: ("bold", 0),
    23: ("italic", False),
    24: ("underline", False),
    25: ("blink", 0),
    27: ("inverse", False),
    28: ("conceal", False),
    29: ("strikethru", False),
    39: ("foreground", None),
    49: ("background", None),
}


class AnsiTracker:
    def __init__(self, text: str):
        self.states: list[State] = []

        try:
            self.new_text = re.sub("\x1b\\[(?P<command>[0-9;]*)m", self.process_sequence, text)
        except (ValueError, IndexError):
            raise
            self.states = []
            self.new_text = None

        if self.new_text == text:
            self.new_text = None

    def process_sequence(self, match: re.Match[str]) -> str:
        state = self.states[-1] if self.states else State()

        parts = match.group("command").split(";")
        i = 0
        while i < len(parts):
            part = parts[i]
            i += 1
            if not part or part == "0":
                state = State()
                continue
            part = int(part)
            if part in value_mapping:
                field, value = value_mapping[part]
            elif 30 <= part <= 37 or 90 <= part <= 97:
                field = "foreground"
                value = str(part)
            elif part == 38:
                field = "foreground"
                if parts[i + 1] == "5":
                    value = ";".join(parts[i + 1 : i + 3])
                    i += 2
                elif parts[i + 1] == "2":
                    value = ";".join(parts[i + 1 : i + 5])
                    i += 4
                else:
                    raise ValueError(f"Unknown color encoding {parts[i-1:]}")
            elif 40 <= part <= 47 or 100 <= part <= 107:
                field = "background"
                value = str(part)
            elif part == 38:
                field = "background"
                if parts[i + 1] == "5":
                    value = ";".join(parts[i + 1 : i + 3])
                    i += 2
                elif parts[i + 1] == "2":
                    value = ";".join(parts[i + 1 : i + 5])
                    i += 4
                else:
                    raise ValueError(f"Unknown color encoding {parts[i-1:]}")
            else:
                raise ValueError(f"Unknown command {parts[i-1:]}")

            state = dataclasses.replace(state, **{field: value})
            continue

        self.states.append(state)
        return PUA_ANSI_CODEPOINT

    def update(self, previous_state: State, new_state: State) -> str:
        if previous_state == new_state:
            return ""
        if not new_state:
            return "\x1b[0m"
        out: list[str] = []

        for field in dataclasses.fields(new_state):
            name = field.name
            old = getattr(previous_state, name)
            new = getattr(new_state, name)
            if old == new:
                continue
            if name == "italic":
                out.append("3" if new else "23")
            elif name == "underline":
                out.append("4" if new else "24")
            elif name == "inverse":
                out.append("7" if new else "27")
            elif name == "conceal":
                out.append("8" if new else "28")
            elif name == "strikethru":
                out.append("9" if new else "29")
            elif name == "bold":
                out.append(("2", "22", "1")[new + 1])
            elif name == "blink":
                out.append(("25", "5", "6")[new])
            elif name == "foreground":
                out.append("39" if new is None else new)
            elif name == "background":
                out.append("49" if new is None else new)
            else:
                raise RuntimeError(f"code bug unhandled {field=}")

        return f"\x1b[{';'.join(out)}m"


def text_wrap(
    text: str, width: int = 70, *, tabsize: int = 8, hyphen: str = "-", combine_space: bool = True, invalid: str = "?"
) -> Iterator[str]:
    at = AnsiTracker(text)
    if not at.new_text:
        yield from apsw.unicode.text_wrap(
            text, width, tabsize=tabsize, hyphen=hyphen, combine_space=combine_space, invalid=invalid
        )
        return
    state_index = 0
    current_state = StartState
    for line in apsw.unicode.text_wrap(
        at.new_text, width, tabsize=tabsize, hyphen=hyphen, combine_space=combine_space, invalid=invalid
    ):
        out: list[str] = [at.update(StartState, current_state)]
        segment_start = 0
        len_line = len(line)
        line = line.rstrip(" ")
        trailing_spaces = " " * (len_line - len(line))
        while True:
            segment_end = line.find(PUA_ANSI_CODEPOINT, segment_start)
            if segment_end == -1:
                out.append(line[segment_start:])
                break
            out.append(line[segment_start:segment_end])
            new_state = at.states[state_index]
            out.append(at.update(current_state, new_state))
            state_index += 1
            current_state = new_state
            segment_start = segment_end + 1
        if trailing_spaces:
            # we don't want these on the trailing spaces as they look
            # really ugly
            if current_state.strikethru or current_state.underline:
                new_state = dataclasses.replace(current_state, strikethru=False, underline=False)
                out.append(at.update(current_state, new_state))
            out.append(trailing_spaces)
        out.append(at.update(current_state, StartState))
        yield "".join(out)


if __name__ == '__main__':
    import sys
    text = open(sys.argv[1], "rt").read()
    for line in text_wrap(text, 12):
        print(f"X| {line} |Y")
