#!/usr/bin/env python3

"Various classes and functions to work with full text search"

from __future__ import annotations

import sys
import unicodedata
import pathlib
import re
import fnmatch
import functools
import itertools
import difflib
import importlib
import multiprocessing
import multiprocessing.pool

# avoid clashing with html as a parameter name
import html as html_module
import html.parser as html_parser_module
import bisect
from dataclasses import dataclass

from typing import Callable, Sequence, Any, Literal

import apsw
import apsw.ext

unicode_categories = {
    "Lu": "Letter Uppercase",
    "Ll": "Letter Lowercase",
    "Lt": "Letter titlecase",
    "Lm": "Letter modifier",
    "Lo": "Letter other",
    "Mn": "Mark nonspacing",
    "Mc": "Mark spacing combining",
    "Me": "Mark enclosing",
    "Nd": "Number decimal digit",
    "Nl": "Number letter",
    "No": "Number other",
    "Pc": "Punctuation connector",
    "Pd": "Punctuation dash",
    "Ps": "Punctuation open",
    "Pe": "Punctuation close",
    "Pi": "Punctuation initial quote",
    "Pf": "Punctuation final quote",
    "Po": "Punctuation other",
    "Sm": "Symbol math",
    "Sc": "Symbol currency",
    "Sk": "Symbol modifier",
    "So": "Symbol other",
    "Zs": "Separator space",
    "Zl": "Separator line",
    "Zp": "Separator paragraph",
    "Cc": "Other control",
    "Cf": "Other format",
    "Cs": "Other surrogate",
    "Co": "Other private use",
    "Cn": "Other not assigned",
}
"Unicode categories and descriptions for reference"

tokenize_reasons: dict[str, int] = {
    "DOCUMENT": apsw.FTS5_TOKENIZE_DOCUMENT,
    "QUERY": apsw.FTS5_TOKENIZE_QUERY,
    "QUERY_PREFIX": apsw.FTS5_TOKENIZE_QUERY | apsw.FTS5_TOKENIZE_PREFIX,
    "AUX": apsw.FTS5_TOKENIZE_AUX,
}

"Mapping between friendly strings and constants for `xTokenize flags <https://www.sqlite.org/fts5.html#custom_tokenizers>`__"


def convert_tokenize_reason(value: str) -> set[int]:
    """Converts a space separated list of :data:`tokenize_reasons` into a set of corresponding values

    Use with :func:`parse_tokenizer_args`"""
    res: set[int] = set()
    for v in value.split():
        if v not in tokenize_reasons:
            raise ValueError(
                f"{ v } is not a tokenizer reason - valid values are { ' '.join(tokenize_reasons.keys()) }"
            )
        res.add(tokenize_reasons[v])
    return res


def convert_unicode_categories(patterns: str) -> set[str]:
    """Returns Unicode categories matching space separated values

    :func:`fnmatch.fnmatchcase` is used to check matches.  An example
    pattern is ``L* Pc`` would return ``{'Pc', 'Lm', 'Lo', 'Lu', 'Lt',
    'Ll'}``

    You can also put ! in front to exclude categories, so ``* !*m``
    would be all categories except those ending in ``m``.

    .. seealso:

        * :data:`unicode_categories`
        * `Wikipedia <https://en.wikipedia.org/wiki/Unicode_character_property#General_Category>`__

    """
    # Figure out categories expanding wild cards
    categories: set[str] = set()
    for cat in patterns.split():
        if cat in unicode_categories:
            categories.add(cat)
            continue
        negate = cat.startswith("!")
        if negate:
            cat = cat[1:]
        found = set(n for n in unicode_categories if fnmatch.fnmatchcase(n, cat))
        if not found:
            raise ValueError(f"'{ cat }' doesn't match any Unicode categories")
        if negate:
            categories -= found
        else:
            categories.update(found)
    return categories


def convert_string_to_python(expr: str) -> Any:
    """Converts a string to a Python object

    This is useful to process command line arguments and arguments to
    tokenizers.  It automatically imports the necessary modules.

    .. warning::

         The string is ultimately :func:`evaluated <eval>` allowing
         arbitrary code execution and side effects.

    Some examples of what is accepted are:

    * 3 + 4
    * apsw.fts.RegexTokenizer
    * snowballstemmer.stemmer("english").stemWord
    * nltk.stem.snowball.EnglishStemmer().stem
    * shutil.rmtree("a/directory/location")  **COULD DELETE ALL FILES**
    """
    parts = expr.split(".")
    imports = {}
    for i in range(1, len(parts)):
        try:
            name = ".".join(parts[:i])
            mod = importlib.import_module(name)
            imports[name] = mod
        except ImportError:
            pass
    return eval(expr, imports)


def convert_number_ranges(numbers: str) -> set[int]:
    """Converts comma separated number ranges

    Takes input like ``2,3-5,17` and converts to
    ``{2, 3, 4, 5, 17}``
    """
    res = set()
    for part in numbers.split(","):
        try:
            res.add(int(part))
            continue
        except ValueError:
            try:
                low, high = part.split("-", 1)
                low = int(low)
                high = int(high)
            except ValueError:
                raise ValueError(f"Unable to turn '{ part }' from '{ numbers }' into a numeric range")
            res.update(range(low, high + 1))
    return res


def tokenizer_test_strings(filename: str | pathlib.Path | None = None) -> tuple[tuple[bytes, str], ...]:
    """Provides utf-8 bytes sequences for interesting test strings

    :param filename: File to load.  If None then the builtin one is used

    :returns: A tuple where each item is a tuple of utf8 bytes and comment str

    The test file should be UTF-8 encoded text.

    If it starts with a # then it is considered to be multiple text sections
    where a # line contains a description of the section.  Any lines beginning
    ## are ignored."""
    # ::TODO:: importlib.resources should be used, but has deprecation galore, and
    # bad python version compatibility
    filename = filename or pathlib.Path(__file__).with_name("fts_test_strings")

    with open(filename, "rb") as f:
        data: bytes = f.read()
        if not data:
            return ((b"", "No data"),)
        if not data.startswith(b"#"):
            return ((data, pathlib.Path(filename).name),)

        test_strings: list[tuple[bytes, str]] = []
        lines = [line for line in data.splitlines() if not line.startswith(b"##")]
        while lines:
            comment = lines.pop(0)[1:].decode("utf8", errors="replace").strip()
            text: list[bytes] = []
            while lines and not lines[0].startswith(b"#"):
                text.append(lines.pop(0))
            test_strings.append((b"\n".join(text).rstrip(), comment))

    return tuple(test_strings)


def StringTokenizer(func: apsw.FTS5TokenizerFactory):
    """Decorator for tokenizers that operate on strings

    FTS5 tokenizers operate on UTF8 bytes for the text and offsets.  This
    decorator provides your tokenizer with text and expects text offsets
    back, performing the conversions for UTF8.
    """

    @functools.wraps(func)
    def string_tokenizer_wrapper(con: apsw.Connection, args: list[str]) -> apsw.Tokenizer:
        inner_tokenizer = func(con, args)

        @functools.wraps(inner_tokenizer)
        def outer_tokenizer(utf8: bytes, flags: int):
            text = utf8.decode("utf8", errors="replace")
            last_pos_bytes: int = 0
            last_pos_str: int = 0
            for start, end, *tokens in inner_tokenizer(text, flags):
                if start < last_pos_str:  # went backwards
                    last_pos_bytes = last_pos_str = 0
                if end < start:  # silently fix
                    end = start
                # utf8 bytes keeping track of last position
                utf8_start = len(text[last_pos_str:start].encode("utf8"))
                utf8_span = len(text[start:end].encode("utf8"))
                yield last_pos_bytes + utf8_start, last_pos_bytes + utf8_start + utf8_span, *tokens
                last_pos_bytes += utf8_start + utf8_span
                last_pos_str = end

        return outer_tokenizer

    return string_tokenizer_wrapper


@StringTokenizer
def PyUnicodeTokenizer(con: apsw.Connection, args: list[str]) -> apsw.Tokenizer:
    """Like the `unicode61 tokenizer <https://www.sqlite.org/fts5.html#unicode61_tokenizer>`__ but uses Python's Unicode database

    The SQLite unicode61 tokenizer uses a `Unicode character database
    <https://www.unicode.org/reports/tr44/>`__ from 2012, providing
    stable compatible behaviour, knowing about `250,000 codepoints
    <https://www.unicode.org/versions/stats/charcountv6_1.html>`__.

    This uses the :data:`version <unicodedata.unidata_version>` included
    in Python, which is updated on each release.  Python 3.12 released in
    2023 has version 15.0 of the Unicode database covering an
    additional `37,000 codepoints
    <https://www.unicode.org/versions/stats/charcountv15_0.html>`__.

    The following tokenizer arguments are accepted with the same semantics
    and defaults as `unicode61`.

    categories
        Space separated Unicode categories to consider part of tokens allowing wildcards.
        Default is ``L* N* Mc Mn`` to get all letters, numbers, and combining marks
    tokenchars
        String of characters always considered part of tokens, no matter the category.
        For example ``@.`` wouldn't break apart email addresses.
    separators
        String of characters always considered not part of tokens, no matter the category.
    single_token_categories
        (Not in unicode61) Any codepoint in this list of wildcard categories becomes a token by itself.
        For example ``So`` (Symbols other) includes emoji, which you probably want as individual
        tokens.

    Use the :func:`SimplifyTokenizer` to convert case, remove diacritics, combining marks, and
    use compatibility code points.
    """
    spec = {
        "categories": TokenizerArgument(
            default="L* N* Mc Mn", convertor=convert_unicode_categories, convert_default=True
        ),
        "tokenchars": "",
        "separators": "",
        "single_token_categories": TokenizerArgument(default="", convertor=convert_unicode_categories),
    }

    options = parse_tokenizer_args(spec, con, args)

    categories = options["categories"]
    tokenchars = set(options["tokenchars"])
    separators = set(options["separators"])
    single_token_categories = set(options["single_token_categories"])

    if tokenchars & separators:
        raise ValueError(f"Codepoints are in both tokens and separators { tokenchars & separators }")

    def tokenize(text: str, flags: int):
        start = None
        token = ""
        i = 0

        for i, codepoint in enumerate(text):
            cat = unicodedata.category(codepoint)
            if codepoint not in separators and cat in single_token_categories:
                if token:
                    yield start, i, token
                yield i, i + 1, codepoint
                token = ""
                start = None
                continue
            if (codepoint in tokenchars or cat in categories) and codepoint not in separators:
                if not token:
                    start = i
                token += codepoint
                continue
            if token:
                yield start, i, token
                token = ""
                start = None
        if token:
            yield start, i + 1, token

    return tokenize


def SimplifyTokenizer(con: apsw.Connection, args: list[str]) -> apsw.Tokenizer:
    """Tokenizer wrapper that simplifies tokens by case conversion, canonicalization, and codepoint removal

    Put this before another tokenizer to simplify its output.  For example:

       simplify case upper pyunicode

    The following tokenizer arguments are accepted, and are applied to each
    token in this order.

    case
        ``upper``, ``lower``, or ``casefold`` to convert case.  :meth:`casefold <str.casefold>` is recommended
    normalize
        Perform Unicode normalization - ``NFD`` ``NFC`` ``NFKD`` ``NFKC``.
        NFKD is recommended
    remove_categories
       Which codepoint categories to remove.  ``M* *m Sk`` is recommended
       to remove all marks, combining codepoints, and modifiers.
    """
    ta = TokenizerArgument
    spec = {
        "case": ta(choices=("upper", "lower", "casefold")),
        "normalize": ta(choices=("NFD", "NFC", "NFKD", "NFKC")),
        "remove_categories": ta(convertor=convert_unicode_categories),
        "+": None,
    }
    options = parse_tokenizer_args(spec, con, args)

    def identity(s: str):
        return s

    # ::TODO:: we want to normalize to NFKD to remove categories so
    # diacritics etc can go, but final should be NFKC
    case = getattr(str, options["case"]) if options["case"] else identity
    normalize = functools.partial(unicodedata.normalize, options["normalize"]) if options["normalize"] else identity
    remove_categories = options["remove_categories"]
    remove = (
        (lambda s: "".join(c for c in s if unicodedata.category(c) not in remove_categories))
        if remove_categories
        else identity
    )

    def tokenize(utf8: bytes, flags: int):
        tok = options["+"]
        for start, end, *tokens in tok(utf8, flags):
            new_tokens = tuple(t for t in (remove(normalize(case(token))) for token in tokens) if t)
            if new_tokens:
                yield start, end, *new_tokens

    return tokenize


@StringTokenizer
def NGramTokenizer(con: apsw.Connection, args: list[str]) -> apsw.Tokenizer:
    """Generates ngrams from the text, useful for completion as you type

    For example if doing 3 (trigram) then ``a big dog`` would result in
    ``'a b', ' bi', 'big', 'ig '`` etc.

    The following tokenizer arguments are accepted

    ngrams
        Numeric ranges to generate.  Smaller values allow showing
        completions with less input but a larger index, while larger
        values will result in quicker searches as the input grows.
        Default is 3.

    include_categories
        Which Unicode categories to include, by default all.  You could
        include everything except punctuation and separators with
        ``* !P* !Z*``

    .. seealso::

       :func:`NGramTokenTokenizer` for generating ngrams from another token source.
    """

    spec = {
        "ngrams": TokenizerArgument(default="3", convertor=convert_number_ranges, convert_default=True),
        "include_categories": TokenizerArgument(
            default="*", convertor=convert_unicode_categories, convert_default=True
        ),
    }

    options = parse_tokenizer_args(spec, con, args)

    def tokenize(text: str, flags: int):
        for start in range(len(text)):
            tokens: list[tuple[int, int, str]] = []
            seen = options["ngrams"].copy()
            end = start
            token = ""
            while seen and end < len(text):
                c = text[end]
                end += 1
                if unicodedata.category(c) not in options["include_categories"]:
                    continue
                token += c
                if len(token) in seen:
                    tokens.append((start, end, token))
                    seen.remove(len(token))
            if not tokens:
                continue
            # if doing a query, only produce largest possible
            if flags & apsw.FTS5_TOKENIZE_QUERY:
                yield tokens[-1][0], tokens[-1][1], tokens[-1][2]
            else:
                for start, end, token in tokens:
                    yield start, end, token

    return tokenize


def NGramTokenTokenizer(con: apsw.Connection, args: list[str]) -> apsw.Tokenizer:
    """Turns a token stream into ngrams, useful for completion as you type

    For example if doing 3 (trigram) then ``('a', 'deep', 'example')`` would result in
    ``('a', 'dee', 'eep', 'exa', 'xam', 'amp', 'mpl', 'ple')`` - ie ngrams are done
    of each token separately.  The entire token is also included in addition to the
    ngrams (before include_categories filtering).

    The following tokenizer arguments are accepted

    ngrams
        Numeric ranges to generate.  Smaller values allow showing
        completions with less input but a larger index, while larger
        values will result in quicker searches as the input grows.
        Default is 3.

    include_categories
        Which codepoints to include in ngrams based on Unicode categories, by default all.
        The token generator will likely already have excluded noise like spacing
        and punctuation.

    .. seealso::

       :func:`NGramTokenizer` for generating ngrams from the source text
    """

    spec = {
        "+": None,
        "ngrams": TokenizerArgument(default="3", convertor=convert_number_ranges, convert_default=True),
        "include_categories": TokenizerArgument(
            default="*", convertor=convert_unicode_categories, convert_default=True
        ),
    }

    options = parse_tokenizer_args(spec, con, args)

    if options["include_categories"] == convert_unicode_categories("*"):

        def transform(t: str):
            return t
    else:

        def transform(t: str):
            cats = options["include_categories"]
            return "".join(c for c in t if unicodedata.category(c) in cats)

    def tokenize(utf8: bytes, flags: int):
        for start, end, *tokens in options["+"](utf8, flags):
            new_tokens: list[str] = []
            for token in tokens:
                if token not in new_tokens:
                    new_tokens.append(token)
                token = transform(token)
                if not token:
                    continue
                for size in options["ngrams"]:
                    for s in shingle(token, size):
                        if s not in new_tokens:
                            new_tokens.append(s)

            if not new_tokens:
                continue

            # if doing a query, only produce largest possible
            if flags & apsw.FTS5_TOKENIZE_QUERY:
                biggest = max(len(token) for token in new_tokens)
                new_tokens = [token for token in new_tokens if len(token) == biggest]

            yield start, end, *new_tokens

    return tokenize


def SynonymTokenizer(get: Callable[[str], None | str | tuple[str]] | None = None) -> apsw.FTS5TokenizerFactory:
    """Adds `colocated tokens <https://www.sqlite.org/fts5.html#synonym_support>`__ such as 1st for first.

    To use you need a callable that takes a str, and returns a str, a sequence of str, or None.
    For example :meth:`dict.get` does that.

    The following tokenizer arguments are accepted.

    reasons
        Which tokenize :data:`tokenize_reasons` you want the lookups to happen in
        as a space separated list.  Default is ``DOCUMENT AUX``.

    get
        Specify a :func:`get <convert_string_to_python>`
    """

    @functools.wraps(get)
    def tokenizer(con: apsw.Connection, args: list[str]) -> apsw.Tokenizer:
        nonlocal get
        spec = {
            "reasons": TokenizerArgument(
                default="DOCUMENT AUX", convertor=convert_tokenize_reason, convert_default=True
            ),
            "+": None,
            **({} if get else {"get": TokenizerArgument(default=get, convertor=convert_string_to_python)}),
        }

        options = parse_tokenizer_args(spec, con, args)

        if "get" in options:
            get = options["get"]

        if get is None:
            raise ValueError("A callable must be provided by decorator, or parameter")

        def tokenize(utf8: bytes, flags: int):
            tok = options["+"]
            if flags not in options["reasons"]:
                yield from tok(utf8, flags)
                return

            for start, end, *tokens in tok(utf8, flags):
                new_tokens = []
                for t in tokens:
                    if t not in new_tokens:
                        new_tokens.append(t)

                    alt = get(t)
                    if alt:
                        if isinstance(alt, str):
                            if alt not in new_tokens:
                                new_tokens.append(alt)
                        else:
                            for t in alt:
                                if t not in new_tokens:
                                    new_tokens.append(t)
                yield start, end, *new_tokens

        return tokenize

    return tokenizer


def StopWordsTokenizer(test: Callable[[str], bool] | None = None) -> apsw.FTS5TokenizerFactory:
    """Removes tokens that are too frequent to be useful

    To use you need a callable that takes a str, and returns a boolean.  If
    ``True`` then the token is ignored.

    The following tokenizer arguments are accepted.

    test
        Specify a :func:`test <convert_string_to_python>`

    """

    @functools.wraps(test)
    def tokenizer(con: apsw.Connection, args: list[str]) -> apsw.Tokenizer:
        nonlocal test
        spec = {
            "+": None,
            **({} if test else {"test": TokenizerArgument(default=test, convertor=convert_string_to_python)}),
        }

        options = parse_tokenizer_args(spec, con, args)

        if "test" in options:
            test = options["test"]

        if test is None:
            raise ValueError("A callable must be provided by decorator, or parameter")

        def tokenize(utf8: bytes, flags: int):
            tok = options["+"]

            if flags == tokenize_reasons["QUERY_PREFIX"]:
                yield from tok(utf8, flags)
                return

            for start, end, *tokens in tok(utf8, flags):
                new_tokens = []
                for t in tokens:
                    if test(t):
                        # stop word - do nothing
                        pass
                    elif t not in new_tokens:
                        new_tokens.append(t)
                if new_tokens:
                    yield start, end, *new_tokens

        return tokenize

    return tokenizer


def TransformTokenizer(transform: Callable[[str], str | Sequence[str]] | None = None) -> apsw.FTS5TokenizerFactory:
    """Transforms tokens to a different token, such as stemming

    To use you need a callable that takes a str, and returns one
    or more str to replace it.

    The following tokenizer arguments are accepted.

    transform
        Specify a :func:`transform <convert_string_to_python>`

    """

    @functools.wraps(transform)
    def tokenizer(con: apsw.Connection, args: list[str]) -> apsw.Tokenizer:
        nonlocal transform
        spec = {
            "+": None,
            **({} if transform else {"transform": TokenizerArgument(convertor=convert_string_to_python)}),
        }

        options = parse_tokenizer_args(spec, con, args)
        if "transform" in options:
            transform = options["transform"]
        if transform is None:
            raise ValueError("A callable must be provided by decorator, or parameter")

        def tokenize(utf8: bytes, flags: int):
            tok = options["+"]

            for start, end, *tokens in tok(utf8, flags):
                new_tokens = []
                for t in tokens:
                    replacement = transform(t)
                    if isinstance(replacement, str):
                        if replacement not in new_tokens:
                            new_tokens.append(replacement)
                    else:
                        for r in replacement:
                            if r not in new_tokens:
                                new_tokens.append(r)
                if new_tokens:
                    yield start, end, *new_tokens

        return tokenize

    return tokenizer


@dataclass
class HTMLText:
    """Result of :func:`extract_html_text`"""

    html: str
    "Original HTML"
    text: str
    "Text extracted from the HTML"
    offsets: list[tuple[int, int]]
    "Maps offsets from extracted text back to corresponding offset in the original HTML"

    def text_offset_to_html_offset(self, offset: int) -> int:
        """Returns the html offset corresponding to the provided text offset

        You can ask for the offset one beyond the end of the text
        because that is needed for tokenizer end offsets.
        """
        if offset < 0 or offset > len(self.text):
            raise ValueError(f"offset { offset } is must be 0 to { len(self.text) } inclusive")
        index = bisect.bisect(self.offsets, (offset, -1))
        if self.offsets[index][0] == offset:
            return self.offsets[index][1]
        text, html = self.offsets[index - 1]
        return offset - text + html


def extract_html_text(html: str) -> HTMLText:
    """Extracts text from HTML using :class:`html.parser.HTMLParser` under the hood"""

    class _HTMLTextExtractor(html_parser_module.HTMLParser):
        # Extracts text from HTML maintaining a table mapping the offsets
        # of the extracted text back tot he source HTML.

        def __init__(self, html: str):
            # we handle charrefs because they are multiple codepoints in
            # the HTML but only one in text - eg "&amp;" is "&"
            super().__init__(convert_charrefs=False)
            # A stack is semantically correct but we (and browsers) don't
            # require correctly balanced tags, and a stack doesn't improve
            # correctness
            self.current_tag: str | None = None
            # each item in result_offsets is
            # - position in result text
            # - position in original text
            self.result_offsets: list[tuple[int, int]] = []
            self.result_text = ""
            # tracking position in source HTML
            self.original_pos = 0
            # svg content is ignored.
            self.svg_nesting_level = 0
            # we omit repeated whitespace because HTML contains a lot
            # of it that isn't part of the visible text.  however we
            # can't do that around entities because they are longer in
            # the html than the text (eg &#97; vs a) and mess up the
            # offsets which rely on one to one.  we have to emit an
            # offset for the entity and what comes next to bound the
            # entity correctly.
            self.can_ws_compress = False
            # All the work is done in the constructor
            self.feed(html)
            self.close()
            # make sure we have have ending offset to terminate forward offset scanning
            self.result_offsets.append((len(self.result_text) + 1, len(html) + 1))

        def append_result_text(self, s: str):
            if self.can_ws_compress and s.strip() == "" and self.result_text and self.result_text[-1].isspace():
                return
            self.result_offsets.append((len(self.result_text), self.original_pos))
            self.result_text += s
            self.can_ws_compress = True

        def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
            self.current_tag = tag.lower()
            if tag.lower() == "svg":
                self.svg_nesting_level += 1
            self.append_result_text(" ")

        def handle_endtag(self, tag: str) -> None:
            self.current_tag = None
            if tag.lower() == "svg":
                self.svg_nesting_level -= 1
            self.append_result_text(" ")

        def handle_data(self, data: str) -> None:
            if self.svg_nesting_level or self.current_tag in {"script", "style"}:
                return

            self.append_result_text(data)

        def handle_entityref(self, name: str):
            if self.svg_nesting_level:
                return
            self.can_ws_compress = False
            self.append_result_text(html_module.unescape("&" + name))
            self.can_ws_compress = False

        def handle_charref(self, name: str) -> None:
            self.handle_entityref("#" + name)

        # treat some other markup as white space
        def ws(self, *args: Any):
            if self.svg_nesting_level:
                return
            self.append_result_text(" ")

        handle_comment = handle_decl = handle_pi = unknown_decl = ws

        def updatepos(self, i: int, j: int) -> int:
            # sometimes it goes to zero or backwards for no reason!
            self.original_pos = max(self.original_pos, j)
            return super().updatepos(i, j)

    h = _HTMLTextExtractor(html)

    return HTMLText(html=html, text=h.result_text, offsets=h.result_offsets)


@StringTokenizer
def HTMLTokenizer(con: apsw.Connection, args: list[str]) -> apsw.Tokenizer:
    """Extracts text from HTML suitable for passing on to other tokenizers

    This should be the first tokenizer in the tokenizer list.  Behind the scenes
    it extracts text from the HTML, and manages the offset mapping between the
    HTML and the text passed on to other tokenizers.
    """
    spec = {"+": None}
    options = parse_tokenizer_args(spec, con, args)

    def tokenize(html: str, flags: int):
        # We only process html for the document/aux, not queries
        if flags & apsw.FTS5_TOKENIZE_QUERY:
            yield from string_tokenize(options["+"], html, flags)
            return

        h = extract_html_text(html)

        offset_map_position = 0
        last_start = 0
        for start, end, *tokens in string_tokenize(options["+"], h.text, flags):
            if start < last_start:  # handle going backwards
                offset_map_position = 0
            # advance start and get offset
            while start >= h.offsets[offset_map_position + 1][0]:
                offset_map_position += 1
            html_start: int = start - h.offsets[offset_map_position][0] + h.offsets[offset_map_position][1]

            # advance end and get offset
            while end >= h.offsets[offset_map_position + 1][0]:
                offset_map_position += 1
            html_end: int = end - h.offsets[offset_map_position][0] + h.offsets[offset_map_position][1]

            yield html_start, html_end, *tokens
            last_start = start

    return tokenize


def string_tokenize(tokenizer: apsw.FTS5Tokenizer, text: str, flags: int):
    """Tokenizer caller to get string offsets back

    Calls the tokenizer doing the conversion of `text` to UTF8, and converting the received
    UTF8 offsets back to `text` offsets.
    """
    utf8 = text.encode("utf8")
    last_pos_str = 0
    for start, end, *tokens in tokenizer(utf8, flags):
        if start < last_pos_str:  # went backwards
            last_pos_str = 0
        if end < start:  # silently fix
            end = start

        # ::TODO:: optimise this like string_tokenizer
        bytes_start, bytes_end = (
            len(utf8[:start].decode("utf8", errors="replace")),
            len(utf8[:end].decode("utf8", errors="replace")),
        )
        yield (
            bytes_start,
            bytes_end,
            *tokens,
        )
        last_pos_str = start


def RegexTokenizer(
    con: apsw.Connection, args: list[str], *, pattern: str | re.Pattern, flags: int = re.NOFLAG
) -> apsw.Tokenizer:
    r"""Finds tokens using a regular expression

    :param pattern: The `regular expression <https://docs.python.org/3/library/re.html#regular-expression-syntax>`__.
        For example :code:`\w+` is all alphanumeric and underscore characters.
    :param flags: `Regular expression flags <https://docs.python.org/3/library/re.html#flags>`__.
       Ignored if `pattern` is an already compiled pattern

    To use:

    .. code:: python

        pattern = r"\d+" # digits
        flags = re.ASCII # only ascii recognised
        tokenizer = functools.partial(apsw.fts.RegexTokenizer, pattern=pattern, flags=flags)
        connection.register_fts5_tokenizer("my_name", tokenizer)

    """
    if not isinstance(pattern, re.Pattern):
        pattern = re.compile(pattern, flags)

    spec = {}

    options = parse_tokenizer_args(spec, con, args)

    def tokenize(text: str, flags: int):
        for match in re.finditer(pattern, text):
            yield *match.span(), match.group()

    return tokenize


@dataclass
class TokenizerArgument:
    "Used as spec values to :func:`parse_tokenizer_args`"

    default: Any = None
    "Value - set to default before parsing"
    choices: Sequence[Any] | None = None
    "Value must be one of these, after conversion"
    convertor: Callable[[str], Any] | None = None
    "Function to convert string value to desired value"
    convert_default: bool = False
    "True if the default value should be run through the convertor"


def parse_tokenizer_args(
    spec: dict[str, TokenizerArgument | Any], con: apsw.Connection, args: list[str]
) -> dict[str, Any]:
    """Parses the arguments to a tokenizer based on spec returning corresponding values

    :param spec: A dictionary where the key is a string, and the value is either
       the corresponding default, or :class:`TokenizerArgument`.
    :param con: Used to lookup other tokenizers
    :param args: A list of strings as received by :class:`apsw.FTS5TokenizerFactory`

    For example to parse  ``["arg1", "3", "big", "ship", "unicode61", "yes", "two"]``

    .. code:: python

        # spec on input
        {
            # Converts to integer
            "arg1": TokenizerArgument(convertor=int, default=7),
            # Limit allowed values
            "big": TokenizerArgument(choices=("ship", "plane")),
            # Accepts any string, with a default
            "small": "hello",
            # gathers up remaining arguments, if you intend
            # to process the results of another tokenizer
            "+": None
        }

        # options on output
        {
            "arg1": 3,
            "big": "ship",
            "small": "hello",
            "+": db.Tokenizer("unicode61", ["yes", "two"])
        }

        # Using "+" in your ``tokenize`` functions
        def tokenize(utf8, flags):
            tok = options["+"]
            for start, end, *tokens in tok(utf8, flags):
                # do something
                yield start, end, *tokens

    .. seealso:: Some useful convertors

        * :func:`convert_unicode_categories`
        * :func:`convert_tokenize_reason`
        * :func:`convert_string_to_python`
        * :func:`convert_number_ranges`

    """
    options: dict[str, Any] = {}
    ac = args[:]
    while ac:
        n = ac.pop(0)
        if n not in spec:
            if "+" not in spec:
                raise ValueError(f"Unexpected parameter name { n }")
            options["+"] = con.fts5_tokenizer(n, ac)
            ac = []
            break
        if not ac:
            raise ValueError(f"Expected a value for parameter { n }")
        v = ac.pop(0)
        if isinstance(spec[n], TokenizerArgument):
            ta = spec[n]
            try:
                if ta.convertor is not None:
                    v = ta.convertor(v)
            except Exception as e:
                if hasattr(e, "add_note"):
                    e.add_note(f"Processing parameter { n } with value '{ v }'")
                raise
            if ta.choices is not None:
                if v not in ta.choices:
                    raise ValueError(f"Parameter { n } value {v!r} was not allowed choice { ta.choices }")
        options[n] = v

    assert len(ac) == 0
    for k, v in list(spec.items()):
        if k not in options and k != "+":
            if isinstance(v, TokenizerArgument):
                options[k] = v.default if not v.convert_default else v.convertor(v.default)
            else:
                options[k] = v

    if "+" in spec and "+" not in options:
        if spec["+"] is not None:
            options["+"] = spec["+"]
        else:
            raise ValueError("Expected additional tokenizer and arguments")

    return options


class FTS5Table:
    """A helpful wrapper around a FTS5 table  !!! Current experiment & thinking

    The table must already exist.  You can use the class method
    :meth:`create` to create a new FTS5 table.
    """

    @dataclass
    class _cache:
        token_data_version: int = -1
        tokens: set[str] = None

    def __init__(self, db: apsw.Connection, name: str, schema: str = "main"):
        if not db.table_exists(schema, name):
            raise ValueError(f"{ schema }.{ name } doesn't exist")
        # ::TODO:: figure out if it is fts5 table
        self.db = db
        self.name = name
        self.schema = schema
        # ::TODO:: figure out if name and schema need quoting and generate
        # self.qname self.qschema
        self._cache: FTS5Table._cache = FTS5Table._cache()

    @functools.cached_property
    def columns(self) -> tuple[str]:
        "Columns of this table"
        return self.db.execute("select name from { self.schema }.table_info(?)", (self.name,)).get

    def query(self, query: str) -> apsw.Cursor:
        "Returns a cursor making the query - rowid first"
        # ::TODO:: it appears you need to do some processing of the results
        # to avoid duplicate rows or something
        # https://sqlite-utils.datasette.io/en/latest/python-api.html#building-sql-queries-with-table-search-sql
        # https://news.ycombinator.com/item?id=38664366
        pass

    def fuzzy_query(self, query: str) -> apsw.Cursor:
        """Returns a cursor making the query - rowid first

        Not all the tokens have to be present in the matched docs"""
        # :TODO: parse query and turn all implicit and explicit AND into
        # OR.  Then add ranking function that takes into account missing
        # tokens in scoring
        return self.db.execute("select rowid, * from { self.schema }.{ self.name }(?) order by rank", (query,))

    def insert(self, *args: str, **kwargs: str) -> None:
        """Does insert with columns by positional or named via kwargs

        * empty string for missing columns?
        * Uses normalize option on all values
        * auto-stringize each value too?
        """
        ...

    # some method helpers pattern, not including all of them yet

    def command_delete(self, *args):
        "Does https://www.sqlite.org/fts5.html#the_delete_command"
        pass

    def command_delete_all(self, *args):
        "Does https://www.sqlite.org/fts5.html#the_delete_all_command"
        pass

    def command_optimize(self):
        "Does https://www.sqlite.org/fts5.html#the_optimize_command"
        pass

    def config_pgsz(self, val: int):
        "Does https://www.sqlite.org/fts5.html#the_pgsz_configuration_option"
        pass

    def command_rebuild(self):
        "Does https://www.sqlite.org/fts5.html#the_rebuild_command"
        pass

    def config(self, name, value=None):
        "Does https://www.sqlite.org/fts5.html#configuration_options_config_table_"
        # check on forum
        # can we store our own stuff here, eg unicodedata version
        # so if it differs then can run rebuild
        # normalize val so insert normalizes all strings
        # perhaps x-apsw prefix?
        pass

    def tokenize(self, utf8: bytes, reason: int, include_offsets=True, include_colocated=True):
        "Tokenize supplied utf8"
        # need to parse config tokenizer into argv
        # and then run
        pass

    def tokens(self) -> set[str]:
        "Return all tokens"
        if self._cache.token_data_version != self.db.data_version(self.schema):
            n = self.fts5vocab_name("row")
            self._cache.tokens = set(term[0] for term in self.db.execute(f"select term from {n}"))
            self._cache.token_data_version = self.db.data_version(self.schema)
        return self._cache.tokens

    def is_token(self, token: str) -> bool:
        """Returns True if it is a known token

        If testing lots of tokens, get :meth:`tokens`
        """
        n = self.fts5vocab_name("row")
        return bool(self.db.execute(f"select term from { n } where term = ?", (token,)).get)

    def combined_tokens(self, tokens: list[str]) -> list[str]:
        """
        Figure out if token list can have adjacent tokens combined
        into other tokens that exist

        ``["play", "station"]`` ->  ``["playstation"]``

        do all have to be present?

        ``["one", "two", "three"]`` ->
           ["onetwothree", "twothree", "onetwo"]

        ?? Sort by token frequency
        """
        # ::TODO:: implement
        return []

    def split_tokens(self, token: str) -> list[list[str]]:
        """
        Figure out of token can be split into multiple tokens that exist

        ``"playstation`` -> ``[["play", "station"]]``

        ``"onetwothree"`` -> ``[ ["one", "twothree"], ["onetwo", "three"]]``

        ?? Sort by token frequency
        """
        # ::TODO:: implement
        return []

    def superset_tokens(self, token: str) -> list[str]:
        """Figure out longer tokens that include this one

        ``one`` -> ``["gone", "phone", "opponent"]``

        ?? Sort by token frequency
        """
        return [t for t in self.tokens() if token in t]

    def token_frequency(self, count: int = 10) -> list[tuple[str, int]]:
        "Most frequent tokens, useful for building a stop words list"
        n = self.fts5vocab_name("row")
        return self.db.execute(f"select term, cnt from { n } order by cnt desc limit { count }").get

    def get_closest_tokens(self, token: str, n: int = 25, cutoff: float = 0.6) -> list[tuple[float, str]]:
        """Returns closest known tokens to ``token`` with score for each

        Calls :func:`token_closeness` with the parameters having the same meaning."""
        return token_closeness(token, self.tokens(), n, cutoff)

    def get_closest_tokens_mp(
        self, pool: multiprocessing.pool.Pool, batch_size: int, token: str, n: int = 25, cutoff: float = 0.6
    ) -> list[tuple[float, str]]:
        """Returns closest known tokens to ``token`` with score for each

        Does the same as :meth:`get_closest_tokens` but uses the
        multiprocessing pool with ``batch_size`` tokens processed at
        once in each work unit.
        """
        results: list[tuple[float, str]] = []
        for res in pool.imap_unordered(
            functools.partial(token_closeness, token, n=n, cutoff=cutoff), batched(self.tokens(), batch_size)
        ):
            results.extend(res)
        results.sort(reverse=True)
        return results[:n]

    @functools.cache
    def fts5vocab_name(self, type: Literal["row"] | Literal["col"] | Literal["instance"]) -> str:
        """
        Creates fts5vocab table in temp and returns name
        """
        name = f"temp.[fts5vocab_{ self.schema }_{ self.name }_{ type }]"

        self.db.execute(
            f"""create virtual table if not exists { name } using fts5vocab("""
            + ",".join(apsw.format_sql_value(v) for v in (self.schema, self.name, type))
            + ")"
        )
        return name

    @classmethod
    def create(
        cls,
        db: apsw.Connection,
        name: str,
        columns: list[str] | None,
        *,
        schema: str = "main",
        tokenizer: list[str] | None = None,
        prefix: str | None = None,
        content: str | None = None,
        contentless_delete: int | None = None,
        content_rowid: str | None = None,
        generate_triggers: bool = False,
        normalize: None | Literal["NFC"] | Literal["NFKC"] | Literal["NFD"] | Literal[NFKD] = None,
    ) -> FTS5Table:
        """Creates the table

        Various kwargs same as `FTS5 options <https://www.sqlite.org/fts5.html#fts5_table_creation_and_initialization>`__

        :param generate_triggers: As in https://www.sqlite.org/fts5.html#external_content_tables

        :param normalize: All text added via :meth:`insert` will be normalized to this

        External content table
        ----------------------

        Run :meth:`command_rebuild` and :meth:`command_optimize` after this create (::TODO:: automatic?)

        Columns can be `None` in which case they will come from external table.
        """
        # ... make table, having fun quoting tokenizer etc
        # for content tables, figure out columns automatically
        inst = cls(db, name, schema)
        # assert inst.columns == columns
        # assert tokenizer == get_args(tokenizer)
        return inst


if hasattr(itertools, "batched"):
    batched = itertools.batched
else:

    def batched(iterable, n):
        ":func:`itertools.batched` equivalent for older Python"
        if n < 1:
            raise ValueError("n must be at least one")
        it = iter(iterable)
        while batch := tuple(itertools.islice(it, n)):
            yield batch


def shingle(token: str, size: int = 3) -> tuple[str, ...]:
    """Returns the token into sequences of ``size`` substrings

    For example ``hello`` size 3 becomes ``('hel', 'ell', 'llo')``.

    This is useful when calculating token closeness as the shingles
    are more representative of word pronunciation and meaning than
    individual letters.
    """
    if (size) < 1:
        raise ValueError(f"size { size } should be at least 1")
    if len(token) < 1:
        raise ValueError("Can't shingle empty token")
    if len(token) <= size:
        return (token,)
    return tuple(token[n : n + size] for n in range(0, len(token) - size + 1))


def token_closeness(
    token: str, tokens: set[str], n: int, cutoff: float, transform: Callable[[str], Any] | None = shingle
) -> list[tuple[float, str]]:
    """
    Uses :func:`difflib.get_close_matches` algorithm to find close matches.

    Note that this is a statistical operation, and has no understanding
    of the tokens and their meaning.  If the ``transform`` parameter is
    None then the comparisons are letter by letter.
    """
    assert n > 0
    assert 0.0 <= cutoff <= 1.0
    result: list[tuple[float, str]] = []
    sm = difflib.SequenceMatcher()
    sm.set_seq2(transform(token) if transform else token)
    for t in tokens:
        if t == token:
            continue
        sm.set_seq1(transform(t) if transform else t)
        if sm.real_quick_ratio() >= cutoff and sm.quick_ratio() >= cutoff and (ratio := sm.ratio()) >= cutoff:
            result.append((ratio, t))
            if len(result) > n:
                result.sort(reverse=True)
                result.pop()
                cutoff = result[-1][0]
    result.sort(reverse=True)
    return result


# To get options set in the create virtual table statement there can be lots of quoting
# of quoting, especially the tokenizer strings.  You can create columns with names
# including single and double quoting, backslashes etc.  So we use a dummy virtual module
# that always throws an exception with the args as received from sqlite.
# This is an early test that SQLite/FTS5 accepts:
#
# CREATE VIRTUAL TABLE ft UsInG fts5(a, [',], [\"=], "prefix=2", 'pr"ef"ix=3  ' , tokenize = '''porter'' ''ascii'''    )
#
# fts5_config.c contains the source that parses these.  Also working is
#  tokenize = [porter 'as de' foo ascii]


class _sqlite_parsed_args(Exception):
    def __init__(self, args):
        self.sqlite_args = args


class _sqlite_parsed_args_vtmodule:
    def Connect(_con, _mod, _db, _table, *args):
        raise _sqlite_parsed_args(args)

    Create = Connect


def get_args(db: apsw.Connection, table_name: str, schema: str = "main"):
    """Returns the declared parameters for the table

    ``CREATE VIRTUAL TABLE ft UsInG fts5(a, [',], [\"=], "prefix=2", prefix   =   2, 'pr"ef"ix=3  ' , tokenize = '''porter'' a ''ascii''')``

    would return:

    ``("prefix", "2"), ("tokenize", ("porter", "a", "ascii")))``

    It understands SQLite and FTS5 quoting rules and gets the information out.  Any
    ``tokenize`` parameter has its value broken out into the individual items.

    """
    sql: str | None = db.execute(f"select sql from [{ schema }].sqlite_schema where name=?", (table_name,)).get
    if sql is None:
        raise ValueError(f"no such table { schema }.{ table_name}")
    modname = _sqlite_parsed_args_vtmodule.__name__
    db.create_module(modname, _sqlite_parsed_args_vtmodule)
    idx = sql.upper().index("USING")
    paren = sql.index("(", idx)
    sqlite_args = None
    try:
        # this will show up in sqlite log so use a descriptive name
        db.execute(
            f"create virtual table [apsw_fts_get_args_{ id(db) }_{ table_name }] using { modname }" + sql[paren:]
        )
        raise RuntimeError("Execution should not have reached here")
    except _sqlite_parsed_args as e:
        sqlite_args = e.sqlite_args
    assert sqlite_args is not None
    # column names and options can be interspersed
    # figure out fts5 decides what is an option

    # options are bareword = value
    # value is one bareword or dequote
    # dequote is [ ' " `" ``  till corresponding close
    # if end quote char is doubled treat it as single and continue
    # can have comments like
    # tokenize='html stoken unicode61 tokenchars _' -- Tokenizer definition
    return sqlite_args


if __name__ == "__main__":
    import html
    import argparse
    import json

    import apsw.bestpractice

    apsw.bestpractice.apply(apsw.bestpractice.recommended)

    # This code evolved a lot, and was not intelligently designed.  Sorry.

    def show_tokenization(options, tok: apsw.FTS5Tokenizer, utf8: bytes, reason: int) -> tuple[str, list[str]]:
        """Runs the tokenizer and produces a html fragment showing the results for manual inspection"""

        offset: int = 0

        @dataclass
        class Row:
            start: int
            end: int
            utf8: bytes
            token_num: int | None = None
            token: str | None = None
            colo: bool = False

        seq: list[Row | str] = []
        for toknum, row in enumerate(tok(utf8, reason)):
            start, end, *tokens = row
            if end < start:
                seq.append(show_tokenization_remark(f"\u21d3 start { start } is after end { end }", "error"))
            if start < offset and options.show_start_overlap:
                seq.append(
                    show_tokenization_remark(
                        f"\u21d3  start { start } is before end of previous item { offset }", "error"
                    )
                )
            if start > offset:
                # white space
                seq.append(Row(start=offset, end=start, utf8=utf8[offset:start]))
            for i, t in enumerate(tokens):
                seq.append(Row(start=start, end=end, utf8=utf8[start:end], token_num=toknum, token=t, colo=i > 0))
            offset = end

        if offset < len(utf8):
            # trailing white space
            seq.append(Row(start=offset, end=len(utf8), utf8=utf8[offset:]))

        # Generate html

        def ud(c: str) -> str:
            r = ""
            gc = unicodedata.category(c)
            explain = unicode_categories
            r += f"{ gc } { explain[gc] }"
            for meth in (
                unicodedata.bidirectional,
                unicodedata.combining,
                unicodedata.east_asian_width,
                unicodedata.mirrored,
            ):
                v = meth(c)
                if v and v != "Na":  # Na is east asian width non-applicable
                    r += f" { meth.__name__ }={ v }"
            return r

        def hex_utf8_bytes(utf8: bytes) -> str:
            codepoints = []
            i = 0
            while i < len(utf8):
                b = utf8[i]
                if b & 0b1111_0000 == 0b1111_0000:
                    codepoints.append(utf8[i : i + 4])
                elif b & 0b1110_0000 == 0b1110_0000:
                    codepoints.append(utf8[i : i + 3])
                elif b & 0b1100_0000 == 0b1100_0000:
                    codepoints.append(utf8[i : i + 2])
                else:
                    codepoints.append(utf8[i : i + 1])
                i += len(codepoints[-1])

            res = []
            for seq in codepoints:
                res.append("<span class=codepbytes>" + "&thinsp;".join("%02x" % x for x in seq) + "</span>")

            return "\u2004 ".join(res)

        def byte_codepoints(b: bytes | str, open="{", close="}") -> str:
            if isinstance(b, bytes):
                b = b.decode("utf8", errors="replace")
            return "<wbr>".join(
                f"<span class=codepoint title='{ html.escape(ud(c), True) }'>"
                f"{ open}{ html.escape(unicodedata.name(c, f'U+{ord(c):04x}')) }{ close }"
                "</span>"
                for c in b
            )

        tokensret = []
        out = ""
        for row in seq:
            if isinstance(row, str):
                out += row
                continue
            if row.token is None:  # space
                out += "<tr class='not-token'>"
                # token num
                out += "<td></td>"
                # start
                out += f"<td>{ row.start }</td>"
                # end
                out += f"<td>{ row.end }</td>"
                # bytes
                out += f"<td>{ hex_utf8_bytes(row.utf8) }</td>"
                # bytes val
                out += f"<td>{ html.escape(row.utf8.decode('utf8', errors='replace')) }</td>"
                # token
                out += "<td></td>"
                # bytes codepoints - already escaped
                out += f"<td>{ byte_codepoints(row.utf8) }</td>"
                # token codepoints
                out += "<td></td>"
                out += "</tr>\n"
                continue

            out += f"<tr class='token {'colo' if row.colo else ''}'>"
            # token num
            out += f"<td>{ row.token_num }</td>"
            # start
            out += f"<td>{ row.start }</td>"
            # end
            out += f"<td>{ row.end }</td>"
            # bytes
            out += f"<td>{ hex_utf8_bytes(row.utf8) }</td>"
            # bytes val
            out += f"<td>{ html.escape(row.utf8.decode('utf8', errors='replace')) }</td>"
            # token
            out += f"<td>{ html.escape(row.token) }</td>"
            # bytes codepoints - already escaped
            out += f"<td>{ byte_codepoints(row.utf8) }</td>"
            # token codepoints - already escaped
            out += f"<td>{ byte_codepoints(row.token) }</td>"
            out += "</tr>\n"
            tokensret.append(row.token)

        return out, tokensret

    # column tips
    ct = [
        "Token number or blank for non-token area",
        "Start byte offset into the utf8 buffer",
        "End byte offset into the utf8 buffer.  This points to the next byte after the token.  End - Start should equal the token length",
        "Hex of the bytes with those making up\neach codepoint alternately underlined",
        "Decoded text from the bytes",
        "Token that was returned",
        "Each codepoint from the bytes",
        "Each codepoint from the token",
    ]

    show_tokenization_header = f"""<table class='tokenization-results'><thead><tr>
            <th title='{ ct[0] }'>#</th>
            <th title='{ ct[1] }'>Start</th>
            <th title='{ ct[2] }'>End</th>
            <th title='{ ct[3] }'>Hex</th>
            <th title='{ ct[4] }'>Bytes</th>
            <th title='{ ct[5] }'>Token</th>
            <th title='{ ct[6] }'>Bytes codepoints</th>
            <th title='{ ct[7] }'>Token codepoints</th
            ></tr></thead><tbody>"""
    show_tokenization_footer = """</tbody></table><details class=infobox><summary>Tips</summary>
    <ul><li>Hover over column headers to get descriptions<li>Hover over codepoints to get category and other information
    <li>You can resize columns from the bottom right of each header cell
    </ul><h3><a href="https://www.unicode.org/reports/tr15/">Normal forms</a></h3><dl>
    <p>Text that <i>looks</i> the same can be represented by different sequences of codepoints, for historical and
    compatibility reasons.  Those different sequences then encode to different sequences of bytes
    and will be considered different tokens, not matching in queries.</p>
    <dt>NFD</dt><dd>Canonical Decomposition breaking codepoints into multiples, so \u212B {ANGSTROM SIGN}
    becomes A {LATIN CAPITAL LETTER A} and {COMBINING RING ABOVE}</dd>
    <dt>NFC</dt><dd>Canonical Composition combining multiple codepoints into one, so \u0043 {LATIN CAPITAL
    LETTER C} and \u0327 {COMBINING CEDILLA} become \u00C7 {LATIN CAPITAL LETTER C WITH CEDILLA}.</dd>
    <dt>NFKD</dt><dd>Compatibility decomposition like NFD but codepoints become compatibility equivalents,
    so 2\u2075 become 2 5, and \u2160 {ROMAN NUMERAL ONE} becomes I (LATIN CAPITAL LETTER I).</dd>
    <dt>NFKC</dt><dd>Compatibility composition like NFC but codepoints become compatibility equivalents.</dd>
    </dl></details>"""
    show_tokenization_css = """
    <style>

    html {
        /* scroll past top header */
        scroll-padding-top: 100px;
    }

    .remark.result td {
        overflow-wrap: anywhere;
    }

    thead {
        position: sticky;
        top: 0;
        background: darkgoldenrod;
    }

    td, th {
        border: 1px solid black;
        padding: 3px;
        min-width: 5px;
    }

    td {
        vertical-align: top;
    }

    th {
        resize: horizontal;
        overflow: auto;
    }

    table {
        border-collapse: collapse
    }

    tr.result {
        background-color: lightblue;
        font-weight: bold;
    }

    tr.toc {
        background-color: powderblue;
        font-weight: bold;
    }

    tr.result .message {
        display: block;
        background-color: white;
        font-weight: normal;
    }

    tr.remark.error {
        background-color: red;
        font-weight: bold;
    }

    /* token number */
    .token td:nth-child(1) {
        text-align: right;
        font-weight: bold;
    }

    /* byte offsets */
    td:nth-child(2), td:nth-child(3) {
        text-align: right;
    }

    /* bytes */
    td:nth-child(4) {
        font-family: monospace;
        font-size: 95%;
    }

    /* non token space */
    .not-token {
        background-color: lightgray;
    }

    /* token */
    .token {
        background-color: lightyellow;
    }

    /* colocated token */
    .token.colo {
        background-color: #efefd0;
    }

    td .codepbytes:nth-child(odd) {
        text-decoration: underline;
    }

    .infobox {
        position: fixed;
        bottom: 0;
        right: 0;
        float: right;
        background-color: khaki;
        border: 1px solid black;
        padding: 3px;
        max-width: 50%;
    }

    .infobox summary {
        font-weight: bold;
        background-color: aquamarine;
        font-size: 110%;
    }

    .compare-original {
        display: block;
        float: left;
        background-color: lightgray;
    }

    .compare-tokens {
        display: block;
        float: left;
        background-color: lightyellow;
        text-wrap: balance;
    }

    .compare-tokens, .compare-original {
        padding: 5px;
    }

    .compare-tokens .ct:nth-child(odd) {
        text-decoration: underline;
    }
    </style>
    """

    def show_tokenization_remark(
        remark: str, kind: str = "notice", id: str = None, link: str = None, compare: str = None
    ) -> str:
        id = f"id='{ id }'" if id is not None else ""
        ls = f"<a href='#{ link }'>" if link else ""
        le = "</a>" if link else ""
        if compare:
            newline = "\n"  # fstring can't have backslash
            left = html.escape(compare[0]).replace(newline, "<br>")
            right = " ".join(f"<span class='ct'>{ html.escape(token) }</span>" for token in compare[1])
            compare = f"<br><span class='compare-original' title='Original UTF8 bytes'>{left}</span><span class='compare-tokens' title='Tokens, alternate ones underlined'>{ right }</span>"
        else:
            compare = ""
        return f"<tr class='remark { kind }' { id }><td colspan=8>{ ls }{ html.escape(remark) }{ le }{ compare }</td></tr>\n"

    con = apsw.Connection("")

    parser = argparse.ArgumentParser(
        prog="python3 -m apsw.fts",
        description="Runs FTS5 tokenizer against test text producing a HTML report for manual inspection.",
    )
    parser.add_argument(
        "--text-file",
        metavar="TEXT-FILE-NAME",
        help="Filename containing test strings.  Default is builtin. "
        """The test file should be UTF-8 encoded text.

        If it starts with a # then it is considered to be multiple text sections
        where a # line contains a description of the section.  Any lines beginning
        ## are ignored.""",
    )
    parser.add_argument(
        "--output",
        "-o",
        type=argparse.FileType("wb"),
        default="-",
        help="Where to send the binary UTF8 encoded output html [stdout]",
    )
    parser.add_argument(
        "--normalize",
        default=None,
        choices="NFC NFKC NFD NFKD".split(),
        help="Normalize the bytes into this form.  Default is no normalization",
    )
    parser.add_argument(
        "--reason",
        choices=list(tokenize_reasons.keys()),
        help="Tokenize reason [%(default)s]",
        default="DOCUMENT",
    )
    parser.add_argument(
        "--register",
        action="append",
        default=[],
        help="Registers tokenizers. Format is name=mod.submod.callable "
        "where name is what is registered with FTS5 and callable is the factory function.  The module containing "
        "callable will be imported.  Specify this option multiple times to register multiple tokenizers. "
        "apsw.fts tokenizers are pre-registered as pyunicode, simplify, html, synonyms, regex, stopwords, "
        "transform, ngramtoken, and ngram",
        metavar="name=mod.part.callable",
    )

    # registrations built in
    con.register_fts5_tokenizer("pyunicode", PyUnicodeTokenizer)
    con.register_fts5_tokenizer("simplify", SimplifyTokenizer)
    con.register_fts5_tokenizer("html", HTMLTokenizer)
    con.register_fts5_tokenizer("ngramtoken", NGramTokenTokenizer)
    con.register_fts5_tokenizer("ngram", NGramTokenizer)
    # ::TODO check these work
    con.register_fts5_tokenizer("synonyms", SynonymTokenizer)
    con.register_fts5_tokenizer("regex", RegexTokenizer)
    con.register_fts5_tokenizer("transform", TransformTokenizer())

    parser.add_argument(
        "--synonyms",
        help="A json file where each key is a token, and the value is either a string, or a list of strings.  A tokenizer named synonyms will be registered doing lookups on that json",
        metavar="FILENAME",
        type=argparse.FileType("rb"),
    )

    parser.add_argument(
        "--regex-flags", help="A pipe separated list of flags - eg 'ASCII | DOTALL' [%(default)s]", default="NOFLAG"
    )

    parser.add_argument(
        "--regex",
        help="A regular expression.  A tokenizer named regex will be registered with the pattern and flags. Beware of shell quoting and backslashes.",
        metavar="PATTERN",
    )

    parser.add_argument(
        "--show-start-overlap",
        action="store_true",
        help="Shows big red message where tokens overlap.  Default is true unless 'gram' is in args in which case it is false "
        "since ngrams/trigrams deliberately overlap tokens",
    )

    parser.add_argument(
        "args",
        nargs="+",
        help="Tokenizer and arguments to run.  FTS5 builtin tokenizers are ascii, trigram, unicode61, and porter.  "
        "For example to run the trigram tokenizer on unicode keeping diacritics use: trigram unicode61 remove_diacritics 0",
    )
    options = parser.parse_args()
    if options.output.isatty():
        parser.error("Refusing to spew HTML to your terminal.  Redirect/pipe output or use the --output option")

    if options.show_start_overlap is None:
        options.show_start_overlap = any("gram" in arg for arg in options.args)

    if options.synonyms:
        data = json.load(options.synonyms)
        assert isinstance(data, dict)
        con.register_fts5_tokenizer("synonyms", SynonymTokenizer(data.get))

    if options.regex:
        flags = 0
        for f in options.regex_flags.split("|"):
            value = getattr(re, f.strip())
            if not isinstance(value, int):
                raise ValueError(f"{ f } doesn't seem to be a valid re flag")
            flags |= value
        con.register_fts5_tokenizer("regex", functools.partial(RegexTokenizer, pattern=options.regex, flags=flags))

    # registrations from args
    for reg in options.register:
        try:
            name, mod = reg.split("=", 1)
            obj = convert_string_to_python(mod)
            con.register_fts5_tokenizer("name", obj)
        except Exception as e:
            if hasattr(e, "add_note"):
                e.add_note(f"Processing --register { reg }")
            raise

    # go
    tok = con.fts5_tokenizer(options.args[0], options.args[1:])

    # we build it all up in memory
    results = []
    for utf8, comment in tokenizer_test_strings(filename=options.text_file):
        if options.normalize:
            utf8 = unicodedata.normalize(options.normalize, utf8.decode("utf8", errors="replace")).encode("utf8")
        h, tokens = show_tokenization(options, tok, utf8, tokenize_reasons[options.reason])
        results.append((comment, utf8, h, options.reason, tokens))

    w = lambda s: options.output.write(s.encode("utf8") + b"\n")

    w('<html><head><meta charset="utf-8"></head><body>')
    w(show_tokenization_css)
    w(show_tokenization_header)
    w(show_tokenization_remark("Args: " + str(sys.argv), kind="args"))
    sections = []
    counter = 1
    for comment, utf8, h, reason, tokens in results:
        normalized = [
            f
            for f in ("NFC", "NFKC", "NFD", "NFKD")
            if unicodedata.is_normalized(f, utf8.decode("utf8", errors="replace"))
        ]
        if normalized:
            forms = ": forms " + " ".join(normalized)
        else:
            forms = ": not normalized"
        w(
            show_tokenization_remark(
                f"{ comment } : { reason } { forms }",
                kind="toc",
                link=counter,
            )
        )
        sections.append(
            show_tokenization_remark(
                f"{ comment } : { reason } { forms }",
                kind="result",
                id=counter,
                compare=(utf8.decode("utf8", errors="replace"), tokens),
            )
        )
        if not h:
            h = "<tr><td colspan=8></i>No bytes</i></td></tr>"
        sections.append(h)
        counter += 1
    for s in sections:
        w(s)
    w(show_tokenization_footer)
    w("</body></html")
    options.output.close()
