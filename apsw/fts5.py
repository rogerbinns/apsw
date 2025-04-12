#!/usr/bin/env python3


""":mod:`apsw.fts5` Various classes and functions to work with full text search.

This includes :class:`Table` for creating and working with FTS5 tables
in a Pythonic way, numerous :ref:`tokenizers <all_tokenizers>`, and
related functionality.
"""

from __future__ import annotations

import collections
import difflib
import fnmatch
import functools

# avoid clashing with html as a parameter name
import html as html_module
import html.parser as html_parser_module
import importlib
import importlib.resources
import json
import math
import pathlib
import re
import sys
import threading
from contextvars import ContextVar
from dataclasses import dataclass
from types import ModuleType

try:
    from typing import Any, Callable, Iterable, Iterator, Literal, Sequence, Self
except ImportError:
    # Self is only available in py3.11+ but the other items import anyway
    pass

import apsw
import apsw._unicode
import apsw.ext
import apsw.fts5query
import apsw.unicode

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
    * apsw.fts5.RegexTokenizer
    * snowballstemmer.stemmer("english").stemWord
    * nltk.stem.snowball.EnglishStemmer().stem
    * shutil.rmtree("a/directory/location")  **COULD DELETE ALL FILES**
    """
    parts = expr.split(".")
    imports: dict[str, ModuleType] = {}
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

    Takes input like ``2,3-5,17`` and converts to
    ``{2, 3, 4, 5, 17}``
    """
    res: set[int] = set()
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


def convert_boolean(value: str) -> bool:
    """Converts to boolean

    Accepts ``0``, ``1``, ``false``, and ``true``"""
    try:
        return {
            "0": False,
            "false": False,
            "1": True,
            "true": True,
        }[value.lower()]
    except KeyError:
        raise ValueError(f"Expected a boolean value from (0, 1, false, true) not '{ value }'")


def tokenizer_test_strings(filename: str | pathlib.Path | None = None) -> tuple[tuple[bytes, str], ...]:
    """Provides utf-8 bytes sequences for interesting test strings

    :param filename: File to load.  If None then the builtin one is used

    :returns: A tuple where each item is a tuple of utf8 bytes and comment str

    The test file should be UTF-8 encoded text.

    If it starts with a # then it is considered to be multiple text sections
    where a # line contains a description of the section.  Any lines beginning
    ## are ignored."""

    if filename is None:
        filename = "fts_test_strings"
        data = importlib.resources.files(apsw).joinpath(filename).read_bytes()
    else:
        with open(filename, "rb") as f:
            data = f.read()

    if not data:
        return ((b"", "No data"),)
    if not data.startswith(b"#"):
        return ((data, pathlib.Path(filename).name),)

    test_strings: list[tuple[bytes, str]] = []
    lines = [line for line in data.splitlines() if not line.startswith(b"##")]
    while lines:
        comment = lines.pop(0)[1:].decode(errors="replace").strip()
        text: list[bytes] = []
        while lines and not lines[0].startswith(b"#"):
            text.append(lines.pop(0))
        test_strings.append((b"\n".join(text).rstrip(), comment))

    return tuple(test_strings)


def StringTokenizer(func: apsw.FTS5TokenizerFactory) ->apsw.Tokenizer:
    """Decorator for tokenizers that operate on strings

    FTS5 tokenizers operate on UTF8 bytes for the text and offsets.
    This decorator provides your tokenizer with text and expects text
    offsets back, performing the conversions back to UTF8 byte
    offsets.
    """

    @functools.wraps(func)
    def string_tokenizer_wrapper(con: apsw.Connection, args: list[str], **kwargs) -> apsw.Tokenizer:
        inner_tokenizer = func(con, args, **kwargs)

        @functools.wraps(inner_tokenizer)
        def outer_tokenizer(utf8: bytes, flags: int, locale: str | None):
            upm = apsw._unicode.to_utf8_position_mapper(utf8)

            for start, end, *tokens in inner_tokenizer(upm.str, flags, locale):
                yield upm(start), upm(end), *tokens

        return outer_tokenizer

    return string_tokenizer_wrapper


def QueryTokensTokenizer(con: apsw.Connection, args: list[str]) -> apsw.Tokenizer:
    """Recognises a special tokens marker and returns those tokens for a query.
    This is useful for making queries directly using tokens, instead
    of pre-tokenized text.

    It must be the first tokenizer in the list.  Any text not using
    the special marker is passed to the following tokenizer.

    See :class:`apsw.fts5query.QueryTokens` for more details on the
    marker format.
    """
    spec = {"+": None}

    options = parse_tokenizer_args(spec, con, args)

    def tokenize(utf8: bytes, flags: int, locale: str | None):
        if flags & apsw.FTS5_TOKENIZE_QUERY:
            decoded = apsw.fts5query.QueryTokens.decode(utf8)
            if decoded is not None:
                for token in decoded.tokens:
                    yield token
                return
        yield from options["+"](utf8, flags, locale)

    return tokenize


@StringTokenizer
def UnicodeWordsTokenizer(con: apsw.Connection, args: list[str]) -> apsw.Tokenizer:
    """Uses `Unicode segmentation <https://www.unicode.org/reports/tr29/>`__ to extract words

    The following tokenizer parameters are accepted.  A segment is considered a word
    if a codepoint matching any of the categories, emoji, or regional indicator is
    present.

    categories
        Default ``L* N*`` to include letters, and numbers.  You should consider ``Pd``
        for punctuation dash if you want words separated with dashes to be considered one word.
        ``Sm`` for maths symbols and ``Sc`` for currency symbols may also be relevant,

    emoji
        ``0`` or ``1`` (default) if emoji are included.  They will be a word
        by themselves.

    regional_indicator
        ``0`` or ``1`` (default) if `regional indicators
        <https://en.wikipedia.org/wiki/Regional_indicator_symbol>`__ like 🇬🇧 🇵🇭
        are included.  They will be a word  by themselves.

    This does a lot better than the `unicode61` tokenizer builtin to FTS5.  It
    understands user perceived characters (made of many codepoints), and punctuation
    within words (eg ``don't`` is considered two words ``don`` and ``t`` by `unicode61`),
    as well as how various languages work.

    For languages where there is no spacing or similar between words, only a dictionary
    can determine actual word boundaries.  Examples include Japanese, Chinese, and Khmer.
    In this case the algorithm returns the user perceived characters individually making
    it similar to :meth:`NGramTokenizer` which will provide good search experience at
    the cost of a slightly larger index.

    Use the :func:`SimplifyTokenizer` to make case insensitive, remove diacritics,
    combining marks, and use compatibility code points.

    See the :ref:`example <example_fts_apsw_unicodewords>`
    """
    spec = {
        "categories": TokenizerArgument(default="L* N*", convertor=convert_unicode_categories, convert_default=True),
        "emoji": TokenizerArgument(default=True, convertor=convert_boolean),
        "regional_indicator": TokenizerArgument(default=True, convertor=convert_boolean),
    }

    options = parse_tokenizer_args(spec, con, args)

    def tokenize(text: str, flags: int, locale: str | None):
        yield from apsw.unicode.word_iter_with_offsets(text, 0, **options)

    return tokenize


def SimplifyTokenizer(con: apsw.Connection, args: list[str]) -> apsw.Tokenizer:
    """Tokenizer wrapper that simplifies tokens by neutralizing case, canonicalization, and diacritic/mark removal

    Put this before another tokenizer to simplify its output.  For example:

       simplify casefold true unicodewords

    The following tokenizer arguments are accepted, and are applied to each
    token in this order.  If you do not specify an argument then it is off.

    strip
        Codepoints become their compatibility representation - for example
        the Roman numeral Ⅲ becomes III.  Diacritics, marks, and similar
        are removed.  See :func:`apsw.unicode.strip`.

    casefold
        Neutralizes case distinction.  See :func:`apsw.unicode.casefold`.

    See the :ref:`example <example_fts_apsw_simplify>`.
    """
    spec = {
        "strip": TokenizerArgument(default=False, convertor=convert_boolean),
        "casefold": TokenizerArgument(default=False, convertor=convert_boolean),
        "+": None,
    }
    options = parse_tokenizer_args(spec, con, args)

    # only 4 choices
    conv = {
        (False, False): lambda t: t,
        (True, False): lambda t: apsw.unicode.strip(t),
        (False, True): lambda t: apsw.unicode.casefold(t),
        (True, True): lambda t: apsw.unicode.casefold(apsw.unicode.strip(t)),
    }[options["strip"], options["casefold"]]

    def tokenize(utf8: bytes, flags: int, locale: str | None):
        tok = options["+"]
        for start, end, *tokens in tok(utf8, flags, locale):
            new_tokens = []
            for token in tokens:
                new = conv(token)
                if new and new not in new_tokens:
                    new_tokens.append(new)
            if new_tokens:
                yield start, end, *new_tokens

    return tokenize


@StringTokenizer
def NGramTokenizer(con: apsw.Connection, args: list[str]) -> apsw.Tokenizer:
    """Generates ngrams from the text

    For example if doing 3 (trigram) then ``a big dog`` would result in
    ``'a b', ' bi', 'big', 'ig ', 'g d', ' do`, 'dog'``

    This is useful for queries where less than an entire word has been
    provided such as doing completions, substring, or suffix matches.  For
    example a query of ``ing`` would find all occurrences even at the end
    of words with ngrams, but not with the :func:`UnicodeWordsTokenizer`
    which requires the query to provide complete words.

    This tokenizer works on units of user perceived characters
    (grapheme clusters) where more than one codepoint can make up one
    user perceived character.

    The following tokenizer arguments are accepted

    ngrams
        Numeric ranges to generate.  Smaller values allow showing
        results with less input but a larger index, while larger
        values will result in quicker searches as the input grows.
        Default is 3.  You can specify :func:`multiple values <convert_number_ranges>`.

    categories
        Which Unicode categories to include, by default all.  You could
        include everything except punctuation and separators with
        ``* !P* !Z*``.

    emoji
        ``0`` or ``1`` (default) if emoji are included, even if `categories`
        would exclude them.

    regional_indicator
        ``0`` or ``1`` (default) if regional indicators  are included, even if `categories`
        would exclude them.

    See :ref:`the example <example_fts_autocomplete>`.
    """

    spec = {
        "ngrams": TokenizerArgument(default="3", convertor=convert_number_ranges, convert_default=True),
        "categories": TokenizerArgument(default="*", convertor=convert_unicode_categories, convert_default=True),
        "emoji": TokenizerArgument(default=True, convertor=convert_boolean),
        "regional_indicator": TokenizerArgument(default=True, convertor=convert_boolean),
    }

    options = parse_tokenizer_args(spec, con, args)

    if any(ngram < 1 for ngram in options["ngrams"]):
        raise ValueError(f"ngrams must be at least 1 in {options['ngrams']=}")

    def tokenize(text: str, flags: int, locale: str | None):
        ntokens = 0

        grapheme_cluster_stream: list[tuple[int, int, str]] = []
        keep = max(options["ngrams"])

        # if doing a query, only produce largest possible
        if flags & apsw.FTS5_TOKENIZE_QUERY:
            produce = [max(options["ngrams"])]
        else:
            produce = sorted(options["ngrams"])

        for token in apsw.unicode.grapheme_iter_with_offsets_filtered(
            text,
            categories=options["categories"],
            emoji=options["emoji"],
            regional_indicator=options["regional_indicator"],
        ):
            grapheme_cluster_stream.append(token)
            for ntoken in produce:
                if len(grapheme_cluster_stream) >= ntoken:
                    yield (
                        grapheme_cluster_stream[-ntoken][0],
                        grapheme_cluster_stream[-1][1],
                        "".join(grapheme_cluster_stream[i][2] for i in range(-ntoken, 0)),
                    )
                    ntokens += 1
            if len(grapheme_cluster_stream) > keep:
                grapheme_cluster_stream.pop(0)

        # if doing a query and we didn't hit the produce then find longest we can
        if flags & apsw.FTS5_TOKENIZE_QUERY and ntokens == 0 and grapheme_cluster_stream:
            largest = -1
            for i in sorted(options["ngrams"]):
                if i <= len(grapheme_cluster_stream):
                    largest = i
            if largest > 0:
                for i in range(0, len(grapheme_cluster_stream) - largest):
                    ntokens += 1
                    yield (
                        grapheme_cluster_stream[i][0],
                        grapheme_cluster_stream[i + largest - 1][1],
                        "".join(grapheme_cluster_stream[j][2] for j in range(i, i + largest)),
                    )

        # text didn't match any of our lengths, so return as is
        if ntokens == 0 and grapheme_cluster_stream:
            yield (
                grapheme_cluster_stream[0][0],
                grapheme_cluster_stream[-1][1],
                "".join(grapheme_cluster_stream[i][2] for i in range(len(grapheme_cluster_stream))),
            )

    return tokenize


def SynonymTokenizer(get: Callable[[str], None | str | tuple[str]] | None = None) -> apsw.FTS5TokenizerFactory:
    """Adds `colocated tokens <https://www.sqlite.org/fts5.html#synonym_support>`__ such as ``1st`` for ``first``.

    To use you need a callable that takes a str, and returns a str, a sequence of str, or None.
    For example :meth:`dict.get` does that.

    The following tokenizer arguments are accepted:

    reasons
        Which tokenize :data:`tokenize_reasons` you want the lookups to happen in
        as a space separated list.  Default is ``QUERY``.

    get
        Specify a :func:`get <convert_string_to_python>`, or use as a decorator.

    See :ref:`the example <example_fts_synonym>`.
    """

    @functools.wraps(get)
    def tokenizer(con: apsw.Connection, args: list[str]) -> apsw.Tokenizer:
        nonlocal get
        spec = {
            "reasons": TokenizerArgument(default="QUERY", convertor=convert_tokenize_reason, convert_default=True),
            "+": None,
            **({} if get else {"get": TokenizerArgument(default=get, convertor=convert_string_to_python)}),
        }

        options = parse_tokenizer_args(spec, con, args)

        if "get" in options:
            get = options["get"]

        if get is None:
            raise ValueError("A callable must be provided by decorator, or parameter")

        def tokenize(utf8: bytes, flags: int, locale: str | None):
            tok = options["+"]
            if flags not in options["reasons"]:
                yield from tok(utf8, flags, locale)
                return

            for start, end, *tokens in tok(utf8, flags, locale):
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

    The following tokenizer arguments are accepted, or use as a decorator.

    test
        Specify a :func:`test <convert_string_to_python>`

    See :ref:`the example <example_fts_stopwords>`.
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

        def tokenize(utf8: bytes, flags: int, locale: str | None):
            tok = options["+"]

            if flags == tokenize_reasons["QUERY_PREFIX"]:
                yield from tok(utf8, flags, locale)
                return

            for start, end, *tokens in tok(utf8, flags, locale):
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

    To use you need a callable that takes a str, and returns a list of
    str, or just a str to use as replacements.  You can return an
    empty list to remove the token.

    The following tokenizer arguments are accepted.

    transform
        Specify a :func:`transform <convert_string_to_python>`, or use
        as a decorator

    See :ref:`the example <example_fts_transform>`.
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

        def tokenize(utf8: bytes, flags: int, locale: str | None):
            tok = options["+"]

            for start, end, *tokens in tok(utf8, flags, locale):
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


def extract_html_text(html: str) -> tuple[str, apsw._unicode.OffsetMapper]:
    """Extracts text from HTML using :class:`html.parser.HTMLParser` under the hood

    :meta private:
    """

    class _HTMLTextExtractor(html_parser_module.HTMLParser):
        # Extracts text from HTML maintaining a table mapping the offsets
        # of the extracted text back tot he source HTML.

        def __init__(self, html: str):
            # we handle charrefs because they are multiple codepoints in
            # the HTML but only one in text - eg "&amp;" is "&"
            super().__init__(convert_charrefs=False)
            # offset mapping
            self.om = apsw._unicode.OffsetMapper()
            # We don't know the end offset so have to wait till next item's
            # start to use as previous end.  this keep track of the item and
            # its offset
            self.last = None
            # A stack is semantically correct but we (and browsers) don't
            # require correctly balanced tags, and a stack doesn't improve
            # correctness
            self.current_tag: str | None = None
            # svg content is ignored.
            self.svg_nesting_level = 0
            # offset in parent class sometimes goes backwards or to zero so
            # we have to track ourselves
            self.real_offset = 0
            # All the work is done in the constructor
            self.feed(html)
            self.close()

            if self.last:
                self.om.add(self.last[0], self.last[1], self.real_offset)
            # ensure there is a terminator
            self.om.add("", self.real_offset, self.real_offset)

        def append_result_text(self, text: str):
            if self.last:
                self.om.add(self.last[0], self.last[1], self.real_offset)
            self.last = (text, self.real_offset)

        def separate(self):
            if self.last is not None:
                self.om.add(self.last[0], self.last[1], self.real_offset)
                self.last = None
            self.om.separate()

        def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
            self.current_tag = tag.lower()
            if tag.lower() == "svg":
                self.svg_nesting_level += 1
            self.separate()

        def handle_endtag(self, tag: str) -> None:
            self.current_tag = None
            if tag.lower() == "svg":
                self.svg_nesting_level -= 1
            self.separate()

        def handle_data(self, data: str) -> None:
            if self.svg_nesting_level or self.current_tag in {"script", "style"}:
                return
            self.append_result_text(data)

        def handle_entityref(self, name: str):
            if self.svg_nesting_level:
                return
            self.append_result_text(html_module.unescape(f"&{name};"))

        def handle_charref(self, name: str) -> None:
            self.handle_entityref("#" + name)

        # treat some other markup as white space
        def ws(self, *args: Any):
            if self.svg_nesting_level:
                return
            self.separate()

        handle_comment = handle_decl = handle_pi = unknown_decl = ws

        def updatepos(self, i: int, j: int) -> int:
            # The parent version does a lot of work trying to keep
            # track of line numbers which is pointless for us.  We
            # also have to prevent going backwards.
            self.real_offset = max(j, self.real_offset)
            return j

    h = _HTMLTextExtractor(html)

    return h.om.text, h.om


@StringTokenizer
def HTMLTokenizer(con: apsw.Connection, args: list[str]) -> apsw.Tokenizer:
    """Extracts text from HTML suitable for passing on to other tokenizers

    This should be before the actual tokenizer in the tokenizer list.
    Behind the scenes it extracts text from the HTML, and manages the
    offset mapping between the HTML and the text passed on to other
    tokenizers.  It also expands entities and charrefs.  Content
    inside `SVG tags <https://en.wikipedia.org/wiki/SVG>`__ is
    ignored.

    If the html doesn't start with optional whitespace then ``<`` or
    ``&``, it is not considered HTML and will be passed on
    unprocessed.  This would typically be the case for queries.

    :mod:`html.parser` is used for the HTML processing.

    See :ref:`the example <example_fts_html>`.
    """
    spec = {"+": None}
    options = parse_tokenizer_args(spec, con, args)

    def tokenize(html: str, flags: int, locale: str | None):
        # we only tokenize what looks like html.  Human typed queries
        # are unlikely to be html.  We allow for ampersand to catch
        # entity searches.
        if not re.match(r"\s*[<&]", html):
            yield from string_tokenize(options["+"], html, flags, locale)
            return

        text, om = extract_html_text(html)

        for start, end, *tokens in string_tokenize(options["+"], text, flags, locale):
            yield om(start), om(end), *tokens

    return tokenize


# matches all quoted strings in JSON including if there are
# backslashes inside
_json_strings = re.compile(r'"([^"\\]*(?:\\.[^"\\]*)*)"', re.DOTALL)

# we want to reject keys - string followed by whitespace and colon
_json_key = re.compile(r"\s*:")

# what backslashes map to
_json_backslash_mapping = {
    "\\": "\\",
    '"': '"',
    "b": "\b",
    "f": "\f",
    "n": "\n",
    "r": "\r",
    "t": "\t",
}


def extract_json(text: str, include_keys: bool) -> tuple[str, apsw._unicode.OffsetMapper]:
    """Extracts text values from JSON text

    Returns the extracted text and an :class:`apsw._unicode.OffsetMapper` to convert locations
    in the extracted text back to the source text.

    :param include_keys: ``False`` to only extract values, ``True`` to also extract
       keys.

    :meta private:
    """
    om = apsw._unicode.OffsetMapper()

    for match in _json_strings.finditer(text):
        if not include_keys:
            if _json_key.match(text, match.span(0)[1]):
                continue
        s, e = match.span(1)
        if s == e:  # empty string test
            continue
        span = match.group(1)
        om.separate()
        if "\\" not in span:
            om.add(span, s, e)
            continue
        offset = s
        while span:
            loc = span.find("\\")
            if loc < 0:
                om.add(span, offset, offset + len(span))
                break
            om.add(span[:loc], offset, offset + loc)
            offset += loc
            if span[loc + 1] == "u":
                code = int(span[loc + 2 : loc + 6], 16)
                if 0xD800 <= code <= 0xDFFF:
                    # Surrogate pair.  You get this with json.dumps as
                    # ensure_ascii parameter defaults to True
                    assert span[loc + 6 : loc + 8] == "\\u"
                    code2 = int(span[loc + 8 : loc + 12], 16)
                    c = chr(0x10000 + (code - 0xD800) * 0x400 + (code2 - 0xDC00))
                    length = 12
                else:
                    c = chr(code)
                    length = 6
            else:
                c = _json_backslash_mapping[span[loc + 1]]
                length = 2
            om.add(c, offset, offset + length)
            offset += length
            span = span[loc + length :]

    return om.text, om


@StringTokenizer
def JSONTokenizer(con: apsw.Connection, args: list[str]) -> apsw.Tokenizer:
    """Extracts text from JSON suitable for passing to a tokenizer

    The following tokenizer arguments are accepted:

    include_keys
      ``0`` (default) or ``1`` if keys are extracted in addition to
      values

    If the JSON doesn't start with optional whitespace then ``{`` or
    ``[``, it is not considered JSON and will be passed on
    unprocessed.  This would typically be the case for queries.

    See :ref:`the example <example_fts_json>`.
    """
    spec = {
        "include_keys": TokenizerArgument(default=False, convertor=convert_boolean),
        "+": None,
    }
    options = parse_tokenizer_args(spec, con, args)

    def tokenize(json: str, flags: int, locale: str | None):
        # we only tokenize what looks like json.  Human typed queries
        # are unlikely to be json.
        if not re.match(r"\s*[{\[]", json):
            yield from string_tokenize(options["+"], json, flags, locale)
            return

        text, mapper = extract_json(json, options["include_keys"])
        for start, end, *tokens in string_tokenize(options["+"], text, flags, locale):
            yield mapper(start), mapper(end), *tokens

    return tokenize


def string_tokenize(tokenizer: apsw.FTS5Tokenizer, text: str, flags: int, locale: str | None):
    """Tokenizer caller to get string offsets back

    Calls the tokenizer doing the conversion of `text` to UTF8, and converting the received
    UTF8 offsets back to `text` offsets.
    """
    upm = apsw._unicode.from_utf8_position_mapper(text)
    for bytes_start, bytes_end, *tokens in tokenizer(upm.bytes, flags, locale):
        yield (
            upm(bytes_start),
            upm(bytes_end),
            *tokens,
        )


@StringTokenizer
def RegexTokenizer(
    con: apsw.Connection, args: list[str], *, pattern: str | re.Pattern, flags: int = 0
) -> apsw.Tokenizer:
    r"""Finds tokens using a regular expression

    :param pattern: The `regular expression
        <https://docs.python.org/3/library/re.html#regular-expression-syntax>`__.
        For example :code:`\w+` is all alphanumeric and underscore
        characters.
    :param flags: `Regular expression flags
       <https://docs.python.org/3/library/re.html#flags>`__.  Ignored
       if `pattern` is an already compiled pattern

    See the :ref:`example <example_fts_apsw_regex>`
    """
    if not isinstance(pattern, re.Pattern):
        pattern = re.compile(pattern, flags)

    spec = {}

    parse_tokenizer_args(spec, con, args)

    def tokenize(text: str, flags: int, locale: str | None):
        for match in re.finditer(pattern, text):
            yield *match.span(), match.group()

    return tokenize


@StringTokenizer
def RegexPreTokenizer(
    con: apsw.Connection, args: list[str], *, pattern: str | re.Pattern, flags: int = 0
) -> apsw.Tokenizer:
    r"""Combines regular expressions and another tokenizer

    :func:`RegexTokenizer` only finds tokens matching a regular
    expression, and ignores all other text.  This tokenizer calls
    another tokenizer to handle the gaps between the patterns it
    finds.  This is useful to extract identifiers and other known
    patterns, while still doing word search on the rest of the text.

    :param pattern: The `regular expression
        <https://docs.python.org/3/library/re.html#regular-expression-syntax>`__.
        For example :code:`\w+` is all alphanumeric and underscore
        characters.
    :param flags: `Regular expression flags
       <https://docs.python.org/3/library/re.html#flags>`__.  Ignored
       if `pattern` is an already compiled pattern

    You must specify an additional tokenizer name and arguments.

    See the :ref:`example <example_fts_apsw_regexpre>`
    """
    if not isinstance(pattern, re.Pattern):
        pattern = re.compile(pattern, flags)

    spec = {
        "+": None,
    }

    options = parse_tokenizer_args(spec, con, args)

    def process_other(substring: str, flags: int, locale: str | None, offset: int):
        for start, end, *tokens in string_tokenize(options["+"], substring, flags, locale):
            yield start + offset, end + offset, *tokens

    def tokenize(text: str, flags: int, locale: str | None):
        last_other = 0

        for match in re.finditer(pattern, text):
            if match.start() > last_other:
                yield from process_other(text[last_other : match.start()], flags, locale, last_other)
            yield *match.span(), match.group()
            last_other = match.end()

        if last_other < len(text):
            yield from process_other(text[last_other:], flags, locale, last_other)

    return tokenize


@dataclass
class TokenizerArgument:
    "Used as spec values to :func:`parse_tokenizer_args` - :ref:`example <example_fts_own_2>`"

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

    .. code-block:: python

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
        def tokenize(utf8, flags, locale):
            tok = options["+"]
            for start, end, *tokens in tok(utf8, flags, locale):
                # do something
                yield start, end, *tokens

    .. seealso:: Some useful convertors

        * :func:`convert_unicode_categories`
        * :func:`convert_tokenize_reason`
        * :func:`convert_string_to_python`
        * :func:`convert_number_ranges`

    See :ref:`the example <example_fts_own_2>`.
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


@dataclass
class MatchInfo:
    "Information about a matched row, returned by :meth:`Table.search`"

    query_info: QueryInfo
    "Overall query information"
    rowid: int
    "Rowid"
    column_size: tuple[int]
    "Size of each column in tokens"
    phrase_columns: tuple[tuple[int], ...]
    "For each phrase a tuple of which columns it occurs in"


@dataclass
class QueryInfo:
    "Information relevant to the query as a whole, returned by :meth:`Table.search`"

    phrases: tuple[tuple[str | None, ...], ...]
    """:attr:`Phrases from the query <apsw.FTS5ExtensionApi.phrases>`

    ``a OR b NOT c AND d`` would result in ``a, b, c, d`` as
    4 separate phrases.
    """


map_tokenizers = {
    "html": HTMLTokenizer,
    "json": JSONTokenizer,
    "ngram": NGramTokenizer,
    "querytokens": QueryTokensTokenizer,
    "simplify": SimplifyTokenizer,
    "unicodewords": UnicodeWordsTokenizer,
}
"APSW provided tokenizers for use with :func:`register_tokenizers`"

map_functions = {
    "subsequence": "apsw.fts5aux.subsequence",
    "position_rank": "apsw.fts5aux.position_rank",
}
"APSW provided auxiliary functions for use with :func:`register_functions`"


def register_tokenizers(db: apsw.Connection, map: dict[str, str | Callable]):
    """Registers tokenizers named in map with the connection, if not already registered

    The map contains the tokenizer name, and either the callable or a
    string which will be automatically :func:`imported
    <convert_string_to_python>`.

    See :data:`map_tokenizers`
    """
    for name, tok in map.items():
        if not db.fts5_tokenizer_available(name):
            db.register_fts5_tokenizer(name, convert_string_to_python(tok) if isinstance(tok, str) else tok)


def register_functions(db: apsw.Connection, map: dict[str, str | Callable]):
    """Registers auxiliary functions named in map with the connection, if not already registered

    The map contains the function name, and either the callable or a
    string which will be automatically :func:`imported
    <convert_string_to_python>`.

    See :data:`map_functions`
    """
    registered_functions = set(db.execute("select name from pragma_function_list").get)
    for name, func in map.items():
        if name not in registered_functions:
            # function names are case insensitive so check again
            for reg_name in registered_functions:
                if 0 == apsw.stricmp(reg_name, name):
                    break
            else:
                db.register_fts5_function(name, convert_string_to_python(func) if isinstance(func, str) else func)


class Table:
    """A helpful wrapper around a FTS5 table

    The table must already exist.  You can use the class method
    :meth:`create` to create a new FTS5 table.

    :param db: Connection to use
    :param name: Table name
    :param schema: Which attached database to use
    """

    @dataclass
    class _cache_class:
        """Data structure representing token cache

        :meta private:
        """

        cookie: int
        "change cookie at time this information was cached"
        tokens: dict[str, int] | None
        "the tokens with how many rows they appear in"
        row_count: int
        "number of rows in the table"
        token_count: int
        "Total number of tokens across all indexed columns in all rows"
        tokens_per_column: list[int]
        "Count of tokens in each column, across all rows.  Unindexed columns have a value of zero"

    def __init__(self, db: apsw.Connection, name: str, schema: str = "main"):
        if not db.table_exists(schema, name):
            raise ValueError(f"Table { schema }.{ name } doesn't exist")
        self._db = db
        self._name = name
        self._schema = schema
        self._qname = quote_name(name)
        self._qschema = quote_name(schema)
        self._cache: Table._cache_class | None = None

        # Do some sanity checking
        assert self.columns == self.structure.columns

        # our helper functions
        register_functions(
            self._db, {func.__name__: func for func in (_apsw_get_statistical_info, _apsw_get_match_info)}
        )

        register_tokenizers(self._db, map_tokenizers)

    def __str__(self):
        return self.__repr__()

    def __repr__(self):
        return f"<FTS5Table { self._schema }.{ self._name} at 0x{id(self):x} on { self._db }>"

    def _get_change_cookie(self) -> int:
        """An int that changes if the content of the table has changed.

        This is useful to validate cached information.
        """
        # See https://sqlite.org/forum/forumpost/2a726411b6974502
        return hash(
            self._db.execute(f"select block from { self._qschema }.{ quote_name(self._name + '_data')} where id=10").get
        )

    change_cookie = property(_get_change_cookie)

    @functools.cached_property
    def columns(self) -> tuple[str, ...]:
        "All columns of this table, including unindexed ones.  Unindexed columns are ignored in queries."

        return tuple(row[1] for row in self._db.execute(f"pragma { self._qschema }.table_info({self._qname})"))

    @functools.cached_property
    def columns_indexed(self) -> tuple[str, ...]:
        "All columns of this table, excluding unindexed ones"
        return tuple(
            row[1]
            for row in self._db.execute(f"pragma { self._qschema }.table_info({self._qname})")
            if row[1] not in self.structure.unindexed
        )

    def column_named(self, name: str) -> str | None:
        """Returns the column matching `name` or `None` if it doesn't exist

        SQLite is ascii case-insensitive, so this tells you the declared name,
        or None if it doesn't exist.
        """
        for column in self.columns:
            if apsw.stricmp(column, name) == 0:
                return column
        return None

    @functools.cached_property
    def structure(self) -> FTS5TableStructure:
        "Structure of the table from the declared SQL"
        found = list(
            sql
            for (sql,) in self._db.execute(
                f"select sql from { self._qschema }.sqlite_schema where type='table' and lower(tbl_name) = lower(?)",
                (self._name,),
            )
        )
        assert len(found) == 1
        return _fts5_vtable_parse(found[0])

    @functools.cached_property
    def quoted_table_name(self) -> str:
        '''Provides the full table name for composing your own queries

        It includes the attached database name and quotes special
        characters like spaces.

        You can't use bindings for table names in queries, so use this
        when constructing a query string::

           my_table = apsw.fts5.Table(con, 'my_table')

           sql = f"""SELECT ... FROM { my_table.quoted_table_name }
                        WHERE ...."""
        '''
        return f"{self._qschema}.{self._qname}"

    def search(self, query: str, locale: str | None = None) -> Iterator[MatchInfo]:
        """Iterates query matches, best matches first

        This avoids the need to write SQL.  See :ref:`the example
        <example_fts_search>`.
        """

        if locale is not None:
            sql = f"select _apsw_get_match_info({self._qname}) from { self.quoted_table_name}(fts5_locale(?,?)) order by rank"
            bindings = (
                locale,
                query,
            )
        else:
            sql = f"select _apsw_get_match_info({self._qname}) from { self.quoted_table_name}(?) order by rank"
            bindings = (query,)

        yield from self._search_internal(sql, bindings)

    def _search_internal(self, sql: str, bindings: apsw.SQLiteValues) -> Iterator[MatchInfo]:
        token = _search_context.set(None)
        qi = None
        try:
            for row in self._db.execute(sql, bindings):
                if qi is None:
                    qi = _search_context.get()
                yield MatchInfo(query_info=qi, **json.loads(row[0]))

        finally:
            _search_context.reset(token)

    def key_tokens(
        self, rowid: int, *, limit: int = 10, columns: str | Sequence[str] | None = None
    ) -> Sequence[tuple[float, str]]:
        """Finds tokens that are dense in this row, but rare in other rows

        This is purely statistical and has no understanding of the
        tokens.  Tokens that occur only in this row are ignored.

        :param rowid: Which row to examine
        :param limit: Maximum number to return
        :param columns: If provided then only look at specified
            column(s), else all indexed columns.
        :returns: A sequence of tuples where each is a tuple of token
           and float score with bigger meaning more unique, sorted
           highest score first.

        See the :ref:`example <example_fts_more>`.

        .. seealso::

            :meth:`text_for_token` to get original document text
            corresponding to a token
        """
        # how many times each token occurs in this row
        token_counter: collections.Counter[str] = collections.Counter()

        if columns is None:
            columns = self.columns_indexed
        elif isinstance(columns, str):
            columns = [columns]

        for column in columns:
            text = self.row_by_id(rowid, column)
            if not text:
                # could be None or empty
                continue
            column_number = self.columns.index(self.column_named(column))
            locale = self._db.execute(f"select fts5_get_locale({self._qname}, {column_number}) from {self.quoted_table_name} where rowid=?", (rowid,)).get
            utf8: bytes = text.encode()
            for token in self.tokenize(utf8, include_colocated=False, include_offsets=False, locale=locale):
                token_counter[token] += 1

        try:
            row_token_count = token_counter.total()
        except AttributeError:
            # Py <= 3.9 doesn't have total so do it the hard way
            row_token_count = sum(token_counter.values())

        # calculate per token score
        scores: list[tuple[float, str]] = []

        threshold = math.log10(row_token_count) if row_token_count else 0

        all_tokens = self.tokens

        for token, occurrences in token_counter.items():
            num_docs = all_tokens.get(token, 0)
            if num_docs < 2 or occurrences < threshold:
                continue

            # This isn't particularly sophisticated, but is good enough
            score = (occurrences / row_token_count) / num_docs

            scores.append((score, token))

        return sorted(scores, reverse=True)[:limit]

    def more_like(
        self, ids: Sequence[int], *, columns: str | Sequence[str] | None = None, token_limit: int = 3
    ) -> Iterator[MatchInfo]:
        """Like :meth:`search` providing results similar to the provided ids.

        This is useful for providing infinite scrolling.  Do a search
        remembering the rowids.  When you get to the end, call this
        method with those rowids.

        :meth:`key_tokens` is used to get key tokens from rows which is
        purely statistical and has no understanding of the text.

        :param ids: rowids to consider
        :param columns: If provided then only look at specified
            column(s), else all indexed columns.
        :param token_limit: How many tokens are extracted from each row.
            Bigger values result in a broader search, while smaller
            values narrow it.

        See the :ref:`example <example_fts_more>`.
        """
        all_tokens: set[str] = set()

        ids = {ids} if isinstance(ids, int) else ids

        if isinstance(columns, str):
            columns = [columns]

        for rowid in ids:
            for _, token in self.key_tokens(rowid, columns=columns, limit=token_limit):
                all_tokens.add(token)

        sql_query = (
            f"select _apsw_get_match_info({self._qname}) from { self.quoted_table_name}(?) where rowid NOT IN ("
            + ",".join("?" * len(ids))
            + ") order by rank"
        )

        if self.supports_query_tokens:
            phrases = [apsw.fts5query.QueryTokens([token]) for token in all_tokens]
        else:
            phrases = [self.text_for_token(token, 1) for token in all_tokens]

        query = {"@": "OR", "queries": phrases}
        if columns:
            query = {"@": "COLUMNFILTER", "columns": columns, "filter": "include", "query": query}

        fts_parsed = apsw.fts5query.from_dict(query)
        fts_query = apsw.fts5query.to_query_string(fts_parsed)

        yield from self._search_internal(sql_query, (fts_query,) + tuple(ids))

    def delete(self, rowid: int) -> bool:
        """Deletes the identified row

        If you are using an external content table then the delete is directed
        to that table.

        :returns: True if a row was deleted
        """
        with self._db:
            c = self._db.total_changes()
            if self.structure.content:
                target_table = f"{self._qschema}.{quote_name(self.structure.content)}"
            else:
                target_table = self.quoted_table_name

            sql = f"delete from { target_table} where "
            sql += quote_name(self.structure.content_rowid or "rowid")
            sql += "=?"

            self._db.execute(sql, (rowid,))
            return c != self._db.total_changes()

    def upsert(self, *args: apsw.SQLiteValue, **kwargs: apsw.SQLiteValue) -> int:
        """Insert or update with columns by positional and keyword arguments

        You can mix and match positional and keyword arguments::

           table.upsert("hello")
           table.upsert("hello", header="world")
           table.upsert(header="world")

        If you specify a ``rowid`` keyword argument that is used as
        the rowid for the insert.  If the corresponding row already
        exists then the row is modified with the provided values.
        rowids are always integers.

        The rowid of the inserted/modified row is returned.

        If you are using an [external
        content](https://www.sqlite.org/fts5.html#external_content_tables)
        table:

        * The insert will be directed to the external content table
        * ``rowid`` will map to the ``content_rowid`` option if used
        * The column names and positions of the FTS5 table, not the external content table is used
        * The FTS5 table is not updated - you should use triggers on
          the external content table to do that.  See the
          ``generate_triggers`` option on :meth:`create`.

        See :ref:`the example <example_fts_update>`
        """
        stmt = self._upsert_sql(len(args), tuple(kwargs.keys()) if kwargs else None)
        if kwargs:
            args = args + tuple(kwargs.values())
        return self._db.execute(stmt, args).get

    @functools.cache
    def _upsert_sql(self, num_args: int, kwargs: tuple[str] | None) -> str:
        "Figure out SQL and column mapping to do the actual upsert"

        columns = self.columns

        if self.structure.content:
            target_table = f"{self._qschema}.{quote_name(self.structure.content)}"
        else:
            target_table = self.quoted_table_name

        num_kwargs = 0 if not kwargs else len(kwargs)
        if num_args + num_kwargs > len(columns):
            total_args = num_args + num_kwargs
            # rowid doesn't count
            if kwargs and any(0 == apsw.stricmp("rowid", k) for k in kwargs):
                total_args -= 1
            if total_args > len(columns):
                raise ValueError(f"Too many values supplied ({total_args}) - max {len(columns)}")
        if num_args + num_kwargs < 1:
            raise ValueError("You must supply some values")

        sql = f"insert or replace into { target_table } ("

        query_column_names = list(columns[:num_args])

        if kwargs:
            seen = set(columns[:num_args])

            for column in kwargs:
                if any(0 == apsw.stricmp(column, c) for c in seen):
                    raise ValueError(f"Column '{column}' provided multiple times")
                seen.add(column)

                if 0 == apsw.stricmp(column, "rowid"):
                    query_column_names.append(self.structure.content_rowid or "rowid")
                    continue

                # find matching column
                for candidate in columns:
                    if 0 == apsw.stricmp(column, candidate):
                        query_column_names.append(column)
                        break
                else:
                    raise ValueError(f"'{column}' is not a column name - {columns=}")
        sql += ",".join(quote_name(col) for col in query_column_names)
        sql += ") values ("
        sql += ",".join("?" for _ in range(num_args + num_kwargs))
        sql += "); select last_insert_rowid()"
        return sql

    # some method helpers pattern, not including all of them yet

    def command_delete(self, rowid: int, *column_values: str):
        """Does `delete <https://www.sqlite.org/fts5.html#the_delete_command>`__

        See :meth:`delete` for regular row deletion.

        If you are using an external content table, it is better to use triggers on
        that table.
        """
        if len(column_values) != len(self.columns):
            raise ValueError(
                f"You need to provide values for every column ({ len(self.columns)}) - got { len(column_values)}"
            )

        values = "('delete',?," + ",".join("?" for _ in range(len(column_values))) + ")"
        cols = f"({ self._qname }, rowid," + ",".join(quote_name(col) for col in self.columns) + ")"
        self._db.execute(
            f"insert into { self._qschema }.{ self._qname }{ cols } values { values }", (rowid, *column_values)
        )

    def command_delete_all(self) -> None:
        """Does `delete all <https://www.sqlite.org/fts5.html#the_delete_all_command>`__

        If you are using an external content table, it is better to use triggers on
        that table.
        """
        self._db.execute(f"insert into { self._qschema}.{ self._qname }({ self._qname}) VALUES('delete-all')")

    def command_integrity_check(self, external_content: bool = True) -> None:
        """Does `integrity check <https://www.sqlite.org/fts5.html#the_integrity_check_command>`__

        If `external_content` is True, then the FTS index is compared to the external content.
        """
        self._db.execute(
            f"insert into { self._qschema}.{ self._qname }({ self._qname}, rank) VALUES('integrity-check', ?)",
            (int(external_content),),
        )

    def command_merge(self, n: int) -> int:
        """Does `merge <https://www.sqlite.org/fts5.html#the_merge_command>`__

        See the documentation for what positive and negative values of `n` mean.

        :returns:  The difference between `sqlite3_total_changes() <https://sqlite.org/c3ref/total_changes.html>`__
                   before and after running the command.
        """
        before = self._db.total_changes()
        self._db.execute(f"insert into { self._qschema}.{ self._qname }({ self._qname}, rank) VALUES('merge', ?)", (n,))
        return self._db.total_changes() - before

    def command_optimize(self) -> None:
        "Does `optimize <https://www.sqlite.org/fts5.html#the_optimize_command>`__"
        self._db.execute(f"insert into { self._qschema}.{ self._qname }({ self._qname}) VALUES('optimize')")

    def command_rebuild(self):
        "Does `rebuild <https://www.sqlite.org/fts5.html#the_rebuild_command>`__"
        self._db.execute(f"insert into { self._qschema}.{ self._qname }({ self._qname}) VALUES('rebuild')")

    # These are the defaults.  The _config table is not updated unless they are changed
    #
    # define FTS5_DEFAULT_PAGE_SIZE   4050
    # define FTS5_DEFAULT_AUTOMERGE      4
    # define FTS5_DEFAULT_USERMERGE      4
    # define FTS5_DEFAULT_CRISISMERGE   16
    # define FTS5_DEFAULT_HASHSIZE    (1024*1024)
    # define FTS5_DEFAULT_DELETE_AUTOMERGE 10
    # define FTS5_DEFAULT_RANK     "bm25"

    def config_automerge(self, val: int | None = None) -> int:
        """Optionally sets, and returns `automerge <https://www.sqlite.org/fts5.html#the_automerge_configuration_option>`__"""
        return self._config_internal("automerge", val, 4)  # type: ignore

    def config_crisismerge(self, val: int | None = None) -> int:
        """Optionally sets, and returns `crisismerge <https://www.sqlite.org/fts5.html#the_crisismerge_configuration_option>`__"""
        return self._config_internal("crisismerge", val, 16)  # type: ignore

    def config_deletemerge(self, val: int | None = None) -> int:
        """Optionally sets, and returns `deletemerge <https://www.sqlite.org/fts5.html#the_deletemerge_configuration_option>`__"""
        return self._config_internal("deletemerge", val, 10)  # type: ignore

    def config_insttoken(self, val: bool | None = None) -> bool:
        """Optionally sets, and returns `insttoken <https://sqlite.org/fts5.html#the_insttoken_configuration_option>`__"""
        return bool(self._config_internal("insttoken", val, False))  # type: ignore

    def config_pgsz(self, val: int | None = None) -> int:
        """Optionally sets, and returns `page size <https://www.sqlite.org/fts5.html#the_pgsz_configuration_option>`__"""
        return self._config_internal("pgsz", val, 4050)  # type: ignore

    def config_rank(self, val: str | None = None) -> str:
        """Optionally sets, and returns `rank <https://www.sqlite.org/fts5.html#the_rank_configuration_option>`__

        When setting rank it must consist of a function name, open
        parentheses, zero or more SQLite value literals that will be
        arguments to the function, and a close parenthesis,  For example
        ``my_func(3, x'aabb', 'hello')``
        """
        return self._config_internal("rank", val, "bm25()")  # type: ignore

    def config_secure_delete(self, val: bool | None = None) -> bool:
        """Optionally sets, and returns `secure-delete <https://www.sqlite.org/fts5.html#the_secure_delete_configuration_option>`__"""
        return bool(self._config_internal("secure-delete", val, False))  # type: ignore

    def config_usermerge(self, val: int | None = None) -> int:
        """Optionally sets, and returns `usermerge <https://www.sqlite.org/fts5.html#the_usermerge_configuration_option>`__"""
        return self._config_internal("usermerge", val, 4)  # type: ignore

    def _config_internal(self, name: str, val: apsw.SQLiteValue, default: apsw.SQLiteValue) -> apsw.SQLiteValue:
        "Internal config implementation"
        if val is not None:
            self._db.execute(
                f"insert into { self._qschema}.{ self._qname }({ self._qname}, rank) VALUES('{name}', ?)", (val,)
            )
        v = self.config(name, prefix="")
        return v if v is not None else default

    def config(self, name: str, value: apsw.SQLiteValue = None, *, prefix: str = "x-apsw-") -> apsw.SQLiteValue:
        """Optionally sets, and gets a `config value <https://www.sqlite.org/fts5.html#configuration_options_config_table_>`__

        If the value is not None, then it is changed.  It is not
        recommended to change SQLite's own values.

        The `prefix` is to ensure your own config names don't clash
        with those used by SQLite.  For example you could remember the
        Unicode version used by your tokenizer, and rebuild if the
        version is updated.

        The advantage of using this is that the names/values will
        survive the FTS5 table being renamed, backed up, restored etc.
        """
        key = prefix + name
        if value is not None:
            self._db.execute(
                f"INSERT OR REPLACE into { self._qschema }.{ quote_name(self._name + '_config') }(k,v) values(?,?)",
                (key, value),
            )
        return self._db.execute(
            f"SELECT v from { self._qschema }.{ quote_name(self._name + '_config') } where k=?", (key,)
        ).get

    @functools.cached_property
    def tokenizer(self) -> apsw.FTS5Tokenizer:
        "Tokenizer instance as used by this table"
        return self._db.fts5_tokenizer(self.structure.tokenize[0], list(self.structure.tokenize[1:]))

    def tokenize(
        self,
        utf8: bytes,
        reason: int = apsw.FTS5_TOKENIZE_DOCUMENT,
        locale: str | None = None,
        include_offsets=True,
        include_colocated=True,
    ):
        "Tokenize the supplied utf8"
        return self.tokenizer(
            utf8, reason, locale, include_offsets=include_offsets, include_colocated=include_colocated
        )

    @functools.cached_property
    def supports_query_tokens(self) -> bool:
        "`True` if you can use :class:`apsw.fts5query.QueryTokens` with this table"
        # we run a tokenization with crafted tokens to detect if
        # QueryTokensTokenizer is present.  hence white space,
        # accents, punctuation, mixed case etc
        tokens = ["  .= -", "\t", "\r\n", "HÉ é"]
        return tokens == self.tokenize(
            apsw.fts5query.QueryTokens(tokens).encode().encode(),
            apsw.FTS5_TOKENIZE_QUERY,
            None,
            include_offsets=False,
            include_colocated=False,
        )

    def _cache_check(self, tokens: bool = False):
        "Ensure cached information is up to date"
        while (
            self._cache is None or self._cache.cookie != self.change_cookie or (tokens and self._cache.tokens is None)
        ):
            with threading.Lock():
                # check if another thread did the work
                cookie = self.change_cookie
                if self._cache is not None and self._cache.cookie == cookie:
                    if tokens and self._cache.tokens is None:
                        # we require tokens
                        pass
                    else:
                        break

                if tokens:
                    n = self.fts5vocab_name("row")
                    all_tokens = dict(self._db.execute(f"select term, doc from { n }"))
                else:
                    all_tokens = None

                vals = {"row_count": 0, "token_count": 0, "tokens_per_column": [0] * len(self.columns)}

                update = self._db.execute(
                    f"select _apsw_get_statistical_info({ self._qname }) from { self.quoted_table_name } limit 1"
                ).get
                if update is not None:
                    vals.update(json.loads(update))

                self._cache = Table._cache_class(cookie=cookie, tokens=all_tokens, **vals)

        return self._cache

    def _tokens(self) -> dict[str, int]:
        """All the tokens as a dict with token as key, and the value being how many rows they are in

        This can take some time on a large corpus - eg 2 seconds on a
        gigabyte dataset with half a million documents and 650,000
        tokens.  It is cached until the next content change.
        """
        return self._cache_check(tokens=True).tokens

    def _row_count(self) -> int:
        "Number of rows in the table"
        return self._cache_check().row_count

    def _token_count(self) -> int:
        "Total number of tokens across all indexed columns in all rows"
        return self._cache_check().token_count

    def _tokens_per_column(self) -> list[int]:
        "Count of tokens in each column, across all rows.  Unindexed columns have a value of zero"
        return self._cache_check().tokens_per_column

    # turn the above into properties
    tokens: dict[str, int] = property(_tokens)
    row_count: int = property(_row_count)
    token_count: int = property(_token_count)
    tokens_per_column: list[int] = property(_tokens_per_column)

    def is_token(self, token: str) -> bool:
        """Returns ``True`` if it is a known token"""
        return token in self.tokens

    def query_suggest(
        self, query: str, threshold: float = 0.01, *, tft_docs: int = 2, locale: str | None = None
    ) -> str | None:
        """Suggests alternate query

        This is useful if a query returns no or few matches.  It is
        purely a statistical operation based on the tokens in the
        query and index. There is no guarantee that there will be more
        (or any) matches.  The query structure (AND, OR, column filters etc)
        is maintained.

        Transformations include:

        * Ensuring column names in column filters are of the closest
          indexed column name
        * Combining such as ``some thing`` to ``something``
        * Splitting such as ``noone`` to ``no one``
        * Replacing unknown/rare words with more popular ones

        The query is parsed, tokenized, replacement tokens
        established, and original text via :meth:`text_for_token` used
        to reconstitute the query.

        :param query: A valid query string
        :param threshold: Fraction of rows between ``0.0`` and
          ``1.00`` to be rare - eg ``0.01`` means a token occurring in
          less than 1% of rows is considered for replacement.  Larger
          fractions increase the likelihood of replacements, while
          smaller reduces it.  A value of ``0`` will only replace
          tokens that are not in the index at all - essentially
          spelling correction only
        :param tft_docs: Passed to :meth:`text_for_token` as the
          ``doc_limit`` parameter.  Larger values produce more
          representative text, but also increase processing time.
        :param locale: Locale used to tokenize the query.
        :returns: ``None`` if no suitable changes were found, or a replacement
          query string.
        """
        # the parsed structure is modified in place
        parsed = apsw.fts5query.parse_query_string(query)

        # save these so we don't constantly check cache or have them
        # change underneath us
        all_tokens = self.tokens
        row_count = self.row_count

        # set if any modifications made
        updated_query = False

        # token count less than this need to be changed
        threshold_rows = int(threshold * self.row_count)

        # which QUERY nodes have already been processed because
        # we process PHRASE children of AND.  The id of objects have
        # to be added because we mutate modes
        done: set[int] = set()

        for _, node in apsw.fts5query.walk(parsed):
            if isinstance(node, apsw.fts5query.COLUMNFILTER):
                new_columns: list[str] = []
                table_columns_upper = []
                indexed_columns = [c for c in self.structure.columns if c not in self.structure.unindexed]
                for column in node.columns:
                    if any(0 == apsw.stricmp(column, table_column) for table_column in indexed_columns):
                        new_columns.append(column)
                        continue
                    # we need to do the get close matches in a case insensitive way
                    if not table_columns_upper:
                        table_columns_upper = [
                            self._db.execute("select upper(?)", (col,)).get for col in indexed_columns
                        ]
                    # re-use closest_tokens even though there aren't tokens
                    replacement = self.closest_tokens(
                        self._db.execute("select upper(?)", (column,)).get,
                        n=1,
                        cutoff=0,
                        min_docs=0,
                        all_tokens=((c, 0) for c in table_columns_upper),
                    )[0][1]
                    # get the original casing back
                    new_columns.append(indexed_columns[table_columns_upper.index(replacement)])
                if new_columns != node.columns:
                    updated_query = True
                    node.columns = new_columns
                continue

            if id(node) in done:
                continue

            if not isinstance(node, (apsw.fts5query.PHRASE, apsw.fts5query.AND)):
                continue

            def is_simple_phrase(n):
                # we only process these and ignore the more
                # complicated ones
                return (
                    isinstance(n, apsw.fts5query.PHRASE)
                    # no query modifiers
                    and n.plus is None
                    and not n.prefix
                    # this means we can space join the adjacent phrases
                    and apsw.fts5query.quote(n.phrase) == n.phrase
                )

            if isinstance(node, apsw.fts5query.AND):
                # we want to work on adjacent PHRASE as though they
                # were one phrase so adjacent tokens can be
                # concatenated.

                seq = []
                i = 0
                while i < len(node.queries):
                    if is_simple_phrase(node.queries[i]):
                        start = i
                        i += 1
                        while i < len(node.queries) and is_simple_phrase(node.queries[i]):
                            i += 1
                        # i now points to next that can't be adjacent
                        seq.append(
                            (
                                apsw.fts5query.PHRASE(" ".join(node.queries[w].phrase for w in range(start, i))),
                                (start, i),
                            )
                        )
                    else:
                        i += 1

            else:
                # we don't touch these
                if node.plus or node.prefix:
                    continue
                seq = [(node, None)]

            done.add(id(node))

            # we have to work backwards so the query_range offsets
            # remain valid
            for phrase, query_range in reversed(seq):
                utf8: bytes = phrase.phrase.encode()
                tokenized = self.tokenize(utf8, apsw.FTS5_TOKENIZE_QUERY, locale)

                # track what happens to each token. False=unchanged,
                # True=deleted, str=new token.  This lets us know if
                # the original text can be recovered from the query or
                # if text_for_token has to be used.
                modified = [False] * len(tokenized)

                # track our progress
                token_num = -1

                while token_num + 1 < len(tokenized):
                    token_num += 1

                    # Deleted/replaced?
                    if modified[token_num] is not False:
                        continue

                    # include colocated
                    tokens: tuple[str, ...] = tokenized[token_num][2:]

                    votes: list[
                        tuple[int, Literal["coalesce"] | Literal["split"] | Literal["replace"], str | tuple[str]]
                    ] = []

                    rows = max(all_tokens.get(token, 0) for token in tokens)

                    # can we coalesce with next token?
                    if rows <= threshold_rows and len(tokens) == 1 and token_num + 1 < len(tokenized):
                        next_tokens = tokenized[token_num + 1][2:]
                        # only try if there is one next token (no colocated)
                        if len(next_tokens) == 1:
                            combined: str = tokens[0] + next_tokens[0]
                            # is combined more popular than separate tokens?
                            if all_tokens.get(combined, 0) > min(
                                all_tokens.get(tokens[0], 0), all_tokens.get(next_tokens[0], 0)
                            ):
                                votes.append((all_tokens[combined], "coalesce", combined))

                    # split apart?
                    if len(tokens) == 1 and rows <= threshold_rows:
                        token = tokens[0]
                        # there could be multiple candidates
                        # eg abc could become ab c, or a bc
                        candidates: list[tuple[str, str]] = []
                        # used a lot
                        test = token.startswith
                        for prefix in all_tokens:
                            if test(prefix) and token[len(prefix) :] in all_tokens:
                                candidates.append((prefix, token[len(prefix) :]))

                        if candidates:
                            best = -1, None, None
                            for prefix, suffix in candidates:
                                split_rows = min(all_tokens[prefix], all_tokens[suffix])
                                if split_rows > best[0]:
                                    best = split_rows, prefix, suffix
                            if best[0] > threshold_rows:
                                votes.append((best[0], "split", best[1:]))

                    # replace with more popular token?
                    if rows <= threshold_rows:
                        replacement = self.closest_tokens(
                            tokens[0],
                            n=10,
                            # we accept anything if current is not a token
                            cutoff=0.6 if rows else 0,
                            # it must be more popular
                            min_docs=max(threshold_rows, rows + 1),
                            all_tokens=all_tokens.items(),
                        )

                        if replacement:
                            # bias how different the tokens are with
                            # how many rows they are in
                            for i, (score, token) in enumerate(replacement):
                                replacement[i] = (score * 2 + math.log1p(all_tokens[token] / row_count), token)

                            replacement.sort(reverse=True)
                            replacement = replacement[0][1]
                            votes.append((all_tokens[replacement], "replace", replacement))

                    if votes:
                        # if the popularity is identical then this is the
                        # preferred order (biggest number wins)
                        priority = {
                            # coalesce and replace can be the same
                            # replacement but we want coalesce because
                            # it consumes the next token
                            "coalesce": 3,
                            "split": 2,
                            # replace is last resort
                            "replace": 1,
                        }

                        _, action, new = sorted(votes, reverse=True, key=lambda x: (x[0], priority[x[1]], x[2]))[0]
                        modified[token_num] = new
                        if action == "coalesce":
                            modified[token_num + 1] = True
                        else:
                            assert action in ("split", "replace")

                # updates?
                if any(item is not False for item in modified):
                    updated_query = True
                    new_text = ""
                    # track inter-token separation
                    last_end = 0
                    # token[0,1] are the offsets into utf8
                    for token, mod in zip(tokenized, modified):
                        # unchanged
                        if mod is False:
                            new_text += utf8[last_end : token[0]].decode() + apsw.fts5query.quote(
                                utf8[token[0] : token[1]].decode()
                            )
                            last_end = token[1]
                            continue
                        # deleted
                        if mod is True:
                            last_end = token[1]
                            continue
                        # replaced with a different token
                        if isinstance(mod, str):
                            new_text += utf8[last_end : token[0]].decode() + apsw.fts5query.quote(
                                self.text_for_token(mod, tft_docs)
                            )
                            last_end = token[1]
                            continue

                        # multiple tokens - take previous as separator else space
                        sep = utf8[last_end : token[0]].decode() if last_end != 0 else " "
                        new_text += sep + sep.join(apsw.fts5query.quote(self.text_for_token(m, tft_docs)) for m in mod)
                        last_end = token[1]

                    phrase.phrase = new_text
                    if query_range:
                        # we need to regenerate a new AND with the
                        # PHRASEs within
                        new_node = apsw.fts5query.parse_query_string(new_text)
                        for _, child in apsw.fts5query.walk(new_node):
                            done.add(id(child))
                        done.add(id(new_node))
                        node.queries[query_range[0] : query_range[1]] = [new_node]

        if updated_query:
            apsw.fts5query._flatten(parsed)

        return apsw.fts5query.to_query_string(parsed) if updated_query else None

    def token_frequency(self, count: int = 10) -> list[tuple[str, int]]:
        """Most frequent tokens, useful for building a stop words list

        This counts the total occurrences of the token, so appearing
        1,000 times in 1 document counts the same as once each in
        1,000 documents.

        .. seealso::

            * :meth:`token_doc_frequency`
            * :ref:`Example <example_fts_tokens>`
        """
        n = self.fts5vocab_name("row")
        return self._db.execute(f"select term, cnt from { n } order by cnt desc limit ?", (count,)).get

    def token_doc_frequency(self, count: int = 10) -> list[tuple[str, int]]:
        """Most frequent occurring tokens, useful for building a stop words list

        This counts the total number of documents containing the
        token, so appearing 1,000 times in 1 document counts as 1,
        while once each in 1,000 documents counts as 1,000.

        .. seealso::

            * :meth:`token_frequency`
            * :ref:`Example <example_fts_tokens>`
        """
        n = self.fts5vocab_name("row")
        return self._db.execute(f"select term, doc from { n } order by doc desc limit ?", (count,)).get

    def text_for_token(self, token: str, doc_limit: int) -> str:
        """Provides the original text used to produce ``token``

        Different text produces the same token because case can be
        ignored, accents and punctuation removed, synonyms and other
        processing.

        This method finds the text that produced a token, by
        re-tokenizing the documents containing the token.  Highest
        rowids are examined first so this biases towards the newest
        content.

        :param token: The token to find
        :param doc_limit: Maximum number of documents to examine.  The
            higher the limit the longer it takes, but the more
            representative the text is.
        :returns: The most popular text used to produce the token in
            the examined documents

        See :ref:`the example <example_fts_tokens>`.
        """
        text_for_token_counter: collections.Counter[bytes] = collections.Counter()

        last = None, None
        tokens: list[tuple[int, int, str]] = []

        # The doc, col come out in random orders.  This resulted in
        # tokenizing the same documents over and over again.  Ordering
        # by doc then col solves that problem.  SQLite has to use a
        # temp btree to do the ordering, but testing with the enron
        # corpus had it taking 2 milliseconds.

        sql = f"""select doc, col, offset
                from { self.fts5vocab_name('instance') }
                where term=?
                order by doc desc, col
                limit ?"""

        for rowid, col, offset in self._db.execute(sql, (token, doc_limit)):
            if (rowid, col) != last:
                # new doc to process
                doc: bytes = self.row_by_id(rowid, col).encode()
                locale = None
                if self.structure.locale:
                    # col from above is a column name, but
                    # fts5_get_locale wants a column number
                    locale = self._db.execute(
                        f"select fts5_get_locale({self._qname}, ?) from {self.quoted_table_name} where rowid=?",
                        (self.columns.index(col), rowid),
                    ).get
                tokens = self.tokenize(doc, locale=locale, include_colocated=False)
                last = rowid, col
            text_for_token_counter[doc[tokens[offset][0] : tokens[offset][1]]] += 1

        try:
            return text_for_token_counter.most_common(1)[0][0].decode()
        except IndexError:
            # most_common returns zero entries either because the
            # token doesn't exist or doc_limit is less than 1
            raise ValueError(f"{token=} not found") from None

    def row_by_id(self, id: int, column: str | Sequence[str]) -> apsw.SQLiteValue | tuple[apsw.SQLiteValue]:
        """Returns the contents of the row `id`

        You can request one column,, or several columns.  If one
        column is requested then just that value is returned, and a
        tuple of values for more than column.

        :exc:`KeyError` is raised if the row does not exist.

        See :ref:`the example <example_fts_update>`.
        """
        if isinstance(column, str):
            for (row,) in self._db.execute(
                f"select { quote_name(column)} from { self.quoted_table_name } where rowid=?", (id,)
            ):
                return row
        else:
            cols = ",".join(quote_name(c) for c in column)
            for row in self._db.execute(f"select {cols} from { self.quoted_table_name } where rowid=?", (id,)):
                return row
        raise KeyError(f"row {id=} not found")

    def closest_tokens(
        self,
        token: str,
        *,
        n: int = 10,
        cutoff: float = 0.6,
        min_docs: int = 1,
        all_tokens: Iterable[tuple[str, int]] | None = None,
    ) -> list[tuple[float, str]]:
        """Returns closest known tokens to ``token`` with score for each

        This uses :func:`difflib.get_close_matches` algorithm to find
        close matches.  Note that it is a statistical operation, and
        has no understanding of the tokens and their meaning.

        :param token: Token to use
        :param n: Maximum number of tokens to return
        :param cutoff: Passed to :func:`difflib.get_close_matches`.
          Larger values require closer matches and decrease
          computation time.
        :param min_docs: Only test against other tokens that appear in
          at least this many rows.  Experience is that about a
          third of tokens appear only in one row.  Larger values
          significantly decrease computation time, but reduce the
          candidates.
        :param all_tokens:  A sequence of tuples of candidate token
          and number of rows it occurs in.  If not provided then
          :attr:`tokens` is used.
        """

        if all_tokens is None:
            all_tokens = self.tokens.items()

        result: list[tuple[float, str]] = []

        # get_close_matches is inlined here to deal with our data
        # shape.  We also keep track of the current best matches
        # dynamically increasing the cutoff value to decrease the
        # total amount of work.

        sm = difflib.SequenceMatcher()
        sm.set_seq2(token)
        for t in all_tokens:
            if t[1] < min_docs or t[0] == token:
                continue
            sm.set_seq1(t[0])
            if sm.real_quick_ratio() >= cutoff and sm.quick_ratio() >= cutoff and (ratio := sm.ratio()) >= cutoff:
                result.append((ratio, t[0]))
                if len(result) > n:
                    result.sort(reverse=True)
                    result.pop()
                    cutoff = result[-1][0]
        result.sort(reverse=True)
        return result

    @functools.cache
    def fts5vocab_name(self, type: Literal["row"] | Literal["col"] | Literal["instance"]) -> str:
        """
        Creates a `fts5vocab table <https://www.sqlite.org/fts5.html#the_fts5vocab_virtual_table_module>`__
        in temp and returns fully quoted name
        """
        base = f"fts5vocab_{ self._schema }_{ self._name }_{ type }".replace('"', '""')

        name = f'temp."{base}"'

        self._db.execute(
            f"""create virtual table if not exists { name } using fts5vocab(
                    {self._qschema}, {self._qname}, "{ type }")"""
        )
        return name

    @classmethod
    def create(
        cls,
        db: apsw.Connection,
        name: str,
        columns: Iterable[str] | None,
        *,
        schema: str = "main",
        unindexed: Iterable[str] | None = None,
        tokenize: Iterable[str] | None = None,
        support_query_tokens: bool = False,
        rank: str | None = None,
        prefix: Iterable[int] | int | None = None,
        content: str | None = None,
        content_rowid: str | None = None,
        contentless_delete: bool = False,
        contentless_unindexed: bool = False,
        columnsize: bool = True,
        detail: Literal["full"] | Literal["column"] | Literal["none"] = "full",
        tokendata: bool = False,
        locale: bool = False,
        generate_triggers: bool = False,
        drop_if_exists: bool = False,
    ) -> Self:
        """Creates the table, returning a :class:`Table` on success

        You can use :meth:`apsw.Connection.table_exists` to check if a
        table already exists.

        :param db: connection to create the table on
        :param name: name of table
        :param columns: A sequence of column names.  If you are using
           an external content table (recommended) you can supply
           ``None`` and the column names will be from the table named by
           the ``content`` parameter
        :param schema: Which attached database the table is being
            created in
        :param unindexed: Columns that will be `unindexed
            <https://www.sqlite.org/fts5.html#the_unindexed_column_option>`__
        :param tokenize: The `tokenize option
            <https://sqlite.org/fts5.html#tokenizers>`__.  Supply as a
            sequence of strings which will be correctly quoted
            together.
        :param support_query_tokens: Configure the `tokenize` option
            to allow :class:`queries using tokens
            <apsw.fts5query.QueryTokens>`.
        :param rank: The `rank option
            <https://www.sqlite.org/fts5.html#the_rank_configuration_option>`__
            if not using the default.  See :meth:`config_rank` for required
            syntax.
        :param prefix: The `prefix option
            <https://sqlite.org/fts5.html#prefix_indexes>`__.  Supply
            an int, or a sequence of int.
        :param content: Name of the external content table.  The
            external content table must be in the same database as the
            FTS5 table.
        :param content_rowid: Name of the `content rowid column
            <https://sqlite.org/fts5.html#external_content_tables>`__
            if not using the default when using an external content
            table
        :param contentless_delete: Set the `contentless delete option
            <https://sqlite.org/fts5.html#contentless_delete_tables>`__
            for contentless tables.
        :param contentless_unindexed: Set the `contentless unindexed
            option
            <https://www.sqlite.org/fts5.html#the_contentless_unindexed_option>`__
            for contentless tables
        :param columnsize: Indicate if the `column size tracking
            <https://sqlite.org/fts5.html#the_columnsize_option>`__
            should be disabled to save space
        :param detail: Indicate if `detail
            <https://sqlite.org/fts5.html#the_detail_option>`__ should
            be reduced to save space
        :param tokendata: Indicate if `tokens have separate data after
            a null char
            <https://sqlite.org/fts5.html#the_tokendata_option>`__
        :param locale: Indicate if a `locale
            <https://www.sqlite.org/fts5.html#the_locale_option>`__ is
            available to tokenizers and stored in the table
        :param generate_triggers: If using an external content table
            and this is ``True``, then `triggers are created
            <https://sqlite.org/fts5.html#external_content_tables>`__
            to keep this table updated with changes to the external
            content table.  These require a table not a view.
        :param drop_if_exists: The FTS5 table will be dropped if it
            already exists, and then created.

        If you create with an external content table, then
        :meth:`command_rebuild` and :meth:`command_optimize` will be
        run to populate the contents.
        """

        qschema = quote_name(schema)
        qname = quote_name(name)
        if columns is None:
            if not content:
                raise ValueError("You need to supply columns, or specify an external content table name")
            # check for a table/view
            if not any(
                0 == apsw.stricmp(content, name)
                for (name,) in db.execute(f"select name from {qschema}.sqlite_schema where type='table' or type='view'")
            ):
                raise ValueError(f"external table {schema=} . {content=} does not exist")
            columns: tuple[str, ...] = tuple(
                name
                for (name,) in db.execute(f"select name from { qschema}.pragma_table_info(?)", (content,))
                if 0 != apsw.stricmp(name, content_rowid or "rowid")
            )
        else:
            columns: tuple[str, ...] = tuple(columns)

        if unindexed is not None:
            unindexed: set[str] = set(unindexed)
            for c in unindexed:
                if c not in columns:
                    raise ValueError(f'column "{ c }" is in unindexed, but not in {columns=}')
        else:
            unindexed: set[str] = set()

        if support_query_tokens and tokenize is None:
            tokenize = ["unicode61"]

        if tokenize is not None:
            tokenize = tuple(tokenize)
            if support_query_tokens and tokenize[0] != "querytokens":
                tokenize = ("querytokens",) + tokenize
            # using outside double quote and inside single quote out
            # of all the combinations available
            qtokenize = quote_name(" ".join(quote_name(arg, "'") for arg in tokenize), '"')
        else:
            qtokenize = None

        if prefix is not None:
            if isinstance(prefix, int):
                prefix: str = str(prefix)
            else:
                prefix = quote_name(" ".join(str(p) for p in prefix), "'")

        qcontent_rowid = quote_name(content_rowid) if content and content_rowid is not None else None
        qcontentless_delete: str | None = str(int(contentless_delete)) if content == "" else None
        qcontentless_unindexed: str | None = str(int(contentless_unindexed)) if content == "" else None
        tokendata: str = str(int(tokendata))
        locale: str = str(int(locale))

        qcontent = quote_name(content) if content is not None else None

        sql: list[str] = []
        if drop_if_exists:
            sql.append(f"drop table if exists { qschema }.{ qname };")
        sql.append(f"create virtual table { qschema }.{ qname} using fts5(")
        sql.append(
            ", ".join(f"{ quote_name(column) + (' UNINDEXED' if column in unindexed else '') }" for column in columns)
        )
        for option, value in (
            ("prefix", prefix),
            ("tokenize", qtokenize),
            ("content", qcontent),
            ("content_rowid", qcontent_rowid),
            ("contentless_delete", qcontentless_delete),
            ("contentless_unindexed", qcontentless_unindexed),
            # for these we omit them for default value
            ("columnsize", "0" if not columnsize else None),
            ("detail", detail if detail != "full" else None),
            ("tokendata", tokendata if tokendata != "0" else None),
            ("locale", locale if locale != "0" else None),
        ):
            if value is not None:
                sql.append(f", { option } = { value}")
        sql.append(")")

        register_tokenizers(db, map_tokenizers)

        with db:
            db.execute("".join(sql))
            inst = cls(db, name, schema=schema)
            if rank:
                try:
                    inst.config_rank(rank)
                except apsw.SQLError:
                    raise ValueError(f"{rank=} is not accepted by FTS5") from None
            if content:
                if generate_triggers:
                    qrowid = quote_name(content_rowid if content_rowid is not None else "_ROWID_")
                    cols = ", ".join(quote_name(column) for column in columns)
                    old_cols = ", ".join(f"old.{quote_name(column)}" for column in columns)
                    new_cols = ", ".join(f"new.{quote_name(column)}" for column in columns)
                    trigger_names = tuple(
                        quote_name(f"fts5sync_{ content }_to_{ name }_{ reason }")
                        for reason in (
                            "insert",
                            "delete",
                            "update",
                        )
                    )
                    db.execute(f"""
drop trigger if exists { qschema }.{ trigger_names[0] };
drop trigger if exists { qschema }.{ trigger_names[1] };
drop trigger if exists { qschema }.{ trigger_names[2] };
create trigger { qschema }.{ trigger_names[0] } after insert on { qcontent }
begin
    insert into { qname }(rowid, { cols }) values (new.{ qrowid }, { new_cols });
end;
create trigger { qschema }.{ trigger_names[1] } after delete on { qcontent }
begin
    insert into { qname }({ qname }, rowid, { cols }) values('delete', old.{ qrowid }, { old_cols });
end;
create trigger { qschema }.{ trigger_names[2] } after update on { qcontent }
begin
    insert into { qname }({ qname }, rowid, { cols }) values('delete', old.{ qrowid }, { old_cols });
    insert into { qname }(rowid, { cols }) values (new.{ qrowid }, { new_cols });
end;
                               """)
                inst.command_rebuild()
                inst.command_optimize()

        return inst


def _apsw_get_statistical_info(api: apsw.FTS5ExtensionApi) -> str:
    "Behind the scenes function used to return stats about the table"
    return json.dumps(
        {
            "row_count": api.row_count,
            "token_count": api.column_total_size(),
            "tokens_per_column": [api.column_total_size(c) for c in range(api.column_count)],
        }
    )


_search_context: ContextVar[QueryInfo | None] = ContextVar("search_context")


def _do_query_info(api: apsw.FTS5ExtensionApi):
    _search_context.set(
        QueryInfo(
            phrases=api.phrases,
        )
    )


def _apsw_get_match_info(api: apsw.FTS5ExtensionApi) -> str:
    if _search_context.get() is None:
        _do_query_info(api)
    return json.dumps(
        {
            "rowid": api.rowid,
            "column_size": tuple(api.column_size(c) for c in range(api.column_count)),
            "phrase_columns": tuple(api.phrase_columns(p) for p in range(api.phrase_count)),
        }
    )


@dataclass(frozen=True)
class FTS5TableStructure:
    """Table structure from SQL declaration available as :attr:`Table.structure`

    See :ref:`the example <example_fts_structure>`"""

    name: str
    "Table nane"
    columns: tuple[str]
    "All column names"
    unindexed: set[str]
    "Which columns are `unindexed <https://www.sqlite.org/fts5.html#the_unindexed_column_option>`__"
    tokenize: tuple[str]
    "`Tokenize <https://www.sqlite.org/fts5.html#tokenizers>`__ split into arguments"
    prefix: set[int]
    "`Prefix <https://www.sqlite.org/fts5.html#prefix_indexes>`__ values"
    content: str | None
    "`External content/content less <https://www.sqlite.org/fts5.html#external_content_and_contentless_tables>`__ or ``None`` for regular"
    content_rowid: str | None
    "`Rowid <https://www.sqlite.org/fts5.html#external_content_tables>`__ if external content table else ``None``"
    contentless_delete: bool | None
    "`Contentless delete option <https://www.sqlite.org/fts5.html#contentless_delete_tables>`__ if contentless table else ``None``"
    contentless_unindexed: bool | None
    "`Contentless unindexed option <https://www.sqlite.org/fts5.html#the_contentless_unindexed_option>`__ if contentless table else ``None``"
    columnsize: bool
    "`Columnsize option <https://www.sqlite.org/fts5.html#the_columnsize_option>`__"
    tokendata: bool
    "`Tokendata option <https://www.sqlite.org/fts5.html#the_tokendata_option>`__"
    locale: bool
    "`Locale option <https://www.sqlite.org/fts5.html#the_locale_option>`__"
    detail: Literal["full"] | Literal["column"] | Literal["none"]
    "`Detail option <https://www.sqlite.org/fts5.html#the_detail_option>`__"


def _fts5_vtable_tokens(sql: str) -> list[str]:
    """Parse a SQL statement from sqlite_main create table using quoting rules"""
    # See tokenize.c in SQLIte source for reference of how various
    # codepoints are treated

    def skip_spacing():
        "Return true if we skipped any spaces or comments"
        nonlocal pos
        original_pos = pos
        while sql[pos] in "\x09\x0a\x0c\x0d\x20":
            pos += 1
            if pos == len(sql):
                return True

        # comments
        if sql[pos : pos + 2] == "--":
            pos += 2
            while sql[pos] != "\n":
                pos += 1
        elif sql[pos : pos + 2] == "/*":
            pos = 2 + sql.index("*/", pos + 2)

        return pos != original_pos

    def absorb_quoted():
        nonlocal pos
        if sql[pos] not in "\"`'[":
            return False
        # doubled quote escapes it, except square brackets
        start = pos + 1
        end = sql[pos] if sql[pos] != "[" else "]"
        while True:
            pos = sql.index(end, pos + 1)
            if sql[pos : pos + 2] == end + end and end != "]":
                pos += 1
                continue
            break
        res.append(sql[start:pos].replace(end + end, end))
        pos += 1
        return True

    def absorb_word():
        # identifier-ish.  fts5 allows integers as column names!
        nonlocal pos

        start = pos

        while pos < len(sql):
            # all non-ascii chars are part of words
            if sql[pos] in "0123456789_ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz" or ord(sql[pos]) >= 0x80:
                pos += 1
            else:
                break
        if pos != start:
            res.append(sql[start:pos])
            return True
        return False

    res: list[str] = []
    pos = 0
    while pos < len(sql):
        if skip_spacing():
            continue
        if absorb_quoted():
            continue
        # punctuation
        if sql[pos] in "=,.;()":
            res.append(sql[pos])
            pos += 1
            continue
        if absorb_word():
            continue
        raise ValueError(f"Don't know how to handle '{sql[pos]}' in '{sql}' at {pos=}")

    return res


def _fts5_vtable_parse(sql: str) -> FTS5TableStructure:
    """Turn sql into structure"""
    tokens = _fts5_vtable_tokens(sql)
    # SQLite uppercases these
    if tokens[:3] != ["CREATE", "VIRTUAL", "TABLE"]:
        raise ValueError(f"Not a virtual table in {sql=}")
    vals: dict[str, Any] = {
        "unindexed": set(),
        "tokenize": ("unicode61",),
        "prefix": set(),
        "content": None,
        "content_rowid": "_ROWID_",
        "columnsize": True,
        "detail": "full",
        "contentless_delete": False,
        "contentless_unindexed": False,
        "tokendata": False,
        "locale": False,
    }
    vals["name"] = tokens[3]
    if tokens[4].upper() != "USING":
        raise ValueError(f"Expected USING in {sql=}")
    if tokens[5].upper() != "FTS5":
        raise ValueError(f"Not using FTS5 in {sql=}")
    assert tokens[6] == "("
    vals["columns"] = []
    pos = 7
    while tokens[pos] not in {")", ";"}:
        vals["columns"].append(tokens[pos])
        pos += 1
        if tokens[pos].upper() == "UNINDEXED":
            vals["unindexed"].add(vals["columns"][-1])
            pos += 1
        if tokens[pos] in {",", ")"}:
            pos += tokens[pos] == ","
            continue
        assert tokens[pos] == "="
        key = vals["columns"].pop()
        value = tokens[pos + 1]
        if key == "tokenize":
            vals[key] = tuple(_fts5_vtable_tokens(value))
        elif key == "prefix":
            if value.isdigit():
                vals[key].add(int(value))
            else:
                vals[key].update(int(v) for v in value.split())
        elif key in {"content", "content_rowid", "detail"}:
            vals[key] = value
        elif key in {"contentless_delete", "contentless_unindexed", "columnsize", "tokendata", "locale"}:
            vals[key] = bool(int(value))
        else:
            raise ValueError(f"Unknown option '{key}' in {sql=}")
        pos += 2
        if tokens[pos] == ",":
            pos += 1

    if vals["content"] != "":
        vals["contentless_delete"] = None
        vals["contentless_unindexed"] = None
    if not vals["content"]:
        vals["content_rowid"] = None
    vals["columns"] = tuple(vals["columns"])
    return FTS5TableStructure(**vals)


def quote_name(name: str, quote: str = '"') -> str:
    """Quotes name to ensure it is parsed as a name

    :meta private:
    """
    name = name.replace(quote, quote * 2)
    return quote + name + quote


if __name__ == "__main__":
    import argparse
    import html
    import json
    import unicodedata

    import apsw.bestpractice

    apsw.bestpractice.apply(apsw.bestpractice.recommended)

    # This code evolved a lot, and was not intelligently designed.  Sorry.

    def show_tokenization(
        options, tok: apsw.FTS5Tokenizer, utf8: bytes, reason: int, locale: str | None
    ) -> tuple[str, list[str]]:
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
        for toknum, row in enumerate(tok(utf8, reason, locale)):
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
            r = f"U+{ord(c):04X} "
            gc = apsw.unicode.category(c)
            explain = unicode_categories
            r += f"{ gc } { explain[gc] }"
            for meth in (
                unicodedata.bidirectional,
                unicodedata.combining,
                unicodedata.mirrored,
            ):
                v = meth(c)
                if v:
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
                b = b.decode(errors="replace")
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
                out += f"<td>{ html.escape(row.utf8.decode(errors='replace')) }</td>"
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
            out += f"<td>{ html.escape(row.utf8.decode(errors='replace')) }</td>"
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
    <li>You are shown what <a href="https://www.unicode.org/reports/tr15/#Introduction">nornal forms</a> each block
        conforms to
    <li>In the original text test file, lines beginning with ## are ignored and lines beginning with # become
        each test block
    <li>Make sure you test the different tokenize reasons!
    </details>"""
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
        prog="python3 -m apsw.fts5",
        description="""Runs FTS5 tokenizer against test text producing a HTML report for manual inspection.

        The FTS5 builtin tokenizers are ascii, trigram, unicode61, and porter. apsw.fts5 tokenizers are
        registered as unicodewords, simplify, json, html, synonyms, regex, stopwords,
        transform, and ngram""",
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
    parser.add_argument("--locale", help="Optional fts5_locale to use")
    parser.add_argument(
        "--register",
        action="append",
        default=[],
        help="Registers tokenizers. Format is name=mod.submod.callable "
        "where name is what is registered with FTS5 and callable is the factory function.  The module containing "
        "callable will be imported.  Specify this option multiple times to register multiple tokenizers.",
        metavar="name=mod.part.callable",
    )

    # registrations built in
    register_tokenizers(con, map_tokenizers)

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
        help="Tokenizers and arguments to run. "
        "For example to run the trigram tokenizer on unicode61 keeping diacritics use: trigram unicode61 remove_diacritics 0",
    )
    options = parser.parse_args()

    # systrem python on macos gives text based output when - is used even though binary was requested
    if hasattr(options.output, "encoding"):
        # so use raw underlying binary
        options.output = options.output.buffer

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
            utf8 = unicodedata.normalize(options.normalize, utf8.decode(errors="replace")).encode()
        h, tokens = show_tokenization(options, tok, utf8, tokenize_reasons[options.reason], options.locale)
        results.append((comment, utf8, h, options.reason, tokens))

    w = lambda s: options.output.write(s.encode() + b"\n")

    w('<html><head><meta charset="utf-8"></head><body>')
    w(show_tokenization_css)
    w(show_tokenization_header)
    w(show_tokenization_remark("Args: " + str(sys.argv), kind="args"))
    sections = []
    counter = 1
    for comment, utf8, h, reason, tokens in results:
        normalized = [
            f for f in ("NFC", "NFKC", "NFD", "NFKD") if unicodedata.is_normalized(f, utf8.decode(errors="replace"))
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
                compare=(utf8.decode(errors="replace"), tokens),
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
