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
import threading

# avoid clashing with html as a parameter name
import html as html_module
import html.parser as html_parser_module
from dataclasses import dataclass

from typing import Callable, Sequence, Any, Literal, Iterable
from types import ModuleType

import apsw
import apsw.ext
import apsw.unicode
import apsw._unicode

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

    FTS5 tokenizers operate on :ref:`UTF8 bytes for the text and offsets <byte_offsets>`.
    This decorator provides your tokenizer with text and expects text offsets
    back, performing the conversions back to UTF8 byte offsets.
    """

    @functools.wraps(func)
    def string_tokenizer_wrapper(con: apsw.Connection, args: list[str], **kwargs) -> apsw.Tokenizer:
        inner_tokenizer = func(con, args, **kwargs)

        @functools.wraps(inner_tokenizer)
        def outer_tokenizer(utf8: bytes, flags: int):
            upm = apsw._unicode.to_utf8_position_mapper(utf8)

            for start, end, *tokens in inner_tokenizer(upm.str, flags):
                yield upm(start), upm(end), *tokens

        return outer_tokenizer

    return string_tokenizer_wrapper


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

    def tokenize(text: str, flags: int):
        yield from apsw.unicode.word_iter_with_offsets(text, 0, **options)

    return tokenize


def SimplifyTokenizer(con: apsw.Connection, args: list[str]) -> apsw.Tokenizer:
    """Tokenizer wrapper that simplifies tokens by neutralizing case conversion, canonicalization, and diacritic/mark removal

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

    def tokenize(utf8: bytes, flags: int):
        tok = options["+"]
        for start, end, *tokens in tok(utf8, flags):
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

    This tokenizer works on units of user perceived characters (grapheme clusters)
    where more than one codepoint can make up what seems to be one character.

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

    """

    spec = {
        "ngrams": TokenizerArgument(default="3", convertor=convert_number_ranges, convert_default=True),
        "categories": TokenizerArgument(default="*", convertor=convert_unicode_categories, convert_default=True),
        "emoji": TokenizerArgument(default=True, convertor=convert_boolean),
        "regional_indicator": TokenizerArgument(default=True, convertor=convert_boolean),
    }

    options = parse_tokenizer_args(spec, con, args)

    def tokenize(text: str, flags: int):
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


def extract_html_text(html: str) -> tuple[str, apsw._unicode.OffsetMapper]:
    """Extracts text from HTML using :class:`html.parser.HTMLParser` under the hood

    :meta private:
    """

    non_spacing_tags = {"i", "b", "span"}

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

        def separate(self, space: bool = True):
            if self.last:
                self.om.add(self.last[0], self.last[1], self.real_offset)
                self.last = None
            if space:
                self.om.separate()

        def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
            self.current_tag = tag.lower()
            if tag.lower() == "svg":
                self.svg_nesting_level += 1
            self.separate(tag.lower() not in non_spacing_tags)

        def handle_endtag(self, tag: str) -> None:
            self.current_tag = None
            if tag.lower() == "svg":
                self.svg_nesting_level -= 1
            self.separate(tag.lower() not in non_spacing_tags)

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
            # the parent version does a lot of work trying to keep
            # track of line numbers which is pointless for us
            self.real_offset = j
            return j

    h = _HTMLTextExtractor(html)

    return h.om.text, h.om


@StringTokenizer
def HTMLTokenizer(con: apsw.Connection, args: list[str]) -> apsw.Tokenizer:
    """Extracts text from HTML suitable for passing on to other tokenizers

    This should be before the actual tokenizer in the tokenizer list.  Behind the scenes
    it extracts text from the HTML, and manages the offset mapping between the
    HTML and the text passed on to other tokenizers.  It also expands
    entities and charrefs.

    If the html doesn't start with whitespace then < or &, it is not
    considered HTML and will be passed on unprocessed.  This would
    typically be the case for queries.
    """
    spec = {"+": None}
    options = parse_tokenizer_args(spec, con, args)

    def tokenize(html: str, flags: int):
        # we only tokenize what looks like html.  Human typed queries
        # are unlikely to be html.  We allow for ampersand to catch
        # entity searches.
        if not re.match(r"\s*[<&]", html):
            yield from string_tokenize(options["+"], html, flags)
            return

        text, om = extract_html_text(html)

        for start, end, *tokens in string_tokenize(options["+"], text, flags):
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
      ``9`` (default) or ``1`` if keys are extracted in addition to
      values

    If the jsom doesn't start with whitespace then { or [, it is not
    considered JSON and will be passed on unprocessed.  This would
    typically be the case for queries.
    """
    spec = {
        "include_keys": TokenizerArgument(default=False, convertor=convert_boolean),
        "+": None,
    }
    options = parse_tokenizer_args(spec, con, args)

    def tokenize(json: str, flags: int):
        # we only tokenize what looks like json.  Human typed queries
        # are unlikely to be json.
        if not re.match(r"\s*[{\[]", json):
            yield from string_tokenize(options["+"], json, flags)
            return

        text, mapper = extract_json(json, options["include_keys"])
        for start, end, *tokens in string_tokenize(options["+"], text, flags):
            yield mapper(start), mapper(end), *tokens

    return tokenize


def string_tokenize(tokenizer: apsw.FTS5Tokenizer, text: str, flags: int):
    """Tokenizer caller to get string offsets back

    Calls the tokenizer doing the conversion of `text` to UTF8, and converting the received
    UTF8 offsets back to `text` offsets.
    """
    upm = apsw._unicode.from_utf8_position_mapper(text)
    for bytes_start, bytes_end, *tokens in tokenizer(upm.bytes, flags):
        yield (
            upm(bytes_start),
            upm(bytes_end),
            *tokens,
        )


@StringTokenizer
def RegexTokenizer(
    con: apsw.Connection, args: list[str], *, pattern: str | re.Pattern, flags: int = re.NOFLAG
) -> apsw.Tokenizer:
    r"""Finds tokens using a regular expression

    :param pattern: The `regular expression <https://docs.python.org/3/library/re.html#regular-expression-syntax>`__.
        For example :code:`\w+` is all alphanumeric and underscore characters.
    :param flags: `Regular expression flags <https://docs.python.org/3/library/re.html#flags>`__.
       Ignored if `pattern` is an already compiled pattern

    See the :ref:`example <example_fts_apsw_regex>`

    """
    if not isinstance(pattern, re.Pattern):
        pattern = re.compile(pattern, flags)

    spec = {}

    parse_tokenizer_args(spec, con, args)

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
    class token_cache_class:
        """Data structure representing token cache

        :meta private:
        """

        token_cache_cookie: int
        "change cookie at time this information was cached"
        tokens: frozenset[str]
        "the tokens"
        doc_frequency: dict[str, float]
        "for each token the proportion of docs containing the token"
        token_frequency: dict[str, float]
        "for each token the proportion of all tokens this token makes up"
        num_docs: int
        "total number of documents"
        num_tokens: int
        "total number of terms"

    def __init__(self, db: apsw.Connection, name: str, schema: str = "main"):
        if not db.table_exists(schema, name):
            raise ValueError(f"{ schema }.{ name } doesn't exist")
        # ::TODO:: figure out if it is fts5 table
        # ::TODO:: figure out if content table, and get content table name for insert method
        self.db = db
        self.name = name
        self.schema = schema
        self.qname = quote_name(name)
        self.qschema = quote_name(schema)
        self._token_cache: FTS5Table.token_cache_class | None = None

    def _get_change_cookie(self) -> int:
        """An int that changes if the content of the table has changed.

        This is useful if information is being cached.

        It is possible through very carefully crafted content to outwit this.
        """
        # See https://sqlite.org/forum/forumpost/2a726411b6974502
        return hash(
            self.db.execute(f"select block from { self.qschema }.{ quote_name(self.name + '_data')} where id=10").get
        )

    change_cookie = property(_get_change_cookie)

    @functools.cached_property
    def columns(self) -> tuple[str, ...]:
        "Columns of this table"
        return tuple(name for (name,) in self.db.execute(f"select name from { self.qschema }.pragma_table_info(?)", (self.name,)))

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
        return self.db.execute("select rowid, * from { self.qschema }.{ self.qname }(?) order by rank", (query,))

    def insert(self, *args: apsw.SQLiteValue, **kwargs: apsw.SQLiteValue) -> None:
        """Does insert with columns by positional or named via kwargs

        * empty string for missing columns?
        * Uses normalize option on all values?
        * auto-stringize each value too?  fts5 doesn't care what
          types you insert
        """
        ...

    # some method helpers pattern, not including all of them yet

    def command_delete(self, rowid: int, *column_values: str):
        """Does `delete <https://www.sqlite.org/fts5.html#the_delete_command>`__"""
        if len(column_values) != len(self.columns):
            raise ValueError(
                f"You need to provide values for every column ({ len(self.columns)}) - got { len(column_values)}"
            )

        values = "('delete',?," + ",".join("?" for _ in range(len(column_values))) + ")"
        cols = f"({ self.qname }, rowid," + ",".join(quote_name(col) for col in self.columns) + ")"
        self.db.execute(
            f"insert into { self.qschema }.{ self.qname }{ cols } values { values }", (rowid, *column_values)
        )

    def command_delete_all(self) -> None:
        "Does `delete all <https://www.sqlite.org/fts5.html#the_delete_all_command>`__"
        self.db.execute(f"insert into { self.qschema}.{ self.qname }({ self.qname}) VALUES('delete-all')")

    def command_integrity_check(self, external_content: bool = True) -> None:
        """Does `integrity check <https://www.sqlite.org/fts5.html#the_integrity_check_command>`__

        If `external_content` is True, then the FTS index is compared to the external content.
        """
        self.db.execute(
            f"insert into { self.qschema}.{ self.qname }({ self.qname}, rank) VALUES('integrity-check', ?)",
            (int(external_content),),
        )

    def command_merge(self, n: int) -> int:
        """Does `merge <https://www.sqlite.org/fts5.html#the_merge_command>`__

        See the documentation for what positive and negative values of `n` mean.

        :returns:  The difference between `sqlite3_total_changes() <https://sqlite.org/c3ref/total_changes.html>`__
                   before and after running the command.
        """
        before = self.db.total_changes()
        self.db.execute(f"insert into { self.qschema}.{ self.qname }({ self.qname}, rank) VALUES('merge', ?)", (n,))
        return self.db.total_changes() - before

    def command_optimize(self) -> None:
        "Does `optimize <https://www.sqlite.org/fts5.html#the_optimize_command>`__"
        self.db.execute(f"insert into { self.qschema}.{ self.qname }({ self.qname}) VALUES('optimize')")

    def command_rebuild(self):
        "Does `rebuild <https://www.sqlite.org/fts5.html#the_rebuild_command>`__"
        self.db.execute(f"insert into { self.qschema}.{ self.qname }({ self.qname}) VALUES('rebuild')")

    # These are the defaults.  The _config table is not updated unless they are changed
    #
    # define FTS5_DEFAULT_PAGE_SIZE   4050
    # define FTS5_DEFAULT_AUTOMERGE      4
    # define FTS5_DEFAULT_USERMERGE      4
    # define FTS5_DEFAULT_CRISISMERGE   16
    # define FTS5_DEFAULT_HASHSIZE    (1024*1024)
    # define FTS5_DEFAULT_DELETE_AUTOMERGE 10
    # #define FTS5_DEFAULT_RANK     "bm25"

    def config_automerge(self, val: int | None = None) -> int:
        """Optionally sets, and returns `automerge <https://www.sqlite.org/fts5.html#the_automerge_configuration_option>`__"""
        return self._config_internal("automerge", val, 4)  # type: ignore

    def config_crisismerge(self, val: int | None = None) -> int:
        """Optionally sets, and returns `crisismerge <https://www.sqlite.org/fts5.html#the_crisismerge_configuration_option>`__"""
        return self._config_internal("crisismerge", val, 16)  # type: ignore

    def config_deletemerge(self, val: int | None = None) -> int:
        """Optionally sets, and returns `deletemerge <https://www.sqlite.org/fts5.html#the_deletemerge_configuration_option>`__"""
        return self._config_internal("deletemerge", val, 16)  # type: ignore

    def config_pgsz(self, val: int | None = None) -> int:
        """Optionally sets, and returns `page size <https://www.sqlite.org/fts5.html#the_pgsz_configuration_option>`__"""
        return self._config_internal("pgsz", val, 4050)  # type: ignore

    def config_rank(self, val: str | None = None) -> str:
        """Optionally sets, and returns `rank <https://www.sqlite.org/fts5.html#the_rank_configuration_option>`__"""
        return self._config_internal("rank", val, "bm25")  # type: ignore

    def config_securedelete(self, val: bool | None = None) -> bool:
        """Optionally sets, and returns `secure-delete <https://www.sqlite.org/fts5.html#the_secure-delete_configuration_option>`__"""
        return bool(self._config_internal("secure-delete", val, False))  # type: ignore

    def config_usermerge(self, val: int | None = None) -> int:
        """Optionally sets, and returns `usermerge <https://www.sqlite.org/fts5.html#the_usermerge_configuration_option>`__"""
        return self._config_internal("usermerge", val, 4)  # type: ignore

    def _config_internal(self, name: str, val: apsw.SQLiteValue, default: apsw.SQLiteValue) -> apsw.SQLiteValue:
        "Internal config implementation"
        if val is not None:
            self.db.execute(
                f"insert into { self.qschema}.{ self.qname }({ self.qname}, rank) VALUES('{name}', ?)", (val,)
            )
        v = self.config(name, prefix="")
        return v if v is not None else default

    def config(self, name: str, value: apsw.SQLiteValue = None, *, prefix: str = "x-apsw-") -> apsw.SQLiteValue:
        """Optionally sets, and gets a `config value <https://www.sqlite.org/fts5.html#configuration_options_config_table_>`__

        If the value is not None, then it is changed.  It is not recommended to change SQLite's own values.

        The `prefix` is to ensure your own config names don't clash with those used by SQLite.  For
        example you could remember the Unicode version used by your tokenizer, and rebuild if the
        version is updated.

        The advantage of using this is that the names/values will survive the fts5 table being renamed.
        """
        key = prefix + name
        if value is not None:
            self.db.execute(
                f"INSERT OR REPLACE into { self.qschema }.{ quote_name(self.name + '_config') }(k,v) values(?,?)",
                (key, value),
            )
        return self.db.execute(
            f"SELECT v from { self.qschema }.{ quote_name(self.name + '_config') } where k=?", (key,)
        ).get

    def tokenize(self, utf8: bytes, reason: int, include_offsets=True, include_colocated=True):
        "Tokenize supplied utf8"
        # need to parse config tokenizer into argvu
        # and then run
        pass

    def _tokens(self) -> frozenset[str]:
        "All tokens in fts index"
        while self._token_cache is None or self._token_cache.token_cache_cookie != self.change_cookie:
            with threading.Lock():
                # check if another thread did the work
                cookie = self.change_cookie
                if self._token_cache is not None and self._token_cache.token_cache_cookie == cookie:
                    break
                n = self.fts5vocab_name("row")
                num_docs = self.db.execute(f"select count(*) from {self.qschema}.{self.qname}").get
                num_tokens = 0
                doc_frequency: dict[str, float] = {}
                token_frequency: dict[str, float] = {}
                for term, doc, cnt in self.db.execute(f"select term, doc, cnt from { n }"):
                    doc_frequency[term] = doc / num_docs
                    token_frequency[term] = cnt
                    num_tokens += cnt
                tokens = frozenset(doc_frequency.keys())
                for token in tokens:
                    token_frequency[token] = token_frequency[token] / num_tokens

                self._token_cache = FTS5Table.token_cache_class(
                    token_cache_cookie=cookie,
                    tokens=tokens,
                    doc_frequency=doc_frequency,
                    token_frequency=token_frequency,
                    num_docs=num_docs,
                    num_tokens=num_tokens,
                )
        return self._token_cache.tokens

    tokens = property(_tokens)

    def is_token(self, token: str) -> bool:
        """Returns True if it is a known token

        If testing lots of tokens, check against :attr:`tokens`
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
        return [t for t in self.tokens if token in t]

    def token_frequency(self, count: int = 10) -> list[tuple[str, int]]:
        "Most frequent tokens, useful for building a stop words list"
        n = self.fts5vocab_name("row")
        return self.db.execute(f"select term, cnt from { n } order by cnt desc limit { count }").get

    def get_closest_tokens(self, token: str, n: int = 25, cutoff: float = 0.6) -> list[tuple[float, str]]:
        """Returns closest known tokens to ``token`` with score for each

        Calls :func:`token_closeness` with the parameters having the same meaning."""
        return token_closeness(token, self.tokens, n, cutoff)

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
            functools.partial(token_closeness, token, n=n, cutoff=cutoff), batched(self.tokens, batch_size)
        ):
            results.extend(res)
        results.sort(reverse=True)
        return results[:n]

    @functools.cache
    def fts5vocab_name(self, type: Literal["row"] | Literal["col"] | Literal["instance"]) -> str:
        """
        Creates fts5vocab table in temp and returns name
        """
        base = f"fts5vocab_{ self.schema }_{ self.name }_{ type }".replace('"', '""')

        name = f'temp."{base}"'

        self.db.execute(
            f"""create virtual table if not exists { name } using fts5vocab(
                    {self.qschema}, {self.qname}, "{ type }")"""
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
        prefix: Iterable[int] | int | None = None,
        content: str | None = None,
        contentless_delete: bool = False,
        content_rowid: str | None = None,
        columnsize: bool = True,
        detail: Literal["full"] | Literal["column"] | Literal["none"] = "full",
        generate_triggers: bool = True,
        drop_if_exists: bool = False,
    ) -> FTS5Table:
        """Creates the table returning a :class:`FTS5Table` on success

        You can use :meth:`apsw.Connection.table_exists` to check if a
        table already exists.

        :param db: connection to create the table on
        :param name: name of table
        :param columns: A sequence of column names.  If you are using
           an external content table (recommended) you can supply
           `None` and the column names will be from the table named by
           the `content` parameter
        :param schema: Which attached database the table is being
            created in
        :param tokenize: The `tokenize option
            <https://sqlite.org/fts5.html#tokenizers>`__.  Supply as a
            sequence of strings which will be correctly quoted
            together.
        :param prefix: The `prefix option
            <https://sqlite.org/fts5.html#prefix_indexes>`__.  Supply
            an int, or a sequence of int.
        :param content: Name of the external content table
        :param content_rowid: Name of the `content rowid column
            <https://sqlite.org/fts5.html#external_content_tables>`__
            if not using the default when using an external content
            table
        :param generate_triggers: If using an external content table
            and this is `True`, then `triggers are created
            <https://sqlite.org/fts5.html#external_content_tables>`__
            to keep this table updated with changes to the external
            content table.
        :param columnsize: Indicate if the `column size tracking
            <https://sqlite.org/fts5.html#the_columnsize_option>`__
            should be disabled to save space
        :param detail: Indicate if `detail
            <https://sqlite.org/fts5.html#the_detail_option>`__ should
            be reduced to save space
        :param contentless_delete: Set the `contentless delete option
            <https://sqlite.org/fts5.html#contentless_delete_tables>`__
            for contentless tables.

        If you create an external content table, then
        :meth:`command_rebuild` and :meth:`command_optimize` will be
        run to populate the contents.
        """
        qschema = quote_name(schema)
        qname = quote_name(name)

        if columns is None:
            if not content:
                raise ValueError("You need to supply columns, or specify an external content table name")
            columns: tuple[str, ...] = tuple(name for (name,) in db.execute(f"select name from { qschema}.pragma_table_info(?)", (content,)))
        else:
            columns: tuple[str, ...] = tuple(columns)

        if unindexed is not None:
            unindexed: set[str] = set(unindexed)
            for c in unindexed:
                if c not in columns:
                    raise ValueError(
                        f"column \"{ c }\" is in unindexed, but not in columns: { ', '.join(quote_name(column) for column in columns ) }"
                    )
        else:
            unindexed: set[str] = set()

        if tokenize is not None:
            # using outside double quote and inside single quote out
            # of all the combinations available
            tokenize = quote_name(" ".join(quote_name(arg, "'") for arg in tokenize), '"')

        if prefix is not None:
            if isinstance(prefix, int):
                prefix: str = str(prefix)
            else:
                prefix = quote_name(" ".join(str(p) for p in prefix), "'")

        qcontent_rowid = quote_name(content_rowid) if content and content_rowid is not None else None
        contentless_delete: str | None = str(int(contentless_delete)) if content == "" else None

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
            ("tokenize", tokenize),
            ("content", qcontent),
            ("content_rowid", qcontent_rowid),
            ("contentless_delete", contentless_delete),
            # for these we omit them for default value
            ("columnsize", "0" if not columnsize else None),
            ("detail", detail if detail != "full" else None),
        ):
            if value is not None:
                sql.append(f", { option } = { value}")
        sql.append(")")

        with db:
            db.execute("".join(sql))
            inst = cls(db, name, schema)
            if content:
                if generate_triggers:
                    qrowid = quote_name(content_rowid if content_rowid is not None else "_rowid_")
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
            assert inst.columns == columns

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


def quote_name(name: str, quote: str = '"') -> str:
    """Quotes name to ensure it is parsed as a name

    :meta private:
    """
    name = name.replace(quote, quote * 2)
    return quote + name + quote


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
        prog="python3 -m apsw.fts",
        description="""Runs FTS5 tokenizer against test text producing a HTML report for manual inspection.

        The FTS5 builtin tokenizers are ascii, trigram, unicode61, and porter. apsw.fts tokenizers are
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
    con.register_fts5_tokenizer("unicodewords", UnicodeWordsTokenizer)
    con.register_fts5_tokenizer("simplify", SimplifyTokenizer)
    con.register_fts5_tokenizer("html", HTMLTokenizer)
    con.register_fts5_tokenizer("ngram", NGramTokenizer)
    con.register_fts5_tokenizer("json", JSONTokenizer)
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
        help="Tokenizers and arguments to run. "
        "For example to run the trigram tokenizer on unicode61 keeping diacritics use: trigram unicode61 remove_diacritics 0",
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
