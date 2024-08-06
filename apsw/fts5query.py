# Process FTS5 queries as documented at https://www.sqlite.org/fts5.html#full_text_query_syntax

# The actual Lemon grammar used is at
# https://sqlite.org/src/file?name=ext/fts5/fts5parse.y

# Tokens https://sqlite.org/src/file?name=ext/fts5/fts5_expr.c
# fts5ExprGetToken

"""
There are 3 representations of a query available:

query string

   This the string syntax `accepted by FTS5
   <https://www.sqlite.org/fts5.html#full_text_query_syntax>`__ where
   you represent AND, OR, NEAR, column filtering etc inline in the
   string.  An example is::

     love AND (title:^"big world" NOT summary:"sunset cruise")

parsed

    This is a hierarchical representation using :mod:`dataclasses`
    with all fields present.  Represented as :class:`QUERY`, it uses
    :class:`PHRASE`, :class:`PHRASES`, :class:`NEAR`,
    :class:`COLUMNFILTER`, :class:`AND`, :class:`NOT`.  The string
    example truncated to a few lines omitting defaults is::

      AND(queries=[PHRASES(phrases=[PHRASE(phrase='love')]),
             NOT(match=COLUMNFILTER(columns=['title'],
                                    filter='include',
                                    query=PHRASES(phrases=[PHRASE(phrase='big '
                                                                         'world',
                                                                  initial=True,


dict

    This is a hierarchical representation using Python
    :class:`dictionaries <dict>` which is easy for logging, storing as
    JSON, and manipulating.  Fields containing default values are
    omitted.  When provided to methods in this module, you do not need
    to provide intermediate PHRASES and PHRASE and just Python lists
    and strings directly.  This is the easiest form to
    programmatically compose and modify queries in. The string example
    truncated to a few lines is::

      {'@': 'AND', 'queries': [
            "love",
            {'@': 'NOT',
              'match': {'@': 'COLUMNFILTER',
                        'columns': ['title'],
                        'filter': 'include',
                        'query': {'@': 'PHRASES',
                                  'phrases': [{'@': 'PHRASE',
                                               'initial': True,
                                               'phrase': 'big world'}]}},

    This form also allows omitting more of the structure like PHRASES in
    favour of a list of str.


.. list-table:: Conversion functions
    :header-rows: 1
    :widths: auto

    * - From type
      - To type
      - Conversion method
    * - query string
      - parsed
      - :func:`parse_query_string`
    * - parsed
      - dict
      - :func:`to_dict`
    * - dict
      - parsed
      - :func:`from_dict`
    * - parsed
      - query string
      - :func:`to_query_string`

Other helpful functionality includes:

* :func:`quote` to appropriately double quote strings
* :func:`extract_with_column_filters` to get a :class:`QUERY` for a node within
  an existing :class:`QUERY` but applying the intermediate column filters.
* :func:`applicable_columns` to work out which columns apply to part of a
  :class:`QUERY`
* :func:`walk` to traverse a parsed query
"""

from __future__ import annotations


import dataclasses

from typing import Any, Sequence, NoReturn, Literal, TypeAlias, Generator

import apsw

QUERY_TOKENS_MARKER = "$!Tokens~"
"Special marker at the start of a string to recognise it as a list of tokens for :class:`QueryTokens`"


@dataclasses.dataclass
class QueryTokens:
    """`FTS5 query strings <https://www.sqlite.org/fts5.html#fts5_strings>`__ are
    passed to `tokenizers
    <https://www.sqlite.org/fts5.html#tokenizers>`__ which extract
    tokens, such as by splitting on whitespace, lower casing text, and
    removing characters like accents.

    If you want to query tokens directly then use this class with the
    :attr:`tokens` member, using it where :attr:`PHRASE.phrase` goes
    and use :func:`to_query_string` to compose your query.

    Your FTS5 table must use the
    :class:`apsw.fts.QueryTokensTokenizer` as the first tokenizer in
    the list.  If the reason for tokenizing includes
    `FTS5_TOKENIZE_QUERY` and the text to be tokenized starts with the
    special marker, then the tokens are returned.  Calling
    :meth:`apsw.fts.FTS5Table.supports_query_tokens` will tell you if
    query tokens are handled correctly.

    You can get the tokens from :attr:`apsw.fts.FTS5Table.tokens` with
    helpers like :meth:`apsw.fts.FTS5Table.closest_tokens`.

    The marker format is text starting with
    :data:`QUERY_TOKENS_MARKER` with each token being separated by
    ``|``.  If there are colocated tokens then ``>`` is used to separate
    them.  For example ``$!Tokens~hello|1st>first|two``"""

    # ::TODO:: fix doc above to be shorter and clearer showing
    # how to wrap one token, multiple tokens, colocated etc

    tokens: list[str | Sequence[str]]
    "The tokens"

    def encode(self) -> str:
        "Produces the tokens encoded with the marker and separator"
        res = ""
        for token in self.tokens:
            if res:
                res += "|"
            if isinstance(token, str):
                res += token
            else:
                res += ">".join(token)
        return QUERY_TOKENS_MARKER + res

    @classmethod
    def decode(cls, data: str | bytes) -> QueryTokens | None:
        "If the marker is present then returns the corresponding :class:`QueryTokens`, otherwise `None`."
        if isinstance(data, bytes) and data.startswith(b"$!Tokens~"):
            data = data.decode("utf8")
        if isinstance(data, str) and data.startswith(QUERY_TOKENS_MARKER):
            stream: list[str | Sequence[str]] = data[len(QUERY_TOKENS_MARKER) :].split("|")
            for i, token in enumerate(stream):
                if "<" in token:
                    stream[i] = token.split(">")
            return cls(stream)
        return None


@dataclasses.dataclass
class PHRASE:
    "One `phrase <https://www.sqlite.org/fts5.html#fts5_phrases>`__"

    phrase: str | QueryTokens
    "Text of the phrase"
    initial: bool = False
    "If True then the  phrase must match the beginning of a column ('^' was used)"
    prefix: bool = False
    "If True then if it is a prefix search on the last token in phrase ('*' was used)"
    sequence: bool = False
    """If True then this phrase must follow tokens of previous phrase ('+' was used).
    initial and sequence can't both be True at the same time"""


@dataclasses.dataclass
class PHRASES:
    "Sequence of PHRASE"

    phrases: Sequence[PHRASE]


@dataclasses.dataclass
class NEAR:
    "`Near query <https://www.sqlite.org/fts5.html#fts5_near_queries>`__"

    phrases: PHRASES
    "Two or more phrases"
    distance: int = 10
    "Maximum distance between the phrases"


@dataclasses.dataclass
class COLUMNFILTER:
    """Limit query to `certain columns <https://www.sqlite.org/fts5.html#fts5_column_filters>`__

    This always reduces the columns that phrase matching will be done
    against.
    """

    columns: Sequence[str]
    "Limit phrase matching by these columns"
    filter: Literal["include"] | Literal["exclude"]
    "Including or excluding the columns"
    query: QUERY
    "query the filter applies to, including all nested queries"


@dataclasses.dataclass
class AND:
    "All queries `must match <https://www.sqlite.org/fts5.html#fts5_boolean_operators>`__"

    queries: Sequence[QUERY]


@dataclasses.dataclass
class OR:
    "Any query `must match <https://www.sqlite.org/fts5.html#fts5_boolean_operators>`__"

    queries: Sequence[QUERY]


@dataclasses.dataclass
class NOT:
    "match `must match <https://www.sqlite.org/fts5.html#fts5_boolean_operators>`__, but no_match `must not <https://www.sqlite.org/fts5.html#fts5_boolean_operators>`__"

    match: QUERY
    no_match: QUERY


# Sphinx makes this real ugly
# https://github.com/sphinx-doc/sphinx/issues/10541
QUERY: TypeAlias = COLUMNFILTER | NEAR | AND | OR | NOT | PHRASES
"""Type representing all query types."""


def to_dict(q: QUERY | PHRASE) -> dict[str, Any]:
    """Converts structure to a dict

    This is useful for pretty printing, logging, saving as JSON,
    modifying etc.

    The dict has a key `@` with value corresponding to the dataclass
    (eg `NEAR`, `PHRASE`, `AND`) and the same field names as the
    corresponding dataclasses.  Only fields with non-default values
    are emitted.
    """

    # @ was picked because it gets printed first if dict keys are sorted, and
    # won't conflict with any other key names

    if isinstance(q, PHRASES):
        return {"@": "PHRASES", "phrases": [to_dict(phrase) for phrase in q.phrases]}

    if isinstance(q, PHRASE):
        res = {"@": "PHRASE", "phrase": q.phrase}
        if q.prefix:
            res["prefix"] = True
        if q.sequence:
            res["sequence"] = True
        if q.initial:
            res["initial"] = True
        return res

    if isinstance(q, AND):
        return {"@": "AND", "queries": [to_dict(query) for query in q.queries]}

    if isinstance(q, OR):
        return {"@": "OR", "queries": [to_dict(query) for query in q.queries]}

    if isinstance(q, NOT):
        return {"@": "NOT", "match": to_dict(q.match), "no_match": to_dict(q.no_match)}

    if isinstance(q, NEAR):
        res = {"@": "NEAR", "phrases": to_dict(q.phrases)}
        if q.distance != 10:
            res["distance"] = q.distance
        return res

    if isinstance(q, COLUMNFILTER):
        return {"@": "COLUMNFILTER", "query": to_dict(q.query), "columns": q.columns, "filter": q.filter}

    raise ValueError(f"Unexpected value {q=}")


_dict_name_class = {
    "PHRASE": PHRASE,
    "PHRASES": PHRASES,
    "NEAR": NEAR,
    "COLUMNFILTER": COLUMNFILTER,
    "AND": AND,
    "OR": OR,
    "NOT": NOT,
}


def from_dict(d: dict[str, Any] | Sequence[str] | str | QueryTokens) -> QUERY:
    """Turns dict back into a :class:`QUERY`

    You can take shortcuts putting `str`, `Sequence[str]`, or
    :class:`QueryTokens` in places where PHRASES, or PHRASE are
    expected.  For example this is accepted::

        {
            "@": "AND,
            "queries": ["hello", "world"]
        }
    """
    if isinstance(d, (str, Sequence, QueryTokens)):
        return _from_dict_as_phrases(d)

    _type_check(d, dict)

    if "@" not in d:
        raise ValueError(f"Expected key '@' in dict {d!r}")

    klass = _dict_name_class.get(d["@"])
    if klass is None:
        raise ValueError(f"\"{d['@']}\" is not a known query type")

    if klass is PHRASE or klass is PHRASES:
        return _from_dict_as_phrases(d)

    if klass is OR or klass is AND:
        queries = d.get("queries")

        if not isinstance(queries, (Sequence, set)) or len(queries) < 1:
            raise ValueError(f"{d!r} 'queries' must be sequence of at least 1 items")

        as_queries = [from_dict(query) for query in queries]
        if len(as_queries) == 1:
            return as_queries[0]

        return klass(as_queries)

    if klass is NEAR:
        phrases = d.get("phrases")

        as_phrases = _from_dict_as_phrases(phrases)
        if len(as_phrases.phrases) < 2:
            raise ValueError(f"NEAR requires at least 2 phrases in {phrases!r}")

        res = klass(as_phrases, _type_check(d.get("distance", 10), int))
        if res.distance < 1:
            raise ValueError(f"NEAR distance must be at least one in {d!r}")
        return res

    if klass is NOT:
        match, no_match = d.get("match"), d.get("no_match")
        if match is None or no_match is None:
            raise ValueError(f"{d!r} must have a 'match' and a 'no_match' key")

        return klass(from_dict(match), from_dict(no_match))

    assert klass is COLUMNFILTER

    columns = d.get("columns")

    if (
        columns is None
        or not isinstance(columns, Sequence)
        or len(columns) < 1
        or not all(isinstance(column, str) for column in columns)
    ):
        raise ValueError(f"{d!r} must have 'columns' key with at least one member sequence, all of str")

    filter = d.get("filter")

    if filter != "include" and filter != "exclude":
        raise ValueError(f"{d!r} must have 'filter' key with value of 'include' or 'exclude'")

    query = d.get("query")
    if query is None:
        raise ValueError(f"{d!r} must have 'query' value")

    return klass(columns, filter, from_dict(query))


def _type_check(v: Any, t: Any) -> Any:
    if not isinstance(v, t):
        raise ValueError(f"Expected {v!r} to be type {t}")
    return v


def _from_dict_as_phrase(item: Any, first: bool) -> PHRASE:
    "Convert anything reasonable into a PHRASE"
    if isinstance(item, (str, QueryTokens)):
        return PHRASE(item)
    if isinstance(item, dict):
        if item.get("@") != "PHRASE":
            raise ValueError(f"{item!r} needs to be a dict with '@': 'PHRASE'")
        phrase = item.get("phrase")
        if phrase is None:
            raise ValueError(f"{item!r} must have phrase member")
        p = PHRASE(
            _type_check(phrase, (str, QueryTokens)),
            _type_check(item.get("initial", False), bool),
            _type_check(item.get("prefix", False), bool),
            _type_check(item.get("sequence", False), bool),
        )
        if p.sequence and first:
            raise ValueError(f"First phrase {item!r} can't have sequence==True")
        if p.sequence and p.initial:
            raise ValueError(f"Can't have both sequence (+) and initial (^) set on same item {item!r}")
        return p
    raise ValueError(f"Can't convert { item!r} to a phrase")


def _from_dict_as_phrases(item: Any) -> PHRASES:
    "Convert anything reasonable into PHRASES"
    if isinstance(item, (str, QueryTokens)):
        return PHRASES([PHRASE(item)])

    if isinstance(item, Sequence):
        phrases: list[PHRASE] = []
        for member in item:
            phrases.append(_from_dict_as_phrase(member, len(phrases) == 0))
        if len(phrases) == 0:
            raise ValueError(f"No phrase found in { member!r}")
        return PHRASES(phrases)

    if not isinstance(item, dict):
        raise ValueError(f"Can't turn {item!r} into phrases")

    kind = item.get("@")
    if kind not in {"PHRASE", "PHRASES"}:
        raise ValueError(f"Expected {item!r} '@' key with value of PHRASE or PHRASES")

    if kind == "PHRASE":
        return PHRASES([_from_dict_as_phrase(item, True)])

    phrases = item.get("phrases")
    if phrases is None or not isinstance(phrases, Sequence):
        raise ValueError(f"Expected 'phrases' value to be a sequence of {item!r}")

    return PHRASES([_from_dict_as_phrase(phrase, i == 0) for i, phrase in enumerate(phrases)])


# parentheses are not needed if the contained item has a lower
# priority than the container
_to_query_string_priority = {
    OR: 10,
    AND: 20,
    NOT: 30,
    # these are really all the same
    COLUMNFILTER: 50,
    NEAR: 60,
    PHRASES: 70,
    PHRASE: 80,
}


def _to_query_string_needs_parens(node: QUERY | PHRASE, child: QUERY | PHRASE) -> bool:
    return _to_query_string_priority[type(child)] < _to_query_string_priority[type(node)]


def to_query_string(q: QUERY | PHRASE) -> str:
    """Returns the corresponding query in text format"""
    if isinstance(q, PHRASE):
        r = ""
        if q.initial:
            r += "^ "
        if q.sequence:
            r += "+ "
        r += quote(q.phrase)
        if q.prefix:
            r += " *"
        return r

    if isinstance(q, PHRASES):
        # They are implicitly high priority AND together
        return " ".join(to_query_string(phrase) for phrase in q.phrases)

    if isinstance(q, (AND, OR)):
        r = ""
        for i, query in enumerate(q.queries):
            if i:
                # technically NEAR AND NEAR can leave the AND out but
                # we make it explicit
                r += " AND " if isinstance(q, AND) else " OR "
            if _to_query_string_needs_parens(q, query):
                r += "("
            r += to_query_string(query)
            if _to_query_string_needs_parens(q, query):
                r += ")"

        return r

    if isinstance(q, NOT):
        r = ""

        if _to_query_string_needs_parens(q, q.match):
            r += "("
        r += to_query_string(q.match)
        if _to_query_string_needs_parens(q, q.match):
            r += ")"

        r += " NOT "

        if _to_query_string_needs_parens(q, q.no_match):
            r += "("
        r += to_query_string(q.no_match)
        if _to_query_string_needs_parens(q, q.no_match):
            r += ")"

        return r

    if isinstance(q, NEAR):
        r = "NEAR(" + to_query_string(q.phrases)
        if q.distance != 10:
            r += f", {q.distance}"
        r += ")"
        return r

    if isinstance(q, COLUMNFILTER):
        r = ""
        if q.filter == "exclude":
            r += "-"
        if len(q.columns) > 1:
            r += "{"
        for i, column in enumerate(q.columns):
            if i:
                r += " "
            r += quote(column)
        if len(q.columns) > 1:
            r += "}"
        r += ": "
        if isinstance(q.query, (PHRASES, PHRASE, NEAR, COLUMNFILTER)):
            r += to_query_string(q.query)
        else:
            r += "(" + to_query_string(q.query) + ")"
        return r

    raise ValueError(f"Unexpected query item {q!r}")


def parse_query_string(query: str) -> QUERY:
    "Returns the corresponding :class:`QUERY` for the query string"
    return _Parser(query).parsed


def quote(text: str | QueryTokens) -> str:
    """Quotes text if necessary to keep as one unit

    eg `hello' -> `hello`, `one two` -> `"one two"`,
    `` -> `""`, `one"two` -> `"one""two"`
    """
    # technically this will also apply to None and empty lists etc
    if not text:
        return '""'
    if isinstance(text, QueryTokens):
        return quote(text.encode())
    if any(c not in "0123456789_ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz" and ord(c) < 0x80 for c in text):
        return '"' + text.replace('"', '""') + '"'
    return text


_walk_attrs = {
    PHRASE: tuple(),
    PHRASES: ("phrases",),
    NEAR: ("phrases",),
    COLUMNFILTER: ("query",),
    AND: ("queries",),
    OR: ("queries",),
    NOT: ("match", "no_match"),
}


def walk(start: QUERY) -> Generator[tuple[tuple[QUERY, ...], QUERY], None, None]:
    """Yields the parents and each node for a query recursively

    The query tree is traversed top down.  Use it like this::

      for parents, node in walk(query):
         # parents will be a tuple of parent nodes
         # node will be current node
         if isinstance(node, PHRASE):
             print(node.phrase)
    """
    # top down - container node first
    yield tuple(), start

    parent = (start,)

    for klass, attrs in _walk_attrs.items():
        if isinstance(start, klass):
            for attr in attrs:
                # the only one where the attribute is not an iterable sequence
                if klass is COLUMNFILTER:
                    for parents, node in walk(getattr(start, attr)):
                        yield parent + parents, node
                else:
                    for child in getattr(start, attr):
                        for parents, node in walk(child):
                            yield parent + parents, node
            return

    raise ValueError(f"{start} is not recognised as a QUERY")


def extract_with_column_filters(node: QUERY, start: QUERY) -> QUERY:
    """Return a new `QUERY` for a query rooted at `start` with child `node`,
    with intermediate :class:`COLUMNFILTER` in between applied.

    This is useful if you want to execute a node from a top level
    query ensuring the column filters apply.
    """
    for parents, child in walk(start):
        if child is node:
            res = node
            for parent in reversed(parents):
                if isinstance(parent, COLUMNFILTER):
                    res = COLUMNFILTER(parent.columns, parent.filter, res)
            return res

    raise ValueError("node is not part of query")


def applicable_columns(node: QUERY, start: QUERY, columns: Sequence[str]) -> set[str]:
    """Return which columns apply to `node`

    You should use :meth:`apsw.fts.FTS5Table.columns_indexed` to get
    the column list for a table.
    """
    query = extract_with_column_filters(node, start)
    columns: set[str] = set(columns)
    while query is not node:
        matches = set()
        for query_column in query.columns:
            for column in columns:
                if 0 == apsw.stricmp(query_column, column):
                    matches.add(column)
                    break
        if query.filter == "include":
            columns = matches
        else:
            columns -= matches
        query = query.query

    return tuple(columns)


class ParseError(Exception):
    """This exception is raised when an error parsing a query string is encountered

    :ivar str query: The query that was being processed
    :ivar str message: Description of error
    :ivar int position: Offset in query where the error occurred

    A simple printer::

        print(exc.query)
        print(" " * exc.position + "^ " + exc.message)
    """

    def __init__(self, query: str, message: str, position: int):
        self.query = query
        self.message = message
        self.position = position


class _Parser:
    """The query tokenization and parsing all in one namespace"""

    class TokenType:
        # these are assigned the same values as generated by
        # lemon, because why not.  fts5parse.h
        EOF = 0
        OR = 1
        AND = 2
        NOT = 3
        TERM = 4
        COLON = 5
        MINUS = 6
        LCP = 7
        RCP = 8
        STRING = 9
        LP = 10
        RP = 11
        CARET = 12
        COMMA = 13
        PLUS = 14
        STAR = 15
        # Add our own
        NEAR = 16

    @dataclasses.dataclass
    class Token:
        tok: _Parser.TokenType
        pos: int
        value: str | None = None

    def __init__(self, query: str):
        self.query = query
        self.tokens = self.get_tokens(query)
        self.token_pos = -1
        if len(self.tokens) == 1:  # only EOF present
            # SQLite says "syntax error" as the message
            self.error("No query provided", None)

        parsed = self.parse_query()
        if self.lookahead.tok != _Parser.TokenType.EOF:
            self.error("Unexpected", self.lookahead)

        self.parsed = parsed

    def error(self, message: str, token: Token | None) -> NoReturn:
        raise ParseError(self.query, message, token.pos if token else 0)

    def _lookahead(self) -> Token:
        return self.tokens[self.token_pos + 1]

    lookahead = property(_lookahead, doc="Lookahead at next token")

    def take_token(self) -> Token:
        self.token_pos += 1
        return self.tokens[self.token_pos]

    def parse_part(self) -> QUERY:
        if self.lookahead.tok in {_Parser.TokenType.MINUS, _Parser.TokenType.LCP} or (
            self.lookahead.tok == _Parser.TokenType.STRING
            and self.tokens[self.token_pos + 2].tok == _Parser.TokenType.COLON
        ):
            return self.parse_colspec()

        if self.lookahead.tok == _Parser.TokenType.LP:
            token = self.take_token()
            query = self.parse_query()
            if self.lookahead.tok != _Parser.TokenType.RP:
                if self.lookahead.tok == _Parser.TokenType.EOF:
                    self.error("unclosed (", token)
                else:
                    self.error(f"Expected ) to close ( at position { token.pos}", self.lookahead)
            self.take_token()
            return query

        if self.lookahead.tok == _Parser.TokenType.NEAR:
            nears: list[NEAR] = []
            # NEAR groups may also be connected by implicit AND
            # operators.  Implicit AND operators group more tightly
            # than all other operators, including NOT
            while self.lookahead.tok == _Parser.TokenType.NEAR:
                nears.append(self.parse_near())

            if len(nears) == 1:
                return nears[0]

            # We make the AND explicit
            return AND(nears)

        return self.parse_phrases()

    infix_precedence = {
        TokenType.OR: 10,
        TokenType.AND: 20,
        TokenType.NOT: 30,
    }

    def parse_query(self, rbp: int = 0):
        res = self.parse_part()

        while rbp < self.infix_precedence.get(self.lookahead.tok, 0):
            token = self.take_token()
            res = self.infix(token.tok, res, self.parse_query(self.infix_precedence[token.tok]))

        return res

    def parse_phrase(self, first: bool) -> PHRASE:
        initial = False
        sequence = False
        if self.lookahead.tok == _Parser.TokenType.CARET:
            initial = True
            self.take_token()
        if not first and not initial and self.lookahead.tok == _Parser.TokenType.PLUS:
            sequence = True
            self.take_token()

        token = self.take_token()
        if token.tok != _Parser.TokenType.STRING:
            self.error("Expected a search term", token)

        res = PHRASE(token.value, initial, False, sequence)

        if self.lookahead.tok == _Parser.TokenType.STAR:
            self.take_token()
            res.prefix = True

        return res

    def parse_phrases(self) -> PHRASES:
        phrases: list[PHRASE] = []

        phrases.append(self.parse_phrase(True))

        while self.lookahead.tok in {_Parser.TokenType.PLUS, _Parser.TokenType.STRING, _Parser.TokenType.CARET}:
            phrases.append(self.parse_phrase(False))

        return PHRASES(phrases)

    def parse_near(self):
        # swallow NEAR
        self.take_token()

        # open parentheses
        token = self.take_token()
        if token.tok != _Parser.TokenType.LP:
            self.error("Expected '(", token)

        # phrases
        phrases = self.parse_phrases()

        if len(phrases.phrases) < 2:
            self.error("At least two phrases must be present for NEAR", self.lookahead)

        # , distance
        distance = 10  # default
        if self.lookahead.tok == _Parser.TokenType.COMMA:
            # absorb comma
            self.take_token()
            # distance
            number = self.take_token()
            if number.tok != _Parser.TokenType.STRING or not number.value.isdigit():
                self.error("Expected number", number)
            distance = int(number.value)

        # close parentheses
        if self.lookahead.tok != _Parser.TokenType.RP:
            self.error("Expected )", self.lookahead)
        self.take_token()

        return NEAR(phrases, distance)

    def parse_colspec(self):
        include = True
        columns: list[str] = []

        if self.lookahead.tok == _Parser.TokenType.MINUS:
            include = False
            self.take_token()

        # inside curlys?
        if self.lookahead.tok == _Parser.TokenType.LCP:
            self.take_token()
            while self.lookahead.tok == _Parser.TokenType.STRING:
                columns.append(self.take_token().value)
            if len(columns) == 0:
                self.error("Expected column name", self.lookahead)
            if self.lookahead.tok != _Parser.TokenType.RCP:
                self.error("Expected }", self.lookahead)
            self.take_token()
        else:
            if self.lookahead.tok != _Parser.TokenType.STRING:
                self.error("Expected column name", self.lookahead)
            columns.append(self.take_token().value)

        if self.lookahead.tok != _Parser.TokenType.COLON:
            self.error("Expected :", self.lookahead)
        self.take_token()

        if self.lookahead.tok == _Parser.TokenType.LP:
            query = self.parse_query()
        elif self.lookahead.tok == _Parser.TokenType.NEAR:
            query = self.parse_part()
        else:
            query = self.parse_phrases()

        return COLUMNFILTER(columns, "include" if include else "exclude", query)

    def infix(self, op: _Parser.TokenType, left: QUERY, right: QUERY) -> QUERY:
        if op == _Parser.TokenType.NOT:
            return NOT(left, right)
        klass = {_Parser.TokenType.AND: AND, _Parser.TokenType.OR: OR}[op]
        if isinstance(left, klass):
            left.queries.append(right)
            return left
        return klass([left, right])

    ## Tokenization stuff follows.  It is all in this parser class
    # to avoid namespace pollution

    single_char_tokens = {
        "(": TokenType.LP,
        ")": TokenType.RP,
        "{": TokenType.LCP,
        "}": TokenType.RCP,
        ":": TokenType.COLON,
        ",": TokenType.COMMA,
        "+": TokenType.PLUS,
        "*": TokenType.STAR,
        "-": TokenType.MINUS,
        "^": TokenType.CARET,
    }

    # case sensitive
    special_words = {
        "OR": TokenType.OR,
        "NOT": TokenType.NOT,
        "AND": TokenType.AND,
        "NEAR": TokenType.NEAR,
    }

    def get_tokens(self, query: str) -> list[Token]:
        def skip_spacing():
            "Return True if we skipped any spaces"
            nonlocal pos
            original_pos = pos
            # fts5ExprIsspace
            while query[pos] in " \t\n\r":
                pos += 1
                if pos == len(query):
                    return True

            return pos != original_pos

        def absorb_quoted():
            nonlocal pos
            if query[pos] != '"':
                return False

            # two quotes in a row keeps one and continues string
            start = pos + 1
            while True:
                pos = query.index('"', pos + 1)
                if query[pos : pos + 2] == '""':
                    pos += 1
                    continue
                break
            res.append(_Parser.Token(_Parser.TokenType.STRING, start, query[start:pos].replace('""', '"')))
            pos += 1
            return True

        def absorb_bareword():
            nonlocal pos
            start = pos

            while pos < len(query):
                # sqlite3Fts5IsBareword
                if (
                    query[pos] in "0123456789_ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz\x1a"
                    or ord(query[pos]) >= 0x80
                ):
                    pos += 1
                else:
                    break
            if pos != start:
                s = query[start:pos]
                res.append(_Parser.Token(self.special_words.get(s, _Parser.TokenType.STRING), start, s))
                return True
            return False

        res: list[_Parser.Token] = []
        pos = 0

        while pos < len(query):
            if skip_spacing():
                continue
            tok = self.single_char_tokens.get(query[pos])
            if tok is not None:
                res.append(_Parser.Token(tok, pos))
                pos += 1
                continue

            if absorb_quoted():
                continue

            if absorb_bareword():
                continue

            raise ValueError(f"Invalid query character '{query[pos]}' in '{query}' at {pos=}")

        # add explicit EOF
        res.append(_Parser.Token(_Parser.TokenType.EOF, pos))

        # fts5 promotes STRING "NEAR" to token NEAR only if followed by "("
        # we demote to get the same effect
        for i in range(len(res) - 1):
            if res[i].tok == _Parser.TokenType.NEAR and res[i + 1].tok != _Parser.TokenType.LP:
                res[i].tok = _Parser.TokenType.STRING

        return res
