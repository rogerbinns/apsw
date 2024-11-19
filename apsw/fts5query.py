# Process FTS5 queries as documented at https://www.sqlite.org/fts5.html#full_text_query_syntax

# The actual Lemon grammar used is at
# https://sqlite.org/src/file?name=ext/fts5/fts5parse.y

# Tokens https://sqlite.org/src/file?name=ext/fts5/fts5_expr.c
# fts5ExprGetToken

"""
:mod:`apsw.fts5query` Create, parse, and modify queries

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
    :class:`PHRASE`,  :class:`NEAR`, :class:`COLUMNFILTER`,
    :class:`AND`, :class:`NOT`, and :class:`OR`.  The string example
    above is::

        AND(queries=[PHRASE(phrase='love', initial=False, prefix=False, plus=None),
                    NOT(match=COLUMNFILTER(columns=['title'],
                                            filter='include',
                                            query=PHRASE(phrase='big world',
                                                        initial=True,
                                                        prefix=False,
                                                        plus=None)),
                        no_match=COLUMNFILTER(columns=['summary'],
                                            filter='include',
                                            query=PHRASE(phrase='sunset cruise',
                                                            initial=False,
                                                            prefix=False,
                                                            plus=None)))])


dict

    This is a hierarchical representation using Python
    :class:`dictionaries <dict>` which is easy for logging, storing as
    JSON, and manipulating.  Fields containing default values are
    omitted.  When provided to methods in this module, you do not need
    to provide intermediate PHRASE - just Python lists and strings
    directly.  This is the easiest form to programmatically compose
    and modify queries in. The string example above is::

        {'@': 'AND',
        'queries': [{'@': 'PHRASE', 'phrase': 'love'},
                    {'@': 'NOT',
                    'match': {'@': 'COLUMNFILTER',
                                'columns': ['title'],
                                'filter': 'include',
                                'query': {'@': 'PHRASE',
                                        'initial': True,
                                        'phrase': 'big world'}},
                    'no_match': {'@': 'COLUMNFILTER',
                                'columns': ['summary'],
                                'filter': 'include',
                                'query': {'@': 'PHRASE',
                                            'phrase': 'sunset cruise'}}}]}

See :ref:`the example <example_fts_query>`.


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

import sys

import dataclasses
from wsgiref.simple_server import sys_version

try:
    from typing import Union, Any, Sequence, NoReturn, Literal, Iterator, TypeAlias
except ImportError:
    # TypeAlias is not in Python <= 3.9
    pass

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
    :class:`apsw.fts5.QueryTokensTokenizer` as the first tokenizer in
    the list.  If the reason for tokenizing includes
    `FTS5_TOKENIZE_QUERY` and the text to be tokenized starts with the
    special marker, then the tokens are returned.
    :attr:`apsw.fts5.Table.supports_query_tokens` will tell you if
    query tokens are handled correctly.
    :meth:`apsw.fts5.Table.create` parameter ``support_query_tokens``
    will ensure the ``tokenize`` table option is correctly set, You
    can get the tokens from :attr:`apsw.fts5.Table.tokens`.

    You can construct QueryTokens like this::

        # One token
        QueryTokens(["hello"])
        # Token sequence
        QueryTokens(["hello". "world", "today"])
        # Colocated tokens use a nested list
        QueryTokens(["hello", ["first", "1st"]])

    To use in a query::

        {"@": "NOT", "match": QueryTokens(["hello", "world"]),
                     "no_match": QueryTokens([["first", "1st"]])}

    That would be equivalent to a query of ``"Hello World" NOT
    "First"`` if tokens were lower cased, and a tokenizer added a
    colocated ``1st`` on seeing ``first``.
    """

    tokens: list[str | Sequence[str]]
    "The tokens"

    @classmethod
    def _zero_encode(cls, s: str) -> str:
        "Encode any zero bytes"
        return s.replace("\0", "$!ZeRo")

    @classmethod
    def _zero_decode(cls, s: str) -> str:
        "Decode any zero bytes"
        return s.replace("$!ZeRo", "\0")

    def encode(self) -> str:
        "Produces the tokens encoded with the marker and separator"
        res = ""
        for token in self.tokens:
            if res:
                res += "|"
            if isinstance(token, str):
                res += self._zero_encode(token)
            else:
                res += ">".join(self._zero_encode(t) for t in token)
        return QUERY_TOKENS_MARKER + res

    @classmethod
    def decode(cls, data: str | bytes) -> QueryTokens | None:
        "If the marker is present then returns the corresponding :class:`QueryTokens`, otherwise `None`."
        if isinstance(data, bytes) and data.startswith(b"$!Tokens~"):
            data = data.decode()
        if isinstance(data, str) and data.startswith(QUERY_TOKENS_MARKER):
            stream: list[str | Sequence[str]] = [
                cls._zero_decode(token) for token in data[len(QUERY_TOKENS_MARKER) :].split("|")
            ]
            for i, token in enumerate(stream):
                if ">" in token:
                    stream[i] = tuple(token.split(">"))
            return cls(stream)
        return None


@dataclasses.dataclass
class PHRASE:
    "One `phrase <https://www.sqlite.org/fts5.html#fts5_phrases>`__"

    phrase: str | QueryTokens
    "Text of the phrase.  If + was used (eg one+two) then it will be a list of phrases"
    initial: bool = False
    "If True then the  phrase must match the beginning of a column (``^`` was used)"
    prefix: bool = False
    "If True then if it is a prefix search on the last token in phrase (``*`` was used)"
    plus: PHRASE | None = None
    "Additional phrase segment, joined by ``+`` in queries"


@dataclasses.dataclass
class NEAR:
    "`Near query <https://www.sqlite.org/fts5.html#fts5_near_queries>`__"

    phrases: Sequence[PHRASE]
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
QUERY: TypeAlias = Union[COLUMNFILTER, NEAR, AND, OR, NOT, PHRASE]
"""Type representing all query types."""


def to_dict(q: QUERY) -> dict[str, Any]:
    """Converts structure to a dict

    This is useful for pretty printing, logging, saving as JSON,
    modifying etc.

    The dict has a key ``@`` with value corresponding to the dataclass
    (eg ``NEAR``, ``PHRASE``, ``AND``) and the same field names as the
    corresponding dataclasses.  Only fields with non-default values
    are emitted.
    """

    # @ was picked because it gets printed first if dict keys are sorted, and
    # won't conflict with any other key names

    if isinstance(q, PHRASE):
        res = {"@": "PHRASE", "phrase": q.phrase}
        if q.prefix:
            res["prefix"] = True
        if q.initial:
            res["initial"] = True
        if q.plus:
            res["plus"] = to_dict(q.plus)
        return res

    if isinstance(q, AND):
        return {"@": "AND", "queries": [to_dict(query) for query in q.queries]}

    if isinstance(q, OR):
        return {"@": "OR", "queries": [to_dict(query) for query in q.queries]}

    if isinstance(q, NOT):
        return {"@": "NOT", "match": to_dict(q.match), "no_match": to_dict(q.no_match)}

    if isinstance(q, NEAR):
        res = {"@": "NEAR", "phrases": [to_dict(phrase) for phrase in q.phrases]}
        if q.distance != 10:
            res["distance"] = q.distance
        return res

    if isinstance(q, COLUMNFILTER):
        return {"@": "COLUMNFILTER", "query": to_dict(q.query), "columns": q.columns, "filter": q.filter}

    raise TypeError(f"Unexpected value {q=}")


_dict_name_class = {
    "PHRASE": PHRASE,
    "NEAR": NEAR,
    "COLUMNFILTER": COLUMNFILTER,
    "AND": AND,
    "OR": OR,
    "NOT": NOT,
}


def from_dict(d: dict[str, Any] | Sequence[str] | str | QueryTokens) -> QUERY:
    """Turns dict back into a :class:`QUERY`

    You can take shortcuts putting `str`  or :class:`QueryTokens` in
    places where PHRASE is expected.  For example this is accepted::

        {
            "@": "AND,
            "queries": ["hello", "world"]
        }
    """
    if isinstance(d, (str, QueryTokens)):
        return PHRASE(d)

    if isinstance(d, (Sequence, set)):
        res = AND([from_dict(item) for item in d])
        if len(res.queries) == 0:
            raise ValueError(f"Expected at least one item in {d!r}")
        if len(res.queries) == 1:
            return res.queries[0]
        return res

    _type_check(d, dict)

    if "@" not in d:
        raise ValueError(f"Expected key '@' in dict {d!r}")

    klass = _dict_name_class.get(d["@"])
    if klass is None:
        raise ValueError(f"\"{d['@']}\" is not a known query type")

    if klass is PHRASE:
        res = PHRASE(
            _type_check(d["phrase"], (str, QueryTokens)),
            initial=_type_check(d.get("initial", False), bool),
            prefix=_type_check(d.get("prefix", False), bool),
        )
        if "plus" in d:
            res.plus = _type_check(from_dict(d["plus"]), PHRASE)

        return res

    if klass is OR or klass is AND:
        queries = d.get("queries")

        if not isinstance(queries, (Sequence, set)) or len(queries) < 1:
            raise ValueError(f"{d!r} 'queries' must be sequence of at least 1 items")

        as_queries = [from_dict(query) for query in queries]
        if len(as_queries) == 1:
            return as_queries[0]

        return klass(as_queries)

    if klass is NEAR:
        phrases = [_type_check(from_dict(phrase), PHRASE) for phrase in d["phrases"]]
        if len(phrases) < 1:
            raise ValueError(f"There must be at least one NEAR phrase in {phrases!r}")
        res = klass(phrases, _type_check(d.get("distance", 10), int))
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
        raise TypeError(f"Expected {v!r} to be type {t}")
    return v


# parentheses are not needed if the contained item has a lower
# priority than the container
_to_query_string_priority = {
    OR: 10,
    AND: 20,
    NOT: 30,
    # these are really all the same
    COLUMNFILTER: 50,
    NEAR: 60,
    PHRASE: 80,
}


def _to_query_string_needs_parens(node: QUERY, child: QUERY) -> bool:
    return _to_query_string_priority[type(child)] < _to_query_string_priority[type(node)]


def to_query_string(q: QUERY) -> str:
    """Returns the corresponding query in text format"""
    if isinstance(q, PHRASE):
        r = ""
        if q.initial:
            r += "^"
        if isinstance(q.phrase, QueryTokens):
            r += quote(q.phrase.encode())
        else:
            r += quote(q.phrase)
        if q.prefix:
            r += "*"
        if q.plus:
            r += " + " + to_query_string(q.plus)
        return r

    if isinstance(q, OR):
        r = ""
        for i, query in enumerate(q.queries):
            if i:
                r += " OR "
            # parens is never hit because OR is the lowest priority
            assert not _to_query_string_needs_parens(q, query)

            r += to_query_string(query)

        return r

    if isinstance(q, AND):
        r = ""
        # see parse_implicit_and()
        implicit_and = (PHRASE, NEAR, COLUMNFILTER)
        for i, query in enumerate(q.queries):
            if i:
                if isinstance(q.queries[i], implicit_and) and isinstance(q.queries[i - 1], implicit_and):
                    r += " "
                else:
                    r += " AND "
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
        r = "NEAR(" + " ".join(to_query_string(phrase) for phrase in q.phrases)
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
        if isinstance(q.query, (PHRASE, NEAR, COLUMNFILTER)):
            r += to_query_string(q.query)
        else:
            r += "(" + to_query_string(q.query) + ")"
        return r

    raise TypeError(f"Unexpected query item {q!r}")


def parse_query_string(query: str) -> QUERY:
    "Returns the corresponding :class:`QUERY` for the query string"
    return _Parser(query).parsed


def quote(text: str | QueryTokens) -> str:
    """Quotes text if necessary to keep it as one unit using FTS5 quoting rules

    Some examples:

    .. list-table::
        :widths: auto
        :header-rows: 1

        * - text
          - return
        * - ``hello``
          - ``hello``
        * - ``one two``
          - ``"one two"``
        * - (empty string)
          - ``""``
        * - ``one"two``
          - ``"one""two"``
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
    # sequences (iterable)
    NEAR: ("phrases",),
    AND: ("queries",),
    OR: ("queries",),
    # non-iterable
    NOT: ("match", "no_match"),
    COLUMNFILTER: ("query",),
}

if sys.version_info >= (3, 10):

    def _is_QUERY(obj):
        return isinstance(obj, QUERY)
else:
    # py 3.9 can't do the above so we always return True.  Providing a
    # non-query will result in an inscrutable error lower in walk
    def _is_QUERY(obj):
        return True


def walk(start: QUERY) -> Iterator[tuple[tuple[QUERY, ...], QUERY]]:
    """Yields the parents and each node for a query recursively

    The query tree is traversed top down.  Use it like this::

      for parents, node in walk(query):
         # parents will be a tuple of parent nodes
         # node will be current node
         if isinstance(node, PHRASE):
             print(node.phrase)
    """
    if not _is_QUERY(start):
        raise TypeError(f"{start} is not recognised as a QUERY")

    # top down - container node first
    yield tuple(), start

    klass = type(start)

    if klass is PHRASE:
        # handled by yield at top of function
        return

    parent = (start,)

    attrs = _walk_attrs[klass]

    # attributes are not an iterable sequence
    if klass in {COLUMNFILTER, NOT}:
        for attr in attrs:
            for parents, node in walk(getattr(start, attr)):
                yield parent + parents, node
        return

    for attr in attrs:
        for child in getattr(start, attr):
            for parents, node in walk(child):
                yield parent + parents, node
        return


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
    """Return which columns apply to ``node``

    You can use :meth:`apsw.fts5.Table.columns_indexed` to get
    the column list for a table.  The column names are matched using
    SQLite semantics (ASCII case insensitive).

    If a query column is not in the provided columns, then
    :exc:`KeyError` is raised.
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
            else:
                raise KeyError(f"No column matching '{query_column}'")
        if query.filter == "include":
            columns = matches
        else:
            columns -= matches
        query = query.query

    return columns


def _flatten(start: QUERY):
    """Reduces nesting depth

    For example if AND contains a child AND then the children can be
    merged into parent.

    If nodes with children (OR / AND) only have one child then they
    can be replaced with the child.

    :meta private:
    """
    for _, node in walk(start):
        if isinstance(node, AND):
            # children have to be flattened bottom up
            for child in node.queries:
                _flatten(child)
            if any(isinstance(child, AND) for child in node.queries):
                new_queries: list[QUERY] = []
                for child in node.queries:
                    if isinstance(child, AND):
                        new_queries.extend(child.queries)
                    else:
                        new_queries.append(child)
                node.queries = new_queries


class ParseError(Exception):
    """This exception is raised when an error parsing a query string is encountered

    A simple printer::

        print(exc.query)
        print(" " * exc.position + "^", exc.message)
    """

    query: str
    "The query that was being processed"
    message: str
    "Description of error"
    position: int
    "Offset in query where the error occurred"

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

    infix_precedence = {
        TokenType.OR: 10,
        TokenType.AND: 20,
        TokenType.NOT: 30,
    }

    def parse_query(self, rbp: int = 0) -> QUERY:
        res = self.parse_implicit_and()

        while rbp < self.infix_precedence.get(self.lookahead.tok, 0):
            token = self.take_token()
            res = self.infix(token.tok, res, self.parse_query(self.infix_precedence[token.tok]))

        return res

    def parse_implicit_and(self) -> QUERY:
        # From FTS5 doc:
        # any sequence of phrases or NEAR groups (including those
        # restricted to matching specified columns) separated only by
        # whitespace are handled as if there were an implicit AND
        # operator between each pair of phrases or NEAR groups.
        # Implicit AND operators are never inserted after or before an
        # expression enclosed in parenthesis. Implicit AND operators
        # group more tightly than all other operators, including NOT.
        sequence: list[QUERY] = []
        sequence.append(self.parse_part())

        while self.lookahead.tok in {
            _Parser.TokenType.MINUS,
            _Parser.TokenType.LCP,
            _Parser.TokenType.NEAR,
            _Parser.TokenType.CARET,
            _Parser.TokenType.STRING,
        }:
            # there is no implicit AND after (query) so we need to
            # reject if current token is ) but it is fine after a NEAR
            if self.tokens[self.token_pos].tok == _Parser.TokenType.RP and not isinstance(sequence[-1], NEAR):
                break

            sequence.append(self.parse_part())

        return sequence[0] if len(sequence) == 1 else AND(queries=sequence)

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
            return self.parse_near()

        return self.parse_phrase()

    def parse_phrase(self) -> PHRASE:
        if self.lookahead.tok not in {_Parser.TokenType.CARET, _Parser.TokenType.STRING}:
            self.error("Expected a search term", self.lookahead)

        initial = False

        sequence: list[PHRASE] = []
        if self.lookahead.tok == _Parser.TokenType.CARET:
            initial = True
            self.take_token()

        while True:
            token = self.take_token()
            if token.tok != _Parser.TokenType.STRING:
                self.error("Expected a search term", token)
            prefix = False
            if self.lookahead.tok == _Parser.TokenType.STAR:
                prefix = True
                self.take_token()
            phrase = QueryTokens.decode(token.value) or token.value
            sequence.append(PHRASE(phrase, initial, prefix))
            if len(sequence) >= 2:
                sequence[-2].plus = sequence[-1]
            initial = False
            if self.lookahead.tok != _Parser.TokenType.PLUS:
                break
            self.take_token()

        return sequence[0]

    def parse_near(self):
        # swallow NEAR and open parentheses
        self.take_token()
        self.take_token()

        # phrases - despite what the doc implies, you can do NEAR(one+two)
        phrases: list[PHRASE] = []

        while self.lookahead.tok not in (_Parser.TokenType.COMMA, _Parser.TokenType.RP):
            phrases.append(self.parse_phrase())

        # the doc says that at least two phrases are required, but the
        # implementation is otherwise
        # https://sqlite.org/forum/forumpost/6303d75d63
        if len(phrases) < 1:
            self.error("Expected phrase", self.lookahead)

        # , distance
        distance = 10  # default
        if self.lookahead.tok == _Parser.TokenType.COMMA:
            # absorb comma
            self.take_token()
            # distance
            number = self.take_token()
            if (
                number.tok != _Parser.TokenType.STRING
                or not number.value.isdigit()
                # this verifies the number was bare and not quoted like
                # NEAR(foo, "10")
                or self.query[number.pos] == '"'
            ):
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
            query = self.parse_part()
        elif self.lookahead.tok == _Parser.TokenType.NEAR:
            query = self.parse_near()
        else:
            query = self.parse_phrase()

        return COLUMNFILTER(columns, "include" if include else "exclude", query)

    def infix(self, op: _Parser.TokenType, left: QUERY, right: QUERY) -> QUERY:
        if op == _Parser.TokenType.NOT:
            return NOT(left, right)
        klass = {_Parser.TokenType.AND: AND, _Parser.TokenType.OR: OR}[op]
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
                found = query.find('"', pos + 1)
                if found < 0:
                    raise ParseError(query, "No ending double quote", start - 1)
                pos = found
                if query[pos : pos + 2] == '""':
                    pos += 1
                    continue
                break
            res.append(_Parser.Token(_Parser.TokenType.STRING, start - 1, query[start:pos].replace('""', '"')))
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

            raise ParseError(query, f"Invalid query character '{query[pos]}'", pos)

        # add explicit EOF
        res.append(_Parser.Token(_Parser.TokenType.EOF, pos))

        # fts5 promotes STRING "NEAR" to token NEAR only if followed by "("
        # we demote to get the same effect
        for i in range(len(res) - 1):
            if res[i].tok == _Parser.TokenType.NEAR and res[i + 1].tok != _Parser.TokenType.LP:
                res[i].tok = _Parser.TokenType.STRING

        return res
