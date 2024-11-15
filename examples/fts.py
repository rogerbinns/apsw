#!/usr/bin/env python3

# This code uses Python's optional typing annotations.  You can
# ignore them and do not need to use them.  If you do use them
# then you must include this future annotations line first.
from __future__ import annotations

from typing import Optional, Iterator, Any

from pprint import pprint
import re
import functools

import apsw
import apsw.ext

# The three FTS5 specific modules
import apsw.fts5
import apsw.fts5aux
import apsw.fts5query

### fts5_check: Is FTS5 available?
# FTS5 is included as part of the SQLite library (usually).

print("FTS5 available:", "ENABLE_FTS5" in apsw.compile_options)


### fts_start: Content setup
# The connection to use.  The database has a table with recipes.

connection = apsw.Connection("recipes.db")

# The content
print(
    connection.execute(
        "SELECT sql FROM sqlite_schema WHERE name='recipes'"
    ).get
)

print(connection.execute("SELECT COUNT(*) FROM recipes").get)

### fts_create: Create/open search table
# Create a table containing the search index using recipes as an
# `external content table
# <https://www.sqlite.org/fts5.html#external_content_tables>`__.

if not connection.table_exists("main", "search"):
    # create does all the hard work
    search_table = apsw.fts5.Table.create(
        connection,
        # The table will be named 'search'
        "search",
        # External content table name.  It has to be in the same
        # database.
        content="recipes",
        # We want the same columns as recipe, so pass `None`.
        columns=None,
        # Triggers ensure that changes to the content table
        # are reflected in the search table
        generate_triggers=True,
        # Use APSW recommended tokenization
        tokenize=[
            # simplify tokenizer
            "simplify",
            # removes case distinction
            "casefold",
            "true",
            # removes accents, uses compatibility codepoints
            "strip",
            "true",
            # unicode algorithm for word boundaries tokenizer
            "unicodewords",
        ],
        # There are many more options you can set
    )

else:
    # Already exists so just the name is needed
    search_table = apsw.fts5.Table(connection, "search")

# You should use this to get the table name when formatting SQL
# queries as they can't use bindings.  It will correctly quote the
# schema (attached database name) and the table name no matter what
# characters, spaces etc are used.

print("quoted name", search_table.quoted_table_name)

### fts_structure: Table structure and statistics
# Examine the structure, options, and statistics

pprint(search_table.structure)

# rank is one of several options you can read or change
print(f"{search_table.config_rank()=}")

# some statistics
print(f"{search_table.row_count=}")

print(f"{search_table.tokens_per_column=}")

### fts_update: Content
# Use :meth:`~apsw.fts5.Table.upsert` to add or change existing
# content and :meth:`~apsw.fts5.Table.delete` to delete a row.  They
# understand external content tables and will do the operations there,
# then the triggers will update the search index.
# :meth:`~apsw.fts5.Table.row_by_id` gets one or more columns from a
# row, and also handles external content tables.

# upsert returns the rowid of an insert or change.
rowid = search_table.upsert(
    # you can use positional parameters so this goes to the first column
    "This ends up in the name field",
    # and keywords
    description="This ends up in the description",
)

print(f"{search_table.row_by_id(rowid, 'name')=}")

# modify that row
search_table.upsert(ingredients="some stuff", rowid=rowid)

# And delete our test row
search_table.delete(rowid)

### fts_search_sql: Searching with SQL.
# You need to specify what should be returned, the FTS query, and
# order by to get the best results first.

sql = """
   SELECT
   -- snippet takes a lot of options!
   snippet(search, -1, '►', '◄', '...', 10) as Snippet
   -- Pass the query like this
   FROM search(?)
   -- Best matches first
   ORDER BY rank
   -- Top 3
   LIMIT 3"""

for query in (
    "cream",
    "orange NOT zest",
    "name:honey",
    "pomegranate OR olives",
):
    print(f"{query=}")
    print(apsw.ext.format_query_table(connection, sql, (query,)))

### fts_search: Search method
# :meth:`~apsw.fts5.Table.search` provides a Pythonic API providing
# information about each matching row, best matches first.

for row in search_table.search("lemon OR guava"):
    # Note how you see overall query info (it is two phrases) and
    # information about the matched row (how many tokens in each
    # column), and which columns each phrase was found in
    pprint(row)
    # only show the first matching row
    break

# Inspect first matching row
name, description = search_table.row_by_id(
    row.rowid, ("name", "description")
)
print((name, description))

### fts_query_suggest: Query correction and suggestion
# If the query contains words that don't exist or are very rare
# (likely spelling difference) then you can provide alternate queries
# that probably improve results.

for query in (
    "jalapno",
    # query structure is maintained
    "orange AND guice",
    # column names are corrected too
    "nyme:(minced OR oyl NOT peenut)",
    # None is returned if all terms are ok
    "sweet onion",
):
    suggest = search_table.query_suggest(query)
    print(f"{query=} {suggest=}")

### fts_tokens: Working with tokens
# Document and query text is processed into tokens, with matches found
# based on those tokens.  Tokens are not visible to the user.
#
# Typically they correspond to "words" in the text, but with
# upper/lower case neutralized, punctuation removed, marks and accents
# removed.

# See what happens with sample text
text = "Don't 2.245e5 Run-Down Déjà 你好世界😂❤️🤣"
# tokenization happens on UTF8
utf8 = text.encode()

# Note offsets into the utf8.  Those could be used to highlight the
# original.
pprint(search_table.tokenize(utf8))

# For our table, the most popular tokens, and what percent of rows
# they are in
print("\nMost popular by row count")
row_count = search_table.row_count
for token, count in search_table.token_doc_frequency():
    print(f"{token:20}{count/row_count:.0%}")

# Most popular tokens, based on total token count
print("\nMost popular by token count")
token_count = search_table.token_count
for token, count in search_table.token_frequency():
    print(f"{token:20}{count/token_count:.0%}")

# Find what text produced a token, by looking at 5 rows.
token = "jalapeno"
text = search_table.text_for_token(token, 5)
print(f"\nText for {token=} is {text}")


### fts_more: Key Tokens and More Like
# :meth:`~apsw.fts5.Table.key_tokens` finds tokens represented in a
# row, but rare in other rows.  This is purely statistical and has no
# idea of the meaning or relationship between tokens.
#
# :meth:`~apsw.fts5.Table.more_like` is given some rowids, extracts
# their key tokens, and starts a search with them, excluding the
# rowids already seen.  This lets you provide "infinite scrolling"
# starting from one or more rows, providing additional similar
# content.
#
# Both methods let you specify specific columns, or all columns
# (default)

# A randomly selected row ...
bbq_rowid = 1642796066805404445
# ... for bbq sauce
print(search_table.row_by_id(bbq_rowid, "name"))

# Note how each token gets a score, with bigger numbers meaning the
# token is more unique
pprint(search_table.key_tokens(bbq_rowid, columns=["name"], limit=3))

# More like based on the ingredients column
for count, match_info in enumerate(
    search_table.more_like([bbq_rowid], columns="ingredients")
):
    # Show the name for each
    print(search_table.row_by_id(match_info.rowid, "name"))
    # We could save each of these rowids and at the end do another
    # more_like including them.  Stop after a few for now.
    if count == 5:
        break

### fts_autocomplete: Autocomplete
# You often want to show results after just a few letters have been
# typed before there is a complete word entered.  This is done by
# indexing sequences of a few letters, called :class:`ngrams
# <apsw.fts5.NGramTokenizer>`.  Ngrams are never shown to the user
# although you can see the snippets below.

if not connection.table_exists("main", "autocomplete"):
    # create does all the hard work
    autocomplete = apsw.fts5.Table.create(
        connection,
        # The table will be named 'search'
        "autocomplete",
        # External content table name.  It has to be in the same
        # database.
        content="recipes",
        # We want the same columns as recipe, so pass `None`.
        columns=None,
        # Triggers ensure that changes to the content table
        # are reflected in the search table
        generate_triggers=True,
        # Use APSW recommended tokenization
        tokenize=[
            # simplify tokenizer
            "simplify",
            # removes case distinction
            "casefold",
            "true",
            # removes accents, uses compatibility codepoints
            "strip",
            "true",
            # ngram tokenizer
            "ngram",
            # How big is each sequence?  This is how many letters have
            # to be typed before any match is possible.  Smaller values
            # result in larger indexes.
            "ngrams",
            "3",
        ],
        # There are many more options you can set
    )

else:
    # Already exists so just the name is needed
    autocomplete = apsw.fts5.Table(connection, "autocomplete")

# do some queries against autocomplete index
sql = """
   SELECT
   -- snippet takes a lot of options!
   snippet(autocomplete, -1, '►', '◄', '...', 10) as Snippet
   -- Pass the query like this
   FROM autocomplete(?)
   -- Best matches first
   ORDER BY rank
   -- Top 3
   LIMIT 3"""

for query in (
    "eam",
    "ora",
    "name:ney",
    "emo jui",
    "barbecue",
):
    print(f"{query=}")
    print(apsw.ext.format_query_table(connection, sql, (query,)))

### fts5_auxfunc: Auxiliary functions
# `Auxiliary functions
# <https://sqlite.org/fts5.html#_auxiliary_functions_>__ are called
# for each matching row.  They can be used to provide `ranking
# (sorting)
# <https://www.sqlite.org/fts5.html#sorting_by_auxiliary_function_results>`__`
# for determining better matches, or returning information about the
# match and row such as `highlights
# <https://www.sqlite.org/fts5.html#the_highlight_function>`__, and
# `snippets
# <https://www.sqlite.org/fts5.html#the_snippet_function>`__.
#
# They are called with :class:`FTS5ExtensionApi` as the first
# parameter, and then any function arguments, and return a value.
# This example shows all the information available during a query.


def row_match(
    api: apsw.FTS5ExtensionApi, *args: apsw.SQLiteValue
) -> apsw.SQLiteValue:
    print("row_match called with", args)
    # Show what information is available from the api
    print(f"{api.rowid=}")
    print(f"{api.row_count=}")
    print(f"{api.column_count=}")
    for col in range(api.column_count):
        print(f"  {col=} {api.column_size(col)=}")
        print(f"  {col=} {api.column_total_size(col)=}")
    print(f"{api.inst_count=}")
    for inst in range(api.inst_count):
        print(f"  {inst=} {api.inst_tokens(inst)=}")
    print(f"{api.phrases=}")
    for phrase in range(len(api.phrases)):
        # which columns the phrase is found in
        print(f"  {phrase=} {api.phrase_columns(phrase)=}")
        # which token numbers
        print(f"  {phrase=} {api.phrase_locations(phrase)=}")
    # the offsets of phrase 3 in column 2
    print(f"{api.phrase_column_offsets(3, 2)=}")

    # note the text is the utf-8 encoded bytes
    print(f"{api.column_text(0)=}")

    # we can get a tokenization of text, useful if you want to extract
    # the original text, add snippets/highlights etc
    print("Tokenized with UTF-8 offsets")
    pprint(api.tokenize(api.column_text(2), api.column_locale(2)))

    # query_phrase is useful for finding out how common a phrase is.
    counts = [0] * len(api.phrases)
    for phrase in range(len(api.phrases)):
        api.query_phrase(phrase, phrase_count, (phrase, counts))

    for i, phrase in enumerate(api.phrases):
        print(f"Phrase {phrase=} occurs { counts[i]:,} times")

    return 7


# This is used as the callback from query_phrase above.  Note that the
# api instance in this call is different than the above function.
def phrase_count(api: apsw.FTS5ExtensionApi, closure):
    phrase, counts = closure

    # increment counts for this phrase
    counts[phrase] += 1
    if counts[phrase] < 5:
        # Show call info the first 4 times for each phrase
        print(f"phrase_count called {api.rowid=} {api.phrases=}")

    # we could do more sophisticated work such as counting how many
    # times it occurs (api.phrase_locations) or which columns
    # (api.phrase_columns).


connection.register_fts5_function("row_match", row_match)

# A deliberately complex query to make the api interesting
query = (
    """("BoiLed eGGs" OR CinnaMON) OR NEAR (drink Cup, 5) NOT Oran*"""
)

# Make all the code above be called. Note how the table name has to be
# the first parameter to our function in the SQL
connection.execute(
    "SELECT row_match(search, 5, 'hello') FROM search(?) order by rank",
    (query,),
)

### fts_query: Query parsing and manipulation
# :mod:`apsw.fts5query` lets you programmatically create, update, and
# parse queries.  There are three forms of query.

# This is the query as accepted by FTS5.
print("query")
print(query)

# That can be parsed into the structure
parsed = apsw.fts5query.parse_query_string(query)
print("\nparsed")
pprint(parsed)

# The parsed form is a little unwieldy to work with so a dict based
# form is available.
as_dict = apsw.fts5query.to_dict(parsed)
print("\nas_dict")
pprint(as_dict)

# Make some changes - delete the first query
del as_dict["queries"][0]

as_dict["queries"].append(
    {
        # add a columnfilter
        "@": "COLUMNFILTER",
        "filter": "include",
        "columns": ["name", "description"],
        # The sub queries are just strings.   The module knows what
        # you mean and will convert them into AND
        "query": ["some thing blue", "sunshine"],
    }
)
print("\nmodified as_dict")
pprint(as_dict)

# Turn it into parsed form
parsed = apsw.fts5query.from_dict(as_dict)
print("\nnew parsed")
pprint(parsed)

# Turn the parsed form back into a query string
query = apsw.fts5query.to_query_string(parsed)
print("\nnew query")
print(query)

### fts5_tokens: Tokenizers
# `Tokenizers <https://sqlite.org/fts5.html#tokenizers>`__ convert
# text into the tokens used to find matching rows.  They work on UTF8
# input providing the beginning and end offsets for each token.  They
# can also provide `more than one token at the same position
# <https://sqlite.org/fts5.html#synonym_support>`__ for example if you
# wanted both ``first`` and ``1st``.
#
# Tokenizers and their arguments are specified as the 'tokenize'
# option when creating a FTS5 table.  You can also call them directly
# from a :meth:`connection <apsw.Connection.fts5_tokenizer>`.  APSW
# provides :ref:`several tokenizers <all_tokenizers>` but lets look at
# `unicode61 <https://sqlite.org/fts5.html#unicode61_tokenizer>`__ -
# the default SQLite tokenizer

tokenizer = connection.fts5_tokenizer("unicode61")

test_text = """🤦🏼‍♂️ v1.2 Grey Ⅲ ColOUR! Don't jump -  🇫🇮你好世界 Straße
    हैलो वर्ल्ड Déjà vu Résumé SQLITE_ERROR"""

# Call the tokenizer to do a tokenization, supplying the reason
# and no locale.
pprint(
    tokenizer(
        test_text.encode("utf8"), apsw.FTS5_TOKENIZE_DOCUMENT, None
    )
)


# Make a function to show output
def show_tokens(
    text,
    tokenizer_name,
    tokenizer_args=None,
    reason=apsw.FTS5_TOKENIZE_DOCUMENT,
):
    print(f"\n{text=:s}")
    print(f"{tokenizer_name=} {tokenizer_args=}")

    tokenizer = connection.fts5_tokenizer(
        tokenizer_name, tokenizer_args
    )
    # exclude the offsets since they clutter the output
    pprint(
        tokenizer(
            text.encode("utf8"),
            reason,
            None,
            include_offsets=False,
        )
    )
    print()


show_tokens("v1.2 SQLITE_ERROR", "unicode61")

# We want the version number and symbol kept together, so use
# the tokenchars parameter.  Many tokenizers take parameters.
show_tokens("v1.2 SQLITE_ERROR", "unicode61", ["tokenchars", "_."])

# Tokenizers can also be chained together.  The porter tokenizer takes
# existing tokens and turns them into a base.  The rightmost tokenizer
# generates tokens, while ones to the left transform them.  This ensures
# you can search for variations of words without having to get them
# exactly right.
show_tokens(
    "Likes liked likely liking cat cats colour color",
    "porter",
    ["unicode61", "tokenchars", "_"],
)

### fts_apsw_unicodewords: apsw.fts5.UnicodeWordsTokenizer
# :func:`apsw.fts5.UnicodeWordsTokenizer` does word segmentation using
# the `Unicode algorithm TR29
# <https://www.unicode.org/reports/tr29/#Word_Boundaries>`__ which
# works well across languages. It understands when punctuation is part
# of words like in ``don't``, that numbers include punctuation, as
# well as emoji and regional indicators

connection.register_fts5_tokenizer(
    "unicodewords", apsw.fts5.UnicodeWordsTokenizer
)

# unicode61 doesn't understand grapheme clusters or
# punctuation in words, or other languages
show_tokens(test_text, "unicode61")

# unicodewords has you covered
show_tokens(test_text, "unicodewords")

### fts_apsw_simplify: apsw.fts5.SimplifyTokenizer
# You may have noticed that there are accents (diacritics) and
# mixed case in the tokens in the example above.  It is
# convenient to remove those.  The :func:`apsw.fts5.SimplifyTokenizer`
# can neutralize case and remove accents and marks, so you can use it
# to filter your own or other tokenizers.

connection.register_fts5_tokenizer(
    "simplify", apsw.fts5.SimplifyTokenizer
)

show_tokens(
    test_text,
    "simplify",
    [
        # casefold is for case insensitive comparisons
        "casefold",
        "1",
        # strip decomposes codepoints to remove accents
        # and marks, and uses compatibility codepoints,
        # an example is Roman numeral Ⅲ becomes III,
        "strip",
        "1",
        # Use unicodewords to get the tokens to simplify
        "unicodewords",
    ],
)

### fts_own: Your own tokenizer
# We will define our own tokenizer to be the same as above, but
# without all those parameters in the table definition.  A tokenizer
# takes the connection and list of string parameters.


def my_tokenizer(
    con: apsw.Connection, params: list[str]
) -> apsw.FTS5Tokenizer:
    # we take no params
    if params:
        raise ValueError("Expected no parameters")

    # Same as above, but no longer in our SQL
    return con.fts5_tokenizer(
        "simplify",
        ["casefold", "1", "strip", "1", "unicodewords"],
    )


connection.register_fts5_tokenizer("mine", my_tokenizer)

# Produces same result as above
show_tokens(test_text, "mine")

### fts_own_2: Your own tokenizer, part 2
# We'll make one entirely our own, not building on any existing
# tokenizer.  Tokenizers operate on UTF8 and byte offsets.  The
# :func:`apsw.fts5.StringTokenizer` decorator lets you operate on
# :class:`str` instead and handles the mapping.
# :func:`apsw.fts5.parse_tokenizer_args` makes it easy to handle
# parameters.


@apsw.fts5.StringTokenizer
def atokenizer(
    con: apsw.Connection, params: list[str]
) -> apsw.FTS5Tokenizer:
    # What we accept
    ta = apsw.fts5.TokenizerArgument
    spec = {
        # two choices
        "big": ta(choices=("ship", "plane")),
        # default value only
        "small": "hello",
        # conversion
        "block": ta(default=2, convertor=int),
    }

    options = apsw.fts5.parse_tokenizer_args(spec, con, params)

    # show what options we got
    print(f"{options=}")

    def tokenize(text: str, reason: int, locale: str | None):
        # See apsw.fts5.tokenize_reasons for mapping from text to number
        print(f"{reason=}")
        # if a locale table and value was used
        print(f"{locale=}")
        # break string in groups of 'block' characters
        for start in range(0, len(text), options["block"]):
            token = text[start : start + options["block"]]
            yield start, start + len(token), token

    return tokenize


connection.register_fts5_tokenizer("atokenizer", atokenizer)

# show full return - note offsets are utf8 bytes
tok = connection.fts5_tokenizer(
    "atokenizer", ["big", "plane", "block", "5"]
)
pprint(tok(test_text.encode("utf8"), apsw.FTS5_TOKENIZE_AUX, None))

### fts_apsw_regex: apsw.fts5.RegexTokenizer
# We can use :mod:`regular expressions <re>`.  Unlike the other
# tokenizers the pattern is not passed as a SQL level parameter
# because there would be a confusing amount of backslashes, square
# brackets and other quoting going on.

pattern = r"\d+"  # digits
flags = re.ASCII  # only ascii recognised
tokenizer = functools.partial(
    apsw.fts5.RegexTokenizer, pattern=pattern, flags=flags
)
connection.register_fts5_tokenizer("my_regex", tokenizer)

# ASCII/Arabic and non-ascii digits
text = "text2abc 3.14 tamil ௦௧௨௩௪ bengali ০১২৩৪ arabic01234"

show_tokens(text, "my_regex")

### fts_apsw_regexpre: apsw.fts5.RegexPreTokenizer
# Use regular expressions to extract tokens of interest such as
# identifiers,  and then use a different tokenizer on the text between
# the regular expression matches.  Contrast to RegexTokenizer above
# which ignores text not matching the pattern.

# For this example our identifiers are two digits slash two letters
text = "73/RS is larger than 65/ST"

# See what unicodewords does
show_tokens(text, "unicodewords")

# Setup RegexPreTokenizer
pattern = r"[0-9][0-9]/[A-Z][A-Z]"
tokenizer = functools.partial(
    apsw.fts5.RegexPreTokenizer, pattern=pattern
)
connection.register_fts5_tokenizer("myids", tokenizer)

# extract myids, leaving the other text to unicodewords
show_tokens(text, "myids", ["unicodewords"])

### fts_html: HTML tokenizer
# The :func:`~apsw.fts5.HTMLTokenizer` will pass on the
# extracted text to another tokenizer.

text = """<title>Big&amp;Small</title><p>Hello <b>world</b>.  B&Agrave;&#97;"""

show_tokens(text, "html", ["unicodewords"])

### fts_json: JSON tokenizer
# :func:`~apsw.fts5.JSONTokenizer` extracts strings from JSON for
# processing by another tokenizer.

import json

data = {
    "name": "A car🚗",
    "items": ["one", 2, "three", {"four": "five"}],
}

text = json.dumps(data, indent=True)

# Keys can be extracted
show_tokens(text, "json", ["include_keys", "1", "unicodewords"])

# or ignored
show_tokens(text, "json", ["include_keys", "0", "unicodewords"])

### fts_synonym: Synonym tokenizer
# :func:`~apsw.fts5.SynonymTokenizer` is useful to add colocated
# tokens.

text = "one Colour first big dog"

# We use a dict
synonyms = {
    "colour": "color",
    "first": "1st",
    "dog": ["puppy", "canine", "k9"],
}

tokenizer = apsw.fts5.SynonymTokenizer(synonyms.get)

connection.register_fts5_tokenizer(
    "synonyms",
    tokenizer,
)

# It is to the left of simplify so we don't have to match all the
# different cases. By default the synonyms tokenizer only applies when
# tokenizing queries.
show_tokens(
    text,
    "synonyms",
    ["simplify", "casefold", "1", "unicodewords"],
    reason=apsw.FTS5_TOKENIZE_QUERY,
)

### fts_stopwords: Stopwords tokenizer
# :func:`~apsw.fts5.StopWordsTokenizer` removes tokens from the
# stream.  Tokens that appear in almost every row aren't useful for
# finding good matches, and increase the size of the index.


@apsw.fts5.StopWordsTokenizer
def ignore(token: str) -> bool:
    # Return True if the token should be ignored.  These are common
    # English tokens that appear in almost all rows of English
    # content.
    return token in {
        "to",
        "the",
        "and",
        "for",
        "you",
        "of",
        "on",
        "is",
    }


text = "On the green hills and blue skies you see forever"

connection.register_fts5_tokenizer("ignore", ignore)

# It is to the left of simplify so we don't have to match all the
# different cases.
show_tokens(
    text, "ignore", ["simplify", "casefold", "1", "unicodewords"]
)

### fts_transform: Transform tokenizer
# :func:`~apsw.fts5.TransformTokenizer` modifies tokens.


@apsw.fts5.TransformTokenizer
def transform(token: str) -> str:
    # we remove trailing 's' and 'ing'
    if token.endswith("s"):
        return token[:-1]
    if token.endswith("ing"):
        return token[:-3]
    return token


text = "Chickens playing towards talking plays talks"

connection.register_fts5_tokenizer("transform", transform)

# It is to the left of simplify so we don't have to match all the
# different cases.
show_tokens(
    text, "transform", ["simplify", "casefold", "1", "unicodewords"]
)

### fts_end: Cleanup
# We can now close the connection, but it is optional.

connection.close()
