#!/usr/bin/env python3

# This code uses Python's optional typing annotations.  You can
# ignore them and do not need to use them.
from __future__ import annotations
from typing import Optional, Iterator, Any

from pprint import pprint

import apsw
import apsw.ext
import apsw.fts

# The sample data we use - recipes with ingredients, instructions, and serving
sample_data = (
    (
        "One egg",
        "Place egg in three cups water until boiling.  Take out after 3 minutes",
        "Peel egg and place on piece of toast!",
    ),
    (
        "1 orange.  One cup water. 1 chicken breast",
        "Cook chicken in pan.  Add water and peeled orange. Sauté until reduced",
        "Cut into strips, and make a tower",
    ),
    (
        "2 pieces of bread. A dollop of jam",
        "Spread jam over one piece of toast, then place other bread on top.  Do not drink.",
        "Eat with a warm glass of milk and no eggs",
    ),
    (
        "Lemon🍋; a-tbsp. of__honey+1 'c' water",
        "Juice lemon, add to boiling? 'water'",
        "Stir-in🥄 HONEY, sniff while it cools, pour into tall cup, "
        "then drink out of ☕ cup.",
    ),
)


connection = apsw.Connection("dbfile")

### fts5_check: Is FTS5 available?
# FTS5 is included as part of the library usually.

print("FTS5 available:", "ENABLE_FTS5" in apsw.compile_options)

### fts_standard: Standard FTS5 usage
# See the SQLite `FTS5 documentation <https://www.sqlite.org/fts5.html>`__

# Create a virtual table
connection.execute(
    """CREATE VIRTUAL TABLE fts_table USING fts5(ingredients,
                   instructions, serving)"""
)

# Add the content
connection.executemany("INSERT INTO fts_table VALUES(?, ?, ?)", sample_data)

# Some simple queries  - FTS5 supports more complex ones and ways of
# expressing them
queries = (
    # Any occurence
    "egg",
    # OR
    "bread OR toast",
    # AND
    "bread AND toast",
    # specific column
    "instructions: juice",
)

for query in queries:
    print(query)
    # show matching rows showing best matches first
    sql = "SELECT * FROM fts_table(?) ORDER BY rank"
    print(apsw.ext.format_query_table(connection, sql, (query,), string_sanitize=0))

### fts5_auxfunc: Auxiliary functions
# `Auxiliary functions <https://sqlite.org/fts5.html#_auxiliary_functions_>`__
# are called for each row of matches, providing ranking for determining
# better matches, or processing the row.  They are called with
# :class:`FTS5ExtensionApi` as the first parameter.


def match_info(api: apsw.FTS5ExtensionApi, *args):
    print("match_info called with", args)
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

    # note the text is the utf-8 encoded bytes
    print(f"{api.column_text(0)=}")

    # we can get a tokenization of text, useful if you want to extract
    # the original text, add highlights etc
    print("Tokenized with UTF-8 offsets")
    pprint(api.tokenize(api.column_text(0)))

    # query_phrase is useful for finding out how common a phrase is.
    counts = [0] * len(api.phrases)
    for phrase in range(len(api.phrases)):
        api.query_phrase(phrase, phrase_count, (phrase, counts))

    for i, phrase in enumerate(api.phrases):
        print(f"Phrase {phrase=} occurs { counts[i] } times")

    return 7


# This is used as the calback from query_phrase above.  Note that the
# api in this call is different than the above functiom.
def phrase_count(api: apsw.FTS5ExtensionApi, closure):
    print(f"phrase_count called {api.rowid=} {api.phrases=}")
    phrase, counts = closure
    # increment counts for this phrase
    counts[phrase] += 1
    # we could do more sophisticated work such as counting how many
    # times it occurs (api.phrase_locations) or which columns
    # (api.phrase_columns).


connection.register_fts5_function("match_info", match_info)

# A deliberately complex query to make the api interesting
query = """
("BoiLING wateR" OR Eggs) AND NEAR (drink Cup, 5) AND jui*
"""

# Note how the table name has to be the first parameter to our
# function in the SQL
connection.execute("SELECT match_info(fts_table, 5, 'hello') FROM fts_table(?)", (query,))

### fts5_end: Close the connection
# When you close the connection, all the registered tokenizers, and
# auxiliary functions are released.  You will need to register them
# again the next time you open a connection and want to use FTS5.
# Consider :attr:`connection_hooks` as an easy way of doing that.

connection.close()
