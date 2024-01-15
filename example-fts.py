#!/usr/bin/env python3

# This code uses Python's optional typing annotations.  You can
# ignore them and do not need to use them.
from __future__ import annotations
from typing import Optional, Iterator, Any

import apsw
import apsw.ext
import apsw.fts

# The sample data we use - recipes with ingredients, instructions, and serving
sample_data = (
    (
        "One egg",
        "Place egg in three cups water until boiling.  Take out after 3 minutes",
        "Peel egg and place on piece of toast",
    ),
    (
        "1 orange.  One cup water. 1 chicken breast",
        "Cook chicken in pan.  Add water and peeled orange. Sauté until reduced",
        "Cut into strips, and make a tower",
    ),
    (
        "2 pieces of bread. A dollop of jam",
        "Spread jam over one piece of toast, then place other bread on top",
        "Eat with a warm glass of milk and no eggs",
    ),
    (
        "Lemon, a tbsp of honey, 1 c water",
        "Juice lemon, add to boiling water",
        "Stir in honey, sniff while it cools, then drink out of tall cup",
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
    """CREATE VIRTUAL TABLE fts_standard USING fts5(ingredients,
                   instructions, serving)"""
)

# Add the content
connection.executemany("INSERT INTO fts_standard VALUES(?, ?, ?)", sample_data)

# Some queries
queries = (
    # Any occurence
    "bread",
    # OR
    "bread OR toast",
    # AND
    "bread AND toast",
    #

):