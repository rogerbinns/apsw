#!/usr/bin/env python3

# This code uses Python's optional typing annotations.  You can
# ignore them and do not need to use them.
from __future__ import annotations
from typing import Optional, Iterator, Any

import os
import sys
import time
import apsw
import apsw.ext
import apsw.fts
import random
import re
from pathlib import Path

connection = apsw.Connection("dbfile")

### fts5_check: Is FTS5 available?
# FTS5 is included as part of the library usually.

print("FTS5 available:", "ENABLE_FTS5" in apsw.compile_options)

### fts: Full Text Search
# See :doc:`textsearch` for full details

# Some silly sample content
connection.execute(
    """
    create table recipes(ingredients, instructions, serving);
    insert into recipes values
      ('One egg', 'Place egg in three cups water until boiling.  Take out after 3 minutes', 'Peel egg and place on piece of toast'),
      ('1 orange.  One cup water. 1 chicken breast', 'Cook chicken in pan.  Add water and peeled orange. Sauté until reduced', 'Cut into strips, and make a tower'),
      ('2 pieces of bread. A dollop of jam', 'Spread jam over one piece of toast, then place other bread on top', 'Eat with a warm glass of milk and no eggs');
"""
)

# be able to search for either form
synonyms = {"1": "one", "one": "1"}
connection.register_fts5_tokenizer("synonyms", apsw.fts.SynonymTokenizer(synonyms.get))


# ignore trailing 's' on all tokens
def ignore_s(token):
    return token.rstrip("s")


connection.register_fts5_tokenizer("plurals", apsw.fts.TransformTokenizer(ignore_s))


# Ignore words
def ignore(token):
    return token in {"and", "of"}


connection.register_fts5_tokenizer("ignore", apsw.fts.StopWordsTokenizer(ignore))

# Use python's more recent unicode db
connection.register_fts5_tokenizer("pyunicode", apsw.fts.PyUnicodeTokenizer)
# Separate wrapper does lower casing, removal of accents etc
connection.register_fts5_tokenizer("simplify", apsw.fts.SimplifyTokenizer)

# How we will tokenize combining the above
tokenize = " ".join(
    apsw.format_sql_value(arg)
    for arg in (
        "ignore",
        "plurals",
        "synonyms",
        "simplify",
        "case", "casefold",
        "remove_categories",   "M* *m Sk",
        "pyunicode",
    )
)

# Can't use bound values, so fstring hence doubled apostrophes above
connection.execute(
    f"""
    create virtual table recipes_search using fts5(ingredients, instructions, serving,
                   tokenize="{tokenize}",
                   content=recipes);
    insert into recipes_search(recipes_search) values('rebuild');
"""
)

