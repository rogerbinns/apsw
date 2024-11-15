#!/usr/bin/env python3

import apsw, apsw.ext, apsw.fts5
import json
import sys
import gzip
import pprint

if len(sys.argv) != 3:
    sys.exit(f"{sys.argv[0]} dbfilenae recipejsonfile.gz")

con = apsw.Connection(sys.argv[1])


def content():
    for line in gzip.open(sys.argv[2], "rt"):
        recipe = json.loads(line)

        # make it 64 bit
        rowid = int(recipe["_id"]["$oid"], 16) & 0x7FFF_FFFF_FFFF_FFFF

        try:
            yield rowid, recipe["name"], recipe["ingredients"], recipe.get("description", None)
        except KeyError:
            pprint.pprint(recipe)
            raise


with con:
    con.execute("create table recipes(name, ingredients, description)")
    with apsw.ext.ShowResourceUsage(sys.stdout, db=con, scope="process"):
        print("import")
        con.executemany("insert into recipes(rowid, name, ingredients, description) values(?,?,?,?)", content())

    with apsw.ext.ShowResourceUsage(sys.stdout, db=con, scope="process"):
        print("words")
        twords = apsw.fts5.Table.create(con, "search", columns=None, content="recipes", tokenize=["simplify", "casefold", "true", "strip", "true", "unicodewords"])

    with apsw.ext.ShowResourceUsage(sys.stdout, db=con, scope="process"):
        print("autocomplete")
        tautocomplete = apsw.fts5.Table.create(con, "autocomplete", columns=None, content="recipes", tokenize=["simplify", "casefold", "true", "strip", "true", "ngram"])