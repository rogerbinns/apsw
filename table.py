import apsw
import apsw.bestpractice
import apsw.fts5
import apsw.ext
import sys
import zlib
from pprint import pprint

import apsw.fts5query

apsw.bestpractice.apply(apsw.bestpractice.recommended)

import unittest


class FTS5Table(unittest.TestCase):
    ### Content used in tests

    lines = [
        line.strip()
        for line in
        # The English is random lines from the APSW documentation.  The
        # non-English is random lines from wikipedia.
        """
    If the html doesn’t start with whitespace then < or &, it is not considered HTML
    This is useful for queries where less than an entire word has been provided such as doing
    Numeric ranges to generate. Smaller values allow showing results with less input but a larger
    Recognises a special tokens marker and returns those tokens for a query. This is useful for
    Tokenizer wrapper that simplifies tokens by neutralizing case conversion, canonicalization, and
    To use you need a callable that takes a str, and returns a str, a sequence of str, or None. For
    This uses difflib.get_close_matches() algorithm to find close matches. Note that it is a
    If the value is not None, then it is changed. It is not recommended to change SQLite’s own
    The prefix is to ensure your own config names don’t clash with those used by SQLite. For
    A sequence of column names. If you are using an external
    This is purely statistical and has no understanding of the tokens. Tokens that occur only in
    This method finds the text that produced a token, by re-tokenizing the documents
    The following tokenizer parameters are accepted. A segment is considered a word if a codepoint
    This is useful to process command line arguments and arguments to tokenizers. It automatically
    APSW provided auxiliary functions for use with register_functions()
    Registers auxiliary functions named in map with the connection, if not already registered
    If it starts with a # then it is considered to be multiple text sections where a # line contains a
    This is a hierarchical representation using Python dictionaries which is easy for logging
    If True then the phrase must match the beginning of a column (‘^’ was used)
    SQL is based around the entire contents of a value. You can test for equality, you can do greater
    onto the same token. For example you could stem run, ran, runs, running, and runners to
    Provided by the apsw.fts5 module. This includes Table for creating and working with FTS5 tables
    シビュラとは恍惚状態で神託を伝えた古代の巫女で、彼女たちの神託をまとめた書
    年免費教育、興建地下鐵路和地方行政改革等重要的政策和建設，使香港的社會面貌出
    توسعه می‌یافت. سیستم‌عامل فری‌بی‌اس‌دی به گونهٔ یک نرم‌افزار آزاد توسعه می‌یابد، این گفته به
    lösen und eine Befreiungs­armee zu rekru­tieren, womit die Macht der Pflanzer in den Sklaven­staaten gebrochen werden
    """.splitlines()
        if line.strip()
    ]

    # crc32 provides a deterministic 32 bit number for each line so it can be
    # used as a rowid no matter the platform.
    content = {
        zlib.crc32(line.encode()): (
            # each column has unique text prepended so we can verify
            # queries are correctly scoped
            "l’étape humphrey humphrey " + line,
            "L'encyclopédie appleby appleby " + line,
        )
        for line in lines
    }

    del lines

    def setUp(self):
        self.db = apsw.Connection("")

    def insert_content(self, table: apsw.fts5.Table):
        for rowid, cols in FTS5Table.content.items():
            val = table.upsert(*cols, rowid=rowid)
            assert val == rowid, f"{val=} {rowid=}"

    def testTableContent(self):
        "Reading, writing, changing table content"

        # the table and column names used deliberately have characters that need to be quoted in
        # SQL so we can verify they are

        self.db.execute(r"""
            create table normal(oid, "with space");
            create table withrowid(special INTEGER PRIMARY KEY AUTOINCREMENT, [oi\ ' " d], "co""12");
            create view view_normal(rowid, "1-2", "select") as select rowid,oid,"with space" from normal;
            """)

        fts = apsw.fts5.Table.create(self.db, "f\"t's5", ["_rowid_", '"'])
        self.insert_content(fts)
        self.assertEqual(fts.row_count, len(FTS5Table.content))

        fts_rowid = apsw.fts5.Table.create(
            self.db, "fts5-2", None, content="withrowid", content_rowid="special", generate_triggers=True
        )
        self.insert_content(fts_rowid)
        self.assertEqual(fts_rowid.row_count, len(FTS5Table.content))

        # populate normnal so rebuild picks up the content because we can't
        self.db.execute('insert into normal(rowid,oid,"with space") select rowid, _rowid_, """" from "f""t\'s5"')
        fts_view = apsw.fts5.Table.create(self.db, "fts_view", None, content="view_normal")
        self.assertRaisesRegex(apsw.SQLError, ".*modify.*is a view", self.insert_content, fts_view)
        self.assertEqual(fts_view.row_count, len(FTS5Table.content))

        tables = fts, fts_rowid, fts_view
        for key, cols in FTS5Table.content.items():
            for table in tables:
                self.assertEqual(table.row_by_id(key, table.structure.columns), cols)
                for column in range(len(cols)):
                    self.assertEqual(table.row_by_id(key, table.structure.columns[column]), cols[column])

        for table in tables:
            for key in FTS5Table.content:
                assert key + 1 not in FTS5Table.content
                for key in FTS5Table.content:
                    column_names = table.structure.columns
                    c = table.change_cookie
                    self.assertRaises(KeyError, table.row_by_id, key + 1, column_names[1])
                    if table is not fts_view:
                        self.assertEqual(key, table.upsert(str(key), rowid=key))
                        self.assertEqual(table.row_by_id(key, column_names[0]), str(key))
                        self.assertNotEqual(c, table.change_cookie)
                        c = table.change_cookie
                        self.assertEqual(key + 10, table.upsert("hello", rowid=key + 10))
                        self.assertEqual("hello", table.row_by_id(key + 10, column_names[0]))
                        self.assertNotEqual(c, table.change_cookie)
                        c = table.change_cookie
                        self.assertTrue(table.delete(key))
                        self.assertTrue(table.delete(key + 10))
                        self.assertRaises(KeyError, table.row_by_id, key, column_names[1])
                        self.assertFalse(table.delete(key))
                        self.assertNotEqual(c, table.change_cookie)

            keys = list(FTS5Table.content.keys())
            if table is fts_view:
                self.assertRaises(ValueError, table.command_delete, keys[0])
                self.assertRaises(ValueError, table.command_delete, keys[0], "one", "two", "three")
            else:
                self.insert_content(table)
                self.assertNotEqual(0, table.row_count)
                if table.structure.content:
                    table.command_delete(99, "one", "two")
                    table.command_delete_all()
                    if not table.structure.content:
                        self.assertEqual(0, table.row_count)
                        table.command_integrity_check()
                    else:
                        # this gives corrupt error on external content
                        # table because the FTS table no longer
                        # matches the external content so we skip
                        self.assertRaises(apsw.CorruptError, table.command_integrity_check)
                else:
                    self.assertRaisesRegex(apsw.SQLError, ".*may only be used with.*", table.command_delete_all)

        # coverage and errors
        self.db.execute("attach '' as 'second'; create table main.under_test(blocker)")
        table = apsw.fts5.Table.create(self.db, "Under_Test", ["one", "Two"], schema="second")

        key = 12345
        self.assertEqual(key, table.upsert(TWO="hello", rOwID=key))
        self.assertEqual("hello", table.row_by_id(key, "tWO"))
        self.assertEqual("hello", self.db.execute("select two from second.under_test").get)

        self.assertRaises(ValueError, table.upsert, "one", "two", "three")
        self.assertRaises(ValueError, table.upsert)
        self.assertRaises(ValueError, table.upsert, "hello", OnE=key)
        self.assertRaises(ValueError, table.upsert, "hello", OnskjdsafE=key)

        apsw.fts5.Table(self.db, "UNDER_TEST", schema="secOND")
        self.assertRaises(ValueError, apsw.fts5.Table, self.db, "UNDER_TEST", schema="zebra")
        self.assertRaises(ValueError, apsw.fts5.Table, self.db, "UNDER_TEST")
        self.assertRaises(ValueError, apsw.fts5.Table, self.db, "xyz", schema="zebra")

    def testConfig(self):
        "config related items"
        name = "test \"']\\ specimen"
        schema = "-1"
        self.db.execute(
            "attach '' as "
            + apsw.fts5.quote_name(schema)
            + "; create table main."
            + apsw.fts5.quote_name(name)
            + "(blocker)"
        )
        table1 = apsw.fts5.Table.create(self.db, name, ["one", "two"], schema=schema)
        table2 = apsw.fts5.Table(self.db, name, schema=schema)

        for name, default, newval in (
            ("automerge", 4, 16),
            ("crisismerge", 16, 8),
            ("deletemerge", 10, 4),
            ("pgsz", 4050, 4072),
            ("rank", "bm25", "bm25(10.0, 5.0)"),
            ("secure_delete", False, True),
            ("usermerge", 4, 16),
        ):
            existing1 = getattr(table1, f"config_{name}")()
            existing2 = getattr(table2, f"config_{name}")()
            self.assertEqual(existing1, existing2)
            self.assertEqual(existing1, default)
            self.assertNotEqual(existing1, newval)
            self.assertIs(type(existing2), type(newval))
            getattr(table1, f"config_{name}")(newval)
            new = getattr(table2, f"config_{name}")()
            self.assertEqual(newval, new)
            self.assertIs(type(new), type(newval))

        table1.config("hello", "world")
        self.assertEqual(table2.config("hello"), "world")
        self.assertIsNone(table2.config("hello", prefix="yes"))

    def testTableCreate(self):
        "Table creation"
        schema = "-1"
        self.db.execute("attach '' as " + apsw.fts5.quote_name(schema))

        # Set everything to non-default
        kwargs = {
            "columns": ("one", "four", "two", "five", "three"),
            "name": "&\"'",
            "unindexed": {"four", "five"},
            "tokenize": ("porter", "ascii"),
            "prefix": {1, 5, 8},
            "content": "",
            "content_rowid": "3'\"4",
            "contentless_delete": True,
            "contentless_unindexed": True,
            "detail": "column",
            "tokendata": True,
            "locale": True,
        }

        apsw.fts5.Table.create(self.db, schema=schema, support_query_tokens=True, rank="bm25 (1,2)", **kwargs)

        table = apsw.fts5.Table(self.db, kwargs["name"], schema=schema)

        for k, v in kwargs.items():
            got = getattr(table.structure, k)
            if k == "tokenize":
                v = ("querytokens",) + v
            elif k == "content_rowid":
                # conrent needs to be a value which it isn't
                v = None
            self.assertEqual(got, v)

        # check items not in structure
        self.assertTrue(table.supports_query_tokens)
        self.assertEqual(table.config_rank(), "bm25 (1,2)")

        # prefix as single int
        t = apsw.fts5.Table.create(self.db, "prefix", ["one", "two"], schema=schema, prefix=3)
        self.assertEqual(t.structure.prefix, {3})
        # drop if exists
        apsw.fts5.Table.create(self.db, "prefix", ["one", "two"], schema=schema, prefix={3, 4}, drop_if_exists=True)
        t2 = apsw.fts5.Table(self.db, "prefix", schema=schema)
        self.assertEqual(t2.structure.prefix, {3, 4})
        # querytokens
        t = apsw.fts5.Table.create(self.db, "sqt", ["one", "two"], schema=schema, support_query_tokens=True)
        t2 = apsw.fts5.Table(self.db, "sqt", schema=schema)
        self.assertTrue(t2.supports_query_tokens)
        self.assertEqual(t2.structure.tokenize, ("querytokens", "unicode61"))

        # errors
        c = apsw.fts5.Table.create
        self.assertRaisesRegex(ValueError, ".*specify an external content.*", c, self.db, "fail", None)
        self.assertRaisesRegex(
            ValueError, ".*is in unindexed, but not in columns.*.*", c, self.db, "fail", ["one"], unindexed=["two"]
        )
        self.assertRaisesRegex(
            apsw.SQLError, ".*already exists.*", c, self.db, kwargs["name"], schema=schema, columns=["one"]
        )

        # coverage for parsing SQL of tables made outside of our create method
        q = apsw.fts5.quote_name
        sql = f"""create virtual table {q(schema)}.parse using fts5(
                    \x09hello\x0d /* comment */ , -- comment
                    123, -- yes fts5 allows numbers as column names
                    🤦🏼‍♂️,
                    detail                    =                    full)
            """
        self.db.execute(sql)
        t = apsw.fts5.Table(self.db, "parse", schema=schema)
        # verifies column names are correctly quoted
        rowid = t.upsert("one", "two", "three")
        self.assertEqual(t.row_by_id(rowid, t.structure.columns), ("one", "two", "three"))

        # not a fts5 table
        self.db.execute(f"create table {q(schema)}.foo(x)")
        self.assertRaisesRegex(ValueError, ".*Not a virtual table.*", apsw.fts5.Table, self.db, "foo", schema=schema)

        # check nothing happened in main or temp
        self.assertIsNone(self.db.execute("select * from sqlite_schema").get)
        self.assertIsNone(self.db.execute("select * from temp.sqlite_schema").get)

    def testMisc(self):
        t = apsw.fts5.Table.create(self.db, "hello", ["one", "two", "three"], unindexed=["two"])
        self.assertIn("<FTS5Table", str(t))
        self.assertIn("<FTS5Table", repr(t))
        self.assertIn("hello", str(t))
        self.assertIn("main", str(t))

        self.assertEqual(t.columns_indexed, ("one", "three"))
        self.assertEqual(t.column_named("one"), "one")
        self.assertEqual(t.column_named("onE"), "one")
        self.assertIsNone(t.column_named("onE "))

    def testContent(self):
        "Content based tests"
        schema = "-1"
        self.db.execute("attach '' as " + apsw.fts5.quote_name(schema))

        t = apsw.fts5.Table.create(
            self.db,
            "😂❤️🤣🤣😭🙏😘",
            ["🇿🇦", "👍🏻"],
            schema=schema,
            support_query_tokens=True,
            tokenize=["simplify", "casefold", "true", "strip", "true", "unicodewords"],
            locale=True
        )
        self.insert_content(t)

        def mq(s):
            return apsw.fts5query.to_query_string(apsw.fts5query.from_dict(s))

        matches = list(t.search(mq("example"), "yes"))
        self.assertEqual(len(matches), 1)
        self.assertEqual(str(matches[0]), "MatchInfo(query_info=QueryInfo(phrases=(('example',),)), rowid=3603631812, column_size=[18, 18], phrase_columns=[[0, 1]])")

        matches = list(t.search("example OR statistical"))
        self.assertEqual(str(matches[0]), "MatchInfo(query_info=QueryInfo(phrases=(('example',), ('statistical',))), rowid=748732035, column_size=[18, 18], phrase_columns=[[], [0, 1]])")
        self.assertEqual(len(matches), 2)

        matches = list(t.search("lösen"))
        self.assertIn("losen", str(matches))
        self.assertIn("rowid=3907740739", str(matches))

        # check nothing happened in main or temp
        self.assertIsNone(self.db.execute("select * from sqlite_schema").get)
        self.assertIsNone(self.db.execute("select * from temp.sqlite_schema").get)


if __name__ == "__main__":
    unittest.main()
