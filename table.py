import apsw
import apsw.bestpractice
import apsw.fts5
import apsw.ext
import sys
import zlib

apsw.bestpractice.apply(apsw.bestpractice.recommended)

import unittest

class FTS5Table(unittest.TestCase):

    ### Content used in tests

    # crc32 provides a stable 32 bit number for each line so it can be
    # used as a rowid
    content = {zlib.crc32(line.strip().encode()):
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
    }

    # this text is always added to each column
    always = [
        line.strip()
        for line in """
    l’étape humphrey
    L'encyclopédie appleby
    """.splitlines()
        if line.strip()
    ]

    def setUp(self):
        self.db = apsw.Connection("")


    def testTableContent(self):
        "Reading, writing, changing table content"

        self.db.execute("""
            create table normal(oid, "with space");
            create table withrowid(special INTEGER PRIMARY KEY AUTOINCREMENT, [oi d], "co""l2");
            create view view_normal(rowid, col1, col2) as select rowid,oid AS "1-2", "with space" AS "select" from normal;
            """)


        def do_insert(table: apsw.fts5.Table):
            for rowid, text in FTS5Table.content.items():
                val = table.upsert(*(FTS5Table.always[i] + " " + text for i in range(len(FTS5Table.always))), rowid=rowid)
                assert val == rowid, f"{val=} {rowid=}"


        fts = apsw.fts5.Table.create(self.db, "f\"t's5", ["_rowid_", '"'])
        do_insert(fts)

        fts_rowid = apsw.fts5.Table.create(self.db, "fts5-2", None, content="withrowid", content_rowid="special")
        do_insert(fts_rowid)

        fts_rowid.row_count

        fts_view=apsw.fts5.Table.create(self.db, "fts_view", None, content="view_normal")
        self.assertRaisesRegex(apsw.SQLError, ".*modify.*is a view", do_insert, fts_view)

        self.assertEqual(fts.row_count, fts_rowid.row_count)
        self.assertEqual(fts.row_count, fts_view.row_count)

        for key in FTS5Table.content:
            self.assertEqual(fts.row_by_id(key, "with space"), fts_rowid.row_by_id(key, "select"))
            self.assertEqual(fts.row_by_id(key, "oid"), fts_view.row_by_id(key, "1-2"))
