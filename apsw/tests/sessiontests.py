#!/usr/bin/env python3

# This testing code deliberately does nasty stuff so mypy isn't helpful
# mypy: ignore-errors
# type: ignore

import unittest
import functools

import apsw
import apsw.ext


class Session(unittest.TestCase):
    def memdb(self, name):
        "helper to get a memory db with connection in uri mode to allow attaches as memdb too"
        return apsw.Connection(f"file:/{name}?vfs=memdb", flags=apsw.SQLITE_OPEN_URI | apsw.SQLITE_OPEN_READWRITE)

    def setUp(self):
        self.db = self.memdb("main")

    def tearDown(self):
        self.db.close()
        del self.db
        for c in apsw.connections():
            c.close()

    # two SQL chunks to setup tables, and a second making a variety of changes
    base_sql = """
    DROP TABLE IF EXISTS two;
    DROP TABLE IF EXISTS three;
    DROP TABLE IF EXISTS one;
    CREATE TABLE one(a PRIMARY KEY, b, c);
    INSERT INTO one VALUES(1, 'one', 1.1), (2, 'two', zeroblob(16385)), (3, 'three', 3.3);
    -- indirects
    CREATE TABLE two(x, y REFERENCES one(a) ON DELETE CASCADE, z, PRIMARY KEY(x,z));
    INSERT INTO two VALUES(2, 2, 2), (2, 2, 3), (3, 3, 3);
"""

    update_sql = """
    INSERT INTO one VALUES(4, 'four', 4.4);
    UPDATE one SET c=zeroblob(16384) WHERE a=3;
    DELETE FROM one WHERE a=2;
    INSERT INTO two VALUES(1, 1, 1);
    DELETE FROM two WHERE z=3;
    UPDATE two SET x=77 WHERE x=3 AND z=3;
    CREATE TABLE three(a PRIMARY KEY);
    INSERT INTO three VALUES (1), (2);
"""

    def testSanity(self):
        "Some sanity checks"

        self.db.execute("create table foo(x PRIMARY KEY,y);")

        session = apsw.Session(self.db, "main")

        session.attach()

        for i in range(20):
            self.db.execute("insert into foo values(?,?)", (i, i))

        # they do have the same content, but different byte stream
        self.assertNotEqual(session.changeset(), session.patchset())

        self.assertEqual(
            20,
            len(
                list(
                    apsw.ext.changeset_to_sql(
                        session.changeset(), functools.partial(apsw.ext.find_columns, connection=self.db)
                    )
                )
            ),
        )

        self.assertEqual(
            20,
            len(
                list(
                    apsw.ext.changeset_to_sql(
                        session.patchset(), functools.partial(apsw.ext.find_columns, connection=self.db)
                    )
                )
            ),
        )

        changeset = session.changeset()
        self.db.execute("delete from foo")
        self.assertEqual(0, self.db.execute("select count(*) from foo").get)
        apsw.Changeset.apply(changeset, self.db)
        self.assertEqual(20, self.db.execute("select count(*) from foo").get)

    def testSessionConfig(self):
        "session config api"
        self.assertRaises(TypeError, apsw.session_config, "hello")
        self.assertRaises(ValueError, apsw.session_config, 3, "hello")
        self.assertRaises(OverflowError, apsw.session_config, 3_000_000_0000, 1)
        self.assertRaises(OverflowError, apsw.session_config, apsw.SQLITE_SESSION_CONFIG_STRMSIZE, 3_000_000_0000)
        self.assertRaises(TypeError, apsw.session_config, apsw.SQLITE_SESSION_CONFIG_STRMSIZE, "hello")
        self.assertRaises(TypeError, apsw.session_config, apsw.SQLITE_SESSION_CONFIG_STRMSIZE, 77, None)

        val = apsw.session_config(apsw.SQLITE_SESSION_CONFIG_STRMSIZE, -1)
        apsw.session_config(apsw.SQLITE_SESSION_CONFIG_STRMSIZE, val + 1)
        self.assertEqual(val + 1, apsw.session_config(apsw.SQLITE_SESSION_CONFIG_STRMSIZE, -1))

    def testSessionDiff(self):
        "diff"
        self.db.execute("""
            CREATE TABLE t(x PRIMARY KEY, y);
            INSERT INTO t VALUES(1,2), (0,0);
            ATTACH '' AS other;
            CREATE TABLE other.t(x, y PRIMARY key);
            INSERT INTO other.t VALUES(1,2);
            ATTACH '' AS another;
            CREATE TABLE another.t(x PRIMARY KEY, y);
            INSERT INTO another.t VALUES(1,3), (2,3);
            CREATE TABLE zebra(one PRIMARY KEY, two);
            INSERT INTO zebra VALUES(1,2);
        """)

        session = apsw.Session(self.db, "main")

        needs_attach = apsw.SQLITE_VERSION_NUMBER < 3050000

        # errors
        if needs_attach:
            session.attach("zebra")
        self.assertRaises(apsw.SchemaChangeError, session.diff, "zebra", "zebra")
        self.assertRaises(apsw.SchemaChangeError, session.diff, "another", "zebra")

        if needs_attach:
            session.attach("t")
        self.assertRaisesRegex(apsw.SchemaChangeError, ".*table schemas do not match.*", session.diff, "other", "t")

        session.diff("another", "t")

        # check it worked
        self.assertEqual(
            [
                "INSERT INTO t(x, y) VALUES (0, 0);",
                "UPDATE t SET y=2 WHERE x = 1 AND y = 3;",
                "DELETE FROM t WHERE x = 2 AND y = 3;",
            ],
            list(
                apsw.ext.changeset_to_sql(
                    session.changeset(), functools.partial(apsw.ext.find_columns, connection=self.db)
                )
            ),
        )

    def testStream(self):
        "various streaming methods"

        self.db.execute(self.base_sql)

        s = apsw.Session(self.db, "main")
        s.config(apsw.SQLITE_SESSION_CONFIG_STRMSIZE, 512)
        s.attach()

        self.db.execute(self.update_sql)

        sc = StreamOutput()
        s.changeset_stream(sc)

        self.assertEqual(s.changeset(), sc.value)

        sp = StreamOutput()
        s.patchset_stream(sp)
        self.assertEqual(s.patchset(), sp.value)

        self.assertNotEqual(s.changeset(), s.patchset())

        self.assertGreater(len(sc.sizes), 1)
        self.assertGreater(len(sp.sizes), 1)

        # streamed input changes should be the same as non-streamed versions
        for kind in s.patchset, s.changeset:
            db_direct = apsw.Connection("")
            db_stream = apsw.Connection("")

            db_direct.execute(self.base_sql)
            apsw.Changeset.apply(kind(), db_direct)

            db_stream.execute(self.base_sql)
            si = StreamInput(kind())
            apsw.Changeset.apply(si, db_stream)
            self.assertGreater(len(si.sizes), 1)

            self.checkDbIdentical(db_direct, db_stream)

        # check errors don't change db
        for kind in s.patchset, s.changeset:
            db1 = apsw.Connection("")
            db2 = apsw.Connection("")
            db1.execute(self.base_sql)
            db2.execute(self.base_sql)

            si = ErrorStreamInput(kind(), 37)

            self.assertRaises(ZeroDivisionError, apsw.Changeset.apply, si, db2)
            self.checkDbIdentical(db1, db2)

        # error output should leak anything
        for kind in s.patchset_stream, s.changeset_stream:
            self.assertRaises(ZeroDivisionError, kind, ErrorStreamOutput(1))

    def checkDbIdentical(self, db1, db2):
        # easy - check the table names etc are the same
        self.assertEqual(
            sorted(db1.execute("select name,type,ncol,wr,strict from pragma_table_list where schema='main'")),
            sorted(db2.execute("select name,type,ncol,wr,strict from pragma_table_list where schema='main'")),
        )
        # brute force - check contents
        for (name,) in db1.execute("select name from pragma_table_list where schema='main'"):
            self.assertEqual(
                sorted(db1.execute(f'select * from "{name}"')),
                sorted(db2.execute(f'select * from "{name}"')),
            )


class StreamOutput:
    def __init__(self):
        self.value = b""
        self.sizes = []

    def __call__(self, data):
        self.sizes.append(len(data))
        self.value += data


class ErrorStreamOutput(StreamOutput):
    def __init__(self, when):
        super().__init__()
        self.when = when

    def __call__(self, data):
        if len(self.sizes) == self.when:
            1 / 0
        return super().__call__(data)


class StreamInput:
    def __init__(self, source):
        self.source = source
        self.offset = 0
        self.sizes = []

    def __call__(self, amount: int):
        self.sizes.append(amount)
        amount = min(len(self.sizes), amount)
        res = self.source[self.offset : self.offset + amount]
        self.offset += amount
        return res


class ErrorStreamInput(StreamInput):
    def __init__(self, source, when):
        super().__init__(source)
        self.when = when

    def __call__(self, amount):
        if len(self.sizes) == self.when:
            1 / 0
        return super().__call__(amount)


has_session = hasattr(apsw, "Session")

if not has_session:
    del Session

__all__ = ("Session",) if has_session else tuple()

if __name__ == "__main__":
    unittest.main()
