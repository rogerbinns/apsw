#!/usr/bin/env python3

# This testing code deliberately does nasty stuff so mypy isn't helpful
# mypy: ignore-errors
# type: ignore

import unittest
import functools

import apsw
import apsw.ext


class Session(unittest.TestCase):
    def setUp(self):
        self.db = apsw.Connection("")

    def tearDown(self):
        self.db.close()
        del self.db
        for c in apsw.connections():
            c.close()

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
        """)

        session = apsw.Session(self.db, "main")
        session.attach("t")

        # no error for this
        session.diff("zebra", "zebra")

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


has_session = hasattr(apsw, "Session")

if not has_session:
    del Session

__all__ = ("Session",) if has_session else tuple()

if __name__ == "__main__":
    unittest.main()
