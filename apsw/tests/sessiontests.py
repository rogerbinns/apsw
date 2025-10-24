#!/usr/bin/env python3

# This testing code deliberately does nasty stuff so mypy isn't helpful
# mypy: ignore-errors
# type: ignore

import unittest
import functools
import sys

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

    # SQL chunks to setup tables, a second making a variety of changes, and a third of extra changes
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

    bonus_sql = """
    INSERT INTO three VALUES(-1), (1.001), (2.002);
    DELETE FROM two WHERE z = 1;
    UPDATE two SET z = zeroblob(16385) WHERE z = 1;
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
        apsw.Changeset.apply(changeset, self.db, filter_change=lambda x: True)
        self.assertEqual(20, self.db.execute("select count(*) from foo").get)

    def testConfig(self):
        "extension config api"
        self.assertRaises(TypeError, apsw.session_config, "hello")
        self.assertRaises(ValueError, apsw.session_config, 3, "hello")
        self.assertRaises(OverflowError, apsw.session_config, 3_000_000_0000, 1)
        self.assertRaises(OverflowError, apsw.session_config, apsw.SQLITE_SESSION_CONFIG_STRMSIZE, 3_000_000_0000)
        self.assertRaises(TypeError, apsw.session_config, apsw.SQLITE_SESSION_CONFIG_STRMSIZE, "hello")
        self.assertRaises(TypeError, apsw.session_config, apsw.SQLITE_SESSION_CONFIG_STRMSIZE, 77, None)

        val = apsw.session_config(apsw.SQLITE_SESSION_CONFIG_STRMSIZE, -1)
        apsw.session_config(apsw.SQLITE_SESSION_CONFIG_STRMSIZE, val + 1)
        self.assertEqual(val + 1, apsw.session_config(apsw.SQLITE_SESSION_CONFIG_STRMSIZE, -1))

    def testSessionConfig(self):
        "session config api"
        session = apsw.Session(self.db, "main")

        self.assertRaises(TypeError, session.config)
        self.assertRaises(TypeError, session.config, "hello")

        self.assertRaises(OverflowError, session.config, 2**40)

        self.assertRaises(OverflowError, session.config, apsw.SQLITE_SESSION_OBJCONFIG_SIZE, 2**40)

        self.assertRaises(ValueError, session.config, -63)

        v = session.config(apsw.SQLITE_SESSION_OBJCONFIG_SIZE, -1)
        session.config(apsw.SQLITE_SESSION_OBJCONFIG_SIZE, not v)
        self.assertEqual(not v, session.config(apsw.SQLITE_SESSION_OBJCONFIG_SIZE, -1))

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
        session.attach("zebra")

        self.assertRaises(apsw.SchemaChangeError, session.diff, "zebra", "zebra")
        self.assertRaises(apsw.SchemaChangeError, session.diff, "another", "zebra")

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

    def testSessionAttributes(self):
        "session attributes"
        session = apsw.Session(self.db, "main")

        self.assertEqual(True, session.enabled)
        self.assertEqual(True, session.is_empty)
        self.assertEqual(False, session.indirect)
        self.assertEqual(0, session.memory_used)

        self.assertRaises(TypeError, setattr, session, "enabled", 3 + 4j)
        self.assertRaises(TypeError, setattr, session, "indirect", 3 + 4j)

        session.indirect = True
        session.attach("one")

        self.db.execute(self.base_sql)

        self.assertEqual(False, session.is_empty)
        self.assertNotEqual(0, session.memory_used)
        self.assertEqual(True, session.indirect)

        for table_change in apsw.Changeset.iter(session.changeset()):
            self.assertEqual(table_change.indirect, True)

        session.indirect = False

        self.db.execute(self.update_sql)
        direct = 0

        changeset = session.changeset()
        for table_change in apsw.Changeset.iter(changeset):
            direct += not table_change.indirect

        self.assertNotEqual(0, direct)

        session.enabled = False
        self.assertEqual(changeset, session.changeset())
        self.assertEqual(False, session.enabled)

        self.db.execute("DELETE FROM two ; DELETE FROM one")

        self.assertEqual(False, session.enabled)

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
        for filter in "filter", "filter_change":
            for val in True, False:
                kwargs = {filter: lambda x: val}
                for kind in s.patchset, s.changeset:
                    db_direct = apsw.Connection("")
                    db_stream = apsw.Connection("")

                    db_direct.execute(self.base_sql)
                    apsw.Changeset.apply(kind(), db_direct, **kwargs)

                    db_stream.execute(self.base_sql)
                    si = StreamInput(kind())
                    apsw.Changeset.apply(si, db_stream, **kwargs)
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

        # error output should not leak anything
        for kind in s.patchset_stream, s.changeset_stream:
            self.assertRaises(ZeroDivisionError, kind, ErrorStreamOutput(1))

        # concat, invert etc
        changeset = s.changeset()
        patchset = s.patchset()
        s2 = apsw.Session(self.db, "main")
        s2.attach()
        self.db.execute(self.bonus_sql)

        changeset2 = s2.changeset()
        patchset2 = s2.patchset()

        concat = apsw.Changeset.concat(changeset, changeset2)
        pconcat = apsw.Changeset.concat(patchset, patchset2)

        # can't mix change and patchsets
        self.assertRaises(apsw.SQLError, apsw.Changeset.concat, changeset, patchset2)
        self.assertRaises(apsw.SQLError, apsw.Changeset.concat, patchset, changeset2)

        # check they work
        so = StreamOutput()
        apsw.Changeset.concat_stream(StreamInput(changeset), StreamInput(changeset2), so)
        self.assertEqual(concat, so.value)

        so = StreamOutput()
        apsw.Changeset.concat_stream(StreamInput(patchset), StreamInput(patchset2), so)
        self.assertEqual(pconcat, so.value)

        invert = apsw.Changeset.invert(changeset)
        # can't invert a patchset
        self.assertRaises(apsw.CorruptError, apsw.Changeset.invert, patchset)

        so = StreamOutput()
        apsw.Changeset.invert_stream(StreamInput(changeset), so)
        self.assertEqual(invert, so.value)

        so = StreamOutput()
        self.assertRaises(apsw.CorruptError, apsw.Changeset.invert_stream, StreamInput(patchset), so)
        self.assertEqual(b"", so.value)

        # error conditions
        self.assertRaises(ZeroDivisionError, apsw.Changeset.invert_stream, ErrorStreamInput(changeset, 4), so)
        self.assertRaises(ZeroDivisionError, apsw.Changeset.invert_stream, StreamInput(changeset), ErrorStreamOutput(2))
        so = StreamOutput()
        self.assertRaises(
            ZeroDivisionError, apsw.Changeset.concat_stream, ErrorStreamInput(patchset, 3), StreamInput(patchset2), so
        )
        self.assertRaises(
            ZeroDivisionError, apsw.Changeset.concat_stream, StreamInput(patchset), ErrorStreamInput(patchset2, 3), so
        )
        self.assertRaises(
            ZeroDivisionError,
            apsw.Changeset.concat_stream,
            StreamInput(patchset),
            StreamInput(patchset2),
            ErrorStreamOutput(1),
        )

        cb = apsw.ChangesetBuilder()
        cb.add(changeset)
        cb.output_stream(StreamOutput())
        self.assertRaises(ZeroDivisionError, cb.output_stream, ErrorStreamOutput(1))

        def handler(reason: int, change: apsw.TableChange):
            if reason in (
                apsw.SQLITE_CHANGESET_DATA,
                apsw.SQLITE_CHANGESET_CONFLICT,
            ):
                return apsw.SQLITE_CHANGESET_REPLACE
            return apsw.SQLITE_CHANGESET_OMIT

        conflict_resolutions = apsw.Changeset.apply(changeset, self.db, rebase=True, conflict=handler)

        rb = apsw.Rebaser()
        rb.configure(conflict_resolutions)
        self.assertRaises(ZeroDivisionError, rb.rebase_stream, ErrorStreamInput(changeset, 2), StreamOutput())
        self.assertRaises(ZeroDivisionError, rb.rebase_stream, StreamInput(changeset2), ErrorStreamOutput(0))
        rb.rebase_stream(StreamInput(changeset2), StreamOutput())

    def checkDbIdentical(self, db1, db2):
        # easy - check the table names etc are the same
        self.assertEqual(
            sorted(db1.execute("select name,type,ncol,wr,strict from pragma_table_list where schema='main'")),
            sorted(db2.execute("select name,type,ncol,wr,strict from pragma_table_list where schema='main'")),
        )
        # brute force - check contents.  sqlite_schema contents will
        # differ due to the presence of indexes etc
        for (name,) in db1.execute(
            "select name from pragma_table_list where schema='main' and name != 'sqlite_schema'"
        ):
            self.assertEqual(
                sorted(db1.execute(f'select * from "{name}"')),
                sorted(db2.execute(f'select * from "{name}"')),
            )

    def testAttach(self):
        "attaching to tables"
        for i in range(20):
            self.db.execute(f'create table "{i}"(x PRIMARY KEY)')

        session = apsw.Session(self.db, "main")

        def tables():
            return set(tc.name for tc in apsw.Changeset.iter(session.changeset()))

        def change(num):
            self.db.execute(f'insert into "{num}" values({num})')

        self.assertEqual(tables(), set())

        # should have no effect
        change(0)
        self.assertEqual(tables(), set())

        # simple case
        session.attach("1")
        change(1)
        self.assertEqual(tables(), {"1"})

        def table_filter(name):
            # should only be called for number tables
            assert 0 <= int(name) < 20

            if name == "2":
                1 / 0
            if name == "3":
                return False
            if name == "4":
                return 4 + 5j
            if name == "5":
                return None
            if name == "8":
                return False

            return True

        session.table_filter(table_filter)
        # no pk - table filter is called anyway failing the assertion
        self.assertRaises(ValueError, self.db.execute, "create table dummy(one); insert into dummy values(1)")

        self.assertRaises(ZeroDivisionError, change, 2)
        self.assertNotIn("2", tables())  # should not be recorded

        change(3)
        self.assertNotIn("3", tables())

        self.assertRaises(TypeError, change, 4)
        self.assertNotIn("4", tables())

        self.assertRaises(TypeError, change, 5)
        self.assertNotIn("5", tables())

        change(6)
        self.assertIn("6", tables())

        self.assertRaises(TypeError, session.table_filter, None)

        session.attach(None)
        change(7)
        self.assertIn("7", tables())

        session.attach()
        change(8)
        self.assertNotIn("8", tables())

    def testClosingChecks(self):
        "closed objects"
        db = apsw.Connection("")
        db.close()
        self.assertRaises(apsw.ConnectionClosedError, apsw.Session, db, "main")

        self.assertRaises(apsw.ConnectionClosedError, apsw.Changeset.apply, b"", db)

        db = apsw.Connection("")
        session = apsw.Session(db, "main")
        # this should close the session
        db.close()
        tested = []
        for attr in [x for x in dir(session) if not x.startswith("__") and not x in ("close",)]:
            tested.append(attr)
            try:
                f = getattr(session, attr)
            except ValueError as e:
                if "The session has been closed" in str(e):
                    continue
                raise
            args = [1, 2, 3, 4][:0]
            self.assertRaisesRegex(ValueError, ".*The session has been closed.*", f, *args)
        self.assertEqual(len(tested), 13)
        # should be harmless
        session.close()

        db = apsw.Connection("")
        db.execute("create table x(y PRIMARY KEY)")
        session = apsw.Session(db, "main")
        session.attach()
        db.execute("insert into x VALUES(3), (4)")
        changeset = session.changeset()
        db.close()
        self.assertGreater(len(changeset), 1)
        for table_change in apsw.Changeset.iter(changeset):
            self.assertIn("column_count", str(table_change))
        self.assertIsNotNone(table_change)
        self.assertIn("out of scope", str(table_change))

        for attr in [x for x in dir(table_change) if not x.startswith("__")]:
            self.assertRaises(apsw.InvalidContextError, getattr, table_change, attr)

        self.assertRaises(apsw.ConnectionClosedError, apsw.Changeset.apply, changeset, db)

        builder = apsw.ChangesetBuilder()
        self.assertRaises(apsw.ConnectionClosedError, builder.schema, db, "main")
        for tc in apsw.Changeset.iter(changeset):
            pass
        self.assertRaises(apsw.InvalidContextError, builder.add_change, tc)
        builder.close()
        builder.close()

        self.assertRaisesRegex(ValueError, ".*has been closed.*", builder.add, changeset)
        for table_change in apsw.Changeset.iter(changeset):
            self.assertRaisesRegex(ValueError, ".*has been closed.*", builder.add_change, table_change)
            break

        self.assertRaisesRegex(ValueError, ".*has been closed.*", builder.output)
        self.assertRaisesRegex(ValueError, ".*has been closed.*", builder.output_stream, StreamOutput())

        builder = apsw.ChangesetBuilder()
        db2 = apsw.Connection("")
        builder.schema(db2, "main")
        # this should close the builder
        db2.close()

        self.assertRaisesRegex(ValueError, ".*has been closed.*", builder.add, changeset)
        for table_change in apsw.Changeset.iter(changeset):
            self.assertRaisesRegex(ValueError, ".*has been closed.*", builder.add_change, table_change)
            break

        self.assertRaisesRegex(ValueError, ".*has been closed.*", builder.output)
        self.assertRaisesRegex(ValueError, ".*has been closed.*", builder.output_stream, StreamOutput())

    def testIter(self):
        "iteration"
        # the other test cases pretty cover everything - this is just for coverage
        self.db.execute(self.base_sql)
        session = apsw.Session(self.db, "main")
        session.attach()

        self.db.execute(self.update_sql)

        changeset = session.changeset()

        self.assertRaises(TypeError, apsw.Changeset.iter, changeset, 1 + 4j)

        self.assertNotEqual(apsw.SQLITE_CHANGESETSTART_INVERT, 0)

        # streaming and non-streaming should give identical content
        # but we have to filter out the address
        non = [str(tc).split(", at 0x")[0] for tc in apsw.Changeset.iter(changeset, flags=apsw.SQLITE_CHANGESETSTART_INVERT)]
        streamed  = [str(tc).split(", at 0x")[0] for tc in apsw.Changeset.iter(StreamInput(changeset), flags=apsw.SQLITE_CHANGESETSTART_INVERT)]
        self.assertEqual(non, streamed)


    def testCorrupt(self):
        "corrupt changesets"
        session = apsw.Session(self.db, "main")
        session.attach()
        self.db.execute(self.base_sql)
        self.db.execute(self.update_sql)
        self.db.execute(self.bonus_sql)

        changeset = session.changeset()

        corrupted = bytearray(changeset)

        # we leave first 75% alone so it smells like a changeset and
        # then corrupt the rest
        for i in range(int(len(corrupted) * 0.75), len(corrupted)):
            corrupted[i] ^= i & 0xFF

        # changesetbuilder should enter a corrupted state
        cb = apsw.ChangesetBuilder()

        self.assertRaises(apsw.CorruptError, cb.add, corrupted)
        # I had expected a persistent error state, but it doesn't so
        # these all succeed
        cb.add(changeset)
        cb.output()
        cb.output_stream(StreamOutput())

        # apply
        db2 = apsw.Connection("")
        db2.execute(self.base_sql)
        db2.execute(self.update_sql)
        db2.execute(self.bonus_sql)

        self.checkDbIdentical(self.db, db2)

        def handler(*args):
            return apsw.SQLITE_CHANGESET_OMIT

        self.assertRaises(
            apsw.CorruptError,
            apsw.Changeset.apply,
            corrupted,
            db2,
            flags=apsw.SQLITE_CHANGESETAPPLY_INVERT,
            conflict=handler,
        )
        self.checkDbIdentical(self.db, db2)

    def testGccWarning(self):
        "prove gcc warning is nonsense"
        # gcc warns for functions with no keyword arguments that the
        # zero length list of keywords is accessed.  I have been
        # unable to suppress the warning with code changes, so this
        # code when run under a sanitizer proves the warning is
        # nonsense.

        # verifies the function takes no parameters and we get keyword error
        regex = r".*invalid keyword argument for.*\.([a-z_]+[(][)] -> .*|__init__[(][)])"

        for meth in (
            apsw.ChangesetBuilder().output,
            apsw.Rebaser,
            apsw.ChangesetBuilder,
            apsw.ChangesetBuilder().close,
            apsw.Session(self.db, "main").close,
        ):
            self.assertRaisesRegex(TypeError, regex, meth, **{"": 3})
            self.assertRaisesRegex(TypeError, regex, meth, **{"hello": 3})
            self.assertRaisesRegex(TypeError, regex, meth, **{"one": 3, "two": 2})

            # no args works
            meth()

    def testConflicts(self):
        "apply and conflict handling"
        # change types
        # delete insert update
        # conflict types
        # data notfound conflict constraint foreignkey
        setup_sql = """
            pragma foreign_keys=off;
            drop table if exists [insert];
            drop table if exists [delete];
            drop table if exists [update];
            create table [insert](one, two, three, PRIMARY KEY(one, two));
            create table [delete](one, two, three PRIMARY KEY);
            insert into [delete] VALUES('one', 2, 3);
            create table [update](one, two PRIMARY KEY, three);
            insert into [update] VALUES('A', 2, 3);
            insert into [update] VALUES('one', 22, 3);
            pragma foreign_keys=on;
        """

        self.db.execute(setup_sql)

        session = apsw.Session(self.db, "main")
        session.attach()

        # each type of change
        self.db.execute("""
            insert into [insert] VALUES('ONE', 'TWO', 'THREE');
            delete from [delete] where two=2;
            update [update] set one='a' where two=2;
            update [update] set two=33 where two=22;
        """)

        changeset = session.changeset()

        # filter check
        for scope in "table", "change":
            for kind in "insert", "delete", "update":
                self.db.execute(setup_sql)

                def tf(name):
                    return name == kind

                def cf(change):
                    return change.name == kind

                kwargs = {"filter": tf} if scope == "table" else {"filter_change": cf}

                counter = self.db.total_changes()
                apsw.Changeset.apply(changeset, self.db, **kwargs)
                self.assertEqual(
                    self.db.total_changes() - counter,
                    {
                        "insert": 1,
                        "delete": 1,
                        "update": 3,
                    }[kind],
                )

        # invert above
        apsw.Changeset.apply(
            changeset,
            self.db,
            flags=apsw.SQLITE_CHANGESETAPPLY_INVERT,
            conflict=lambda *args: apsw.SQLITE_CHANGESET_OMIT,
        )

        db2 = apsw.Connection("")
        db2.execute(setup_sql)
        self.checkDbIdentical(self.db, db2)

        # filter error - we can't report them to SQLite but the filter should
        # have returned false and no changes made
        for err in (lambda x, y, z: True, lambda x: 1 / 0):
            self.assertRaises((TypeError, ZeroDivisionError), apsw.Changeset.apply, changeset, self.db, filter=err)
            # no changes should have happened because we returned
            # false in the filter due to the error
            self.checkDbIdentical(self.db, db2)

            # repeat with filter_change
            self.assertRaises(
                (TypeError, ZeroDivisionError), apsw.Changeset.apply, changeset, self.db, filter_change=err
            )
            self.checkDbIdentical(self.db, db2)

        self.assertRaises(
            ValueError, apsw.Changeset.apply, changeset, self.db, filter=lambda x: 1 / 0, filter_change=lambda x: 1 / 0
        )

        def handler(*args):
            nonlocal handler_return

            return handler_return

        self.db.execute("update [delete] set two = 77")

        for handler_return in (None, apsw, 1 + 5j, "hello", sys.maxsize * 1024):
            self.assertRaises(
                (TypeError, OverflowError),
                apsw.Changeset.apply,
                changeset,
                self.db,
                filter=lambda n: n == "delete",
                conflict=handler,
            )

        handler_return = 77
        self.assertRaisesRegex(
            ValueError,
            ".*is not valid SQLITE_CHANGESET_ value.*",
            apsw.Changeset.apply,
            changeset,
            self.db,
            filter=lambda n: n == "delete",
            conflict=handler,
        )

        # exercise some of the conflicts
        self.db.execute(setup_sql)

        self.db.execute("insert into [insert] values('ONE', 'TWO', 7)")

        def handler(reason, tc):
            self.assertEqual(reason, apsw.SQLITE_CHANGESET_CONFLICT)
            self.assertEqual(tc.op, "INSERT")
            self.assertEqual(tc.old, None)
            self.assertEqual(tc.new, ("ONE", "TWO", "THREE"))
            self.assertEqual(tc.conflict, ("ONE", "TWO", 7))
            return apsw.SQLITE_CHANGESET_REPLACE

        apsw.Changeset.apply(changeset, self.db, filter=lambda n: n == "insert", conflict=handler)

        self.assertEqual(self.db.execute("select * from [insert]").get, ("ONE", "TWO", "THREE"))

        self.db.execute("""
            create table deliberate(one, two,
                FOREIGN KEY (one) REFERENCES [delete](three));
            insert into deliberate values(3, 3)""")

        def handler(reason, tc):
            self.assertEqual(reason, apsw.SQLITE_CHANGESET_FOREIGN_KEY)
            self.assertEqual(tc.op, "Undocumented op 0")
            self.assertEqual(tc.fk_conflicts, 1)
            return apsw.SQLITE_CHANGESET_OMIT

        apsw.Changeset.apply(changeset, self.db, filter=lambda n: n == "delete", conflict=handler)

        self.db.execute(
            "drop table deliberate; "
            + setup_sql
            + """
            delete from [update] where two=22
        """
        )

        def handler(reason, tc):
            self.assertEqual(reason, apsw.SQLITE_CHANGESET_NOTFOUND)
            self.assertEqual(tc.op, "DELETE")
            self.assertEqual(tc.old, ("one", 22, 3))
            self.assertEqual(tc.new, None)
            self.assertEqual(tc.conflict, None)
            return handler_return

        handler_return = apsw.SQLITE_CHANGESET_REPLACE
        self.assertRaises(
            apsw.MisuseError, apsw.Changeset.apply, changeset, self.db, filter=lambda n: n == "update", conflict=handler
        )
        handler_return = apsw.SQLITE_CHANGESET_OMIT
        apsw.Changeset.apply(changeset, self.db, filter=lambda n: n == "update", conflict=handler)

        self.db.execute(
            "drop table [insert]; create table [insert](one TEXT CHECK (one != 'ONE'), two, three, PRIMARY KEY(one, two))"
        )

        def handler(reason, tc):
            self.assertEqual(reason, apsw.SQLITE_CHANGESET_CONSTRAINT)
            self.assertEqual(tc.op, "INSERT")
            self.assertEqual(tc.old, None)
            self.assertEqual(tc.new, ("ONE", "TWO", "THREE"))
            self.assertEqual(tc.conflict, None)
            return handler_return

        handler_return = apsw.SQLITE_CHANGESET_REPLACE
        self.assertRaises(
            apsw.MisuseError, apsw.Changeset.apply, changeset, self.db, filter=lambda n: n == "insert", conflict=handler
        )

        handler_return = apsw.SQLITE_CHANGESET_OMIT
        apsw.Changeset.apply(changeset, self.db, filter=lambda n: n == "insert", conflict=handler)

        handler_return = apsw.SQLITE_CHANGESET_ABORT
        self.assertRaises(
            apsw.AbortError, apsw.Changeset.apply, changeset, self.db, filter=lambda n: n == "insert", conflict=handler
        )

        self.db.execute("update [delete] set two=77")

        def handler(reason, tc):
            self.assertEqual(reason, apsw.SQLITE_CHANGESET_DATA)
            self.assertEqual(tc.op, "DELETE")
            self.assertEqual(tc.old, ("one", 2, 3))
            self.assertEqual(tc.new, None)
            self.assertEqual(tc.conflict, ("one", 77, 3))
            return handler_return

        handler_return = apsw.SQLITE_CHANGESET_OMIT
        apsw.Changeset.apply(changeset, self.db, filter=lambda n: n == "delete", conflict=handler)
        self.assertEqual(1, self.db.execute("select count(*) from [delete]").get)

        handler_return = apsw.SQLITE_CHANGESET_REPLACE
        apsw.Changeset.apply(changeset, self.db, filter=lambda n: n == "delete", conflict=handler)
        self.assertEqual(0, self.db.execute("select count(*) from [delete]").get)

    def testNoPrimaryKey(self):
        "check when tables have no primary key"
        self.db.execute("""
            create table [insert](one);
            create table [delete](one);
            insert into [delete] values('one');
            create table [update](hello, rowid, oid);
            insert into [update] values('one', 'two', 'three');
            """)
        session = apsw.Session(self.db, "main")
        session.config(apsw.SQLITE_SESSION_OBJCONFIG_ROWID, True)
        session.attach()

        self.db.execute("""
            insert into [insert] values(1);
            delete from [delete] where one='one';
            update [update] set hello='xxx' where oid='three';

        """)

        changeset = session.changeset()

        seen = {"INSERT": 0, "DELETE": 0, "UPDATE": 0}
        for change in apsw.Changeset.iter(changeset):
            seen[change.op] += 1

        self.assertEqual(seen, {"INSERT": 1, "DELETE": 1, "UPDATE": 1})

        self.assertEqual(
            sorted(
                apsw.ext.changeset_to_sql(
                    changeset,
                    get_columns=functools.partial(
                        apsw.ext.find_columns,
                        connection=self.db,
                    ),
                )
            ),
            sorted(
                [
                    'INSERT INTO "insert"(_rowid_, one) VALUES (1, 1);',
                    """DELETE FROM "delete" WHERE _rowid_ = 1 AND one = 'one';""",
                    """UPDATE "update" SET hello='xxx' WHERE _rowid_ = 1 AND hello = 'one';""",
                ]
            ),
        )

    def testColumnTypes(self):
        "exotic column types"
        self.db.execute("""
            create table foo(__hidden__one, two DEFAULT 4, three GENERATED ALWAYS AS (2+length(two)), four INT PRIMARY KEY);
            insert into foo(__hidden__one, two, four) values (1,2,3), (11,22,33), (111,222,333);
        """)

        session = apsw.Session(self.db, "main")

        session.attach("foo")

        self.db.execute("""
            insert into foo(four) values(444);
            update foo set two = 2.2 where four=333;
            delete from foo where __hidden__one=1;
        """)

        self.assertEqual(
            sorted(
                apsw.ext.changeset_to_sql(
                    session.changeset(),
                    get_columns=functools.partial(
                        apsw.ext.find_columns,
                        connection=self.db,
                    ),
                )
            ),
            sorted(
                [
                    "INSERT INTO foo(two, four) VALUES (4, 444);",
                    "DELETE FROM foo WHERE four = 3 AND two = 2;",
                    "UPDATE foo SET two=2.2 WHERE four = 333 AND two = 222;",
                ]
            )
            if "ENABLE_HIDDEN_COLUMNS" in apsw.compile_options
            else sorted(
                [
                    "INSERT INTO foo(__hidden__one, two, four) VALUES (NULL, 4, 444);",
                    "DELETE FROM foo WHERE four = 3 AND __hidden__one = 1 AND two = 2;",
                    "UPDATE foo SET two=2.2 WHERE four = 333 AND two = 222;",
                ]
            ),
        )


# handy debugging functions
def changeset_to_sql(title, changeset, db):
    print("-" * len(title))
    print(title)
    print(f"{len(changeset)=}")
    print()
    for line in apsw.ext.changeset_to_sql(
        changeset,
        get_columns=functools.partial(
            apsw.ext.find_columns,
            connection=db,
        ),
    ):
        print(line)
    print()


def show_conflict(reason, change):
    print(apsw.mapping_session_conflict[reason], change)
    return apsw.SQLITE_CHANGESET_OMIT


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
        self.offset += len(res)
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
