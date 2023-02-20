#!/usr/bin/env python3

import sys
import pathlib
import gc
import math

sys.path.insert(0, str(pathlib.Path(__file__).parent.absolute() / "tools"))

import genfaultinject

returns = genfaultinject.returns

call_remap = {v: k for k, v in genfaultinject.call_map.items()}

has_faulted = set()

to_fault = set()

Proceed = 0x1FACADE
"magic value keep going (ie do not inject a return value)"

expect_exception = set()

FAULTT = ZeroDivisionError
FAULTS = "Fault injection synthesized failure"

FAULT = FAULTT, FAULTS


def apswattr(name):
    # this is need because we don't do the top level import of apsw
    assert "apsw" in sys.modules
    return getattr(sys.modules["apsw"], name)


previous_func = None


def FaultCall(key):
    global previous_func
    fname = call_remap.get(key[0], key[0])
    try:
        if fname in returns["pyobject"]:
            expect_exception.add(MemoryError)
            raise MemoryError(FAULTS)

        if fname == "sqlite3_threadsafe":
            expect_exception.add(EnvironmentError)
            return 0

        if fname == "sqlite3_close":
            expect_exception.add(apswattr("ConnectionNotClosedError"))
            expect_exception.add(apswattr("IOError"))
            return apswattr("SQLITE_IOERR")

        if fname == "sqlite3_enable_shared_cache":
            expect_exception.add(apswattr("Error"))
            return 0xFE  # also does unknown error code to make_exception

        if fname in {"sqlite3_value_type", "sqlite3_column_type"}:
            if key[2] == "apswvtabUpdate":
                expect_exception.add(TypeError)
            else:
                expect_exception.add(apswattr("Error"))
            return 12345

        # pointers with 0 being failure
        if fname in {"sqlite3_backup_init", "sqlite3_malloc64", "sqlite3_mprintf"}:
            expect_exception.add(apswattr("SQLError"))
            expect_exception.add(MemoryError)
            return 0

        if fname.startswith("sqlite3_"):
            expect_exception.add(apswattr("TooBigError"))
            return apswattr("SQLITE_TOOBIG")

        if fname.startswith("PyLong_As"):
            expect_exception.add(OverflowError)
            return (-1, OverflowError, FAULTS)

        if fname.startswith("Py"):
            # for ones returning -1 on error
            expect_exception.add(FAULTT)
            return (-1, *FAULT)

    finally:
        to_fault.discard(key)
        has_faulted.add(key)
        if key[2] == "apswvtabFindFunction" or previous_func == "apswvtabFindFunction":
            expect_exception.update({TypeError, ValueError})
        previous_func = key[2]

    print("Unhandled", key)
    breakpoint()


def called(key):
    # we can't do this because it messes up the import machinery
    if key[0] == "PyUnicode_AsUTF8" and key[2] == "apsw_getattr":
        return Proceed

    if expect_exception:
        # already have faulted this round
        if key not in has_faulted:
            to_fault.add(key)
        return Proceed
    if key in has_faulted:
        return Proceed
    print(key)
    return FaultCall(key)


sys.apsw_fault_inject_control = called
sys.apsw_should_fault = lambda *args: False


def exercise():
    "This function exercises the code paths where we have fault injection"

    # The module is not imported outside because the init function has
    # several fault injection locations

    import apsw, apsw.ext

    try:
        apsw.config(apsw.SQLITE_CONFIG_URI, 1)
        apsw.config(apsw.SQLITE_CONFIG_MULTITHREAD)
        apsw.config(apsw.SQLITE_CONFIG_PCACHE_HDRSZ)
        apsw.ext.log_sqlite(level=0)
    except apsw.MisuseError:
        pass

    apsw.initialize()
    apsw.status(apsw.SQLITE_STATUS_MEMORY_USED)

    for n in """
            SQLITE_VERSION_NUMBER apswversion compile_options keywords memoryused
            sqlite3_sourceid sqlitelibversion using_amalgamation vfsnames
        """.split():
        obj = getattr(apsw, n)
        if callable(obj):
            obj()

    apsw.enablesharedcache(False)

    for v in ("a'bc", "ab\0c", b"aabbcc", None, math.nan, math.inf, -0.0, -math.inf, 3.1):
        apsw.format_sql_value(v)

    con = apsw.Connection("")
    con.wal_autocheckpoint(1)
    con.execute("pragma page_size=512; pragma auto_vacuum=FULL; pragma journal_mode=wal; create table foo(x)")
    with con:
        con.executemany("insert into foo values(zeroblob(1023))", [tuple() for _ in range(500)])

    con.autovacuum_pages(lambda *args: 1)
    for i in range(20):
        con.wal_autocheckpoint()
        victim = con.execute("select rowid from foo order by random limit 1").fetchall()[0][0]
        con.execute("delete from foo where rowid=?", (victim,))


    con.config(apsw.SQLITE_DBCONFIG_ENABLE_TRIGGER, 1)
    con.setauthorizer(None)
    con.authorizer = None
    con.collationneeded(None)
    con.collationneeded(lambda *args: 0)
    con.enableloadextension(True)
    con.setbusyhandler(None)
    con.setbusyhandler(lambda *args: True)
    con.createscalarfunction("failme", lambda x: x + 1)
    cur = con.cursor()
    for _ in cur.execute("select failme(3)"):
        cur.description
        cur.description_full
        cur.getdescription()

    apsw.allow_missing_dict_bindings(True)
    con.execute("select :a,:b,$c", {'a': 1, 'c': 3})
    con.execute("select ?, ?, ?, ?", (None, "dsadas", b"xxx", 3.14))
    apsw.allow_missing_dict_bindings(False)

    class Source:

        def Connect(self, *args):
            return "create table ignored(c0, c1, c2, c3)", Source.Table()

        class Table:

            def BestIndexObject(self, iio):
                apsw.ext.index_info_to_dict(iio)
                for n in range(iio.nConstraint):
                    if iio.get_aConstraintUsage_in(n):
                        iio.set_aConstraintUsage_in(n, True)
                        iio.set_aConstraintUsage_argvIndex(n, 1)
                iio.estimatedRows = 7
                return True

            def Open(self):
                return Source.Cursor()

            def UpdateDeleteRow(self, rowid):
                pass

            def UpdateInsertRow(self, rowid, fields):
                return 77

            def UpdateChangeRow(self, rowid, newrowid, fields):
                pass

            def FindFunction(self, name, nargs):
                if nargs == 1:
                    return lambda x: 6
                return [apsw.SQLITE_INDEX_CONSTRAINT_FUNCTION, lambda *args: 7]

        class Cursor:

            def Filter(self, *args):
                self.pos = 0

            def Eof(self):
                return self.pos >= 7

            def Column(self, n):
                return [None, ' ' * n, b"aa" * n, 3.14 * n][n]

            def Next(self):
                self.pos += 1

            def Rowid(self):
                return self.pos

            def Close(self):
                pass

    con.createmodule("vtable", Source(), use_bestindex_object=True, iVersion=3, eponymous=True)
    con.overloadfunction("vtf", 2)
    con.overloadfunction("vtf", 1)
    con.execute("select * from vtable where c2>2 and c1 in (1,2,3)")
    con.execute("create virtual table fred using vtable()")
    con.execute("select vtf(c3) from fred where c3>5; select vtf(c2,c1) from fred where c3>5").fetchall()
    con.execute("delete from fred where c3>5")
    n = 2
    con.execute("insert into fred values(?,?,?,?)", [None, ' ' * n, b"aa" * n, 3.14 * n])
    con.execute("insert into fred(ROWID, c1) values (99, NULL)")
    con.execute("update fred set c2=c3 where rowid=3; update fred set rowid=990 where c2=2")

    def func(*args):
        return 3.14

    con.createscalarfunction("func", func)
    con.execute("select func(1,null,'abc',x'aabb')")

    class SumInt:

        def __init__(self):
            self.v = 0

        def step(self, arg):
            self.v += arg

        def inverse(self, arg):
            self.v -= arg

        def final(self):
            return self.v

        def value(self):
            return self.v

    con.create_window_function("sumint", SumInt)

    for row in con.execute("""
            CREATE TABLE t3(x, y);
            INSERT INTO t3 VALUES('a', 4),
                                ('b', 5),
                                ('c', 3),
                                ('d', 8),
                                ('e', 1);
            -- Use the window function
            SELECT x, sumint(y) OVER (
            ORDER BY x ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING
            ) AS sum_y
            FROM t3 ORDER BY x;
        """):
        pass

    for n in """db_names cacheflush changes filename filename_journal
                filename_wal getautocommit in_transaction interrupt last_insert_rowid
                open_flags open_vfs release_memory sqlite3pointer system_errno
                totalchanges txn_state
        """.split():
        obj = getattr(con, n)
        if callable(obj):
            obj()

    con.execute("create table blobby(x); insert into blobby values(?)", (apsw.zeroblob(99), ))
    blob = con.blobopen("main", "blobby", "x", con.last_insert_rowid(), True)
    blob.write(b"hello world")
    blob.seek(80)
    blob.read(10)
    blob.close()

    con.cache_stats(True)



    con2 = apsw.Connection("")
    with con2.backup("main", con, "main") as backup:
        backup.step(1)
    del con2

    con.close()
    del sys.modules["apsw.ext"]
    gc.collect()
    apsw.shutdown()
    del sys.modules["apsw"]


exc_happened = []


def unraisehook(*details):
    exc_happened.append(details[:2])


sys.unraisablehook = unraisehook
sys.excepthook = unraisehook


def verify_exception(tested):
    ok = any(e[0] in expect_exception for e in exc_happened) or any(FAULTS in str(e[1]) for e in exc_happened)
    if not ok:
        print("\nExceptions failed to verify")
        print(f"Got { exc_happened }")
        print(f"Expected { expect_exception }")
        print(f"Testing { tested }")
        sys.exit(1)


complete = False
last = set(), set()
while True:
    exc_happened = []
    expect_exception = set()
    try:
        exercise()
        if not to_fault:
            complete = True
            break
    except Exception as e:
        exc_happened.append(sys.exc_info()[:2])
        verify_exception(has_faulted - last[1])

    now = set(to_fault), set(has_faulted)
    if now == last:
        print("\nUnable to make progress")
        exercise()
        break
    else:
        last = now

if complete:
    print("\nComplete")

for n in sorted(has_faulted):
    print(n)

print(f"Total faults: { len(has_faulted) }")

if to_fault:
    t = f"Failed to fault { len(to_fault) }"
    print("=" * len(t))
    print(t)
    print("=" * len(t))
    for f in sorted(to_fault):
        print(f)
    sys.exit(1)
