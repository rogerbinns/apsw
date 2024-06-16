#!/usr/bin/env python3
from __future__ import annotations

import sys
import gc
import math
import traceback
import os
import glob
import inspect
import atexit
import random

import tempfile

import genfaultinject

tmpdir = tempfile.TemporaryDirectory(prefix="fitest-")
atexit.register(tmpdir.cleanup)
print("tmpdir", tmpdir.name)

testing_recursion = False


def exercise(example_code, expect_exception):
    "This function exercises the code paths where we have fault injection"

    global testing_recursion
    testing_recursion = False

    def file_cleanup():
        if "apsw" in sys.modules:
            for c in sys.modules["apsw"].connections():
                c.close(True)
        for f in glob.glob(f"{ tmpdir.name }/dbfile*") + glob.glob(f"{ tmpdir.name }/myobfudb*"):
            os.remove(f)

    file_cleanup()

    # The module is not imported outside because the init function has
    # several fault injection locations

    import apsw, apsw.ext

    try:
        apsw.config(apsw.SQLITE_CONFIG_URI, 1)
        apsw.config(apsw.SQLITE_CONFIG_MULTITHREAD)
        apsw.config(apsw.SQLITE_CONFIG_PCACHE_HDRSZ)
        apsw.config(apsw.SQLITE_CONFIG_LOG, None)
        apsw.ext.log_sqlite(level=0)
    except apsw.MisuseError:
        pass

    try:
        apsw.this_can_error
    except AttributeError:
        pass

    apsw.initialize()
    apsw.log(3, "A message")
    apsw.status(apsw.SQLITE_STATUS_MEMORY_USED)

    apsw.connections()
    if expect_exception:
        return

    for n in """
            SQLITE_VERSION_NUMBER apsw_version compile_options keywords memory_used
            sqlite3_sourceid sqlite_lib_version using_amalgamation vfs_names
            memory_high_water
        """.split():
        obj = getattr(apsw, n)
        if callable(obj):
            obj()

    apsw.soft_heap_limit(1_000_000_000)
    apsw.hard_heap_limit(1_000_000_000)
    apsw.randomness(32)
    apsw.enable_shared_cache(False)
    apsw.release_memory(1024)
    apsw.exception_for(3)
    try:
        apsw.exception_for(0xfe)
    except ValueError:
        pass

    for v in ("a'bc", "ab\0c", b"aabbcc", None, math.nan, math.inf, -0.0, -math.inf, 3.1):
        apsw.format_sql_value(v)

    con = apsw.Connection("")
    apsw.connections()
    con.wal_autocheckpoint(1)

    extfname = "./testextension.sqlext"
    if os.path.exists(extfname):
        con.enable_load_extension(True)
        con.load_extension(extfname)
        con.execute("select half(7)")

    con.execute(
        "pragma page_size=512; pragma auto_vacuum=FULL; pragma journal_mode=wal; create table foo(x)").fetchall()

    def trace(*args):
        return True

    con.exec_trace = trace
    with con:
        con.executemany("insert into foo values(zeroblob(1023))", [tuple() for _ in range(500)])
    con.exec_trace = None

    apsw.zeroblob(77).length()

    con.autovacuum_pages(lambda *args: 1)
    for i in range(20):
        con.wal_autocheckpoint(1)
        victim = con.execute("select rowid from foo order by random() limit 1").fetchall()[0][0]
        con.execute("delete from foo where rowid=?", (victim, ))

    con.config(apsw.SQLITE_DBCONFIG_ENABLE_TRIGGER, 1)
    con.set_authorizer(None)
    con.authorizer = None
    con.collation_needed(None)
    con.collation_needed(lambda *args: 0)
    con.enable_load_extension(True)
    con.set_busy_handler(None)
    con.set_busy_handler(lambda *args: True)
    con.set_busy_timeout(99)
    con.create_scalar_function("failme", lambda x: x + 1)
    cur = con.cursor()
    for _ in cur.execute("select failme(3)"):
        cur.description
        if hasattr(cur, "description_full"):
            cur.description_full
        cur.get_description()

    if expect_exception:
        return

    apsw.allow_missing_dict_bindings(True)
    con.execute("select :a,:b,$c", {'a': 1, 'c': 3})
    con.execute("select ?, ?, ?, ?", (None, "dsadas", b"xxx", 3.14))
    apsw.allow_missing_dict_bindings(False)

    if expect_exception:
        return

    for query in ("select 3,4", "select 3; select 4", "select 3,4; select 4,5", "select 3,4; select 5", "select 3"):
        con.execute(query).get
    con.executemany("select ?", [(i, ) for i in range(10)]).get

    con.execute("/* comment */").get

    cur = con.cursor()
    cur.execute("select 3").fetchall()
    cur.get

    apsw.ext.query_info(con, "select ?2, $three", actions=True, expanded_sql=True)

    con.pragma("user_version")
    con.pragma("user_version", 7)

    class Source:

        def Connect(self, *args):
            con.vtab_config(apsw.SQLITE_VTAB_CONSTRAINT_SUPPORT, 1)
            return "create table ignored(c0, c1, c2, c3)", Source.Table()

        class Table:

            def BestIndexObject(self, iio: apsw.IndexInfo):
                apsw.ext.index_info_to_dict(iio)
                for n in range(iio.nConstraint):
                    if iio.get_aConstraintUsage_in(n):
                        iio.set_aConstraintUsage_in(n, True)
                        iio.set_aConstraintUsage_argvIndex(n, 1)
                iio.estimatedRows = 7
                iio.orderByConsumed = False
                iio.estimatedCost = 33

                return True

            def BestIndex(self, *args):
                return (None, 23, "some string", True, 321321)

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

    con.create_module("vtable", Source(), use_bestindex_object=True, iVersion=3, eponymous=True)
    con.create_module("vtable2", Source(), use_bestindex_object=False, iVersion=3, eponymous=True)
    con.overload_function("vtf", 2)
    con.overload_function("vtf", 1)
    con.execute("select * from vtable where c2>2 and c1 in (1,2,3)")
    con.execute("create virtual table fred using vtable()")
    con.execute("select vtf(c3) from fred where c3>5; select vtf(c2,c1) from fred where c3>5 order by c2").fetchall()
    con.execute("select vtf(c3) from vtable2 where c3>5; select vtf(c2,c1) from vtable2 where c3>5 order by c2 desc, c1"
                ).fetchall()
    con.execute("delete from fred where c3>5")
    n = 2
    con.execute("insert into fred values(?,?,?,?)", [None, ' ' * n, b"aa" * n, 3.14 * n])
    con.execute("insert into fred(ROWID, c1) values (99, NULL)")
    con.execute("update fred set c2=c3 where rowid=3; update fred set rowid=990 where c2=2")

    con.drop_modules(["something", "vtable", "something else"])

    con.set_profile(lambda: 1)
    con.set_profile(None)

    # has to be done on a real file not memory db
    con2 = apsw.Connection("/tmp/fitesting")
    con2.pragma("user_version", 77)
    con2.read("main", 0, 0, 0x1ffff)  # larger fires sanity check assertion

    # this is to work MakeSqliteMsgFromPyException
    def meth(*args):
        raise apsw.SchemaChangeError("a" * 16384)

    Source.Table.BestIndexObject = meth
    try:
        con.execute("select * from vtable where c2>2")
    except apsw.SchemaChangeError:
        pass

    con.drop_modules(None)

    def func(*args):
        return 3.14

    con.create_scalar_function("func", func)
    con.execute("select func(1,null,'abc',x'aabb')")

    if expect_exception:
        return

    def do_nothing():
        pass

    con.set_rollback_hook(do_nothing)
    con.execute("begin; create table goingaway(x,y,z); rollback")
    con.set_rollback_hook(None)

    con.collation_needed(lambda *args: con.create_collation("foo", lambda *args: 0))
    con.execute(
        "create table col(x); insert into col values ('aa'), ('bb'), ('cc'); select * from col order by x collate foo")

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

    def wf():
        x = SumInt()
        return (x, SumInt.step, SumInt.final, SumInt.value, SumInt.inverse)

    con.create_window_function("sumint", SumInt)
    con.create_window_function("sumint2", wf)

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
            SELECT x, sumint2(y) OVER (
            ORDER BY x ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING
            ) AS sum_y
            FROM t3 ORDER BY x;
        """):
        pass

    for n in """db_names cache_flush changes filename filename_journal
                filename_wal get_autocommit in_transaction interrupt last_insert_rowid
                open_flags open_vfs release_memory sqlite3_pointer system_errno
                total_changes txn_state
        """.split():
        obj = getattr(con, n)
        if callable(obj):
            obj()

    con.execute("create table blobby(x); insert into blobby values(?)", (apsw.zeroblob(990), ))
    blob = con.blob_open("main", "blobby", "x", con.last_insert_rowid(), True)
    blob.write(b"hello world")
    blob.seek(80)
    blob.read(10)
    m = bytearray(b"12345678")
    blob.read_into(m)
    blob.tell()
    blob.read(0)
    blob.seek(blob.length())
    blob.read(10)
    blob.seek(0)
    blob.reopen(con.last_insert_rowid())
    blob.close()

    if expect_exception:
        return

    con.cache_stats(True)
    con.deserialize("main", con.serialize("main"))

    apsw.connection_hooks = [lambda x: None] * 3
    x = apsw.Connection("")
    c = x.cursor()
    try:
        x.backup("main", con, "main")
    except apsw.ThreadingViolationError:
        pass
    del c

    con2 = apsw.Connection("")
    with con2.backup("main", con, "main") as backup:
        while backup.remaining:
            backup.step(1)
            backup.page_count
    backup.finish()
    del con2

    con.close()
    del con
    if expect_exception:
        return

    vname = "foo"
    v = apsw.VFS(vname, iVersion=3)
    registered = [vfs for vfs in apsw.vfs_details() if vfs["zName"] == vname][0]
    meth_names = [n for n in registered if n.startswith("x")]
    for ver in (1, 2, 3):
        v = apsw.VFS(f"{vname}{ver}", exclude=set(random.sample(meth_names, 5)))
        apsw.vfs_details()
        try:
            v.xCurrentTime()
        except Exception:
            pass

    class MYVFS(apsw.VFS):

        def __init__(self):
            super().__init__("testvfs", "", maxpathname=0)

    vfs = MYVFS()

    vfs.xRandomness(77)
    vfs.xGetLastError()
    v = None
    while True:
        v = vfs.xNextSystemCall(v)
        if v is None:
            break

    if expect_exception:
        return

    testdbname = f"{ tmpdir.name }/dbfile-testdb"
    flags = [apsw.SQLITE_OPEN_MAIN_DB | apsw.SQLITE_OPEN_CREATE | apsw.SQLITE_OPEN_READWRITE, 0]
    apsw.VFSFile("testvfs", testdbname, flags)

    longdbname = testdbname + "/." * (4096 - len(testdbname) // 2)
    try:
        apsw.VFSFile("testvfs", longdbname, flags)
    except apsw.CantOpenError:
        pass

    vfs.xGetLastError()

    if expect_exception:
        return

    vfs.xOpen(testdbname, flags)

    if expect_exception:
        return

    class myvfs(apsw.VFS):

        def __init__(self, name="apswfivfs", parent=""):
            self.parent = parent
            super().__init__(name, parent)

        def xDelete(self, name, syncdir):
            return super().xDelete(name, syncdir)

        def xOpen(self, name, flags):
            return myvfsfile(self.parent, name, flags)

    class myvfsfile(apsw.VFSFile):

        def __init__(self, parent, filename, flags):
            hasattr(filename, "parameters") and filename.parameters
            super().__init__(parent, filename, flags)

    vfsinstance = myvfs()
    vfsinstance2 = myvfs("apswfivfs2", "apswfivfs")

    try:
        vfsinstance2.xDelete("no/such/file", True)
    except apsw.IOError:
        pass

    vfsinstance2.xFullPathname("abc.txt")

    file_cleanup()

    import apsw.tests
    apsw.tests.testtimeout = False
    apsw.tests.vfstestdb(f"{ tmpdir.name }/dbfile-delme-vfswal", "apswfivfs2", mode="wal")

    file_cleanup()
    apsw.tests.testtimeout = True
    apsw.tests.vfstestdb(f"{ tmpdir.name }/dbfile-delme-vfsstd", "apswfivfs")

    if expect_exception:
        return

    file_cleanup()
    exec(example_code, {"print": lambda *args: None}, None)

    if expect_exception:
        return

    if False:
        # This does recursion error, which also causes lots of last chance
        # exception printing to stderr, making the noise unhelpful.  The
        # code has been validated at the time of writing this comment.
        testing_recursion = True
        vfsinstance.parent = "apswfivfs2"
        apsw.tests.testtimeout = False
        apsw.tests.vfstestdb(f"{ tmpdir.name }/dbfile-delme-vfswal", "apswfivfs2", mode="wal")
        testing_recursion = False

    apsw.set_default_vfs(apsw.vfs_names()[0])
    apsw.unregister_vfs("apswfivfs")

    del vfsinstance
    del vfsinstance2

    del sys.modules["apsw.ext"]
    gc.collect()
    del apsw


class Tester:

    Proceed = 0x1FACADE
    "magic value keep going (ie do not inject a return value)"
    ProceedReturn18 = 0x2FACADE
    "Call function, but then pretend it returned 18"

    FAULTT = ZeroDivisionError
    FAULTS = "Fault injection synthesized failure"

    FAULT = FAULTT, FAULTS

    def __init__(self):
        self.returns = genfaultinject.returns
        self.call_remap = {v: k for k, v in genfaultinject.call_map.items()}

        sys.apsw_fault_inject_control = self.fault_inject_control

        lines, start = inspect.getsourcelines(exercise)
        end = start + len(lines)
        self.start_line = start
        self.end_line = end

        with open("example-code.py", "rt") as f:
            code = f.read()
            # we do various transformations but must keep the line numbers the same
            code = code.replace("import os", "import os,contextlib")
            # make it use tmpfs
            code = code.replace('"dbfile"', f'"{ tmpdir.name }/dbfile-delme-example"')
            code = code.replace("myobfudb", f"{ tmpdir.name }/myobfudb-example")
            # logging will fail
            code = code.replace("apsw.ext.log_sqlite()",
                                "with contextlib.suppress(apsw.MisuseError): apsw.ext.log_sqlite(level=0)")
        self.example_code_lines = len(code.split("\n"))
        self.example_code = compile(code, "example-code.py", 'exec')

    @staticmethod
    def apsw_attr(name: str):
        # this is need because we don't do the top level import of apsw
        assert "apsw" in sys.modules
        return getattr(sys.modules["apsw"], name)

    def FaultCall(self, key):
        apsw_attr = self.apsw_attr
        fname = self.call_remap.get(key[0], key[0])
        try:
            if fname in self.returns["pointer"]:
                self.expect_exception.append(MemoryError)
                return 0, MemoryError, self.FAULTS

            if fname == "sqlite3_threadsafe":
                self.expect_exception.append(EnvironmentError)
                return 0

            # we need these to succeed at the SQLite level but still return
            # an error.  Otherwise there will be memory leaks.
            if fname in {
                    "sqlite3_close",
                    "sqlite3_vfs_unregister",
                    "sqlite3_backup_finish",
            }:
                self.expect_exception.append(apsw_attr("ConnectionNotClosedError"))
                self.expect_exception.append(apsw_attr("TooBigError"))  # code 18
                return self.ProceedReturn18

            if fname == "sqlite3_deserialize":
                # it frees the buffer even on error
                self.expect_exception.append(apsw_attr("TooBigError"))  # code 18
                return self.ProceedReturn18

            if fname == "sqlite3_enable_shared_cache":
                self.expect_exception.append(apsw_attr("Error"))
                return 0xFE  # also does unknown error code to make_exception

            if fname == "sqlite3_load_extension":
                self.expect_exception.append(apsw_attr("ExtensionLoadingError"))
                return self.apsw_attr("SQLITE_TOOBIG")

            if fname == "sqlite3_vtab_in_next":
                self.expect_exception.append(ValueError)
                return self.apsw_attr("SQLITE_TOOBIG")

            # internal routine
            if fname == "connection_trace_and_exec":
                self.expect_exception.append(MemoryError)
                return (-1, MemoryError, self.FAULTS)

            # pointers with 0 being failure
            if fname in {
                    "sqlite3_backup_init", "sqlite3_malloc64", "sqlite3_mprintf", "sqlite3_column_name",
                    "sqlite3_aggregate_context", "sqlite3_expanded_sql"
            }:
                self.expect_exception.append(apsw_attr("SQLError"))
                self.expect_exception.append(MemoryError)
                return 0

            if fname.startswith("sqlite3_"):
                self.expect_exception.append(apsw_attr("TooBigError"))
                return self.apsw_attr("SQLITE_TOOBIG")

            if fname.startswith("PyLong_As"):
                self.expect_exception.append(OverflowError)
                return (-1, OverflowError, self.FAULTS)

            if fname == "PyBuffer_IsContiguous":
                # the PyObject_GetBuffer call fails non-contiguous
                # anyway, but this is being doubly sure
                self.expect_exception.append(TypeError)
                return 0

            if fname.startswith("Py") or fname in {"_PyBytes_Resize", "getfunctionargs"}:
                # for ones returning -1 on error
                self.expect_exception.append(self.FAULTT)
                return (-1, *self.FAULT)

        finally:
            self.to_fault.pop(key, None)
            self.has_faulted_ever.add(key)
            self.faulted_this_round.append(key)
            if key[2] == "apswvtabFindFunction" or (self.last_key and self.last_key[2] == "apswvtabFindFunction"):
                self.expect_exception.extend([TypeError, ValueError])
            self.last_key = key

        print("Unhandled", key)
        breakpoint()

    def fault_inject_control(self, key):
        if testing_recursion and key[2] in {"apsw_write_unraisable", "apswvfs_excepthook"}:
            return self.Proceed
        if self.runplan is not None:
            if not self.runplan:
                return self.Proceed
            elif isinstance(self.runplan, str):
                if key[0] != self.runplan:
                    return self.Proceed
            elif key == self.runplan[0]:
                self.runplan.pop(0)
            else:
                return self.Proceed
        else:
            if self.expect_exception:
                # already have faulted this round
                if key not in self.has_faulted_ever and key not in self.to_fault:
                    self.to_fault[key] = self.faulted_this_round[:]
                return self.Proceed
            if key in self.has_faulted_ever:
                return self.Proceed

        line, percent = self.get_progress()
        if self.runplan is not None:
            print("  Pre" if self.runplan else "Fault", end=" ")
        print(f"faulted: { len(self.has_faulted_ever): 4} / new: { len(self.to_fault): 3}"
              f" cur: { int(percent): 3}%  L{ line } { key }")
        try:
            return self.FaultCall(key)
        finally:
            assert self.expect_exception
            assert key in self.has_faulted_ever

    def exchook(self, *args):
        if len(args) > 1:
            self.add_exc(args[1])
        else:
            self.add_exc(args[0].exc_value)

    def add_exc(self, e):
        if e:
            self.last_exc = e
        while e:
            self.exc_happened.append((type(e), e))
            e = e.__context__

    def __enter__(self):
        return self

    def __exit__(self, e1, e2, e3):
        if e2:
            self.add_exc(e2)
        return True  # do not raise

    def get_progress(self):
        # work out what progress in exercise
        ss = traceback.extract_stack()
        curline = self.start_line
        exercise_ok = True
        for frame in ss:
            if frame.filename == __file__ and exercise_ok and frame.name == "exercise":
                curline = max(curline, frame.lineno)
            if frame.filename == "example-code.py":
                if exercise_ok:
                    exercise_ok = False
                    curline = frame.lineno
                curline = max(curline, frame.lineno)

        total_lines = self.end_line - self.start_line + self.example_code_lines
        if exercise_ok:
            curline_pretty = f"{ curline }(exercise)"
            pos = curline - self.start_line
        else:
            curline_pretty = f"{ curline }(example) "
            pos = curline + (self.end_line - self.start_line)

        return curline_pretty, 100 * pos / total_lines

    def verify_exception(self, tested):
        ok = any(e[0] in self.expect_exception for e in self.exc_happened) or any(self.FAULTS in str(e[1])
                                                                                  for e in self.exc_happened)
        # these faults happen in fault handling so can't fault report themselves.
        if tested and list(tested)[0][2] in {
                "apsw_set_errmsg", "apsw_get_errmsg", "apsw_write_unraisable", "MakeSqliteMsgFromPyException",
                "apswvfs_excepthook"
        }:
            return
        if len(self.exc_happened) < len(tested):
            if len(tested) >= 2 and (tested[0][0], tested[1][0]) == ("_PyObject_New", "sqlite3_backup_finish"):
                # backup finish error is ignored because we are handling the
                # object_new error
                pass
            elif tested[-1][2] in {"MakeSqliteMsgFromPyException", "apsw_write_unraisable", "apswvfs_excepthook"}:
                # already handling an exception
                pass
            elif tested[-1][2] == "apswvfsfile_xFileControl":
                # we deliberately ignore errors getting VFSNAMES
                if tested[-1][0] == "PyUnicode_AsUTF8" and tested[-1][4] in {"qualname", "module"}:
                    ok = True
                elif tested[-1][0] == "sqlite3_mprintf":
                    ok = True
            else:
                ok = False
        if not ok:
            print("\nExceptions failed to verify")
            print(f"Got { self.exc_happened }")
            print(f"Expected { self.expect_exception }")
            print(f"Testing { tested }")
            if len(self.exc_happened) < len(tested):
                print("Fewer exceptions observed than faults generated")
            if self.last_exc:
                print("Traceback:")
                tbe = traceback.TracebackException(type(self.last_exc),
                                                   self.last_exc,
                                                   self.last_exc.__traceback__,
                                                   capture_locals=False,
                                                   compact=True)
                for line in tbe.format():
                    print(line)
            sys.exit(1)

    def run(self):
        # keys that we will fault in the future.  we saw these keys while a
        # call had already faulted, so we have to do those same faults again
        # to see this one.  value is list of those previous faults
        self.to_fault = {}
        # keys that have ever faulted across all loops
        self.has_faulted_ever = set()

        self.last_key = None
        use_runplan = False
        complete = False

        sys.excepthook = sys.unraisablehook = self.exchook
        while not complete:
            # exceptions that happened this loop
            self.exc_happened = []
            # exceptions we expected to happen this loop
            self.expect_exception = []
            # keys we faulted this round
            self.faulted_this_round = []

            if use_runplan:
                if len(self.to_fault) == 0:
                    complete = True
                    self.runplan = "sqlite3_shutdown"
                else:
                    for k, v in self.to_fault.items():
                        self.runplan = v + [k]
                        break
            else:
                self.runplan = None

            self.last_exc = None  # it is ok to see this line when faulting apsw_write_unraisable (comes from PyErr_Print)
            with self:
                try:
                    if complete:
                        # we do this at the very end
                        import apsw
                        apsw.shutdown()
                    else:
                        exercise(self.example_code, self.expect_exception)
                        if not use_runplan and not self.faulted_this_round:
                            use_runplan = True
                            print("\nExercising locations that require multiple failures\n")
                            continue
                finally:
                    if "apsw" in sys.modules:
                        for c in sys.modules["apsw"].connections():
                            c.close()
                    gc.collect()

            self.verify_exception(self.faulted_this_round)

        if complete:
            print("\nAll faults exercised")

        print(f"Total faults: { len(self.has_faulted_ever) }")

        if self.to_fault:
            t = f"Failed to fault { len(self.to_fault) }"
            print("=" * len(t))
            print(t)
            print("=" * len(t))
            for f in sorted(self.to_fault):
                print(f)
            sys.exit(1)

        sys.excepthook = sys.__excepthook__
        sys.unraisablehook = sys.__unraisablehook__


t = Tester()
t.run()
