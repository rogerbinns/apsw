import sys
import pprint
import enum

has_faulted = set()

to_fault = set()


Proceed = 0x1FACADE
"magic value keep going (ie do not inject a return value)"


# should be same as in genfaultinject.py
returns = {
    "pyobject": "PySet_New convert_value_to_pyobject getfunctionargs PyModule_Create2 PyErr_NewExceptionWithDoc".split(),
    "no_gil": "sqlite3_threadsafe".split(),
    "with_gil": "PyType_Ready PyModule_AddObject PyModule_AddIntConstant PyLong_AsLong PyLong_AsLongLong".split(),
}


expect_exception = set()

FAULT = ZeroDivisionError, "Fault injection synthesized failure"


def FaultCall(key):
    try:
        if key[0] in returns["pyobject"]:
            expect_exception.add(MemoryError)
            raise MemoryError()
        if key[0] == "sqlite3_threadsafe":
            expect_exception.add(EnvironmentError)
            return 0
        if key[0].startswith("PyLong_As"):
            expect_exception.add(OverflowError)
            return (-1, OverflowError, FAULT[1])
        if key[0].startswith("Py"):
            # for ones returning -1 on error
            expect_exception.add(FAULT[0])
            return (-1, *FAULT)

    finally:
        to_fault.discard(key)
        has_faulted.add(key)

    print("Unhandled", key)
    breakpoint()


def called(key):
    if expect_exception:
        # already have faulted this round
        if key not in has_faulted:
            to_fault.add(key)
        return Proceed
    if key in has_faulted:
        return Proceed
    return FaultCall(key)


sys.apsw_fault_inject_control = called
sys.apsw_should_fault = lambda *args: False


def exercise():
    "This function exercises the code paths where we have fault injection"

    # The module is not imported outside because the init function has
    # several fault injection locations

    import apsw
    apsw.keywords

    import apsw.ext

    con = apsw.Connection("")

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

    con.execute("select * from vtable where c2>2 and c1 in (1,2,3)")
    con.execute("create virtual table fred using vtable()")
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


last = None
while True:
    print("remaining", len(to_fault), "done", len(has_faulted), end="             \r", flush=True)
    expect_exception = set()
    try:
        exercise()
        break
    except Exception as e:
        complete = False
        assert sys.exc_info(
        )[0] in expect_exception, f"Expected { type(e) }/{ sys.exc_info()[1] } to be in { expect_exception }"

    now = set(to_fault), set(has_faulted)
    if now == last and len(to_fault):
        print("Unable to make progress")
        exercise()
        break
    else:
        last = now

print("Complete                                    ")
assert not to_fault, "Remaining { to_fault }"


for n in sorted(has_faulted):
    print(n)