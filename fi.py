import sys
import pprint
import enum

has_faulted = set()

to_fault = set()


class ReturnCode(enum.IntEnum):
    "Encapsulates the magic return values from apsw_fault_inject_control"

    Proceed = 0x1FACADE
    "keep going changing nothing"

    ProceedClearException = 0x2FACADE
    "clear exception, keep going"

    ProceedAndCallBack = 0x3FACADE
    "keep going, changing nothing, call with result"

    ProceedClearExceptionAndCallBack = 0x4FACADE
    "clear exception, keep going, call with result"


def called(is_call, fault_function, callid, call_location, exc_info, retval):
    if False:
        d = {
            "is_call": is_call,
            "callid": callid,
            "fault_function": fault_function,
            "call_location": {
                "filename": call_location[0],
                "funcname": call_location[1],
                "linenum": call_location[2],
                "strargs": call_location[3],
            },
            "exc_info": {
                "exc_type": exc_info[0],
                "exc_value": exc_info[1],
                "exc_traceback": exc_info[2],
            },
            "retval": retval
        }
        pprint.pprint(d, compact=True)

    key = (fault_function, call_location)
    if is_call:
        if key in has_faulted:
            return ReturnCode.Proceed
        else:
            if fault_function in ("PySet_New", ):
                has_faulted.add(key)
                raise MemoryError()
        return ReturnCode.ProceedAndCallBack
    if fault_function in ("PySet_New", ):
        breakpoint()
        fault = retval is None or all(e is not None for e in exc_info)
        if fault:
            has_faulted.add(key)
            to_fault.remove(key)
    else:
        assert False, f"unknown { fault_function }"
    return None


sys.apsw_fault_inject_control = called
sys.apsw_should_fault = lambda *args: False


def exercise():
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
                return True

            def Open(self):
                return Source.Cursor()

        class Cursor:

            def Filter(self, *args):
                self.pos = 0

            def Eof(self):
                return self.pos >= 7

            def Column(self, n):
                return self.pos

            def Next(self):
                self.pos += 1

    con.createmodule("vtable", Source(), use_bestindex_object=True, iVersion=3, eponymous=True)

    con.execute("select * from vtable where c2>2 and c1 in (1,2,3)")

    # we reached the end
    return True


last = None
complete = False
while not complete:
    print("remaining", len(to_fault), "done", len(has_faulted))
    try:
        complete = exercise()
    except Exception:
        complete = False
    now = set(to_fault), set(has_faulted)
    if now == last and len(to_fault):
        print("Unable to make progress")
        exercise()
    else:
        last = now

print("Complete")

for n in sorted(has_faulted):
    print(n)