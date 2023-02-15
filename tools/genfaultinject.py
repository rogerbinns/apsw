#!/usr/bin/python

proto = """
typedef enum
{
    FICProceed = 7,
    FICProceed_And_Call_With_Result = 8,
    FICReturnThis = 9,
} FaultInjectControlVerb;

static FaultInjectControlVerb
APSW_FaultInjectControl(int is_call, const char *faultfunction, const char *filename, const char *funcname, int linenum, const char *args, PyObject **obj);
"""

pyobject_return = """
({
    PyObject *_res = 0;
    PyGILState_STATE gilstate = PyGILState_Ensure();
    switch (APSW_FaultInjectControl(1, "PySet_New", __FILE__, __func__, __LINE__, #__VA_ARGS__, &_res))
    {
    case FICProceed:
        assert(_res == 0);
        _res = PySet_New(__VA_ARGS__);
        break;
    case FICProceed_And_Call_With_Result:
        assert(_res == 0);
        _res = PySet_New(__VA_ARGS__);
        APSW_FaultInjectControl(0, "PySet_New", __FILE__, __func__, __LINE__, #__VA_ARGS__, &_res);
        /* fallthrough  */
    case FICReturnThis:
        assert(_res || PyErr_Occurred());
        assert(!(_res && PyErr_Occurred()));
        break;
    }
    PyGILState_Release(gilstate);
    _res;
})
"""


def get_definition(s):
    if s in returns["pyobject"]:
        t = pyobject_return.replace("PySet_New", s)
    else:
        print("unknown template " + s)
        breakpoint()
        1 / 0
    t = t.strip().split("\n")
    maxlen = max(len(l) for l in t)
    for i in range(len(t)):
        t[i] += " " * (maxlen - len(t[i])) + " \\\n"
    return "".join(t)


def genfile(symbols):
    res = []
    res.append(f"""
#ifndef APSW_FAULT_INJECT_INCLUDED
{ proto }
#define APSW_FAULT_INJECT_INCLUDED
#endif

#ifdef APSW_FAULT_INJECT_OFF
""")
    for s in symbols:
        res.append(f"#undef { s }")

    res.append("""
#undef APSW_FAULT_INJECT_OFF
#else
""")
    for s in symbols:
        res.append(f"#define {s}(...) \\\n{ get_definition(s) }")

    res.append("#endif\n")

    return "\n".join(res)


returns = {"pyobject": "PySet_New convert_value_to_pyobject".split()}

if __name__ == '__main__':
    import sys
    all = set()
    for v in returns.values():
        all.update(v)
    r = genfile(all)
    open(sys.argv[1], "wt").write(r)