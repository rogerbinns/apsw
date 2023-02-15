
#ifndef APSW_FAULT_INJECT_INCLUDED

typedef enum
{
    FICProceed = 7,
    FICProceed_And_Call_With_Result = 8,
    FICReturnThis = 9,
} FaultInjectControlVerb;

static FaultInjectControlVerb
APSW_FaultInjectControl(int is_call, const char *faultfunction, const char *filename, const char *funcname, int linenum, const char *args, PyObject **obj);
#define APSW_FAULT_INJECT_INCLUDED
#endif

#ifdef APSW_FAULT_INJECT_OFF
#undef PySet_New
#undef convert_value_to_pyobject
#undef APSW_FAULT_INJECT_OFF
#else

#define PySet_New(...)                                                                                      \
    ({                                                                                                      \
        PyObject *_res = 0;                                                                                 \
        PyGILState_STATE gilstate = PyGILState_Ensure();                                                    \
        switch (APSW_FaultInjectControl(1, "PySet_New", __FILE__, __func__, __LINE__, #__VA_ARGS__, &_res)) \
        {                                                                                                   \
        case FICProceed:                                                                                    \
            assert(_res == 0);                                                                              \
            _res = PySet_New(__VA_ARGS__);                                                                  \
            break;                                                                                          \
        case FICProceed_And_Call_With_Result:                                                               \
            assert(_res == 0);                                                                              \
            _res = PySet_New(__VA_ARGS__);                                                                  \
            APSW_FaultInjectControl(0, "PySet_New", __FILE__, __func__, __LINE__, #__VA_ARGS__, &_res);     \
            /* fallthrough  */                                                                              \
        case FICReturnThis:                                                                                 \
            assert(_res || PyErr_Occurred());                                                               \
            assert(!(_res && PyErr_Occurred()));                                                            \
            break;                                                                                          \
        }                                                                                                   \
        PyGILState_Release(gilstate);                                                                       \
        _res;                                                                                               \
    })

#define convert_value_to_pyobject(...)                                                                                                     \
    ({                                                                                                                                     \
        PyObject *_res = 0;                                                                                                                \
        PyGILState_STATE gilstate = PyGILState_Ensure();                                                                                   \
        switch (APSW_FaultInjectControl(1, "convert_value_to_pyobject", __FILE__, __func__, __LINE__, #__VA_ARGS__, &_res))                \
        {                                                                                                                                  \
        case FICProceed:                                                                                                                   \
            assert(_res == 0);                                                                                                             \
            _res = convert_value_to_pyobject(__VA_ARGS__);                                                                                 \
            break;                                                                                                                         \
        case FICProceed_And_Call_With_Result:                                                                                              \
            assert(_res == 0);                                                                                                             \
            _res = convert_value_to_pyobject(__VA_ARGS__);                                                                                 \
            APSW_FaultInjectControl(0, "convert_value_to_pyobject", __FILE__, __func__, __LINE__, #__VA_ARGS__, &_res); /* fallthrough  */ \
        case FICReturnThis:                                                                                                                \
            assert(_res || PyErr_Occurred());                                                                                              \
            assert(!(_res && PyErr_Occurred()));                                                                                           \
            break;                                                                                                                         \
        }                                                                                                                                  \
        PyGILState_Release(gilstate);                                                                                                      \
        _res;                                                                                                                              \
    })
#endif