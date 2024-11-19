#!/usr/bin/env python3

import sys
import os
import ctypes

if len(sys.argv) != 2 or not os.path.isfile(sys.argv[1]):
    sys.exit("Expected filename of sqlite3 shared library/dll")

lib = ctypes.cdll.LoadLibrary(sys.argv[1])

func = lib.sqlite3_compileoption_get
func.argtypes = [ctypes.c_int]
func.restype = ctypes.c_char_p

i = 0
while True:
    s = func(i)
    if not s:
        break
    s = "SQLITE_" + s.decode("utf8")
    s = s.split("=", 1)
    if len(s) == 1:
        s.append("")
    print(f"#undef  { s[0] }")
    print(f"#define { s[0] } { s[1] }")
    i += 1
