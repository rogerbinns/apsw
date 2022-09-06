#!/usr/bin/env python3

import argparse
import ctypes
import ctypes.util


argp = argparse.ArgumentParser()
argp.add_argument("library",
                  help="Path or filename to sqlite3 shared library/dll "
                       "(as accepted by ctypes.cdll.LoadLibrary(), default: "
                       "use find_library())",
                  nargs="?")
args = argp.parse_args()

if args.library is None:
    args.library = ctypes.util.find_library("sqlite3")
    if args.library is None:
        argp.error("sqlite3 library not found, please pass library name/path")
lib = ctypes.cdll.LoadLibrary(args.library)

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
