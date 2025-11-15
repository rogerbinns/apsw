#!/usr/bin/env python3

import os
import json
import sysconfig
import shlex
import glob


def generate(options):
    def v(name: str) -> str:
        return sysconfig.get_config_var(name)

    def p(name: str) -> str:
        return sysconfig.get_path(name)

    # Python values
    cmd = [v("CC")]
    for n in "CFLAGS", "CCSHARED":
        cmd.extend(shlex.split(v(n)))
    cmd.extend(("-I", p("include")))

    # Our values
    cmd.extend(
        [
            "-DAPSW_USE_SQLITE_CFG_H",
            "-Isqlite3",
            "-Isrc",
            "-DSQLITE_ENABLE_FTS5",
            "-DSQLITE_ENABLE_SESSION",
            "-DSQLITE_ENABLE_CARRAY",
            "-UNDEBUG",
        ]
    )

    out = []
    for f in glob.glob("src/*.c"):
        match f:
            case "src/unicode.c" | "src/_unicodedb.c":
                comp_file = "src/unicode.c"
            case _:
                comp_file = "src/apsw.c"

        out.append({"directory": os.getcwd(), "file": f, "arguments": cmd + ["-c", comp_file]})


    print(json.dumps(out, indent=2, sort_keys=True))


if __name__ == "__main__":
    import argparse

    p = argparse.ArgumentParser(description="Generates a compile_commands.json using the current Python")

    options = p.parse_args()

    generate(options)
