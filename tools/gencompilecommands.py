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
            "-DAPSW_USE_SQLITE_AMALGAMATION",
            "-DAPSW_USE_SQLITE_CFG_H",
            "-Isqlite3",
            "-Isrc",
            "-DSQLITE_ENABLE_FTS5",
        ]
    )

    out = []
    for f in glob.glob("src/*.c"):
        main_file = "src/unicode.c" if "unicode" in f else "src/apsw.c"
        out.append({"directory": os.getcwd(), "file": f, "arguments": cmd + ["-c", main_file]})

    print(json.dumps(out, indent=2))


if __name__ == "__main__":
    import argparse

    p = argparse.ArgumentParser(description="Generates a compile_commands.json using the current Python")

    options = p.parse_args()

    generate(options)
