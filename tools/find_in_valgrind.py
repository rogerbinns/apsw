#!/usr/bin/env python3

# Given the name of a valgrind output file this outputs chunks that
# mention source files from APSW.  This is necessary because many of
# the stack traces only contain code from Python
#
# A typical line looks like this:
#
# ==3321556==    by 0x1FD163: _PyEval_EvalFrameDefault (ceval.c:4772)
#
# We want to find lines that mention APSW source files only

import re
import sys
import os
import glob

src_dir: str = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src"))

src_files: list[str] = [f.split(os.sep)[-1] for f in glob.glob(os.path.join(src_dir, "*.c"))]

pattern = r"\((" + "|".join(src_files) + r"):[0-9]+\)"

section = r"==[0-9]+== [^\s]"


def process_file(name: str):
    cur_section = None
    cur_section_num = None
    has_output = False
    with open(name, "rt") as f:
        for num, line in enumerate(f, 1):
            if re.match(section, line):
                cur_section = line
                cur_section_num = num
                if has_output:
                    print()
                    has_output = False
                continue
            if re.search(pattern, line):
                if cur_section:
                    print(f"{ cur_section_num }:\t{ cur_section }", end="")
                    cur_section = None
                print(f"{ num }:\t{ line }", end="")
                has_output = True


if __name__ == '__main__':
    process_file(sys.argv[1])