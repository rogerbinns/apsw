# python3
#
# See the accompanying LICENSE file.
#
# Work out how much coverage we actually have across the various source files

import glob
import os


def output(filename: str, executed: int, total: int) -> None:
    # Python bug "% 3.2f" doesn't behave correctly (100.00 is formatted with leading space!)
    percent = 100 * executed / max(total, 1)
    op = [f"{filename:40}", f"{executed:6,}", "  /", f"{total:6,}", "\t    ", "% 3.2f%%" % (percent,)]
    if percent == 100:
        op[-1] = "100.00%"
    print("".join(op))


lines_executed = 0
lines_total = 0

names = glob.glob("src/*.c.gcov")
names.sort()

for f in names:
    if f.startswith("sqlite3") or f.endswith("faultinject.c.gcov") or f.endswith("_unicodedb.c.gcov"):
        continue
    file_exec = 0
    file_total = 0

    in_test_fixture = False
    with open(f, "rt") as fd:
        for line in fd:
            if ":" not in line:
                continue
            count, linenum, line = line.split(":", 2)
            line = line.strip()
            if in_test_fixture:
                if line.startswith("#else") or line.startswith("#endif"):
                    in_test_fixture = False
                continue
            if line == "#ifdef APSW_FAULT_INJECT":
                in_test_fixture = True
                continue
            count = count.strip()
            if count == "-":
                continue
            if count != "#####":
                lines_executed += 1
                file_exec += 1
            lines_total += 1
            file_total += 1
    n = os.path.splitext(f)[0]
    output(n, file_exec, file_total)

print("\n")
output("Total", lines_executed, lines_total)
