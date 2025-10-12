# python3
#
# See the accompanying LICENSE file.
#
# Work out how much coverage we actually have across the various source files

from filecmp import cmp
import glob
import os


def output(filename: str, executed: int, total: int) -> None:
    # Python bug "% 3.2f" doesn't behave correctly (100.00 is formatted with leading space!)
    percent = 100 * executed / max(total, 1)
    op = [f"{filename:40}", f"{executed:6,}", "  /", f"{total:6,}", "\t    ", "% 4.2f%%" % (percent,)]
    if percent == 100:
        op[-1] = "100.00%"
    print("".join(op))


# we want sqlite3 first, then unicodedb, then unicode, then the rest
def priority(n):
    if "/sqlite3.c" in n:
        return 1
    if "/_unicodedb.c" in n:
        return 2
    if "/unicode.c" in n:
        return 3
    return 4


names = [fn for fn in glob.glob("src/*.c.gcov") if "faultinject.c" not in fn] + ["sqlite3/sqlite3.c.gcov"]
names.sort(key=lambda x: (priority(x), x))


lines_executed = lines_total = 0

lastp = None

for f in names:
    p = priority(f)
    if p == 4 and lastp == 3:
        lines_executed = lines_total = 0
    file_exec = set()
    file_total = set()

    in_test_fixture = False
    with open(f, "rt") as fd:
        for line in fd:
            if ":" not in line:
                continue
            try:
                count, linenum, line = line.split(":", 2)
            except ValueError:
                # gcc/gcov have been putting duplicate copies of
                # functions in the output with the function name
                # and a colon on a line by themselves
                continue
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
            linenum = int(linenum)
            if count != "#####":
                file_exec.add(linenum)
            file_total.add(linenum)
    n = os.path.splitext(f)[0]
    if lastp == 3:
        print("-" * 30 + "\n")
    output(n, len(file_exec), len(file_total))
    lines_executed += len(file_exec)
    lines_total = len(file_total)
    if p < 4:
        print()
    lastp = p

print("\n")
output("Total", lines_executed, lines_total)
