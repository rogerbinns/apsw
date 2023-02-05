# python3
#
# See the accompanying LICENSE file.
#
# Work out how much coverage we actually have

import glob
import os


def output(filename: str, executed: int, total: int) -> None:
    # Python bug "% 3.2f" doesn't behave correctly (100.00 is formatted with leading space!)
    percent = 100 * executed / max(total, 1)
    op = [f"{filename:40}", f"{executed:6,}", "  /", f"{total:6,}", "\t    ", "% 3.2f%%" % (percent, )]
    if percent == 100:
        op[-1] = "100.00%"
    print("".join(op))


lines_executed = 0
lines_total = 0

names = glob.glob("src/*.c.gcov")
names.sort()

for f in names:
    if f.startswith("sqlite3"):
        continue
    file_exec = 0
    file_total = 0
    for line in open(f, "rt"):
        if ":" not in line: continue
        line = line.split(":", 1)[0].strip()
        if line == "-":
            continue
        if line != "#####":
            lines_executed += 1
            file_exec += 1
        lines_total += 1
        file_total += 1
    n = os.path.splitext(f)[0]
    output(n, file_exec, file_total)

print("\n")
output("Total", lines_executed, lines_total)
