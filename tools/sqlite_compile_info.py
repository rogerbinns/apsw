#!/usr/bin/env python3

import os

import sqlite3
import apsw


def figure_out_libsqlite3() -> str | None:
    "Works out what libsqlite3 is dynamically linked into this process"
    maps = f"/proc/{ os.getpid() }/maps"
    if not os.path.exists(maps):
        return None
    for line in open(maps, "rt"):
        # not every line has a filename
        filename = line.split()[-1]
        if "/libsqlite3.so" in filename:
            return filename
    return None


def transmogrify(compile_options: list[str]) -> list[tuple[str, None | int | str]]:
    res: list[tuple[str, None | int | str]] = []
    for co in compile_options:
        cos = co.split("=")
        assert len(cos) in {1, 2}
        if len(cos) == 1:
            res.append((cos[0], None))
            continue
        if cos[1].isdigit():
            cos[1] = int(cos[1])
        res.append(tuple(cos))
    return res


def get_differences(
    left: list[tuple[str, None | int | str]], left_name: str, right: list[tuple[str, None | int | str]], right_name: str
) -> list[tuple[str, str]]:
    res: list[tuple[str, str]] = []

    def fmt(v):
        if v[1] is not None:
            return f"{ v[0] }={ v[1] }"
        return v[0]

    li = ri = 0

    while li < len(left) or ri < len(right):
        try:
            l = left[li]
            r = right[ri]
        except IndexError:
            break
        if l == r:
            li += 1
            ri += 1
            continue
        if l[0] == r[0]:
            assert l[1] != r[1]
            res.append((f"difference { l[0] }", f"{ left_name } = { l[1] }  :   { right_name } = { r[1]}"))
            li += 1
            ri += 1
            continue
        if l[0] < r[0]:
            res.append((f"Only in { left_name }", fmt(l)))
            li += 1
            continue
        else:
            assert l[0] > r[0]
            res.append((f"Only in { right_name }", fmt(r)))
            ri += 1
            continue

    for l in left[li:]:
        res.append((f"Only in { left_name }", fmt(l)))
    for r in right[ri:]:
        res.append((f"Only in { right_name }", fmt(r)))

    return res


def main(options) -> None:
    out: list[tuple[str, str]] = []
    so = figure_out_libsqlite3()
    if so:
        out.append(("Dynamically loaded", so))
    out.append(("py sqlite3 version", sqlite3.sqlite_version))
    out.append(("apsw sqlite3 version", apsw.sqlite_lib_version()))
    out.append(("apsw amalgamation", apsw.using_amalgamation))

    pyco = sorted(transmogrify([row[0] for row in sqlite3.connect("").execute("pragma compile_options")]))
    apco = sorted(transmogrify(apsw.Connection("").pragma("compile_options")))

    out.extend(get_differences(pyco, "pysqlite3", apco, "apsw"))

    w = max(len(k) for k, _ in out)

    for k, v in out:
        print(f"{ k:>{ w }} ", v)


if __name__ == "__main__":
    import argparse

    p = argparse.ArgumentParser(
        description="Examines compilation options used for SQLite between "
        "APSW and sqlite3 module (usually with system sqlite library)"
    )

    options = p.parse_args()

    main(options)
