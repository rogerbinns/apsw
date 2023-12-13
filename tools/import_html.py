#!/usr/bin/env python3

from __future__ import annotations


import zipfile
import pathlib

import apsw
import apsw.bestpractice

apsw.bestpractice.apply(apsw.bestpractice.recommended)


def get_zip_content(filename: str):
    with zipfile.ZipFile(filename) as zf:
        for member in zf.infolist():
            if pathlib.PurePosixPath(member.filename).suffix.lower() in {".html", ".htm"}:
                yield member.filename, zf.read(member).decode("utf8")


def do_import(filename: str, con: apsw.Connection, table_name: str) -> None:
    with con:
        con.execute(f"drop table if exists { table_name }")
        con.execute(f"create table { table_name }(filename, content)")
        con.executemany(f"insert into { table_name } values(?,?)", get_zip_content(filename))


if __name__ == "__main__":
    import argparse

    p = argparse.ArgumentParser(description="Imports zip archive .html contents into a SQLite table")
    p.add_argument("filename", help="Name of the zip source file")
    p.add_argument("database", help="Name of the SQLite database.  It will be created if it doesn't exist")
    p.add_argument("tablename", help="Table name.  It will be replaced if it already exists.")

    options = p.parse_args()

    do_import(options.filename, apsw.Connection(options.database), options.tablename)
