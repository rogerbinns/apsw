#!/usr/bin/env python3

from __future__ import annotations

from typing import Generator

import tarfile
import email.parser
import email.policy
import re


import apsw
import apsw.bestpractice

apsw.bestpractice.apply(apsw.bestpractice.recommended)


def get_content(name: str, total: int) -> Generator[tuple[str, str, str], None, None]:
    count = 0
    if total:
        print("0%", end="", flush=True)
        nextp = 1
    with tarfile.open(name) as tf:
        while (member := tf.next()) is not None:
            if not member.isfile():
                continue
            # we could just split on the first \r\n\r\n which will
            # separate headers from body.  but that is working on
            # bytes and we really want correct unicode, so the email
            # package is used to do the bytes -> text conversion.
            msg = email.parser.BytesParser(policy=email.policy.default).parse(tf.extractfile(member))
            body = None
            headers = None
            for part in msg.walk():
                if part.get_content_type() == "text/plain":
                    body = part.get_payload()
                    headers = ""
                    for h in part.keys():
                        try:
                            headers += h + ": " + " ".join(part.get_all(h, [])) + "\n"
                        except TypeError:
                            # about 20 messages causes this exception deep in the email package
                            continue
                    break
            assert (body is None and headers is None) or (body is not None and headers is not None)
            if not body or not headers:
                continue
            # strip base 64 from body
            if True:
                body = re.sub(r"\s[abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789+/]{76}\s", "", body)
            yield member.name, headers, body
            count += 1
            if total and (percent := int(count * 100 / total)) >= nextp:
                if percent % 10 == 0:
                    print(f"{ percent }%", end="", flush=True)
                else:
                    print(".", end="", flush=True)
                nextp += 1
    if total:
        print("100%")


def do_import(filename: str, con: apsw.Connection, table_name: str, progress: int) -> None:
    with con:
        con.execute(f"drop table if exists { table_name }")
        con.execute(f"create table { table_name }(filename, header, content)")
        con.executemany(f"insert into { table_name } values(?,?,?)", get_content(filename, progress))
    con.execute("vacuum")


if __name__ == "__main__":
    import argparse

    description = """\
Imports the enron email collection into a SQLite table.
It is useful for doing full text search on.

You can download the .tar.gz files from
https://www.cs.cmu.edu/~enron/

The table is created with three columns:

  filename inside the email collection
  headers
  body

There is a large amount of base64 encoded content quoted
in replies that is stripped.

About 2GB of text in half a million rows are added.
"""
    p = argparse.ArgumentParser(description=description, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument(
        "--no-progress", dest="show_progress", default="true", action="store_false", help="Do not show progress"
    )
    p.add_argument("filename", help="Name of the .tar.gz file")
    p.add_argument("database", help="Name of the SQLite database.  It will be created if it doesn't exist")
    p.add_argument("tablename", nargs="?", default="enron_email", help="Table name [%(default)s]")

    options = p.parse_args()
    do_import(
        options.filename, apsw.Connection(options.database), options.tablename, 517402 if options.show_progress else 0
    )
