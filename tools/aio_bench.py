#!/usr/bin/env python3

"""
A crude benchmark comparing apsw and aiosqlite performance, especially
around iteration
"""

import apsw
import apsw.aio
import aiosqlite
import asyncio
import time
import contextlib
import resource
import sqlite3

setup = """
create table foo(one, two, three, four, five);
insert into foo values(1, 2, 3, 4, 5);
insert into foo values(5, 4, 3, 2, 1);
""".split(";")

insert = """
insert into foo values(?,?,?,?,?)
"""

source = """select * from foo limit 1000"""

length = "select count(*) from foo"


async def apsw_bench(prefetch: int):
    async with contextlib.aclosing(await apsw.Connection.as_async(apsw.aio.AsyncIO(), ":memory:")) as con:
        apsw.async_cursor_prefetch.set(prefetch)

        for sql in setup:
            async with await con.execute(sql):
                pass

        while True:
            async with await con.execute(length) as cursor:
                async for (count,) in cursor:
                    pass
            if count > 100_000:
                break

            data = []
            async with await con.execute(source) as cursor:
                async for row in cursor:
                    data.append(row)

            async with await con.executemany(insert, data):
                pass


async def sqlite3_bench(prefetch: int):
    async with aiosqlite.connect(":memory:", iter_chunk_size=prefetch) as con:
        for sql in setup:
            async with con.execute(sql):
                pass

        while True:
            async with con.execute(length) as cursor:
                async for (count,) in cursor:
                    pass
            if count > 100_000:
                break

            data = []
            async with con.execute(source) as cursor:
                async for row in cursor:
                    data.append(row)

            async with con.executemany(insert, data) as cursor:
                pass


def run_apsw(prefetch: int):
    b4 = time.monotonic(), time.process_time(), resource.getrusage(resource.RUSAGE_THREAD).ru_utime
    asyncio.run(apsw_bench(prefetch))
    after = time.monotonic(), time.process_time(), resource.getrusage(resource.RUSAGE_THREAD).ru_utime
    print(f"apsw {prefetch=}")
    show_times(b4, after)


def run_aiosqlite3(prefetch: int):
    b4 = time.monotonic(), time.process_time(), resource.getrusage(resource.RUSAGE_THREAD).ru_utime
    asyncio.run(sqlite3_bench(prefetch))
    after = time.monotonic(), time.process_time(), resource.getrusage(resource.RUSAGE_THREAD).ru_utime
    print(f"aiosqlite {prefetch=}")
    show_times(b4, after)


def show_times(one, two):
    print(f"""\
            Wall clock: {two[0] - one[0]:7.3f}
         CPU (process): {two[1] - one[1]:7.3f}
CPU (main thread only): {two[2] - one[2]:7.3f}""")


print(f"""\
   APSW SQLite version: {apsw.sqlite_lib_version()}
sqlite3 SQLite version: {sqlite3.sqlite_version}

""")

for prefetch in (1, 10, 20, 50, 100):
    run_aiosqlite3(prefetch)
    run_apsw(prefetch)
    print()
