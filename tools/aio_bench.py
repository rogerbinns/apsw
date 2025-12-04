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
import trio
import anyio

try:
    import uvloop
except ImportError:
    uvloop = None

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


def get_times():
    return time.monotonic(), time.process_time(), resource.getrusage(resource.RUSAGE_THREAD).ru_utime


async def apsw_bench(prefetch: int):
    async with contextlib.aclosing(await apsw.Connection.as_async(":memory:")) as con:
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

            async with con.executemany(insert, data):
                pass


print(f"""\
   APSW SQLite version: {apsw.sqlite_lib_version()}
sqlite3 SQLite version: {sqlite3.sqlite_version}

""")

print(f"{'Library':>25s} {'Prefetch':>10s} {'Wall':>10s} {'CpuProcess':>15s} {'CpuMainThread':>15s}")


def show(library, prefetch, start, end):
    print(
        f"{library:>25s} {prefetch:>10} {end[0] - start[0]:10.3f} {end[1] - start[1]:15.3f} {end[2] - start[2]:15.3f}"
    )


for prefetch in (1, 2, 16, 64, 512, 1_000_000):
    for name in ["AsyncIO",  "Trio", "AnyIO/asyncio", "AnyIO/trio"]:
        with apsw.aio.contextvar_set(apsw.async_controller, getattr(apsw.aio, name.split("/")[0])):
            for loop_factory in [False] + ([True] if uvloop else []):
                used_loop_factory = False
                start = get_times()
                match name:
                    case "AsyncIO":
                        asyncio.run(apsw_bench(prefetch), loop_factory=uvloop.new_event_loop if loop_factory else None)
                        used_loop_factory = True
                    case "Trio":
                        trio.run(apsw_bench, prefetch)
                    case "AnyIO/asyncio":
                        anyio.run(apsw_bench, prefetch, backend="asyncio", backend_options={"uvloop": loop_factory})
                        used_loop_factory = True
                    case "AnyIO/trio":
                        anyio.run(apsw_bench, prefetch, backend="trio")
                end = get_times()
                show(f"apsw {name}{' uvloop' if loop_factory else ''}", prefetch, start, end)
                if not used_loop_factory:
                    break
    for loop_factory in [False] + ([True] if uvloop else []):
        start = get_times()
        asyncio.run(sqlite3_bench(prefetch), loop_factory=uvloop.new_event_loop if loop_factory else None)
        end = get_times()
        show(f"aiosqlite{' uvloop' if loop_factory else ''}", prefetch, start, end)
