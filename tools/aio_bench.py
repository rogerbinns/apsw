#!/usr/bin/env python3

"""
A crude benchmark comparing apsw and aiosqlite performance, especially
around iteration
"""

import apsw
import aiosqlite
import asyncio
import time
import contextlib
import resource
import sqlite3
import trio

try:
    import uvloop
except ImportError:
    uvloop = None

setup = """
create table foo(one, two, three, four, five);
insert into foo values(1, 2, 3, 4, 5);
insert into foo values(5, 4, 3, 2, 1);
"""

insert = """
insert into foo values(?,?,?,?,?)
"""

source = """select * from foo limit 1000"""

length = "select count(*) from foo"


def get_times():
    return time.monotonic(), time.process_time(), resource.getrusage(resource.RUSAGE_THREAD).ru_utime


async def apsw_bench(prefetch: int):
    # APSW connection with does transaction control
    async with contextlib.aclosing(await apsw.Connection.as_async(":memory:")) as con:
        apsw.async_cursor_prefetch.set(prefetch)

        await con.execute(setup)

        while True:
            count =  await (await con.execute(length)).get
            if count > 300_000:
                break

            data = []
            async for row in await con.execute(source):
                    data.append(row)

            await con.executemany(insert, data)


async def sqlite3_bench(prefetch: int):
    # aiosqlite async with closes on exit
    async with aiosqlite.connect(":memory:", iter_chunk_size=prefetch) as con:

        await con.executescript(setup)

        while True:
            count = (await con.execute_fetchall(length))[0][0]

            if count > 300_000:
                break

            data = []
            # the async with is needed to close the cursor
            # on completion.  it has to be handled in the correct
            # thread - apsw has no such limitation.
            async with con.execute(source) as cursor:
                async for row in cursor:
                    data.append(row)

            await con.executemany(insert, data)


print(f"""\
   APSW SQLite version: {apsw.sqlite_lib_version()}
sqlite3 SQLite version: {sqlite3.sqlite_version}

""")

print(f"{'Library':>25s} {'Prefetch':>8s} {'Wall':>6s} {'CpuTotal':>10s} {'CpuLoop':>10s} {'CpuWorker':>10s}")


def show(library, prefetch, start, end):
    wall = end[0] - start[0]
    cpu_total = end[1]-start[1]
    cpu_async = end[2] - start[2]
    cpu_worker = cpu_total - cpu_async
    print(
        f"{library:>25s} {prefetch:>8} {wall:6.3f} {cpu_total:10.3f} {cpu_async:>10.3f} {cpu_worker:>10.3f}"
    )


for prefetch in (1, 2, 16, 64, 512, 8192, 65536):
    for name in ["AsyncIO",  "Trio"]:
        for loop_factory in [False] + ([True] if uvloop else []):
            used_loop_factory = False
            start = get_times()
            match name:
                case "AsyncIO":
                    asyncio.run(apsw_bench(prefetch), loop_factory=uvloop.new_event_loop if loop_factory else None)
                    used_loop_factory = True
                case "Trio":
                    trio.run(apsw_bench, prefetch)
            end = get_times()
            show(f"apsw {name}{' uvloop' if loop_factory else ''}", prefetch, start, end)
            if not used_loop_factory:
                break
    for loop_factory in [False] + ([True] if uvloop else []):
        start = get_times()
        asyncio.run(sqlite3_bench(prefetch), loop_factory=uvloop.new_event_loop if loop_factory else None)
        end = get_times()
        show(f"aiosqlite{' uvloop' if loop_factory else ''}", prefetch, start, end)
