#!/usr/bin/env python3

"""
A crude benchmark comparing apsw and aiosqlite performance, especially
around iteration
"""

import asyncio
import contextlib
import resource
import sqlite3
import time

import aiosqlite
import anyio
import trio

import apsw
import apsw.aio

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
    with apsw.aio.contextvar_set(apsw.async_cursor_prefetch, prefetch):

        # APSW connection async with does transaction control
        async with contextlib.aclosing(await apsw.Connection.as_async(":memory:")) as con:
            start = get_times()

            await con.execute(setup)

            while True:
                count =  (await (await con.execute(length)).fetchall())[0][0]
                if count > 300_000:
                    break

                data = []
                async for row in await con.execute(source):
                        data.append(row)

                await con.executemany(insert, data)
            return start, get_times()


async def aiosqlite_bench(prefetch: int):
    # aiosqlite async with closes on exit
    async with aiosqlite.connect(":memory:", iter_chunk_size=prefetch) as con:
        start = get_times()

        await con.executescript(setup)

        while True:
            count = (await (await con.execute(length)).fetchall())[0][0]

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

        return start, get_times()


# sqlite3.sqlite_version is what aiosqlite is using.  this is to
# confirm they are using the same sqlite library/version
print(f"""\
   APSW SQLite version: {apsw.sqlite_lib_version()}
sqlite3 SQLite version: {sqlite3.sqlite_version}

""")

print(f"{'Library':>25s} {'Prefetch':>8s} {'Wall':>8s} {'CpuTotal':>10s} {'CpuEvtLoop':>12s} {'CpuDbWorker':>12s}")


def show(library, prefetch, start, end):
    wall = end[0] - start[0]
    cpu_total = end[1]-start[1]
    cpu_async = end[2] - start[2]
    cpu_worker = cpu_total - cpu_async
    print(
        f"{library:>25s} {prefetch:>8,} {wall:8.3f} {cpu_total:10.3f} {cpu_async:>12.3f} {cpu_worker:>12.3f}"
    )

modes = ["AsyncIO"]
if uvloop:
    modes.append("AsyncIO uvloop")
modes.append("Trio")
modes.append("AnyIO asyncio")
if uvloop:
    modes.append("AnyIO asyncio uvloop")
modes.append("AnyIO trio")
modes.append("aiosqlite")
if uvloop:
    modes.append("aiosqlite uvloop")

for prefetch in (1, 2, 16, 64, 512, 8192, 65536):
    for mode in modes:
        match mode:
            case "AsyncIO":
                with apsw.aio.contextvar_set(apsw.async_controller, apsw.aio.AsyncIO):
                    start, end = asyncio.run(apsw_bench(prefetch))
            case "AsyncIO uvloop":
                with apsw.aio.contextvar_set(apsw.async_controller, apsw.aio.AsyncIO):
                    start, end = asyncio.run(apsw_bench(prefetch), loop_factory=uvloop.new_event_loop)
            case "Trio":
                with apsw.aio.contextvar_set(apsw.async_controller, apsw.aio.Trio):
                    start, end = trio.run(apsw_bench, prefetch)
            case "AnyIO asyncio":
                with apsw.aio.contextvar_set(apsw.async_controller, apsw.aio.AnyIO):
                    start, end = anyio.run(apsw_bench, prefetch, backend="asyncio")
            case "AnyIO asyncio uvloop":
                with apsw.aio.contextvar_set(apsw.async_controller, apsw.aio.AnyIO):
                    start, end = anyio.run(apsw_bench, prefetch, backend="asyncio", backend_options={"use_uvloop": True})
            case "AnyIO trio":
                with apsw.aio.contextvar_set(apsw.async_controller, apsw.aio.AnyIO):
                    start, end = anyio.run(apsw_bench, prefetch, backend="trio")
            case "aiosqlite":
                start, end = asyncio.run(aiosqlite_bench(prefetch))
            case "aiosqlite uvloop":
                start, end = asyncio.run(aiosqlite_bench(prefetch), loop_factory=uvloop.new_event_loop)
            case _:
                raise Exception(f"Unhandled {mode=}")
        show(f"{'apsw ' if 'aiosqlite' not in mode else ''}{mode}", prefetch, start, end)

