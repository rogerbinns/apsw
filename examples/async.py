#!/usr/bin/env python3

# This code uses Python's optional typing annotations.  You can
# ignore them and do not need to use them.  If you do use them
# then you must include this future annotations line first.
from __future__ import annotations

import asyncio
import contextlib

from pprint import pprint

import apsw
import apsw.ext
import apsw.bestpractice


### async_basics: Basics
# Use :meth:`Connection.as_async` to get an async connection, and
# async for to iterate results.  We also apply :doc:`best practice
# <bestpractice>` and use :func:`contextlib.aclosing` to ensure the
# database is closed while the event loop is still running.

apsw.bestpractice.apply(apsw.bestpractice.recommended)


async def basics():
    db = await apsw.Connection.as_async(":memory:")

    # always close database
    async with contextlib.aclosing(db):
        # create some rows
        await db.execute("""
            CREATE TABLE numbers(value, name);
            INSERT INTO numbers VALUES (1, 'one'), (20, 'twenty'),
                         (3, 'three'), (10, 'ten');
        """)

        # query - note we have to await the cursor
        async for value, name in await db.execute(
            "SELECT value, name FROM numbers ORDER BY value DESC"
        ):
            print(f"{value=} {name=}")


# Run with asyncio. trio and anyio work too.
asyncio.run(basics())

### async_callbacks: Functions and callbacks
# Any function/callback can be sync or async.


# an async version
async def a_add(one, two):
    return one + two


# and a sync version
def s_add(one, two):
    return one + two


# how about an async update hook?
async def my_hook(op: int, dbname: str, table: str, rowid: int):
    print("update {op=} {dbname=} {table=} {rowid=}")


async def callbacks():
    db = await apsw.Connection.as_async(":memory:")

    # always close database
    async with contextlib.aclosing(db):
        await db.create_scalar_function("a_add", a_add)
        await db.create_scalar_function("s_add", s_add)

        # use them both in the same query.  we need to await execute
        # to get the cursor, and then await the fetchall on the
        # cursor.
        print(
            await (
                await db.execute("SELECT a_add(1, 2), s_add(3, 4)")
            ).fetchall()
        )

        await db.set_update_hook(my_hook)


asyncio.run(callbacks())

### async_cleanup: Cleanup
# No cleanup needed.  You can use sync and async connections at the
# same time without limitation.
