#!/usr/bin/env python3

# This code uses Python's optional typing annotations.  You can
# ignore them and do not need to use them.  If you do use them
# then you must include this future annotations line first.
from __future__ import annotations

import asyncio
import contextlib

from pprint import pprint

# trio and anyio also work
import trio
import anyio

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


asyncio.run(basics())

### async_callbacks: Functions and callbacks
# Any function/callback can be sync or async.


# an async scalar function
async def a_add(one, two):
    return one + two


# and a sync scalar function
def s_add(one, two):
    return one + two


# how about an async update hook?
async def my_hook(op: int, dbname: str, table: str, rowid: int):
    print(f"update {op=} {dbname=} {table=} {rowid=}")


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

        # A regular callback
        await db.set_update_hook(my_hook)

        await db.execute(
            "CREATE TABLE x(y); INSERT INTO x VALUES(42)"
        )

# use anyio this time
anyio.run(callbacks)

### async_cancellation: Cancellation
# Often a group of tasks are run at the same time.  The frameworks
# provide a way to group tasks, wait until all are complete, and if
# any fail, then cancel uncompleted ones, and raise the resulting
# exceptions.
#
# * :external+python:label:`except * <except_star>` and
#   :class:`ExceptionGroup` Python syntax for catching multiple
#   exceptions such as from a group of tasks
# * :class:`asyncio.TaskGroup`
# * :external+trio:label:`Trio tasks <tasks>`
# * :external+anyio:doc:`AnyIO tasks <tasks>`
#
# This example shows asyncio, but the principles are the same across
# all the frameworks.

async def cancellation():

    # this will block SQLite for the sleep duration
    async def sleep(duration):
        await asyncio.sleep(duration)
        return duration

    async def deliberate_error():
        1 / 0
        return 3


    async def wrap(f):
        # create_task only accepts async def functions
        return await f

    db = await apsw.Connection.as_async(":memory:")

    # always close database
    async with contextlib.aclosing(db):

        await db.create_scalar_function("sleep", sleep)

        # create some tasks in a task group
        try:
            async with asyncio.TaskGroup() as tg:

                    task1 = tg.create_task(wrap(db.execute("SELECT sleep(0.1)")))

                    task2 = asyncio.sleep(1)
                    task3 = db.execute("SELECT * FROM sqlite_schema")
                    task4 = deliberate_error()

                    # this one has the error
                    await task4
        except * ZeroDivisionError:
            print("got zero division error")

        await task2

        print(f"{task1.cancelled()=} {task1.done()=}")
        print(f"{task3.cancelled()=} {task3.done()=}")

        await task1

asyncio.run(cancellation())

### async_todo: TODO TODO TODO
# * timeout
# * async with db transaction
# * running things in worker thread
# * virtual tables
# * ext trace & showresourceusage
# * blob
# * backup
# * session

pass

### async_cleanup: Cleanup
# No cleanup needed.  You can use sync and async connections at the
# same time without limitation.
