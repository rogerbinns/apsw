#!/usr/bin/env python3

# This testing code deliberately does nasty stuff so mypy isn't helpful
# mypy: ignore-errors
# type: ignore

import contextlib
import contextvars
import unittest

import apsw
import apsw.aio


#### ::TODO::  tests to add
#
# cancels
#
# inheritance of connection/cursor
#
# blob needs aenter / aexit
# backup: aclose
# cursor get/fetchall need to close the cursor (or maybe not?)


class Async(unittest.TestCase):
    async def asyncTearDown(self, coro):
        try:
            return await coro
        finally:
            for c in apsw.connections():
                await c.aclose()

    def tearDown(self):
        if apsw.connections():
            raise RuntimeError(f"Connections were left open {apsw.connections()=}")

    def testOverwrite(self):
        "make sure module contextvars can't be overwritten"
        for name in "async_controller", "async_run_coro", "async_cursor_prefetch":
            self.assertRaisesRegex(AttributeError, ".*Do not overwrite apsw.*context", setattr, apsw, name, 3)

    async def atestContextVars(self, fw):
        db = await apsw.Connection.as_async(":memory:")
        await db.create_scalar_function("sync_cvar", sync_get_cvar)
        await db.create_scalar_function("async_cvar", async_get_cvar)

        cvar_inside = contextvars.ContextVar("inside")
        with apsw.aio.contextvar_set(cvar_inside, "one"):
            self.assertEqual("one", await (await db.execute("select sync_cvar('inside')")).get)
            with apsw.aio.contextvar_set(cvar_inside, "two"):
                self.assertEqual("two", await (await db.execute("select async_cvar('inside')")).get)
                with apsw.aio.contextvar_set(cvar_outside, "three"):
                    self.assertEqual(
                        ("three", "two"),
                        await (await db.execute("select sync_cvar('outside'), async_cvar('inside')")).get,
                    )
                    self.assertEqual(
                        ("three", "two"),
                        await (await db.execute("select async_cvar('outside'), sync_cvar('inside')")).get,
                    )
                    self.assertEqual(
                        ("three", "two"),
                        await (await db.execute("select async_cvar('outside'), async_cvar('inside')")).get,
                    )
                    self.assertEqual(
                        ("three", "two"),
                        await (await db.execute("select sync_cvar('outside'), sync_cvar('inside')")).get,
                    )
            self.assertEqual("one", await (await db.execute("select async_cvar('inside')")).get)

        with self.assertRaises(LookupError):
            await (await db.execute("select async_cvar('inside')")).get

        with self.assertRaises(LookupError):
            await (await db.execute("select sync_cvar('inside')")).get

    async def atestClosing(self, fw):
        "check aclose can be called multiple times, even after object is closed"
        db = await apsw.Connection.as_async(":memory:")

        cursor = await db.execute("select 3")

        await cursor.aclose()
        await cursor.aclose()
        cursor.close()
        cursor.close()

        await db.aclose()
        await db.aclose()
        db.close()
        db.close()

        for obj in cursor, db:
            await obj.aclose()
            obj.close()

    async def atestIteration(self, fw):
        "cursor iteration corner cases"
        db = await apsw.Connection.as_async(":memory:")
        await db.execute("create table x(y)")
        async with db:
            await db.executemany("insert into x values(?)", ((x,) for x in range(450)))

        def error_at(value, error):
            if value == error:
                1 / 0
            return value

        await db.create_scalar_function("error_at", error_at)

        cur = await db.execute("select * from x")
        # do anext without aiter call first
        self.assertRaisesRegex(RuntimeError, "__anext__  called without calling __aiter__", cur.__anext__)

        # check exception sequencing at different batch sizes
        for prefetch in range(1, 50):
            for error in range(0, 500):
                apsw.async_cursor_prefetch.set(prefetch)
                values = []
                try:
                    async for (y,) in await db.execute("select error_at(y, ?) from x order by y", (error,)):
                        values.append(y)
                except ZeroDivisionError:
                    pass
                self.assertEqual(values, list(range(min(450, error))))

        # check sequential anext without waiting for previous to complete still
        # give correct answer
        for prefetch in range(1, 50):
            for error in range(0, 500):
                apsw.async_cursor_prefetch.set(prefetch)
                values = []

                try:
                    the_iter = aiter(await db.execute("select error_at(y, ?) from x order by y", (error,)))
                    futures = []
                    with BatchSends(db):
                        for i in range(500):
                            futures.append(anext(the_iter))
                    for f in futures:
                        values.append(await f)
                except ZeroDivisionError:
                    pass

                self.assertEqual(values, list(range(min(450, error))))

    def get_all_atests(self):
        for n in dir(self):
            if "atestA" <= n <= "atestZ":
                yield getattr(self, n)

    def testAsyncIO(self):
        global asyncio
        try:
            import asyncio
        except ImportError:
            return

        for fn in self.get_all_atests():
            with self.subTest(fw="asyncio", fn=fn):
                asyncio.run(self.asyncTearDown(fn("asyncio")))

    def testTrio(self):
        global trio
        try:
            import trio
        except ImportError:
            return

        for fn in self.get_all_atests():
            with self.subTest(fw="trio", fn=fn):
                trio.run(self.asyncTearDown, fn("trio"))

    def testAnyIO(self):
        backends = ["asyncio"]
        try:
            import trio

            backends.append("trio")
        except ImportError:
            pass
        try:
            global anyio
            import anyio
        except ImportError:
            return

        for be in backends:
            for fn in self.get_all_atests():
                with self.subTest(fw=f"anyio/{be}", fn=fn):
                    anyio.run(self.asyncTearDown, fn("anyio"), backend=be)


cvar_outside = contextvars.ContextVar("outside")


def sync_get_cvar(name):
    for k in contextvars.copy_context():
        if k.name == name:
            return contextvars.copy_context()[k]
    raise LookupError(f"{name=} not found")


async def async_get_cvar(name):
    return sync_get_cvar(name)


@contextlib.contextmanager
def BatchSends(conn):
    # we swizzle the queue out of the way, grab all the items and then
    # submit them all at the end
    class Batcher:
        def __init__(self):
            self.batch = []

        def put(self, item):
            self.batch.append(item)

    batcher = Batcher()
    actual_queue = conn.async_controller.queue
    conn.async_controller.queue = batcher
    try:
        yield
    finally:
        conn.async_controller.queue = actual_queue
        for item in batcher:
            actual_queue.put(item)


__all__ = ("Async",)

if __name__ == "__main__":
    unittest.main()
