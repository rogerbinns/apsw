#!/usr/bin/env python3

# This testing code deliberately does nasty stuff so mypy isn't helpful
# mypy: ignore-errors
# type: ignore

import contextlib
import contextvars
import threading
import unittest

import apsw
import apsw.aio
import apsw.bestpractice


#### ::TODO::  tests to add
#
# timeouts
#
# cancels
#
# inheritance of connection/cursor
#
# blob needs aenter / aexit
# backup: aclose
# cursor get/fetchall need to close the cursor (or maybe not?)
#
# multiple connections active at once (also backup with this)


class Async(unittest.TestCase):
    async def asyncTearDown(self, coro):
        # we need this one because the event loop must still be
        # running in order to do aclose
        try:
            return await coro
        finally:
            for c in apsw.connections():
                await c.aclose()

    def tearDown(self):
        if apsw.connections():
            raise RuntimeError(f"Connections were left open {apsw.connections()=}")

        for thread in threading.enumerate():
            if thread not in self.active_threads:
                raise RuntimeError(f"Leaked thread {thread=}")

    def setUp(self):
        self.active_threads = list(threading.enumerate())

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

        # values we check
        NUM_ENTRIES = 450
        PREFETCH_VALUES = (1, 2, 3, 5, 7, 13, 33, 97, 214, 448, 449, 450, 451, 452, 1024, 2048)
        LIMITS = (0, 1, 2, 3, 4, 6, 7, 8, 32, 33, 34, 100, 448, 449, 450, 451, 452)

        db = await apsw.Connection.as_async(":memory:")
        await db.execute("create table x(y INTEGER PRIMARY KEY)")
        async with db:
            await db.executemany("insert into x values(?)", ((x,) for x in range(NUM_ENTRIES)))

        def error_at(value, error):
            if value == error:
                1 / 0
            return value

        await db.create_scalar_function("error_at", error_at)

        cur = await db.execute("select * from x")
        # do anext without aiter call first
        self.assertRaisesRegex(RuntimeError, "__anext__  called without calling __aiter__", cur.__anext__)

        # end of query at different batch sizes
        for prefetch in PREFETCH_VALUES:
            for limit in LIMITS:
                apsw.async_cursor_prefetch.set(prefetch)
                values = []
                async for (y,) in await db.execute("select y from x order by y LIMIT ?", (limit,)):
                    values.append(y)
                self.assertEqual(values, list(range(min(NUM_ENTRIES, limit))))

        # check exception sequencing at different batch sizes
        for prefetch in PREFETCH_VALUES:
            for error in LIMITS:
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
        for prefetch in PREFETCH_VALUES:
            for error in LIMITS:
                apsw.async_cursor_prefetch.set(prefetch)
                values = []

                try:
                    the_iter = aiter(await db.execute("select error_at(y, ?) from x order by y", (error,)))
                except ZeroDivisionError:
                    self.assertEqual(error, 0)
                    continue
                futures = []
                with BatchSends(db):
                    for i in range(error + 10):
                        futures.append(anext(the_iter))
                seen_zerodiv = False
                for f in futures:
                    try:
                        values.append((await f)[0])
                    except ZeroDivisionError:
                        seen_zerodiv = True
                    except StopAsyncIteration:
                        self.assertTrue(seen_zerodiv or error >= NUM_ENTRIES)

                self.assertEqual(values, list(range(min(450, error))))

    async def atestConfigure(self, fw):
        auto = apsw.aio.Auto()
        auto.close()

        class CT(type(auto)):
            def configure1(self, db):
                1 / 0

            def configure2(self):
                1 / 0

            def configure99(inner_self, db):
                self.assertIs(inner_self, db.async_controller)
                super().configure(db)

        with apsw.aio.contextvar_set(apsw.async_controller, CT):
            with self.assertRaises(ZeroDivisionError):
                CT.configure = CT.configure1
                await apsw.Connection.as_async("")

            with self.assertRaises(TypeError):
                CT.configure = CT.configure2
                await apsw.Connection.as_async("")

            CT.configure = CT.configure99
            await apsw.Connection.as_async("")

        try:
            # make sure bestpractice works
            apsw.bestpractice.apply(apsw.bestpractice.recommended)
            apsw.config(apsw.SQLITE_CONFIG_LOG, None)

            await apsw.Connection.as_async("")
            await apsw.Connection.as_async(":memory")

        finally:
            apsw.connection_hooks = []

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
        for item in batcher.batch:
            actual_queue.put(item)


__all__ = ("Async",)

if __name__ == "__main__":
    unittest.main()
