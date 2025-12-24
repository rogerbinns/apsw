#!/usr/bin/env python3

# This testing code deliberately does nasty stuff so mypy isn't helpful
# mypy: ignore-errors
# type: ignore

import contextlib
import contextvars
import threading
import unittest
import sys

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

    async def atestCancel(self, fw):
        if sys.version_info < (3, 11):
            global ExceptionGroup
            ExceptionGroup = Exception

        event = getattr(sys.modules[fw], "Event")()

        async def set_event():
            event.set()

        async def func(x, y):
            await event.wait()
            if x == y:
                1 / 0
            return x + y

        db = await apsw.Connection.as_async("")
        await db.create_scalar_function("func", func)

        match fw:
            case "asyncio":
                if sys.version_info < (3, 11):
                    # py 3.10 doesn't have TaskGroup
                    return

                async def wait_on(f):
                    return await f

                try:
                    async with asyncio.TaskGroup() as tg:
                        task1 = tg.create_task(wait_on(db.execute("select func(4, 5), func(4, 4)")))
                        task2 = tg.create_task(wait_on(db.execute("select func(3,4)")))
                        task3 = tg.create_task(wait_on(db.pragma("user_version", 7)))

                        tg.create_task(set_event())

                except ExceptionGroup:
                    pass

                with self.assertRaises(ZeroDivisionError):
                    await task1

                with self.assertRaises(asyncio.CancelledError):
                    await task2

                with self.assertRaises(asyncio.CancelledError):
                    await task3

            case "trio":

                class Retval:
                    pass

                retvals = [Retval(), Retval(), Retval()]

                async def wait_on(index, f):
                    try:
                        retvals[index].value = await f
                    except BaseException as exc:
                        retvals[index].exception = exc
                        raise

                try:
                    async with trio.open_nursery() as n:
                        n.start_soon(wait_on, 0, db.execute("select func(4, 5), func(4, 4)"))
                        n.start_soon(wait_on, 1, db.execute("select func(3,4)"))
                        n.start_soon(wait_on, 2, db.pragma("user_version", 7))

                        n.start_soon(set_event)

                except ExceptionGroup:
                    pass

                self.assertIsInstance(retvals[0].exception, ZeroDivisionError)
                self.assertIsInstance(retvals[1].exception, trio.Cancelled)
                self.assertIsInstance(retvals[2].exception, trio.Cancelled)

            case "anyio":

                class Retval:
                    pass

                retvals = [Retval(), Retval(), Retval()]

                async def wait_on(index, f):
                    try:
                        retvals[index].value = await f
                    except BaseException as exc:
                        retvals[index].exception = exc
                        raise

                try:
                    async with anyio.create_task_group() as tg:
                        tg.start_soon(wait_on, 0, db.execute("select func(4, 5), func(4, 4)"))
                        tg.start_soon(wait_on, 1, db.execute("select func(3,4)"))
                        tg.start_soon(wait_on, 2, db.pragma("user_version", 7))

                        tg.start_soon(set_event)
                except ExceptionGroup:
                    pass

                cancelled = anyio.get_cancelled_exc_class()

                self.assertIsInstance(retvals[0].exception, ZeroDivisionError)
                self.assertIsInstance(retvals[1].exception, cancelled)
                self.assertIsInstance(retvals[2].exception, cancelled)

            case _:
                raise NotImplementedError

        self.assertEqual(0, await db.pragma("user_version"))

    async def atestTimeout(self, fw):
        Event = getattr(sys.modules[fw], "Event")
        sleep = getattr(sys.modules[fw], "sleep")

        match fw:
            case "asyncio":
                time = asyncio.get_running_loop().time
                timeout_exc_class = TimeoutError
            case "trio":
                time = trio.lowlevel.current_clock().current_time
                timeout_exc_class = trio.TooSlowError
            case "anyio":
                time = anyio.current_time
                if "trio" in sys.modules:
                    # this is where the underlying framework leaks
                    # because we don't do an anyio specific controller
                    timeout_exc_class = (TimeoutError, getattr(sys.modules["trio"], "TooSlowError"))
                else:
                    timeout_exc_class = TimeoutError
            case _:
                raise NotImplementedError

        def check_timeout(exc):
            # other exceptions can happen during timeout exceptions so
            # check them all
            got_timeout = False
            while exc is not None:
                if isinstance(exc, timeout_exc_class):
                    got_timeout = True
                    break
                exc = exc.__cause__ or exc.__context__
            if not got_timeout:
                raise

        event = Event()

        timed_out = None

        async def func():
            nonlocal timed_out
            try:
                await sleep(3600)
            except BaseException as exc:
                timed_out = exc
                raise

        async def block():
            await event.wait()
            return True

        db = await apsw.Connection.as_async("")
        await db.create_scalar_function("func", func)
        await db.create_scalar_function("block", block)
        await db.create_scalar_function("sync_cvar", sync_get_cvar)
        await db.create_scalar_function("async_cvar", async_get_cvar)

        # deadline should work for all frameworks - check deadline is passed
        # to both sync and async callbacks
        v = time() + 3600
        with apsw.aio.contextvar_set(apsw.aio.deadline, v):
            self.assertEqual(v, await (await db.execute("select sync_cvar(?)", ("apsw.aio.deadline",))).get)
            self.assertEqual(v, await (await db.execute("select async_cvar(?)", ("apsw.aio.deadline",))).get)

        # block queue and check second item gets timed out
        db.execute("select block()")
        with apsw.aio.contextvar_set(apsw.aio.deadline, time()):
            task2 = db.execute("select func()")
            event.set()

        try:
            await task2
            1 / 0  # should not be reached
        except BaseException as exc:
            check_timeout(exc)

        def timeout_seq():
            # the sequences we try to get func executing and then
            # timeout, in seconds.
            yield -10
            yield -1
            yield 0
            i = 0.001
            yield i
            while i < 5:
                i *= 2
                yield i
            raise Exception("Unable to find timeout value that works")

        # have timeout in the async func sleep
        for timeout in timeout_seq():
            with apsw.aio.contextvar_set(apsw.aio.deadline, time() + timeout):
                task = db.execute("select func()")
            try:
                await task
            except BaseException as exc:
                check_timeout(exc)
            if timed_out:
                break

        match fw:
            case "asyncio":
                # nothing else to test
                pass
            case "trio":
                # We support both trio current_effective_deadline and
                # our deadline context var with the latter taking
                # priority.  It has been been tested above.  Now
                # check current_effective_deadline works

                async def current_effective_deadline():
                    return trio.current_effective_deadline()

                await db.create_scalar_function("ced", current_effective_deadline)

                v = time() + 3600
                with trio.fail_at(v):
                    # sqlite worker thread should not have deadline set because using ced
                    with self.assertRaises(LookupError):
                        await (await db.execute("select sync_cvar(?)", ("apsw.aio.deadline",))).get
                    with self.assertRaises(LookupError):
                        await (await db.execute("select async_cvar(?)", ("apsw.aio.deadline",))).get
                    # and this should be set
                    self.assertEqual(v, await (await db.execute("select ced()")).get)

            case "anyio":
                timed_out = False
                for timeout in timeout_seq():
                    try:
                        with anyio.fail_after(timeout):
                            with apsw.aio.contextvar_set(apsw.aio.deadline, anyio.current_effective_deadline()):
                                task = db.execute("select func()")
                                await task
                    except BaseException as exc:
                        check_timeout(exc)
                    if timed_out:
                        break

            case _:
                raise NotImplementedError

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
        backends = []
        try:
            global asyncio
            import asyncio

            backends.append("asyncio")
        except ImportError:
            pass

        try:
            global trio
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
                    match be:
                        case "asyncio":
                            backend_options = {"debug": True}
                        case "trio":
                            backend_options = {}
                    anyio.run(self.asyncTearDown, fn("anyio"), backend=be, backend_options=backend_options)


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
