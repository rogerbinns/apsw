#!/usr/bin/env python3

# This testing code deliberately does nasty stuff so mypy isn't helpful
# mypy: ignore-errors
# type: ignore

import contextlib
import contextvars
import functools
import threading
import unittest
import tempfile
import sys
import math
import inspect
import re

import apsw
import apsw.aio
import apsw.bestpractice
import apsw.ext


class Async(unittest.TestCase):
    async def asyncTearDown(self, coro):
        # we need this one because the event loop must still be
        # running in order to do aclose
        try:
            # we have to set it to its value because there is no way to get just the token
            with apsw.aio.contextvar_set(apsw.aio.check_progress_steps, apsw.aio.check_progress_steps.get()):
                return await coro
        finally:
            for c in apsw.connections():
                if c.is_async:
                    await c.aclose()
                else:
                    c.close()

    def tearDown(self):
        if apsw.connections():
            raise RuntimeError(f"Connections were left open {apsw.connections()=}")

        # we could be running before the threads have shutdown due to GIL
        leaked = set(threading.enumerate()) - self.active_threads
        for thread in leaked:
            # so have a grace period
            thread.join(1.0)
            if thread.is_alive():
                raise Exception(f"Leaked {thread=}")

    def setUp(self):
        self.verbose = self._outcome.result.showAll
        self.active_threads = set(threading.enumerate())

    def testOverwrite(self):
        "make sure module contextvars can't be overwritten"
        for name in "async_controller", "async_cursor_prefetch":
            self.assertRaisesRegex(AttributeError, ".*Do not overwrite apsw.*context", setattr, apsw, name, 3)
        # used to be a contextvar, now thread local
        self.assertIsNone(apsw.async_run_coro)
        self.assertRaises(TypeError, setattr, apsw.async_run_coro, 3)
        x = lambda: 3
        apsw.async_run_coro = x
        self.assertIs(apsw.async_run_coro, x)
        self.assertRaises(TypeError, setattr, apsw, "async_run_coro", 3+4j)
        res = []
        def check():
            res.append(apsw.async_run_coro)

        t = threading.Thread(target=check)
        t.start()
        t.join()

        self.assertEqual(res, [None])

    def testBadController(self):
        class BC:
            def send1(inner_self, call):
                self.assertRaises(RuntimeError, call, 3)
                self.assertRaises(RuntimeError, call, three=3)
                call()

            def send2(inner_self, call):
                call()
                call()

            def configure1(inner_self, db):
                pass

            def close1(inner_self):
                1 / 0

        unraised = None
        orig = sys.unraisablehook

        def hook(arg):
            nonlocal unraised
            unraised = arg

        sys.unraisablehook = hook
        try:
            with apsw.aio.contextvar_set(apsw.async_controller, BC):
                BC.send = BC.send1
                self.assertRaisesRegex(AttributeError, ".*no attribute 'configure'.*", apsw.Connection.as_async, "")
                self.assertIs(unraised.exc_type, AttributeError)
                self.assertIn("no attribute 'close'", str(unraised.exc_value))
                self.assertEqual([], apsw.connections())
                unraised = None
                BC.send = BC.send2
                BC.configure = BC.configure1
                BC.close = BC.close1
                self.assertRaisesRegex(RuntimeError, ".*only be called once.*", apsw.Connection.as_async, "")
                # we can't guarantee exactly when gc runs for the failure above
                if apsw.connections():
                    apsw.connections()[0].close()
                self.assertIs(unraised.exc_type, ZeroDivisionError)
                unraised = None
        finally:
            sys.unraisablehook = orig

        self.assertIsNone(unraised)

    def verifyCoroutine(self, coro):
        self.assertTrue(inspect.isawaitable(coro))
        self.assertTrue(inspect.iscoroutine(coro))

    async def atestInitFail(self, fw):
        "ensure cleanup on init failures"

        # Connection.__init__ in worker fails
        with self.assertRaises(TypeError):
            await apsw.Connection.as_async(one=2)

        # Connection init never even called
        class BadInit(apsw.Connection):
            def __init__(self):
                1 / 0

        with self.assertRaises(ZeroDivisionError):
            await BadInit.as_async()

        def error():
            1 / 0

        with apsw.aio.contextvar_set(apsw.async_controller, error):
            with self.assertRaises(ZeroDivisionError):
                await apsw.Connection.as_async("")

        class Bad1:
            def send(*args):
                1 / 0

            def close(*args):
                pass

        with apsw.aio.contextvar_set(apsw.async_controller, Bad1):
            with self.assertRaises(ZeroDivisionError):
                await apsw.Connection.as_async("")

        class Bad2:
            async def send(*args):
                1 / 0

            def close(*args):
                pass

        with apsw.aio.contextvar_set(apsw.async_controller, Bad2):
            with self.assertRaises(ZeroDivisionError):
                await apsw.Connection.as_async("")

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

    async def atestInheritance(self, fw):
        class DerivedCur(apsw.Cursor):
            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)
                self.colour = "gold"

        class DerivedCon(apsw.Connection):
            def __init__(self, colour, flags, filename):
                self.colour = colour
                super().__init__(filename, flags)
                self.cursor_factory = DerivedCur

        d = await DerivedCon.as_async("yellow", apsw.SQLITE_OPEN_READWRITE | apsw.SQLITE_OPEN_CREATE, ":memory:")

        self.assertEqual(d.colour, "yellow")
        self.assertIsInstance(d, DerivedCon)

        cur = await d.execute("select 3;")
        self.assertEqual(cur.colour, "gold")
        self.assertIsInstance(cur, DerivedCur)

        self.assertEqual(await cur.get, 3)

    async def atestBlob(self, fw):
        db = await apsw.Connection.as_async(":memory:")

        await db.execute(
            "create table dummy(column); insert into dummy(rowid, column) values(73, x'aabbcc'), (74, x'aabbcc');"
        )

        blob = db.blob_open("main", "dummy", "column", 73, True)
        self.verifyCoroutine(blob)
        blob = await blob

        self.assertEqual(3, blob.length())

        data = bytearray(3)
        await blob.read_into(data)
        self.assertEqual(data, b"\xaa\xbb\xcc")

        blob.seek(1)
        await blob.write(b"\x11\x22")
        blob.seek(0)
        self.assertEqual(await blob.read(), b"\xaa\x11\x22")

        blob.seek(3)
        with self.assertRaises(ValueError):
            await blob.write(b"hello world")

        await blob.reopen(74)
        with self.assertRaises(apsw.SQLError):
            await blob.reopen(423)

        with self.assertRaises(TypeError):
            with blob:
                pass

        async with blob:
            pass

        self.assertRaises(ValueError, blob.length)

    async def atestBackup(self, fw):
        db = await apsw.Connection.as_async(":memory:")
        db2 = await apsw.Connection.as_async(":memory:")
        await db2.pragma("page_size", 512)
        await db2.execute("create table dummy(x)")
        await db2.executemany("insert into dummy values(?)", (("a" * 4096,) for _ in range(33)))

        backup = await db.backup("main", db2, "main")

        await backup.step()
        # immediate values
        for v in ("remaining", "page_count", "done"):
            getattr(backup, v)

        with self.assertRaises(TypeError):
            await backup.finish()

        await backup.afinish()
        await backup.afinish()

        backup = await db.backup("main", db2, "main")

        with self.assertRaises(TypeError):
            with backup:
                pass

        async with backup:
            while not backup.done:
                await backup.step(1)

        fut = backup.afinish()
        self.verifyCoroutine(fut)
        await fut

        backup = await db.backup("main", db2, "main")

        async with backup:
            while not backup.done:
                await backup.step(1)

        self.assertTrue(backup.done)

    async def atestClosingSync(self, fw):
        "check sync close works too"
        return await self.atestClosing(fw, sync=True)

    async def atestClosing(self, fw, *, sync=False):
        "check aclose can be called multiple times, even after object is closed"
        db = await apsw.Connection.as_async(":memory:")

        cursor = await db.execute(
            "create table dummy(column); insert into dummy(rowid, column) values(73, x'aabbcc'), (74, x'aabbcc');"
        )

        blob = await db.blob_open("main", "dummy", "column", 73, False)

        db2 = await apsw.Connection.as_async(":memory:")
        await db2.pragma("page_size", 512)
        await db2.execute("create table dummy(x)")
        await db2.executemany("insert into dummy values(?)", (("a" * 4096,) for _ in range(129)))

        if sync:
            cursor.close()
        self.assertRaises(TypeError, cursor.aclose, "hello")
        fut = cursor.aclose()
        self.verifyCoroutine(fut)
        await fut
        fut = cursor.aclose()
        self.verifyCoroutine(fut)
        await fut
        cursor.close()
        cursor.close()

        if sync:
            blob.close()
        self.assertRaises(TypeError, blob.aclose, "hello")
        fut = blob.aclose()
        self.verifyCoroutine(fut)
        await fut
        fut = blob.aclose()
        self.verifyCoroutine(fut)
        await fut
        blob.close()
        blob.close()

        backup = db.backup("main", db2, "main")
        self.verifyCoroutine(backup)
        backup = await backup

        if sync:
            backup.close()
        self.assertRaises(TypeError, blob.aclose, "hello")
        fut = backup.aclose()
        self.verifyCoroutine(fut)
        await fut
        fut = backup.aclose()
        self.verifyCoroutine(fut)
        await fut
        backup.close()
        backup.close()

        if hasattr(apsw, "Session"):
            session = await apsw.aio.make_session(db, "main")
            if sync:
                session.close()
            self.assertRaises(TypeError, session.aclose, "hello")
            fut = session.aclose()
            self.verifyCoroutine(fut)
            await fut
            fut = session.aclose()
            self.verifyCoroutine(fut)
            await fut
            session.close()
            session.close()
        else:
            # keep following lines happy
            session = db

        if sync:
            db.close()
        self.assertRaises(TypeError, db.aclose, "hello")
        fut = db.aclose()
        self.verifyCoroutine(fut)
        await fut
        fut = db.aclose()
        self.verifyCoroutine(fut)
        await fut

        db.close()
        db.close()

        for obj in cursor, db, blob, backup, session:
            if sync:
                obj.close()
                obj.close()
            await obj.aclose()
            await obj.aclose()
            self.assertRaises(TypeError, obj.aclose, "hello")
            obj.close()
            obj.close()

        self.assertRaises(apsw.ConnectionClosedError, db.execute, "select 3")

    async def atestIteration(self, fw):
        "cursor iteration"

        db = await apsw.Connection.as_async(":memory:")

        # verify dataclasses work
        await db.execute("""create table data(one, two, three);
                         insert into data values('one', -1, 2), ('two', 4, -2), ('three', 3, 0)""")
        db.row_trace = apsw.ext.DataClassRowFactory()
        async for row in await db.execute("select one AS hello, (two+three) AS total FROM data"):
            match row.hello:
                case "one":
                    self.assertEqual(row.total, 1)
                case "two":
                    self.assertEqual(row.total, 2)
                case "three":
                    self.assertEqual(row.total, 3)
                case _:
                    self.fail(f"should not happen {row=}")
        db.row_trace = None

        # cases we check
        NUM_ENTRIES = 103
        PREFETCH_VALUES = (1, 2, 3, 5, 7, 13, 102, 103, 104, 2048)
        LIMITS = (0, 1, 2, 3, 101, 102, 103, 104)

        await db.execute("create table x(y INTEGER PRIMARY KEY)")
        async with db:
            await db.executemany("insert into x values(?)", ((x,) for x in range(NUM_ENTRIES)))

        def error_at(value, error):
            if value == error:
                1 / 0
            return value

        await db.create_scalar_function("error_at", error_at)

        fut = db.execute("select * from x")
        self.verifyCoroutine(fut)
        cur = await fut

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
                self.assertEqual(values, list(range(min(NUM_ENTRIES, error))))

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

                self.assertEqual(values, list(range(min(NUM_ENTRIES, error))))

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
            await apsw.Connection.as_async(":memory:")
            apsw.Connection(":memory:")

        finally:
            apsw.connection_hooks = []

    async def atestCancel(self, fw):
        # this is the only framework specific bit
        Event = getattr(sys.modules[fw], "Event")
        sleep = getattr(sys.modules[fw], "sleep")

        # async/sync callback cancellation
        event = Event()

        async def infinite_loop():
            event.set()
            while True:
                await sleep(0)

        db = await apsw.Connection.as_async("")
        await db.create_scalar_function("infinite_loop", infinite_loop)

        match fw:
            case "asyncio":
                task1 = asyncio.create_task(db.execute("select infinite_loop()"))
                # this gets queued but should not run because task1 is running
                task2 = asyncio.create_task(db.pragma("user_version", 3))

                await event.wait()
                await sleep(0.01)
                task2.cancel()
                task1.cancel()
                with self.assertRaises(asyncio.CancelledError):
                    await task1
                with self.assertRaises(asyncio.CancelledError):
                    await task2

                self.assertEqual(0, await db.pragma("user_version"))

                task = asyncio.create_task(db.execute(fractal_sql))
                await sleep(0.05)
                task.cancel()
                with self.assertRaises(asyncio.CancelledError):
                    await task

            case "trio":

                async def wrap(call, ready, cancelled):
                    ready.set()
                    try:
                        await call()
                    except trio.Cancelled:
                        cancelled.set()
                        raise

                ready = trio.Event(), trio.Event()
                cancelled = trio.Event(), trio.Event()

                async with trio.open_nursery() as nursery:
                    nursery.start_soon(
                        wrap, functools.partial(db.execute, "select infinite_loop()"), ready[0], cancelled[0]
                    )
                    await ready[0].wait()

                    nursery.start_soon(wrap, functools.partial(db.pragma, "user_version", 7), ready[1], cancelled[1])
                    await ready[1].wait()

                    nursery.cancel_scope.cancel()

                await cancelled[0].wait()
                await cancelled[1].wait()

                self.assertEqual(0, await db.pragma("user_version"))
            case "anyio":

                async def wrap(call, ready, cancelled):
                    ready.set()
                    try:
                        await call()
                    except anyio.get_cancelled_exc_class():
                        cancelled.set()
                        raise

                ready = anyio.Event(), anyio.Event()
                cancelled = anyio.Event(), anyio.Event()

                async with anyio.create_task_group() as nursery:
                    nursery.start_soon(
                        wrap, functools.partial(db.execute, "select infinite_loop()"), ready[0], cancelled[0]
                    )
                    await ready[0].wait()

                    nursery.start_soon(wrap, functools.partial(db.pragma, "user_version", 7), ready[1], cancelled[1])
                    await ready[1].wait()

                    nursery.cancel_scope.cancel()

                await cancelled[0].wait()
                await cancelled[1].wait()

                self.assertEqual(0, await db.pragma("user_version"))

    async def atestTimeout(self, fw):
        sleep = getattr(sys.modules[fw], "sleep")
        Event = getattr(sys.modules[fw], "Event")


        apsw.aio.check_progress_steps.set(10)
        db = await apsw.Connection.as_async("")


        match fw:
            case "asyncio":
                time = asyncio.get_running_loop().time
                timeout_exc_class = TimeoutError
            case "trio":
                time = trio.current_time
                timeout_exc_class = trio.TooSlowError
            case "anyio":
                time = anyio.current_time
                timeout_exc_class = TimeoutError
                # If not using AnyIO controller with trio, then trio's timeout can leak
                if isinstance(db.async_controller, apsw.aio.Trio):
                    timeout_exc_class = (TimeoutError, trio.TooSlowError)

        # check apsw.aio.deadline first
        async def block():
            while True:
                await sleep(0)

        await db.create_scalar_function("block", block)
        await db.create_scalar_function("sync_cvar", sync_get_cvar)
        await db.create_scalar_function("async_cvar", async_get_cvar)

        # deadline should work for all frameworks - check deadline is passed
        # to both sync and async callbacks
        v = time() + 3600
        with apsw.aio.contextvar_set(apsw.aio.deadline, v):
            self.assertEqual(v, await (await db.execute("select sync_cvar(?)", ("apsw.aio.deadline",))).get)
            self.assertEqual(v, await (await db.execute("select async_cvar(?)", ("apsw.aio.deadline",))).get)

        cura = await db.execute("select 3; select block()")

        with apsw.aio.contextvar_set(apsw.aio.deadline, time() - 0.01):
            # this should be queued but never run
            fut = db.pragma("user_version", 7)

            with self.assertRaises(timeout_exc_class):
                await cura.get

            with self.assertRaises(timeout_exc_class):
                await fut

        # check it never ran
        self.assertEqual(0, await db.pragma("user_version"))

        # straight forward sync code
        cura = db.execute(fractal_sql)
        with apsw.aio.contextvar_set(apsw.aio.deadline, time() + 0.02):
            await sleep(0.01)
            with self.assertRaises(timeout_exc_class):
                await (await cura).get

        if fw == "asyncio":
            # no native timeout handling so nothing else to test
            return

        ced = getattr(sys.modules[fw], "current_effective_deadline")
        fail_after = getattr(sys.modules[fw], "fail_after")

        async def current_effective_deadline():
            return ced()

        await db.create_scalar_function("ced", current_effective_deadline)

        with fail_after(1234):
            # deadline should not be set because using ced
            with self.assertRaises(LookupError):
                await (await db.execute("select sync_cvar(?)", ("apsw.aio.deadline",))).get
            with self.assertRaises(LookupError):
                await (await db.execute("select async_cvar(?)", ("apsw.aio.deadline",))).get
            # and this should almost match.  anyio+asyncio ends up
            # with rounding errors on each call so we reduce precision
            # of check.  debug python builds also increase the delta
            this_ced = ced()
            that_ced = await (await db.execute("select ced()")).get

            if that_ced == math.inf and fw == "anyio":
                # if using anyio < 4.11.0 with asyncio backend
                # then out asyncio controller is used and so there
                # is no current effective deadline to extract and it
                # gives inf
                pass
            else:
                # The values should be exactly equal, but with anyio they
                # differ by some of the digits after the decimal point on
                # slow/debug builds, so we allow a divergence of up to 1
                # second
                self.assertLessEqual(that_ced - this_ced, 1)

        timed_out = Event()

        async def func():
            try:
                await sleep(1000)
            except:
                # trio does Cancelled here - the caller gets timeout exception
                timed_out.set()
                raise

        await db.create_scalar_function("func", func)

        # we need to ensure the async function is running hence delays
        # until it gets executed
        for timeout in (0, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5):
            try:
                with fail_after(timeout):
                    await (await db.execute("select func()")).get
            except timeout_exc_class:
                if timed_out.is_set():
                    break
        else:
            self.fail("Timeout never happened")

        # the timeout value from above is used because that was enough
        # time to go from worker back to event loop, so it should be
        # enough to be deep in the sql
        with self.assertRaises(timeout_exc_class):
            with fail_after(timeout):
                await (await db.execute(fractal_sql)).get

    async def atestSession(self, fw):
        if not hasattr(apsw, "Session"):
            with self.assertRaisesRegex(apsw.MisuseError, ".*The session extension is not enabled and available.*"):
                await apsw.aio.make_session(None, None)
            return

        strmsize = apsw.session_config(apsw.SQLITE_SESSION_CONFIG_STRMSIZE, 0)
        try:
            apsw.session_config(apsw.SQLITE_SESSION_CONFIG_STRMSIZE, 1)
            db = await apsw.Connection.as_async(":memory:")

            self.assertRaisesRegex(TypeError, ".*apsw.aio.make_session.*", apsw.Session, db, "main")

            session = await apsw.aio.make_session(db, "main")
            await session.attach()

            await db.execute(
                "create table foo(x INTEGER PRIMARY KEY); insert into foo values(1), (2), (3), (4), (5), (6)"
            )

            # sanity and async streamer
            big = await session.changeset()

            out = b""

            async def w(b):
                nonlocal out
                out += b

            res = await session.changeset_stream(w)

            self.assertEqual(out, big)
            self.assertIsNone(res)

        finally:
            apsw.session_config(apsw.SQLITE_SESSION_CONFIG_STRMSIZE, strmsize)

    async def atestConnection(self, fw):
        "Connection methods and properties"
        adb = await apsw.Connection.as_async(":memory:")
        sdb = apsw.Connection(":memory:")

        self.assertTrue(adb.is_async)
        self.assertFalse(sdb.is_async)

        self.assertIsNotNone(adb.async_controller)
        self.assertRaises(TypeError, getattr, sdb, "async_controller")

        self.assertRaisesRegex(TypeError, ".*at least one argument.*", adb.async_run, func=print)
        self.assertRaisesRegex(TypeError, ".*async in sync.*", sdb.async_run, print)

        self.assertRaisesRegex(TypeError, ".*Expected a callable.*", adb.async_run, 3)

        def got_what(*args, **kwargs):
            return {"got": {"args": args, "kwargs": kwargs}}

        self.assertEqual((await adb.async_run(got_what)).pop("got"), {"args": tuple(), "kwargs": {}})

        self.assertEqual(
            (await adb.async_run(got_what, "one", "two", "three", six="six", four="four", five="five")).pop("got"),
            {"args": ("one", "two", "three"), "kwargs": {"four": "four", "five": "five", "six": "six"}},
        )

        cvar = contextvars.ContextVar("dummy")
        cvar.set(7)

        def val():
            return cvar.get()

        with apsw.aio.contextvar_set(cvar, 8):
            x = adb.async_run(val)

        self.assertEqual(8, await x)

        async def foo():
            return 3

        x = adb.async_run(foo)
        self.assertEqual(3, await (await x))

    async def atestVTable(self, fw):
        sleep = getattr(sys.modules[fw], "sleep")

        async def vtable_gen(start=0, end=10, foo="foo"):
            for i in range(start, end):
                await sleep(0)
                yield i, foo

        async def vtable_coro(start=0, end=10, foo="foo"):
            await sleep(0)
            return [
                (0 + start, "a " + foo),
                (1 + end, foo + " b"),
            ]

        vtable_coro.columns = vtable_gen.columns = ("red", "green")
        vtable_coro.column_access = vtable_gen.column_access = apsw.ext.VTColumnAccess.By_Index

        db = await apsw.Connection.as_async(":memory:")

        await apsw.ext.make_virtual_module(db, "vtable_gen", vtable_gen)
        await apsw.ext.make_virtual_module(db, "vtable_coro", vtable_coro)

        self.assertEqual(
            [(7, "orange"), (8, "orange"), (9, "orange")],
            await (await db.execute("select * from vtable_gen where start=7 and foo='orange'")).get,
        )
        self.assertEqual(
            [(0, "a red"), (3, "red b")],
            await (await db.execute("select * from vtable_coro where end=2 and foo='red'")).get,
        )

    async def atestStr(self, fw):
        "ensure async str/repr says that"

        with tempfile.TemporaryDirectory(prefix="apsw-atestStr") as tempd:

            def get(x):
                # extracts the useful bit and normalizes the address
                # which is libc dependent
                mo = re.match("<(.*) at 0?[Xx]?([0-9a-fA-F]+)>", x)
                return f"{mo.group(1)} at 0x{mo.group(2).lower().lstrip('0')}"

            def saddr(o):
                return f" at {hex(id(o))}"

            class Banana(apsw.Connection):
                pass

            class BananaCursor(apsw.Cursor):
                pass

            if hasattr(apsw, "Session"):

                class BananaSession(apsw.Session):
                    pass
            else:
                BananaSession = None

            # the strings we expect in str
            tag_a = " (async) "
            tag_aw = " (async: worker thread) "
            tag_c = " (closed) "

            # starting objects
            acon = await apsw.Connection.as_async(f"{tempd}/abc")
            scon = apsw.Connection(f"{tempd}/def")
            acur = acon.cursor()
            scur = scon.cursor()
            await acon.execute(
                "create table dummy(column);insert into dummy(rowid, column) values(73, x'aabbcc'), (74, x'aabbcc');"
            )
            scon.execute(
                "create table dummy(column);insert into dummy(rowid, column) values(73, x'aabbcc'), (74, x'aabbcc');"
            )
            ablob = await acon.blob_open("main", "dummy", "column", 73, False)
            sblob = scon.blob_open("main", "dummy", "column", 73, False)

            bacon = await Banana.as_async(f"{tempd}/hij")
            bscon = Banana(f"{tempd}/klmn")
            bacon.cursor_factory = BananaCursor
            bacur = bacon.cursor()
            bscon.cursor_factory = BananaCursor
            bscur = bscon.cursor()

            sbackup = bscon.backup("main", scon, "main")
            abackup = await bacon.backup("main", scon, "main")

            to_test = [
                (sblob, ablob, acon.async_run, "apsw.Blob"),
                (scur, acur, acon.async_run, "apsw.Cursor"),
                (bscur, bacur, bacon.async_run, "BananaCursor"),
                (sbackup, abackup, bacon.async_run, "apsw.Backup"),
            ]

            if BananaSession:
                ssession = apsw.Session(scon, "main")
                asession = await apsw.aio.make_session(acon, "main")
                bssession = BananaSession(bscon, "main")
                basession = await acon.async_run(BananaSession, acon, "main")

                to_test.extend(
                    (
                        (ssession, asession, acon.async_run, "apsw.Session"),
                        (bssession, basession, acon.async_run, "BananaSession"),
                    )
                )

            to_test.extend(
                (
                    (scon, acon, acon.async_run, "apsw.Connection"),
                    (bscon, bacon, bacon.async_run, "Banana"),
                )
            )

            for sobj, aobj, async_run, klass_name in to_test:
                # sync object
                s = get(str(sobj))
                addr=saddr(sobj)
                self.assertNotIn(" object ", s)
                self.assertNotIn(tag_a, s)
                self.assertNotIn(tag_c, s)
                self.assertNotIn(tag_aw, s)
                self.assertTrue(s.startswith(klass_name))
                self.assertTrue(s.endswith(addr))

                # async object in event loop
                s = get(str(aobj))
                addr = saddr(aobj)
                self.assertNotIn(" object ", s)
                self.assertIn(tag_a, s)
                self.assertNotIn(tag_c, s)
                self.assertNotIn(tag_aw, s)
                self.assertTrue(s.startswith(klass_name))
                self.assertTrue(s.endswith(addr))

                # async object in worker thread
                s = get(await async_run(lambda: str(aobj)))
                addr = saddr(aobj)
                self.assertNotIn(" object ", s)
                self.assertNotIn(tag_a, s)
                self.assertNotIn(tag_c, s)
                self.assertIn(tag_aw, s)
                self.assertTrue(s.startswith(klass_name))
                self.assertTrue(s.endswith(addr))

                # after closing
                sobj.close()
                addr = saddr(sobj)
                s = get(str(sobj))
                self.assertNotIn(" object ", s)
                self.assertNotIn(tag_a, s)
                self.assertIn(tag_c, s)
                self.assertNotIn(tag_aw, s)
                self.assertTrue(s.startswith(klass_name))
                self.assertTrue(s.endswith(addr))

                aobj.close()
                s = get(str(aobj))
                addr = saddr(aobj)
                self.assertNotIn(" object ", s)
                self.assertNotIn(tag_a, s)
                self.assertIn(tag_c, s)
                self.assertNotIn(tag_aw, s)
                self.assertTrue(s.startswith(klass_name))
                self.assertTrue(s.endswith(addr))

        # get unavailable database name due to mutex being held in another thread
        scon = apsw.Connection("")
        e_wait = threading.Event()
        e_continue = threading.Event()

        def blocker():
            e_wait.set()
            e_continue.wait()
            return 7

        scon.create_scalar_function("blocker", blocker)
        t = threading.Thread(target=lambda: scon.execute("select blocker()").get)
        t.start()
        e_wait.wait()
        s = get(str(scon))
        try:
            self.assertIsNotNone(re.match(r"apsw.Connection \"\(unavailable\)\" at 0x[0-9a-fA-F]+", s))
        finally:
            e_continue.set()
            t.join()
            scon.close()

        acon = await apsw.Connection.as_async("")
        e_wait = getattr(sys.modules[fw], "Event")()
        e_continue = getattr(sys.modules[fw], "Event")()

        async def blocker():
            e_wait.set()
            await e_continue.wait()
            return 7

        await acon.create_scalar_function("blocker", blocker)

        match fw:
            case "asyncio":
                task = asyncio.create_task(acon.execute("select blocker()"))

                await e_wait.wait()
                s = get(str(acon))
                e_continue.set()
                await task

            case "trio":
                async with trio.open_nursery() as nursery:
                    nursery.start_soon(acon.execute, "select blocker()")
                    await e_wait.wait()
                    s = get(str(acon))
                    e_continue.set()

            case "anyio":
                async with anyio.create_task_group() as tg:
                    tg.start_soon(acon.execute, "select blocker()")
                    await e_wait.wait()
                    s = get(str(acon))
                    e_continue.set()

        try:
            self.assertIsNotNone(re.match(r"apsw.Connection \(async\) \"\(unavailable\)\" at 0x[0-9a-fA-F]+", s))
        finally:
            await acon.aclose()

    def get_all_atests(self):
        for n in dir(self):
            if "atestA" <= n <= "atestZ":
                if self.verbose:
                    print(">>> ", n)
                yield getattr(self, n)

    def testAsyncIO(self):
        global asyncio
        try:
            import asyncio
        except ImportError:
            return

        for fn in self.get_all_atests():
            with self.subTest(fw="asyncio", fn=fn):
                asyncio.run(self.asyncTearDown(fn("asyncio")), debug=False)

    def testTrio(self):
        import importlib.metadata
        global trio
        try:
            import trio

            ver = tuple(map(int, importlib.metadata.version("trio").split(".")))
            if ver < (0, 20, 0):
                if self.verbose:
                    print(f"trio {ver=} is too old to run tests")
                return

        except (ImportError, TypeError):
            return

        for fn in self.get_all_atests():
            with self.subTest(fw="trio", fn=fn):
                trio.run(self.asyncTearDown, fn("trio"))

    def testAnyIO(self):
        import importlib.metadata

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

            ver = tuple(map(int, importlib.metadata.version("trio").split(".")))
            if ver >= (0, 31, 0):
                backends.append("trio")
            elif self.verbose:
                print(f"trio {ver=} is too old for anyio")
        except (ImportError, TypeError):
            pass

        if not backends:
            return

        try:
            global anyio
            import anyio
        except ImportError:
            return

        ver = tuple(map(int, importlib.metadata.version("anyio").split(".")))
        if ver < (4, 0):
            # our tests use the v4 apis
            if self.verbose:
                print(f"anyio {ver=} is too old to run the tests")
            return

        for be in backends:
            if self.verbose:
                print(f"!!! AnyIO backend {be}")

            for fn in self.get_all_atests():
                with self.subTest(fw="anyio", be=be, fn=fn):
                    match be:
                        case "asyncio":
                            backend_options = {"debug": False}
                        case "trio":
                            backend_options = {}
                    anyio.run(self.asyncTearDown, fn("anyio"), backend=be, backend_options=backend_options)

    def testNoIO(self):
        "no async running"
        self.assertRaisesRegex(
            RuntimeError, "Unable to determine current Async environment", apsw.Connection.as_async, ""
        )


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


# does a lot of work to test timeouts/cancellation
fractal_sql = """
    WITH RECURSIVE
    xaxis(x) AS (VALUES(-2.0) UNION ALL SELECT x+0.05 FROM xaxis WHERE x<1.2),
    yaxis(y) AS (VALUES(-1.0) UNION ALL SELECT y+0.1 FROM yaxis WHERE y<1.0),
    m(iter, cx, cy, x, y) AS (
        SELECT 0, x, y, 0.0, 0.0 FROM xaxis, yaxis
        UNION ALL
        SELECT iter+1, cx, cy, x*x-y*y + cx, 2.0*x*y + cy FROM m
        WHERE (x*x + y*y) < 4.0 AND iter< 800000 -- this should be 28 and controls how much work is done
    ),
    m2(iter, cx, cy) AS (
        SELECT max(iter), cx, cy FROM m GROUP BY cx, cy
    ),
    a(t) AS (
        SELECT group_concat( substr(' .+*#', 1+min(iter/7,4), 1), '')
        FROM m2 GROUP BY cy
    )
    SELECT group_concat(rtrim(t),x'0a') FROM a;"""


__all__ = ("Async",)

if __name__ == "__main__":
    unittest.main()
