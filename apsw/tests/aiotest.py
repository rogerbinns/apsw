#!/usr/bin/env python3

# This testing code deliberately does nasty stuff so mypy isn't helpful
# mypy: ignore-errors
# type: ignore

import contextlib
import contextvars
import threading
import unittest
import sys
import inspect
import time

import apsw
import apsw.aio
import apsw.bestpractice


#### ::TODO::  tests to add
#
# inheritance of connection/cursor
# session
# virtual tables especially ext wrapper
#
# multiple connections active at once (also backup with this)


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

    def verifyFuture(self, future):
        # verify futures match apsw.aio.AsyncResult
        self.assertTrue(inspect.isawaitable(future))

        # methods can be implemented in python or c and inspect sees
        # those differently
        is_method = lambda x: inspect.ismethod(x) or inspect.isbuiltin(x)

        for n in dir(apsw.aio.AsyncResult):
            if not n.startswith("_"):
                self.assertHasAttr(future, n)
                self.assertTrue(is_method(getattr(future, n)))

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

    async def atestBlob(self, fw):
        db = await apsw.Connection.as_async(":memory:")

        await db.execute("create table dummy(column); insert into dummy(rowid, column) values(73, x'aabbcc'), (74, x'aabbcc');")

        blob = db.blob_open("main", "dummy", "column", 73, True)
        self.verifyFuture(blob)
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
        await db2.executemany("insert into dummy values(?)", (("a"*4096,) for _ in range(129)))

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
                backup.step(1)

        fut = backup.afinish()
        self.verifyFuture(fut)
        await fut

        backup = await db.backup("main", db2, "main")

        async with backup:
            while not backup.done:
                backup.step(1)

        self.assertTrue(backup.done)

    async def atestClosing(self, fw):
        "check aclose can be called multiple times, even after object is closed"
        db = await apsw.Connection.as_async(":memory:")

        cursor = await db.execute("create table dummy(column); insert into dummy(rowid, column) values(73, x'aabbcc'), (74, x'aabbcc');")

        blob =await db.blob_open("main", "dummy", "column", 73, False)

        db2 = await apsw.Connection.as_async(":memory:")
        await db2.pragma("page_size", 512)
        await db2.execute("create table dummy(x)")
        await db2.executemany("insert into dummy values(?)", (("a"*4096,) for _ in range(129)))


        fut = cursor.aclose()
        self.verifyFuture(fut)
        await fut
        fut = cursor.aclose()
        self.verifyFuture(fut)
        await fut
        cursor.close()
        cursor.close()

        fut = blob.aclose()
        self.verifyFuture(fut)
        await fut
        fut = blob.aclose()
        self.verifyFuture(fut)
        await fut
        blob.close()
        blob.close()

        backup = db.backup("main", db2, "main")
        self.verifyFuture(backup)
        backup = await backup

        fut=backup.aclose()
        self.verifyFuture(fut)
        await fut
        fut = backup.aclose()
        self.verifyFuture(fut)
        await fut
        backup.close()
        backup.close()

        fut = db.aclose()
        self.verifyFuture(fut)
        await fut
        fut = db.aclose()
        self.verifyFuture(fut)
        await fut

        db.close()
        db.close()

        for obj in cursor, db, blob, backup:
            await obj.aclose()
            await obj.aclose()
            obj.close()
            obj.close()

        self.assertRaises(apsw.ConnectionClosedError, db.execute, "select 3")

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

        fut = db.execute("select * from x")
        self.verifyFuture(fut)
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
        "Cancellation calls on futures"

        # this is the only framework specific bit
        Event = getattr(sys.modules[fw], "Event")
        match fw:
            case "asyncio":
                cancelled_exc = asyncio.CancelledError
            case "trio":
                cancelled_exc = apsw.aio.TrioFuture.Cancelled
            case "anyio":
                cancelled_exc = []
                if "asyncio" in sys.modules:
                    cancelled_exc.append(asyncio.CancelledError)
                if "trio" in sys.modules:
                    cancelled_exc.append(apsw.aio.TrioFuture.Cancelled)

                cancelled_exc = tuple(cancelled_exc)

        event = Event()

        async def func():
            await event.wait()
            return 3

        apsw.aio.check_progress_steps.set(10)

        db = await apsw.Connection.as_async("")
        await db.create_scalar_function("func", func)

        fut = db.execute("select func(), func()")
        self.assertFalse(fut.done())
        res = fut.cancel()
        self.assertEqual(res, fut.cancelled())
        event.set()

        with self.assertRaises(cancelled_exc):
            await fut
        self.assertTrue(fut.done())

        fut = db.execute(fractal_sql)
        self.assertFalse(fut.done())
        release_gil()
        res = fut.cancel()
        self.assertEqual(res, fut.cancelled())
        with self.assertRaises(cancelled_exc):
            await fut
        self.assertTrue(fut.done())

    async def atestCancelFramework(self, fw):
        "Cancellation done by the framework"
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
                    self.verifyFuture(task1)
                    await task1

                with self.assertRaises(asyncio.CancelledError):
                    self.verifyFuture(task2)
                    await task2

                with self.assertRaises(asyncio.CancelledError):
                    self.verifyFuture(task3)
                    await task3

            case "trio":

                class Retval:
                    pass

                retvals = [Retval(), Retval(), Retval()]

                async def wait_on(index, f):
                    self.verifyFuture(f)
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
                    self.verifyFuture(f)
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

        apsw.aio.check_progress_steps.set(10)
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
        release_gil()
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
                release_gil()
            try:
                await task
            except BaseException as exc:
                check_timeout(exc)
                break

        # now in long query
        for timeout in timeout_seq():
            if timeout <= 0:
                continue
            with apsw.aio.contextvar_set(apsw.aio.deadline, time() + timeout):
                task = db.execute(fractal_sql)
                release_gil()
            try:
                await task
            except BaseException as exc:
                check_timeout(exc)
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
                for timeout in timeout_seq():
                    try:
                        with anyio.fail_after(timeout):
                            with apsw.aio.contextvar_set(apsw.aio.deadline, anyio.current_effective_deadline()):
                                task = db.execute("select func()")
                                await task
                    except BaseException as exc:
                        check_timeout(exc)
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
                asyncio.run(self.asyncTearDown(fn("asyncio")), debug=False)

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

def release_gil():
    # called to ensure another thread has a chance to run
    for i in range(5):
        time.sleep(0)

__all__ = ("Async",)

if __name__ == "__main__":
    unittest.main()
