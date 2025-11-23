#!/usr/bin/env python3

# This testing code deliberately does nasty stuff so mypy isn't helpful
# mypy: ignore-errors
# type: ignore

import functools
import inspect
import threading
import unittest
import importlib.resources
import json

import queue
from typing import Literal

import apsw
import apsw.aio


def sync_await(obj):
    it = obj.__await__()
    try:
        while True:
            next(it)
    except StopIteration as exc:
        return exc.value


# The result is one of
# sync   - can only be used on sync connections (async gives exception)
# async  - can only be used on async connections (sync gives exception)
# dual   - returns awaitable in async, value in sync
# value  - returns a value in sync or async
def get_meta(
    klass: str, member: str, kind: Literal["function" | "attribute"]
) -> Literal["sync" | "async" | "dual" | "value"]:
    assert kind in {"function", "attribute"}

    data = json.loads(importlib.resources.files("apsw.tests").joinpath("async_meta.json").read_text())

    assert klass in data, f"{klass} not found"

    section = data[klass]

    # explicit?
    for k in "sync async dual value".split():
        if member in section.get(k, tuple()):
            return k

    # defaults
    match member:
        case "__enter__" | "__exit__":
            return "sync"
        case "__aenter__" | "__aexit__" | "aclose":
            return "async"
        case "__repr__" | "__str__":
            return "value"
        case _:
            return "dual" if kind == "function" else "value"


skip = set(dir(object())) - {"__repr__", "__str__"}


def hardended_hasattr(object, name):
    if name in {"__repr__", "__str__"}:
        return False
    try:
        return inspect.getattr_static(object, name) or hasattr(object, name)
    except AttributeError:
        return False


def is_open(con):
    try:
        sync_await(con.filename)
        return True
    except apsw.ConnectionClosedError:
        return False


class Async(unittest.TestCase):
    def tearDown(self):
        while apsw.connections():
            c = apsw.connections()[0]
            try:
                c.close()
            except:
                pass
            try:
                c.aclose()
            except:
                pass

    def testOverwrite(self):
        "make sure module contextvars can't be overwritten"
        for name in "async_controller", "async_run_coro", "async_cursor_prefetch":
            self.assertRaisesRegex(AttributeError, ".*Do not overwrite apsw.*context", setattr, apsw, name, 3)

    def classifyOne(self, send, is_attr, object, klass, member, value=None):
        # send is None for async access, callable for sync
        if is_attr:
            try:
                if send:
                    if value is not None:
                        send(functools.partial, setattr, object, member, value)
                    else:
                        send(functools.partial, getattr, object, member)
                else:
                    v = getattr(object, member) if value is None else setattr(object, member, value)
                    if hasattr(v, "__await__"):
                        return "async"
                return "value"
            except TypeError as exc:
                if not send and exc.args[0] == "Using sync method in async context X2":
                    return "exception"
                if send and exc.args[0] == "Using async method in sync context X1":
                    return "exception"
                raise

        match (klass, member):
            case (_, "__aexit__") | (_, "__exit__"):
                args = (None, None, None)
            case _:
                args = tuple()

        try:
            if send:
                sync_await(send(functools.partial(getattr(object, member), *args)))
            else:
                if hasattr(getattr(object, member)(*args), "__await__"):
                    return "async"
            return "value"
        except TypeError as exc:
            if not send and exc.args[0] == "Using sync method in async context X2":
                return "exception"
            if send and exc.args[0] == "Using async method in sync context X1":
                return "exception"
            raise

    def testMetaJson(self):
        apsw.async_controller.set(SimpleController)

        con = sync_await(apsw.Connection.as_async(""))

        for name in dir(con):
            if name in skip:
                continue

            if not is_open(con):
                con = sync_await(apsw.Connection.as_async(""))

            is_attr = hardended_hasattr(object, name)

            print(f"Connection {name=} {is_attr=}")

            kind_sync = self.classifyOne(con.async_controller.send, is_attr, con, "Connection", name)

            if not is_open(con):
                con = sync_await(apsw.Connection.as_async(""))

            kind_async = self.classifyOne(None, is_attr, con, "Connection", name)

            if is_attr:
                # check writable
                try:
                    kind_async_setattr = self.classifyOne(None, is_attr, con, "Connection", name, lambda *args: False)
                    self.assertEqual(kind_async, kind_async_setattr)
                except AttributeError as exc:
                    if "objects is not writable" in str(exc):
                        pass
                    else:
                        raise

            match (kind_sync, kind_async):
                case ("value", "value"):
                    kind = "value"
                case ("exception", "async"):
                    kind = "async"
                case ("value", "exception"):
                    kind = "sync"
                case _:
                    raise ValueError(f"{kind_sync=} {kind_async=}")

            expected_kind = get_meta("Connection", name, "attribute" if is_attr else "function")

            self.assertEqual(kind, expected_kind, f"Connection {name=}")


class SimpleController:
    # This is awaitable that blocks the caller
    class Request:
        call: Callable
        event: threading.Event
        result: Any
        is_exception: bool

        def __await__(self):
            if False:
                yield
            self.event.wait()
            if self.is_exception:
                raise self.result
            return self.result

    def __init__(self):
        self.queue = queue.SimpleQueue()
        threading.Thread(daemon=True, target=self.worker_thread_run, args=(self.queue,)).start()

    def close(self):
        self.queue.put(None)
        self.queue = None

    def send(self, call):
        req = SimpleController.Request()
        req.event = threading.Event()
        req.call = call
        self.queue.put(req)
        return req

    def worker_thread_run(self, q):
        while (req := q.get()) is not None:
            try:
                req.result = req.call()
                req.is_exception = False
            except BaseException as exc:
                req.result = exc
                req.is_exception = True
            req.event.set()


class RawController:
    "This is used to find out what an API returned"

    class Request:
        call: Callable
        event: threading.Event
        result: Any
        is_exception: bool

    def __init__(self):
        self.queue = queue.SimpleQueue()
        threading.Thread(daemon=True, target=self.worker_thread_run, args=(self.queue,)).start()

    def close(self):
        self.queue.put(None)
        self.queue = None

    def send(self, call):
        req = RawController.Request()
        req.event = threading.Event()
        req.call = call
        self.queue.put(req)
        req.event.wait()
        if req.is_exception:
            raise req.result
        return req.result

    def worker_thread_run(self, q):
        while (req := q.get()) is not None:
            try:
                req.result = req.call()
                req.is_exception = False
            except BaseException as exc:
                req.result = exc
                req.is_exception = True
            req.event.set()


__all__ = ("Async",)

if __name__ == "__main__":
    unittest.main()
