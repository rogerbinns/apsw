#!/usr/bin/env python3

# This testing code deliberately does nasty stuff so mypy isn't helpful
# mypy: ignore-errors
# type: ignore

import threading
import unittest

import apsw
import apsw.aio

def run(coro):
    try:
        while True:
            coro.send(None)
    except StopIteration as exc:
        return exc.value


class Async(unittest.TestCase):

    def testOverwrite(self):
        "make sure module contextvars can't be overwritten"
        for name in "async_controller", "async_run_coro", "async_cursor_prefetch":
            self.assertRaisesRegex(AttributeError, ".*Do not overwrite apsw.*context", setattr, apsw, name, 3)


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