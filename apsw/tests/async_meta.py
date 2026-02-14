#!/usr/bin/env python3

# This testing code deliberately does nasty stuff so mypy isn't helpful
# mypy: ignore-errors
# type: ignore


# This code verifies the async_meta.json file and the code match each
# other.  It also provides a get_meta function used when building type
# stubs and documentation.

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


# The result is one of
# sync   - can only be used on sync connections (async gives exception)
# async  - can only be used on async connections (sync gives exception)
# dual   - returns awaitable in async, value in sync
# value  - returns a value in sync or async
#
# for attributes dual means awaitable get and worker thread set
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
        case "__enter__" | "__exit__" | "__iter__" | "__next__":
            return "sync"
        case "__aenter__" | "__aexit__" | "aclose" | "__aiter__" | "__anext__":
            # aiter is technically value but we only want it on async objects
            # so that is handled elsewhere
            return "async"
        case "__repr__" | "__str__" | "__bool__" | "close":
            return "value"
        case _:
            return "dual" if kind == "function" else "value"

# Classes where we fake there being an async version
ASYNCABLE = {"Connection", "Cursor", "Blob", "Backup", "Session"}


def sync_await(obj):
    it = obj.__await__()
    try:
        while True:
            next(it)
    except StopIteration as exc:
        return exc.value



skip = set(dir(object())) - {"__repr__", "__str__"}


def is_method(object, name):
    return inspect.ismethoddescriptor(getattr(type(object), name))


class AsyncMeta(unittest.TestCase):
    def tearDown(self):
        for c in apsw.connections():
            c.close(True)

    def classifyOne(self, send, is_attr, object, klass, member, value=None):
        # send is None for async access, callable for sync
        if is_attr:
            try:
                if send:
                    if value is not None:
                        send(functools.partial(setattr, object, member, value))
                    else:
                        send(functools.partial(getattr, object, member))
                else:
                    v = getattr(object, member) if value is None else setattr(object, member, value)
                    if hasattr(v, "__await__"):
                        return "async"
                return "value"
            except TypeError as exc:
                if not send and exc.args[0] == "Using sync method in async context":
                    return "exception"
                if send and exc.args[0] == "Using async method in sync context":
                    return "exception"
                raise

        if member in {"__aexit__", "__exit__"}:
            args = (None, None, None)
        else:
            args = tuple()

            match klass:
                case "Connection":
                    match member:
                        case "async_run":
                            args = (lambda : 1,)
                        case "autovacuum_pages" | "collation_needed":
                            args = (lambda *args: 1 / 0,)
                        case "backup":
                            args = "main", apsw.Connection(""), "main"
                        case "blob_open":
                            args = "main", "dummy", "column", 73, False
                        case "column_metadata":
                            args = "main", "dummy", "column"
                        case "config":
                            args = apsw.SQLITE_DBCONFIG_ENABLE_FKEY, -1
                        case (
                            "create_aggregate_function"
                            | "create_collation"
                            | "create_module"
                            | "create_scalar_function"
                            | "create_window_function"
                            | "register_fts5_function"
                            | "register_fts5_tokenizer"
                        ):
                            args = "foo", lambda *args: 1 / 0
                        case "db_filename" | "readonly" | "serialize" | "vfsname":
                            args = ("main",)
                        case "deserialize":
                            args = "main", apsw.Connection("").serialize("main")
                        case (
                            "drop_modules"
                            | "preupdate_hook"
                            | "set_authorizer"
                            | "set_busy_handler"
                            | "set_commit_hook"
                            | "set_exec_trace"
                            | "set_profile"
                            | "set_progress_handler"
                            | "set_rollback_hook"
                            | "set_row_trace"
                            | "set_update_hook"
                            | "set_wal_hook"
                        ):
                            args = (None,)
                        case (
                            "enable_load_extension"
                            | "set_busy_timeout"
                            | "set_last_insert_rowid"
                            | "wal_autocheckpoint"
                        ):
                            # some of these depend on True being subclass of int
                            args = (True,)
                        case "execute":
                            args = ("select 3",)
                        case "executemany":
                            args = "select ?", ((i,) for i in range(10))
                        case "file_control":
                            args = "main", 12343, 0
                        case "fts5_tokenizer" | "fts5_tokenizer_available":
                            args = ("ascii",)
                        case "limit":
                            args = (apsw.SQLITE_LIMIT_COLUMN,)
                        case "load_extension":
                            args = ("this does not exist I hope",)
                        case "overload_function":
                            args = "dummy", 33
                        case "pragma":
                            args = ("user_version",)
                        case "read":
                            args = "main", 0, 0, 4096
                        case "setlk_timeout":
                            args = 1, 1
                        case "status":
                            args = (apsw.SQLITE_DBSTATUS_LOOKASIDE_USED,)
                        case "table_exists":
                            args = (None, "hello")
                        case "trace_v2" | "vtab_config":
                            args = (0,)
                case "Cursor":
                    match member:
                        case "execute":
                            args = ("select 3",)
                        case "executemany":
                            args = "insert into dummy values(?)", ((i,) for i in range(5))
                        case "set_exec_trace" | "set_row_trace":
                            args = (None,)
                case "Blob":
                    match member:
                        case "read":
                            args = (1,)
                        case "read_into":
                            args = bytearray(10), 0, 1
                        case "reopen":
                            args = (74,)
                        case "seek":
                            args = (1,)
                        case "write":
                            args = (b"a",)

                case "Session":
                    match member:
                        case "changeset_stream" | "patchset_stream" | "table_filter":
                            args = (lambda x: True,)
                        case "config":
                            args = apsw.SQLITE_SESSION_OBJCONFIG_SIZE, -1
                        case "diff":
                            args = "other", "dummy"

        try:
            if send:
                sync_await(send(functools.partial(getattr(object, member), *args)))
            else:
                if hasattr(getattr(object, member)(*args), "__await__"):
                    return "async"
            return "value"
        except TypeError as exc:
            if not send and exc.args[0] == "Using sync in async context":
                return "exception"
            if send and exc.args[0] == "Using async in sync context":
                return "exception"
            raise
        except apsw.ExtensionLoadingError:
            return "value"
        except apsw.SQLError:
            if (klass, member) == ("Connection", "read"):
                return "value"
            raise
        except apsw.InvalidContextError:
            if klass == "Connection" and member in {"vtab_config", "vtab_on_conflict"}:
                return "value"
            raise

    def testMetaJson(self):
        apsw.async_controller.set(SimpleController)

        objects = {
            "Connection": None,
            "Cursor": None,
            "Blob": None,
            "Backup": None,
            "Session": None,
            # Changeset.apply is too difficult to include
        }

        assert ASYNCABLE == set(objects.keys())

        changeset = None

        def ensure_objects(klass):
            # various operations result in the database etc being closed
            # so this ensures they are open but also some things can't be
            # done while active cursors etc are in play
            nonlocal changeset

            if objects["Connection"] is None:
                self.tearDown()
                for k in objects:
                    objects[k] = None

            if klass != "Connection":
                ensure_objects("Connection")

            if not objects[klass]:
                match klass:
                    case "Connection":
                        value = sync_await(apsw.Connection.as_async(""))
                        sync_await(
                            value.execute("""
                                create table dummy(column);
                                create table dummy2(column INTEGER PRIMARY KEY);
                                insert into dummy(rowid, column) values(73, x'aabbcc'), (74, x'aabbcc');
                                """)
                        )
                    case "Cursor":
                        value = sync_await(objects["Connection"].execute("SELECT ?, * from dummy", ("hello",)))
                    case "Blob":
                        value = sync_await(objects["Connection"].blob_open("main", "dummy", "column", 73, True))
                    case "Backup":
                        value = sync_await(objects["Connection"].backup("main", apsw.Connection(""), "main"))
                    case "Session":
                        sync_await(objects["Connection"].execute("attach ':memory:' as other; create table other.dummy(column INTEGER PRIMARY KEY)"))
                        value = sync_await(apsw.aio.make_session(objects["Connection"], "main"))
                        sync_await(value.attach())
                        sync_await(
                            objects["Connection"].execute("""
                        insert into dummy2 values(1), (2), (3), (4);
                        """)
                        )
                        if changeset is None:
                            changeset = sync_await(value.changeset())

                    case _:
                        1 / 0
                objects[klass] = value

        def all_the_things():
            res = []
            for name in objects:
                ensure_objects(name)
                for attr in dir(objects[name]):
                    if attr in skip or attr in {"as_async", "async_run", "__next__", "aclose"} or (name, attr) in old_names:
                        continue
                    res.append((name, attr))
                self.tearDown()
                for name in objects:
                    objects[name] = None
            return res

        last_klass = None

        # potentially screw up functioning
        malfunction = {
            "cursor_factory",
            "exec_trace",
            "row_trace",
            "finish",
            "afinish",
            "close",
            "aclose",
            "__exit__",
            "__aexit__",
            "finish",
        }

        pre = {"__aexit__": "__aenter__", "__exit__": "__enter__", "__next__": "__iter__", "__anext__": "__aiter__"}

        for klass, name in all_the_things():
            with self.subTest(klass=klass, name=name):

                if klass != last_klass or name in malfunction:
                    objects["Connection"] = None

                ensure_objects(klass)

                is_attr = not is_method(objects[klass], name)

                ensure_objects(klass)

                if name in pre:
                    self.classifyOne(None, is_attr, objects[klass], klass, pre[name])

                kind_async = self.classifyOne(None, is_attr, objects[klass], klass, name)

                if name in malfunction:
                    objects["Connection"] = None

                ensure_objects(klass)

                if name in pre:
                    self.classifyOne(objects["Connection"].async_controller.send, is_attr, objects[klass], klass, pre[name])

                kind_sync = self.classifyOne(
                    objects["Connection"].async_controller.send, is_attr, objects[klass], klass, name
                )

                if is_attr:
                    # check writable (mutex assertions)
                    match name:
                        case "transaction_mode":
                            value = "DEFERRED"
                        case "enabled" | "indirect":
                            value = True
                        case _:
                            value = lambda *args: False
                    try:
                        setattr(objects[klass], name, value)
                    except AttributeError as exc:
                        if "objects is not writable" in str(exc) or "readonly attribute" in str(exc):
                            pass
                        else:
                            raise
                    except TypeError as exc:
                        if exc.args[0] in {"Using sync in async context", "Using async in sync context"}:
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
                    case ("value", "async"):
                        kind = "dual"
                    case ("exception", "value"):
                        kind = "async"
                    case _:
                        raise ValueError(f"{kind_sync=} {kind_async=}")

                expected_kind = get_meta(klass, name, "attribute" if is_attr else "function")

                self.assertEqual(kind, expected_kind, f"{klass=} {name=}")


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

    def configure(self, db):
        pass

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


# generated from tools/renames.json
old_names = {
    ("apsw", "apswversion"),
    ("apsw", "enablesharedcache"),
    ("apsw", "exceptionfor"),
    ("apsw", "memoryhighwater"),
    ("apsw", "memoryused"),
    ("apsw", "releasememory"),
    ("apsw", "softheaplimit"),
    ("apsw", "sqlitelibversion"),
    ("apsw", "vfsnames"),
    ("Backup", "pagecount"),
    ("Blob", "readinto"),
    ("Connection", "blobopen"),
    ("Connection", "cacheflush"),
    ("Connection", "collationneeded"),
    ("Connection", "createaggregatefunction"),
    ("Connection", "createcollation"),
    ("Connection", "createmodule"),
    ("Connection", "createscalarfunction"),
    ("Connection", "enableloadextension"),
    ("Connection", "exectrace"),
    ("Connection", "filecontrol"),
    ("Connection", "getautocommit"),
    ("Connection", "getexectrace"),
    ("Connection", "getrowtrace"),
    ("Connection", "loadextension"),
    ("Connection", "overloadfunction"),
    ("Connection", "rowtrace"),
    ("Connection", "setauthorizer"),
    ("Connection", "setbusyhandler"),
    ("Connection", "setbusytimeout"),
    ("Connection", "setcommithook"),
    ("Connection", "setexectrace"),
    ("Connection", "setprofile"),
    ("Connection", "setprogresshandler"),
    ("Connection", "setrollbackhook"),
    ("Connection", "setrowtrace"),
    ("Connection", "setupdatehook"),
    ("Connection", "setwalhook"),
    ("Connection", "sqlite3pointer"),
    ("Connection", "totalchanges"),
    ("Cursor", "exectrace"),
    ("Cursor", "getconnection"),
    ("Cursor", "getdescription"),
    ("Cursor", "getexectrace"),
    ("Cursor", "getrowtrace"),
    ("Cursor", "rowtrace"),
    ("Cursor", "setexectrace"),
    ("Cursor", "setrowtrace"),
}


if __name__ == "__main__":
    unittest.main()
