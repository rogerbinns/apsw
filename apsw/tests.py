#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# See the accompanying LICENSE file.

# This testing code deliberately does nasty stuff so mypy isn't helpful
# mypy: ignore-errors

import array
import collections
import collections.abc
import contextlib
import dataclasses
import gc
import getpass
import glob
import inspect
import io
import itertools
import json
import logging
import math
import mmap
import os
import pickle
import platform
import queue
import random
import re
import shlex
import shutil
import struct
import sys
import tempfile
import textwrap
import threading
import time
import traceback
import typing
import warnings


def ShouldFault(name, pending_exception):
    # You can't use print because calls involving print can be nested
    # and will then have a fatal exception.  Directly writing
    # to stderr is ok like this:
    #
    #     os.write(2, f"SF { name } { pending_exception }\n".encode("utf8"))
    return False


sys.apsw_should_fault = ShouldFault

import apsw

if not all(hasattr(apsw, name) for name in ("initialize", "format_sql_value")):
    msg = """
The compiled portion of APSW is not present where the tests are being run.
It needs to be compiled and in-place first.
    """.strip()
    sys.exit(msg)

import apsw.bestpractice
import apsw.ext
import apsw.shell


def print_version_info():
    print("                Python ", sys.executable, sys.version_info, " ".join(platform.architecture()))
    print("Testing with APSW file ", apsw.__file__)
    print("          APSW version ", apsw.apsw_version())
    print("    SQLite lib version ", apsw.sqlite_lib_version())
    print("SQLite headers version ", apsw.SQLITE_VERSION_NUMBER)
    print("    Using amalgamation ", apsw.using_amalgamation, flush=True)


# sigh
iswindows = sys.platform in ('win32', )

# prefix for test files (eg if you want it on tmpfs)
TESTFILEPREFIX = os.environ.get("APSWTESTPREFIX", "")

# size of C int in bits
sizeof_c_int = len(struct.pack('i', 0)) * 8


def read_whole_file(name, mode, encoding=None):
    if "t" in mode and not encoding:
        encoding = "utf8"
    if encoding:
        f = open(name, mode, encoding=encoding)
    else:
        f = open(name, mode)
    try:
        return f.read()
    finally:
        f.close()


# If two is present then one is encoding
def write_whole_file(name, mode, data, *, encoding=None):
    if "t" in mode and not encoding:
        encoding = "utf8"
    if encoding:
        f = open(name, mode, encoding=encoding)
    else:
        f = open(name, mode)
    try:
        f.write(data)
    finally:
        f.close()


# unittest stuff from here on

import unittest

try:
    import ctypes

    import _ctypes
except:
    ctypes = None
    _ctypes = None

# yay
is64bit = ctypes and ctypes.sizeof(ctypes.c_size_t) >= 8

# Make curnext switch between the iterator and fetchone alternately
_nextcounter = 0


def curnext(cursor, *args):
    global _nextcounter
    _nextcounter += 1
    if _nextcounter % 2:
        return next(cursor, *args)
    res = cursor.fetchone()
    if res is None:
        if args:
            return args[0]
        return None
    return res


# got here via apsw_write_unraisable in C code
def ehook(etype, evalue, etraceback):
    sys.stderr.write("Unraisable exception " + str(etype) + ":" + str(evalue) + "\n")
    traceback.print_tb(etraceback)


if sys.excepthook == sys.__excepthook__:
    sys.excepthook = ehook


# helper functions
def randomintegers(howmany):
    for i in range(howmany):
        yield (random.randint(0, 9999999999), )


def randomstring(length):
    l = list("abcdefghijklmnopqrstuvwxyz0123456789")
    while len(l) < length:
        l.extend(l)
    l = l[:length]
    random.shuffle(l)
    return "".join(l)


# An instance of this class is used to get the -1 return value to the
# C api PyObject_IsTrue
class BadIsTrue(int):

    def __bool__(self):
        1 / 0


# helper class - runs code in a separate thread
class ThreadRunner(threading.Thread):

    def __init__(self, callable, *args, **kwargs):
        threading.Thread.__init__(self)
        self.daemon - True
        self.callable = callable
        self.args = args
        self.kwargs = kwargs
        self.q = queue.Queue()
        self.started = False

    def start(self):
        if not self.started:
            self.started = True
            threading.Thread.start(self)

    def go(self):
        self.start()
        t, res = self.q.get()
        if t:  # result
            return res
        else:  # exception
            raise res[1].with_traceback(res[2])

    def run(self):
        try:
            self.q.put((True, self.callable(*self.args, **self.kwargs)))
        except:
            self.q.put((False, sys.exc_info()))


# Windows doesn't allow files that are open to be deleted.  Even after
# we close them, tagalongs such as virus scanners, tortoisesvn etc can
# keep them open.  But the good news is that we can rename a file that
# is in use.  This background thread does the background deletions of the
# renamed files
def bgdel():
    q = bgdelq
    while True:
        name = q.get()
        while os.path.exists(name):
            try:
                if os.path.isfile(name):
                    os.remove(name)
                else:
                    shutil.rmtree(name)
            except:
                pass
            if os.path.exists(name):
                time.sleep(0.1)


bgdelq = queue.Queue()
bgdelthread = threading.Thread(target=bgdel)
bgdelthread.daemon = True
bgdelthread.start()


def deletefile(name):
    try:
        os.remove(name)
    except:
        pass
    l = list("abcdefghijklmn")
    random.shuffle(l)
    newname = name + "-n-" + "".join(l)
    count = 0
    while os.path.exists(name):
        count += 1
        try:
            os.rename(name, newname)
        except:
            if count > 30:  # 3 seconds we have been at this!
                # So give up and give it a stupid name.  The sooner
                # this so called operating system withers into obscurity
                # the better
                n = list("abcdefghijklmnopqrstuvwxyz")
                random.shuffle(n)
                n = "".join(n)
                try:
                    os.rename(name, "windowssucks-" + n + ".deletememanually")
                except:
                    pass
                break
            # Make windows happy
            time.sleep(0.1)
            gc.collect()
    if os.path.exists(newname):
        bgdelq.put(newname)
        # Give bg thread a chance to run
        time.sleep(0.1)


openflags = apsw.SQLITE_OPEN_READWRITE | apsw.SQLITE_OPEN_CREATE | apsw.SQLITE_OPEN_URI


# main test class/code
class APSW(unittest.TestCase):

    maxDiff = 16384

    connection_nargs={ # number of args for function.  those not listed take zero
        'create_aggregate_function': 2,
        'create_collation': 2,
        'create_scalar_function': 3,
        'collation_needed': 1,
        'set_authorizer': 1,
        'set_busy_handler': 1,
        'set_busy_timeout': 1,
        'set_commit_hook': 1,
        'set_profile': 1,
        'set_rollback_hook': 1,
        'set_update_hook': 1,
        'set_progress_handler': 2,
        'enableloadextension': 1,
        'create_module': 2,
        'file_control': 3,
        'set_exec_trace': 1,
        'set_row_trace': 1,
        '__enter__': 0,
        '__exit__': 3,
        'backup': 3,
        'wal_autocheckpoint': 1,
        'set_wal_hook': 1,
        'readonly': 1,
        'db_filename': 1,
        'set_last_insert_rowid': 1,
        'serialize': 1,
        'deserialize': 2,
        'autovacuum_pages': 1,
        }

    cursor_nargs = {
        'execute': 1,
        'executemany': 2,
        'set_exec_trace': 1,
        'set_row_trace': 1,
    }

    blob_nargs = {'write': 1, 'read': 1, 'read_into': 1, 'reopen': 1, 'seek': 2}

    def deltempfiles(self):
        for name in ("testdb", "testdb2", "testdb3", "testfile", "testfile2", "testdb2x", "test-shell-1",
                     "test-shell-1.py", "test-shell-in", "test-shell-out", "test-shell-err"):
            for i in "-shm", "-wal", "-journal", "":
                if os.path.exists(TESTFILEPREFIX + name + i):
                    deletefile(TESTFILEPREFIX + name + i)

    def setUp(self):
        apsw.config(apsw.SQLITE_CONFIG_LOG, None)
        apsw.connection_hooks = []
        # clean out database and journals from last runs
        for c in apsw.connections():
            c.close()
        gc.collect()
        self.deltempfiles()
        self.db = apsw.Connection(TESTFILEPREFIX + "testdb", flags=openflags)
        self.warnings_filters = warnings.filters
        # zipvfs causes some test failures - issue #394
        for name in (
                'zipvfs',
                'cksmvfs',
                'zipvfsonly',
                'multiplex',
        ):
            if name in apsw.vfs_names():
                apsw.unregister_vfs(name)

    def tearDown(self):
        apsw.config(apsw.SQLITE_CONFIG_LOG, None)
        if self.db is not None:
            self.db.close(True)
        del self.db
        for c in apsw.connections():
            c.close()
        gc.collect()
        self.deltempfiles()
        warnings.filters = self.warnings_filters
        getattr(warnings, "_filters_mutated", lambda: True)()

    def suppressWarning(self, name):
        if hasattr(__builtins__, name):
            warnings.simplefilter("ignore", getattr(__builtins__, name))

    def assertRaisesRoot(self, exctype, *args, **kwargs):
        # With chained exceptions verifies the first exception raised matches type
        # assertRaises checks the last raised type not the root cause
        if not args and not kwargs:  # context manager

            class mgr:

                def __enter__(self):
                    return self

                def __exit__(innerself, etype, e, etb):
                    if not etype or not e:
                        self.fail("Expected an exception in assertRaisesRoot context manager")
                    while e.__context__:
                        e = e.__context__
                    self.assertIsInstance(e, exctype)
                    return True

            return mgr()
        try:
            return args[0](*args[1:], **kwargs)
        except BaseException as e:
            while e.__context__:
                e = e.__context__
            self.assertIsInstance(e, exctype)

    def assertTableExists(self, tablename):
        self.assertTrue(self.db.table_exists(None, tablename))

    def assertTableNotExists(self, tablename):
        self.assertFalse(self.db.table_exists(None, tablename))

    def assertTablesEqual(self, dbl, left, dbr, right):
        # Ensure tables have the same contents.  Rowids can be
        # different and select gives unordered results so this is
        # quite challenging
        l = dbl.cursor()
        r = dbr.cursor()
        # check same number of rows
        lcount = l.execute("select count(*) from [" + left + "]").get
        rcount = r.execute("select count(*) from [" + right + "]").get
        self.assertEqual(lcount, rcount)
        # check same number and names and order for columns
        lnames = [row[1] for row in l.execute("pragma table_info([" + left + "])")]
        rnames = [row[1] for row in r.execute("pragma table_info([" + left + "])")]
        self.assertEqual(lnames, rnames)
        # read in contents, sort and compare
        lcontents = l.execute("select * from [" + left + "]").fetchall()
        rcontents = r.execute("select * from [" + right + "]").fetchall()
        lcontents.sort(key=lambda x: repr(x))
        rcontents.sort(key=lambda x: repr(x))
        self.assertEqual(lcontents, rcontents)

    def assertRaisesUnraisable(self, exc, func, *args, **kwargs):
        return self.baseAssertRaisesUnraisable(True, exc, func, args, kwargs)

    def assertMayRaiseUnraisable(self, exc, func, *args, **kwargs):
        """Like assertRaisesUnraisable but no exception may be raised.

        If one is raised, it must have the expected type.
        """
        return self.baseAssertRaisesUnraisable(False, exc, func, args, kwargs)

    def baseAssertRaisesUnraisable(self, must_raise, exc, func, args, kwargs):
        orig = sys.excepthook, getattr(sys, "unraisablehook", None)
        try:
            called = []

            def ehook(*args):
                if len(args) == 1:
                    t = args[0].exc_type
                    v = args[0].exc_value
                    tb = args[0].exc_traceback
                else:
                    t, v, tb = args
                called.append((t, v, tb))

            sys.excepthook = sys.unraisablehook = ehook

            try:
                try:
                    return func(*args, **kwargs)
                except:
                    # This ensures frames have their local variables
                    # cleared before we put the original excepthook
                    # back.  Clearing the variables results in some
                    # more SQLite operations which also can raise
                    # unraisables.  traceback.clear_frames was
                    # introduced in Python 3.4 and unittest was
                    # updated to call it in assertRaises.  See issue
                    # 164
                    if hasattr(traceback, "clear_frames"):
                        traceback.clear_frames(sys.exc_info()[2])
                    raise
            finally:
                if must_raise and len(called) < 1:
                    self.fail("Call %s(*%s, **%s) did not do any unraisable" % (func, args, kwargs))
                if len(called):
                    self.assertIsInstance(called[0][1], exc)  # check it was the correct type
        finally:
            sys.excepthook, sys.unraisablehook = orig

    def testSanity(self):
        "Check all parts compiled and are present"
        # check some error codes etc are present - picked first middle and last from lists in code
        apsw.SQLError
        apsw.MisuseError
        apsw.NotADBError
        apsw.ThreadingViolationError
        apsw.BindingsError
        apsw.ExecTraceAbort
        apsw.SQLITE_FCNTL_SIZE_HINT
        apsw.mapping_file_control["SQLITE_FCNTL_SIZE_HINT"] == apsw.SQLITE_FCNTL_SIZE_HINT
        apsw.URIFilename
        apsw.SQLITE_INDEX_CONSTRAINT_NE  # ticket 289
        self.assertTrue(len(apsw.sqlite3_sourceid()) > 10)

    def testModuleExposed(self):
        "Check what is exposed and usage"
        for name in "Connection", "Cursor", "Blob", "Backup", "zeroblob", "VFS", "VFSFile", "URIFilename":
            self.assertTrue(hasattr(apsw, name), "expected name apsw." + name)

        for name in "Blob", "Backup":
            self.assertRaisesRegex(TypeError, "cannot create .* instances", getattr(apsw, name))

    def testConnection(self):
        "Test connection opening"
        # bad keyword arg
        self.assertRaises(TypeError, apsw.Connection, ":memory:", user="nobody")
        # wrong types
        self.assertRaises(TypeError, apsw.Connection, 3)
        # bad file (cwd)
        self.assertRaises(apsw.CantOpenError, apsw.Connection, ".")
        # bad open flags can't be tested as sqlite accepts them all - ticket #3037
        # self.assertRaises(apsw.CantOpenError, apsw.Connection, "<non-existent-file>", flags=65535)

        # bad vfs
        self.assertRaises(TypeError, apsw.Connection, "foo", vfs=3, flags=-1)
        self.assertRaises(apsw.SQLError, apsw.Connection, "foo", vfs="jhjkds")

        self.assertIsInstance(self.db.system_errno, int)

    def testConnectionFileControl(self):
        "Verify sqlite3_file_control"
        # Note that testVFS deals with success cases and the actual vfs backend
        self.assertRaises(TypeError, self.db.file_control, 1, 2)
        self.assertRaises(TypeError, self.db.file_control, "main", 1001, "foo")
        self.assertRaises(OverflowError, self.db.file_control, "main", 1001, 45236748972389749283)
        self.assertEqual(self.db.file_control("main", 1001, 25), False)
        self.assertRaises(apsw.SQLError, self.db.read, "kljlkjlkj", 0, 0, 10)
        self.assertRaises(TypeError, self.db.read, "kljlkjlkj", 0, deliberate="error")
        self.assertRaises(ValueError, self.db.read, "kljlkjlkj", 0, -1, -1)
        self.assertRaises(ValueError, self.db.read, "kljlkjlkj", 0, 0, -1)
        self.assertRaises(ValueError, self.db.read, "main", 77, 0, 1)
        self.assertRaises(ValueError, self.db.read, "main", 0, -1, 1)

        # Connection.read fails on memory databases
        db2 = apsw.Connection("")
        db2.pragma("user_version", 2)
        self.assertRaises(apsw.SQLError, db2.read, "main", 0, 0, 16384)
        db2.close()

        self.assertEqual((False, b"\0" * 10), self.db.read("main", 0, 50 * 1024 * 1024, 10))
        # db and journal are empty unless we do something
        self.db.execute("create table foo(x); begin immediate; insert into foo values(3); insert into foo values(4)")
        self.assertEqual((True, b'SQLite format 3'), self.db.read("main", 0, 0, 15))
        self.assertTrue(any(b != 0 for b in self.db.read("main", 1, 0, 15)[1]))

    def testConnectionVfsname(self):
        "Verify vfsname function"
        self.assertRaises(TypeError, self.db.vfsname, 1, 2)
        self.assertRaises(TypeError, self.db.vfsname, b"main")
        self.assertRaises(TypeError, self.db.vfsname)

        # our db should be the default - also handle multipleciphers shim
        self.assertTrue(
            self.db.vfsname("main") == apsw.vfs_names()[0]
            or self.db.vfsname("main").startswith(apsw.vfs_names()[0] + "/")
        )
        # temp always gives none
        self.assertEqual(None, self.db.vfsname("temp"))

        # as does nonsense
        self.assertEqual(None, self.db.vfsname("nonsense"))
        self.db.pragma("user_version", 3)
        self.db.execute(f"attach '{self.db.filename}' as nonsense")
        self.assertEqual(self.db.vfsname("main"), self.db.vfsname("nonsense"))

        class CustomVFS(apsw.VFS):
            def __init__(self):
                super().__init__("custom", "")

            def xOpen(self, name, flags):
                return CorrectHorseBatteryStaple(name, flags)

        class CorrectHorseBatteryStaple(apsw.VFSFile):
            def __init__(self, name, flags):
                super().__init__("", name, flags)

        xx = CustomVFS()

        con2 = apsw.Connection(self.db.filename, vfs="custom")

        self.assertIn("CorrectHorseBatteryStaple", con2.vfsname("main"))

        class CustomVFS2(apsw.VFS):
            def __init__(self):
                super().__init__("custom2", "")

        xx2 = CustomVFS2()

        con3 = apsw.Connection(self.db.filename, vfs="custom2")

        self.assertTrue(con3.vfsname("main").endswith("/" + self.db.vfsname("main")))

    def testConnectionConfig(self):
        "Test Connection.config function"
        self.assertRaises(TypeError, self.db.config)
        self.assertRaises(TypeError, self.db.config, "three")
        x = 0x7fffffff
        self.assertRaises(OverflowError, self.db.config, x * x * x * x * x)
        self.assertRaises(ValueError, self.db.config, 82397)
        self.assertRaises(TypeError, self.db.config, apsw.SQLITE_DBCONFIG_ENABLE_FKEY, "banana")
        for i in apsw.SQLITE_DBCONFIG_ENABLE_FKEY, apsw.SQLITE_DBCONFIG_ENABLE_TRIGGER, apsw.SQLITE_DBCONFIG_ENABLE_QPSG:
            self.assertEqual(1, self.db.config(i, 1))
            self.assertEqual(1, self.db.config(i, -1))
            self.assertEqual(0, self.db.config(i, 0))
        self.assertEqual(0, self.db.config(apsw.SQLITE_DBCONFIG_REVERSE_SCANORDER, -1))
        self.db.pragma("reverse_unordered_selects", 1)
        self.assertEqual(1, self.db.config(apsw.SQLITE_DBCONFIG_REVERSE_SCANORDER, -1))
        self.db.config(apsw.SQLITE_DBCONFIG_REVERSE_SCANORDER, 0)
        self.assertEqual(0, self.db.pragma("reverse_unordered_selects"))

    def testConnectionMetadata(self):
        "Test uses of sqlite3_table_column_metadata"
        self.db.create_collation("BreadFruit", lambda x, y: 1)
        self.db.execute("""
            create table table1(x);
            create table temp.table2(x);

            create table table3(
                one  INTEGER PRIMARY KEY AUTOINCREMENT,
                two  Bananas COLLATE BreadFruit,
                three [   ] NOT NULL);
            """)
        self.assertRaises(TypeError, self.db.table_exists, 3, 4)
        self.assertRaises(TypeError, self.db.column_metadata, 1, 2, 3)
        self.assertTrue(self.db.table_exists(None, "table1"))
        self.assertTrue(self.db.table_exists("main", "table1"))
        self.assertTrue(self.db.table_exists(None, "table2"))
        self.assertTrue(self.db.table_exists("temp", "table2"))
        self.assertFalse(self.db.table_exists("main", "table2"))

        for colname, expected in (
            ("one", ('INTEGER', 'BINARY', False, True, True)),
            ("two", ('Bananas', 'BreadFruit', False, False, False)),
            ("three", ('   ', 'BINARY', True, False, False)),
        ):
            self.assertEqual(self.db.column_metadata(None, "table3", colname), expected)
        self.assertRaises(apsw.SQLError, self.db.column_metadata, "not a db", "not a table", "not a column")

    def testConnectionNames(self):
        "Test Connection.db_names"
        self.assertRaises(TypeError, self.db.db_names, 3)
        expected = ["main", "temp"]
        self.assertEqual(expected, self.db.db_names())
        for t in "", APSW.wikipedia_text:
            self.db.cursor().execute(f"attach '{ self.db.db_filename('main') }' as '{ t }'")
            expected.append(t)
        self.assertEqual(expected, self.db.db_names())
        while True:
            t = f"{ expected[-1] }-{ len(expected) }"
            try:
                self.db.cursor().execute(f"attach '{ self.db.db_filename('main') }' as '{ t }'")
            except apsw.SQLError:
                # SQLError: too many attached databases - max ....
                break
            expected.append(t)
        self.assertEqual(expected, self.db.db_names())
        while len(expected) > 2:
            i = random.randint(2, len(expected) - 1)
            self.db.cursor().execute(f"detach '{ expected[i] }'")
            del expected[i]
            self.assertEqual(expected, self.db.db_names())

    def testBackwardsCompatibility(self):
        "Verifies changed names etc are still accessible through the old ones"
        self.assertIs(apsw.main, apsw.shell.main)
        self.assertIs(apsw.Shell, apsw.shell.Shell)

    def testModuleStringFunctions(self):
        "Tests various string comparison/matching functions"
        self.assertRaises(TypeError, apsw.strlike, 1, 2, 3)
        self.assertRaises(TypeError, apsw.strlike, None, "3", 3)
        self.assertEqual(0, apsw.strlike("%c", "abc"))
        self.assertNotEqual(0, apsw.strlike("q%c", "abc"))
        self.assertRaises(TypeError, apsw.strglob, 1, 2)
        self.assertRaises(TypeError, apsw.strglob, None, None)
        self.assertEqual(0, apsw.strglob("*.*", "abc.txt"))
        self.assertNotEqual(0, apsw.strglob("b*.*", "abc.txt"))
        self.assertRaises(TypeError, apsw.stricmp, None, "s")
        self.assertRaises(TypeError, apsw.strnicmp, "s", "s", "s")
        self.assertEqual(0, apsw.stricmp("ABC", "abc"))
        self.assertNotEqual(0, apsw.stricmp("ABC", "abcd"))
        self.assertEqual(0, apsw.strnicmp("ABC", "abc", 77))
        self.assertEqual(0, apsw.strnicmp("ABC", "abcd", 3))
        self.assertNotEqual(0, apsw.strnicmp("ABC", "abcd", 4))

    def testCursorFactory(self):
        "Test Connection.cursor_factory"
        seqbindings = ((3, ), ) * 3
        self.assertEqual(self.db.cursor_factory, apsw.Cursor)
        for not_callable in (None, apsw, 3):
            try:
                self.db.cursor_factory = not_callable
                1 / 0
            except TypeError:
                pass

        def error():
            1 / 0

        self.db.cursor_factory = error
        self.assertRaises(TypeError, self.db.execute, "select 3")
        self.assertRaises(TypeError, self.db.executemany, "select 3", seqbindings)

        def error(_):
            return 3

        self.db.cursor_factory = error
        self.assertRaises(TypeError, self.db.execute, "select 3")
        self.assertRaises(TypeError, self.db.executemany, "select 3")

        class error:

            def __init__(self, _):
                pass

        self.db.cursor_factory = error
        self.assertRaises(AttributeError, self.db.execute, "select 3")
        self.assertRaises(AttributeError, self.db.executemany, "select ?", seqbindings)

        class inherits(apsw.Cursor):
            pass

        self.db.cursor_factory = inherits
        self.assertEqual(self.db.execute("select 3").fetchall(), self.db.cursor().execute("select 3").fetchall())
        self.assertEqual(
            self.db.executemany("select ?", seqbindings).fetchall(),
            self.db.cursor().executemany("select ?", seqbindings).fetchall())
        # kwargs
        self.assertEqual(
            self.db.execute(bindings=tuple(), statements="select 3").fetchall(),
            self.db.cursor().execute(bindings=None, statements="select 3").fetchall())
        self.assertEqual(
            self.db.executemany(sequenceofbindings=seqbindings, statements="select ?").fetchall(),
            self.db.cursor().executemany(statements="select ?", sequenceofbindings=seqbindings).fetchall())

        # check cursor_factory across closes
        class big:
            # make the class consume some memory
            memory = b"12345678" * 4096

        db2 = apsw.Connection("")
        self.assertEqual(db2.cursor_factory, apsw.Cursor)
        db2.cursor_factory = big
        self.assertEqual(db2.cursor_factory, big)
        db2.close()
        # factory becomes None when closing
        self.assertIsNone(db2.cursor_factory)
        # if this leaks it will show up in memory reports
        db2.cursor_factory = big
        del big

    def testMemoryLeaks(self):
        "MemoryLeaks: Run with a memory profiler such as valgrind and debug Python"
        # make and toss away a bunch of db objects, cursors, functions etc - if you use memory profiling then
        # simple memory leaks will show up
        c = self.db.cursor()
        c.execute("create table foo(x)")
        vals = [[1], [None], [math.pi], ["kjkljkljl"], ["\u1234\u345432432423423kjgjklhdfgkjhsdfjkghdfjskh"],
                [b"78696ghgjhgjhkgjkhgjhg\xfe\xdf"]]
        c.executemany("insert into foo values(?)", vals)
        for i in range(MEMLEAKITERATIONS):
            db = apsw.Connection(TESTFILEPREFIX + "testdb")
            db.create_aggregate_function("aggfunc", lambda x: x)
            db.create_scalar_function("scalarfunc", lambda x: x)
            db.set_busy_handler(lambda x: False)
            db.set_busy_timeout(1000)
            db.set_commit_hook(lambda x=1: 0)
            db.set_rollback_hook(lambda x=2: 1)
            db.set_update_hook(lambda x=3: 2)
            db.set_wal_hook(lambda *args: 0)
            db.collation_needed(lambda x: 4)

            def rt1(c, r):
                db.set_row_trace(rt2)
                return r

            def rt2(c, r):
                c.set_row_trace(rt1)
                return r

            def et1(c, s, b):
                db.set_exec_trace(et2)
                return True

            def et2(c, s, b):
                c.set_exec_trace(et1)
                return True

            for i in range(120):
                c2 = db.cursor()
                c2.set_row_trace(rt1)
                c2.set_exec_trace(et1)
                for row in c2.execute("select * from foo" + " " * i):  # spaces on end defeat statement cache
                    pass
            del c2
            db.close()

    def testBindings(self):
        "Check bindings work correctly"
        c = self.db.cursor()
        c.execute("create table foo(x,y,z)")
        vals = (
            ("(?,?,?)", (1, 2, 3)),
            ("(?,?,?)", [1, 2, 3]),
            ("(?,?,?)", range(1, 4)),
            ("(:a,$b,:c)", {
                'a': 1,
                'b': 2,
                'c': 3
            }),
            ("(1,?,3)", (2, )),
            ("(1,$a,$c)", {
                'a': 2,
                'b': 99,
                'c': 3
            }),
            # some unicode fun
            ("($\N{LATIN SMALL LETTER E WITH CIRCUMFLEX},:\N{LATIN SMALL LETTER A WITH TILDE},$\N{LATIN SMALL LETTER O WITH DIAERESIS})",
             (1, 2, 3)),
            ("($\N{LATIN SMALL LETTER E WITH CIRCUMFLEX},:\N{LATIN SMALL LETTER A WITH TILDE},$\N{LATIN SMALL LETTER O WITH DIAERESIS})",
             {
                 "\N{LATIN SMALL LETTER E WITH CIRCUMFLEX}": 1,
                 "\N{LATIN SMALL LETTER A WITH TILDE}": 2,
                 "\N{LATIN SMALL LETTER O WITH DIAERESIS}": 3,
             }))

        for str, bindings in vals:
            c.execute("insert into foo values" + str, bindings)
            self.assertEqual(curnext(c.execute("select * from foo")), (1, 2, 3))
            c.execute("delete from foo")

        # currently missing dict keys come out as null
        self.assertRaises(TypeError, apsw.allow_missing_dict_bindings, object)
        apsw.allow_missing_dict_bindings(True)
        c.execute("insert into foo values(:a,:b,$c)", {'a': 1, 'c': 3})  # 'b' deliberately missing
        apsw.allow_missing_dict_bindings(False)
        self.assertEqual((1, None, 3), curnext(c.execute("select * from foo")))
        c.execute("delete from foo")

        # these ones should cause errors
        vals = (
            (apsw.BindingsError, "(?,?,?)", (1, 2)),  # too few
            (apsw.BindingsError, "(?,?,?)", (1, 2, 3, 4)),  # too many
            (apsw.BindingsError, "(?,?,?)", None),  # none at all
            (apsw.BindingsError, "(?,?,?)", {
                'a': 1
            }),  # ? type, dict bindings (note that the reverse will work since all
            # named bindings are also implicitly numbered
            (TypeError, "(?,?,?)", 2),  # not a dict or sequence
            (TypeError, "(:a,:b,:c)", {
                'a': 1,
                'b': 2,
                'c': self
            }),  # bad type for c
        )
        for exc, str, bindings in vals:
            self.assertRaises(exc, c.execute, "insert into foo values" + str, bindings)

        # with multiple statements
        c.execute("insert into foo values(?,?,?); insert into foo values(?,?,?)", (99, 100, 101, 102, 103, 104))
        self.assertRaises(apsw.BindingsError, c.execute,
                          "insert into foo values(?,?,?); insert into foo values(?,?,?); insert some more",
                          (100, 100, 101, 1000, 103))  # too few
        self.assertRaises(apsw.BindingsError, c.execute, "insert into foo values(?,?,?); insert into foo values(?,?,?)",
                          (101, 100, 101, 1000, 103, 104, 105))  # too many
        # check the relevant statements did or didn't execute as appropriate
        self.assertEqual(curnext(self.db.cursor().execute("select count(*) from foo where x=99"))[0], 1)
        self.assertEqual(curnext(self.db.cursor().execute("select count(*) from foo where x=102"))[0], 1)
        self.assertEqual(curnext(self.db.cursor().execute("select count(*) from foo where x=100"))[0], 1)
        self.assertEqual(curnext(self.db.cursor().execute("select count(*) from foo where x=1000"))[0], 0)
        self.assertEqual(curnext(self.db.cursor().execute("select count(*) from foo where x=101"))[0], 1)
        self.assertEqual(curnext(self.db.cursor().execute("select count(*) from foo where x=105"))[0], 0)

        # check there are some bindings!
        self.assertRaises(apsw.BindingsError, c.execute, "create table bar(x,y,z);insert into bar values(?,?,?)")

        # across executemany
        vals = ((1, 2, 3), (4, 5, 6), (7, 8, 9))
        c.executemany("insert into foo values(?,?,?);", vals)
        for x, y, z in vals:
            self.assertEqual(curnext(c.execute("select * from foo where x=?", (x, ))), (x, y, z))

        # with an iterator
        def myvals():
            for i in range(10):
                yield {'a': i, 'b': i * 10, 'c': i * 100}

        c.execute("delete from foo")
        c.executemany("insert into foo values($a,:b,$c)", myvals())
        c.execute("delete from foo")

        # errors for executemany
        self.assertRaises(TypeError, c.executemany, "statement", 12, 34, 56)  # incorrect num params
        self.assertRaises(TypeError, c.executemany, "statement", 12)  # wrong type
        self.assertRaises(apsw.SQLError, c.executemany, "syntax error", [(1, )])  # error in prepare

        def myiter():
            yield 1 / 0

        self.assertRaises(ZeroDivisionError, c.executemany, "statement", myiter())  # immediate error in iterator

        def myiter():
            yield self

        self.assertRaises(TypeError, c.executemany, "statement", myiter())  # immediate bad type
        self.assertRaises(TypeError, c.executemany, "select ?", ((self, ), (1)))  # bad val
        c.executemany("statement", ())  # empty sequence

        # error in iterator after a while
        def myvals():
            for i in range(2):
                yield {'a': i, 'b': i * 10, 'c': i * 100}
            1 / 0

        self.assertRaises(ZeroDivisionError, c.executemany, "insert into foo values($a,:b,$c)", myvals())
        self.assertEqual(curnext(c.execute("select count(*) from foo"))[0], 2)
        c.execute("delete from foo")

        # return bad type from iterator after a while
        def myvals():
            for i in range(2):
                yield {'a': i, 'b': i * 10, 'c': i * 100}
            yield self

        self.assertRaises(TypeError, c.executemany, "insert into foo values($a,:b,$c)", myvals())
        self.assertEqual(curnext(c.execute("select count(*) from foo"))[0], 2)
        c.execute("delete from foo")

        # some errors in executemany
        self.assertRaises(apsw.BindingsError, c.executemany, "insert into foo values(?,?,?)", ((1, 2, 3), (1, 2, 3, 4)))
        self.assertRaises(apsw.BindingsError, c.executemany, "insert into foo values(?,?,?)", ((1, 2, 3), (1, 2)))

        # incomplete execution across executemany
        c.executemany("select * from foo; select ?", ((1, ), (2, )))  # we don't read
        self.assertRaises(apsw.IncompleteExecutionError, c.executemany, "begin")

        # set type (pysqlite error with this)
        c.execute("create table xxset(x,y,z)")
        c.execute("insert into xxset values(?,?,?)", set((1, 2, 3)))
        c.executemany("insert into xxset values(?,?,?)", (set((4, 5, 6)), ))
        result = [(1, 2, 3), (4, 5, 6)]
        for i, v in enumerate(c.execute("select * from xxset order by x")):
            self.assertEqual(v, result[i])

    def testCursor(self):
        "Check functionality of the cursor"
        c = self.db.cursor()
        # shouldn't be able to manually create
        self.assertRaises(TypeError, apsw.Cursor)
        self.assertRaises(TypeError, apsw.Cursor, 3)
        self.assertRaises(TypeError, apsw.Cursor, c)

        class consub(apsw.Connection):
            pass

        con2 = consub("")
        assert isinstance(con2, apsw.Connection) and not type(con2) == apsw.Connection
        apsw.Cursor(con2)

        # give bad params
        self.assertRaises(TypeError, c.execute)
        self.assertRaises(TypeError, c.execute, "foo", "bar", "bam")

        # empty statements
        c.execute("")
        c.execute(" ;\n\t\r;;")

        # unicode
        self.assertEqual(3, curnext(c.execute(u"select 3"))[0])

        # does it work?
        c.execute("create table foo(x,y,z)")
        # table should be empty
        entry = -1
        for entry, values in enumerate(c.execute("select * from foo")):
            pass
        self.assertEqual(entry, -1, "No rows should have been returned")
        # add ten rows
        for i in range(10):
            c.execute("insert into foo values(1,2,3)")
        for entry, values in enumerate(c.execute("select * from foo")):
            # check we get back out what we put in
            self.assertEqual(values, (1, 2, 3))
        self.assertEqual(entry, 9, "There should have been ten rows")
        # does get_connection return the right object
        self.assertIs(c.get_connection(), self.db)
        self.assertIs(c.connection, self.db)
        self.assertIs(c.connection, c.get_connection())
        # check get_description - note column with space in name and [] syntax to quote it
        cols = (
            ("x a space", "INTEGER"),
            ("y", "TEXT"),
            ("z", "foo"),
            ("a", "char"),
            ("\N{LATIN SMALL LETTER E WITH CIRCUMFLEX}\N{LATIN SMALL LETTER A WITH TILDE}",
             "\N{LATIN SMALL LETTER O WITH DIAERESIS}\N{LATIN SMALL LETTER U WITH CIRCUMFLEX}"),
        )
        c.execute("drop table foo; create table foo (%s)" % (", ".join(["[%s] %s" % (n, t) for n, t in cols]), ))
        c.execute("insert into foo([x a space]) values(1)")
        c.execute(
            "create temp table two(fred banana); insert into two values(7); create temp view three as select fred as [a space] from two"
        )
        c.execute("select 3")  # see issue #370
        has_full = any(o == "ENABLE_COLUMN_METADATA" or o.startswith("ENABLE_COLUMN_METADATA=")
                       for o in apsw.compile_options) if apsw.using_amalgamation else hasattr(c, "description_full")
        for row in c.execute("select * from foo"):
            self.assertEqual(cols, c.get_description())
            self.assertEqual(has_full, hasattr(c, "description_full"))
            self.assertEqual(cols, tuple([d[:2] for d in c.description]))
            self.assertEqual((None, None, None, None, None), c.description[0][2:])
            self.assertEqual(list(map(len, c.description)), [7] * len(cols))
        if has_full:
            for row in c.execute("select * from foo join three"):
                self.assertEqual(c.description_full,
                                 (('x a space', 'INTEGER', 'main', 'foo', 'x a space'),
                                  ('y', 'TEXT', 'main', 'foo', 'y'), ('z', 'foo', 'main', 'foo', 'z'),
                                  ('a', 'char', 'main', 'foo', 'a'), ('êã', 'öû', 'main', 'foo', 'êã'),
                                  ('a space', 'banana', 'temp', 'two', 'fred')))
        # check description caching isn't broken
        cols2 = cols[1:4]
        for row in c.execute("select y,z,a from foo"):
            self.assertEqual(cols2, c.get_description())
            self.assertEqual(cols2, tuple([d[:2] for d in c.description]))
            self.assertEqual((None, None, None, None, None), c.description[0][2:])
            self.assertEqual(list(map(len, c.description)), [7] * len(cols2))
        # execution is complete ...
        self.assertRaises(apsw.ExecutionCompleteError, c.get_description)
        self.assertRaises(apsw.ExecutionCompleteError, lambda: c.description)
        if has_full:
            self.assertRaises(apsw.ExecutionCompleteError, lambda: c.description_full)
        self.assertRaises(StopIteration, lambda xx=0: next(c))
        self.assertRaises(StopIteration, lambda xx=0: next(c))
        # fetchone is used throughout, check end behaviour
        self.assertEqual(None, c.fetchone())
        self.assertEqual(None, c.fetchone())
        self.assertEqual(None, c.fetchone())
        # nulls for get_description
        for row in c.execute("pragma user_version"):
            self.assertEqual(c.get_description(), (('user_version', None), ))
        # incomplete
        c.execute("select * from foo; create table bar(x)")  # we don't bother reading leaving
        self.assertRaises(apsw.IncompleteExecutionError, c.execute, "select * from foo")  # execution incomplete
        self.assertTableNotExists("bar")
        # autocommit
        self.assertEqual(True, self.db.get_autocommit())
        c.execute("begin immediate")
        self.assertEqual(False, self.db.get_autocommit())
        # pragma
        c.execute("pragma user_version")
        c.execute("pragma pure=nonsense")
        # error
        self.assertRaises(apsw.SQLError, c.execute,
                          "create table bar(x,y,z); this is a syntax error; create table bam(x,y,z)")
        self.assertTableExists("bar")
        self.assertTableNotExists("bam")
        # fetchall
        self.assertEqual(c.fetchall(), [])
        self.assertEqual(c.execute("select 3; select 4").fetchall(), [(3, ), (4, )])
        # readonly, explain & expanded_sql attributes
        res = None

        # tracing errors
        self.assertRaises(TypeError, setattr, c, "exec_trace", 3)
        self.assertRaises(TypeError, setattr, self.db, "exec_trace", 3)
        self.assertRaises(TypeError, setattr, c, "row_trace", 3)
        self.assertRaises(TypeError, setattr, self.db, "row_trace", 3)
        self.assertIsNone(c.row_trace)
        xx = lambda *args: 1 / 0
        c.row_trace = xx
        self.assertIs(c.row_trace, xx)
        c.row_trace = None

        def tracer(cur, query, bindings):
            nonlocal res
            res = {
                "cursor": cur,
                "query": query,
                "bindings": bindings,
                "readonly": cur.is_readonly,
                "explain": cur.is_explain
            }
            return True

        self.assertIsNone(c.exec_trace)
        c.set_exec_trace(tracer)
        self.assertIs(c.exec_trace, tracer)
        c.execute("pragma user_version")
        self.assertIs(res["cursor"], c)
        self.assertTrue(res["readonly"])
        self.assertEqual(res["explain"], 0)
        c.execute("explain pragma user_version")
        self.assertEqual(res["explain"], 1)
        c.execute("explain query plan select 3")
        self.assertEqual(res["explain"], 2)
        c.execute("pragma user_version=42")
        self.assertFalse(res["readonly"])
        biggy = "9" * 24 * 1024
        ran = False
        for row in c.execute("select ?,?", (biggy, biggy)):
            ran = True
            self.assertEqual(f"select '{ biggy }','{ biggy }'", c.expanded_sql)
            existing = self.db.limit(apsw.SQLITE_LIMIT_LENGTH, 25 * 1024)
            self.assertRaises(MemoryError, getattr, c, "expanded_sql")
            self.db.limit(apsw.SQLITE_LIMIT_LENGTH, existing)
        self.assertTrue(ran)
        # keyword args
        c.execute("pragma user_version=73", bindings=None, can_cache=False, prepare_flags=0).fetchall()
        c.executemany(statements="select ?", sequenceofbindings=((1, ), (2, )), can_cache=False,
                      prepare_flags=0).fetchall()

        # non-contiguous buffers
        self.assertRaises(BufferError, c.execute, "select ?", (memoryview(b"234567890")[::2], ))

    def testIssue373(self):
        "issue 373: dict type checking in bindings"

        class not_a_dict:
            pass

        class dict_lookalike(collections.abc.Mapping):

            def __getitem__(self, _):
                return 99

            def __iter__(*args):
                raise NotImplementedError

            def __len__(*args):
                raise NotImplementedError

        class errors_be_here:

            def __instancecheck__(self, _):
                1 / 0

            def __subclasscheck__(self, _):
                1 / 0

        class dict_with_error:

            def __getitem__(self, _):
                1 / 0

        collections.abc.Mapping.register(dict_with_error)

        class coerced_to_list:
            # this is not registered as dict, and instead PySequence_Fast will
            # turn it into a list calling the method for each key
            def __getitem__(self, key):
                if key < 10:
                    return key
                1 / 0

        class dict_subclass(dict):
            pass

        class list_subclass(list):
            pass

        self.assertRaises(TypeError, self.db.execute, "select :name", not_a_dict())
        self.assertEqual([(99, )], self.db.execute("select :name", dict_lookalike()).fetchall())
        # make sure these aren't detected as dict
        for thing in (1, ), {1}, [1], list_subclass([1]):
            self.assertRaises(TypeError, self.db.execute("select :name", thing))

        self.assertRaises(TypeError, self.db.execute, "select :name", errors_be_here())
        self.assertRaises(ZeroDivisionError, self.db.execute, "select :name", dict_with_error())
        apsw.allow_missing_dict_bindings(True)
        self.assertEqual([(None, )], self.db.execute("select :name", {}).fetchall())
        self.assertEqual([(None, )], self.db.execute("select :name", dict_subclass()).fetchall())
        apsw.allow_missing_dict_bindings(False)
        self.assertRaises(ZeroDivisionError, self.db.execute, "select ?", coerced_to_list())

        # same tests with executemany
        self.assertRaises(TypeError, self.db.executemany, "select :name", (not_a_dict(), ))
        self.assertEqual([(99, )], self.db.executemany("select :name", [dict_lookalike()]).fetchall())
        # make sure these aren't detected as dict
        for thing in (1, ), {1}, [1]:
            self.assertRaises(TypeError, self.db.executemany("select :name", [thing]))

        self.assertRaises(TypeError, self.db.executemany, "select :name", errors_be_here())
        self.assertRaises(ZeroDivisionError, self.db.executemany, "select :name", dict_with_error())
        apsw.allow_missing_dict_bindings(True)
        self.assertEqual([(None, )], self.db.executemany("select :name", ({}, )).fetchall())
        self.assertEqual([(None, )], self.db.executemany("select :name", [dict_subclass()]).fetchall())
        apsw.allow_missing_dict_bindings(False)
        self.assertRaises(ZeroDivisionError, self.db.executemany, "select ?", (coerced_to_list(), ))

    def testIssue376(self):
        "Whitespace treated as incomplete execution"
        c = self.db.cursor()
        for statement in (
                "select 3",
                "select 3;",
                "select 3; ",
                "select 3; ;\t\r\n; ",
        ):
            c.execute(statement)
            # should not throw incomplete
            c.execute("select 4")
            self.assertEqual([(3, ), (4, )], c.execute(statement + "; select 4").fetchall())

    def testIssue425(self):
        "Infinite recursion"

        class VFSA(apsw.VFS):

            def __init__(self):
                super().__init__("vfsa", "")

            def xOpen(self, name, flags):
                return VFSAFile(name, flags)

        class VFSAFile(apsw.VFSFile):

            def __init__(self, filename, flags):
                super().__init__("vfsb", filename, flags)

        class VFSB(apsw.VFS):

            def __init__(self):
                super().__init__("vfsb", "")

            def xOpen(self, name, flags):
                return VFSBFile(name, flags)

        class VFSBFile(apsw.VFSFile):

            def __init__(self, filename, flags):
                super().__init__("vfsa", filename, flags)

        a = VFSA()
        b = VFSB()
        sys.setrecursionlimit(200)
        print("A message due to RecursionError is possible, and what is being tested", file=sys.stderr)
        self.assertRaises(RecursionError, apsw.Connection, "testdb", vfs="vfsa")
        sys.setrecursionlimit(1000)

        def handler():  # incorrect number of arguments on purpose
            pass

        try:
            apsw.config(apsw.SQLITE_CONFIG_LOG, handler)
            self.assertRaisesUnraisable(TypeError, apsw.log, 11, "recursion error forced")
        finally:
            apsw.config(apsw.SQLITE_CONFIG_LOG, None)

    def testTypes(self):
        "Check type information is maintained"

        # zeroblob in functions
        def func(n: int):
            return apsw.zeroblob(n)

        self.db.create_scalar_function("func", func)

        self.assertEqual(self.db.execute("select func(17)").fetchall()[0][0], b"\0" * 17)

        c = self.db.cursor()
        c.execute("create table foo(row,x)")

        vals = test_types_vals

        for i, v in enumerate(vals):
            c.execute("insert into foo values(?,?)", (i, v))

        # add function to test conversion back as well
        def snap(*args):
            return args[0]

        self.db.create_scalar_function("snap", snap)

        # now see what we got out
        count = 0
        for row, v, fv in c.execute("select row,x,snap(x) from foo"):
            count += 1
            if type(vals[row]) is float:
                self.assertAlmostEqual(vals[row], v)
                self.assertAlmostEqual(vals[row], fv)
            else:
                self.assertEqual(vals[row], v)
                self.assertEqual(vals[row], fv)
        self.assertEqual(count, len(vals))

        # check some out of bounds conditions
        # integer greater than signed 64 quantity (SQLite only supports up to that)
        self.assertRaises(OverflowError, c.execute, "insert into foo values(9999,?)", (922337203685477580799, ))
        self.assertRaises(OverflowError, c.execute, "insert into foo values(9999,?)", (-922337203685477580799, ))

        # not valid types for SQLite
        self.assertRaises(TypeError, c.execute, "insert into foo values(9999,?)", (apsw, ))  # a module
        self.assertRaises(TypeError, c.execute, "insert into foo values(9999,?)", (type, ))  # type
        self.assertRaises(TypeError, c.execute, "insert into foo values(9999,?)", (dir, ))  # function

        # check nothing got inserted
        self.assertEqual(0, curnext(c.execute("select count(*) from foo where row=9999"))[0])

    def testMissingDictBindings(self):
        "How missing bindings are handled"
        orig = apsw.allow_missing_dict_bindings(True)
        self.assertEqual(self.db.execute("select :foo, $bar", {"foo": 3, "XXX": 4}).fetchall(), [(3, None)])
        apsw.allow_missing_dict_bindings(False)
        self.assertRaises(KeyError, self.db.execute, "select :foo, $bar", {"foo": 3, "XXX": 4})
        apsw.allow_missing_dict_bindings(orig)

    def testFormatSQLValue(self):
        "Verify text formatting of values"
        wt = APSW.wikipedia_text
        vals = (
            (3, "3"),
            (3.1, "3.1"),
            (-3, "-3"),
            (-3.1, "-3.1"),
            (9223372036854775807, "9223372036854775807"),
            (-9223372036854775808, "-9223372036854775808"),
            (None, "NULL"),
            ("ABC", "'ABC'"),
            ("\N{BLACK STAR} \N{WHITE STAR} \N{LIGHTNING} \N{COMET} ",
             "'" + "\N{BLACK STAR} \N{WHITE STAR} \N{LIGHTNING} \N{COMET} " + "'"),
            ("\N{BLACK STAR} \N{WHITE STAR} ' \N{LIGHTNING} \N{COMET} ",
             "'" + "\N{BLACK STAR} \N{WHITE STAR} '' \N{LIGHTNING} \N{COMET} " + "'"),
            ("", "''"),
            ("'", "''''"),
            ("'a", "'''a'"),
            ("a'", "'a'''"),
            ("''", "''''''"),
            ("'" * 20000, "'" + "'" * 40000 + "'"),
            ("\0", "''||X'00'||''"),
            ("\0\0\0", "''||X'00'||''||X'00'||''||X'00'||''"),
            ("AB\0C", "'AB'||X'00'||'C'"),
            ("A'B'\0C", "'A''B'''||X'00'||'C'"),
            ("\0A'B", "''||X'00'||'A''B'"),
            ("A'B\0", "'A''B'||X'00'||''"),
            (b"ABDE\0C", "X'414244450043'"),
            (b"", "X''"),
            (wt, "'" + wt + "'"),
            (wt[:77] + "'" + wt[77:], "'" + wt[:77] + "''" + wt[77:] + "'"),
            # SQLite doesn't understand 'inf', 'nan' or negative zero
            # so output what it does
            (math.inf, "1e999"),
            (-math.inf, "-1e999"),
            (math.nan, "NULL"),
            (-0.0, "0.0"),
        )
        for vin, vout in vals:
            out = apsw.format_sql_value(vin)
            self.assertEqual(out, vout)
            for row in self.db.execute("select " + vout):
                if vin in (math.nan, -0.0):
                    continue
                self.assertEqual(row[0], vin)
        # Errors
        self.assertRaises(TypeError, apsw.format_sql_value, apsw)
        self.assertRaises(TypeError, apsw.format_sql_value)

    def testVTableStuff(self):
        "Test new stuff added for Virtual tables"

        columns = [f"c{ n }" for n in range(30)]

        class Source:
            indexinfo_saved = None
            bio_callback = lambda *args: True
            filter_callback = lambda *args: 0
            create_callback = lambda: 0

            sn_called = set()

            schema = f"create table ignored({ ','.join(columns) })"

            def Create(self, *args):
                Source.create_callback()
                return self.schema, Source.Table()

            Connect = Create

            def ShadowName(self, suffix):
                Source.sn_called.add((id(self), suffix))

            class Table:

                def BestIndexObject(Self, o):
                    Source.indexinfo_saved = o
                    return Source.bio_callback(o)

                def BestIndex(self, constraints, orderby):
                    # non-string for idxstr
                    return [[0], 1, object()]

                def Open(self):
                    return Source.Cursor()

                def Disconnect(self):
                    pass

                Destroy = Disconnect

                def UpdateChangeRow(self_table, *args):
                    self.assertEqual(apsw.mapping_conflict_resolution_modes[self.db.vtab_on_conflict()],
                                     Source.expected_conflict)

                def UpdateDeleteRow(self, rowid):
                    pass

                def Savepoint1(self, level):
                    pass

                def Savepoint2(self):
                    1 / 0

                def Release1(self, level):
                    pass

                def Release2(self):
                    1 / 0

                def RollbackTo1(self, level):
                    pass

                def RollbackTo2(self):
                    1 / 0

            class Cursor:
                max_row = -1

                def Filter(self, *args):
                    self.rownum = 0
                    return Source.filter_callback(*args)

                def Eof(self):
                    return self.rownum >= self.max_row

                def Close(self):
                    return

                def Rowid(self):
                    return self.rownum

                def Column(self, num):
                    return self.rownum

                def Next(self):
                    self.rownum += 1

        self.db.create_module("foo", Source(), use_bestindex_object=True)
        self.db.create_module("foo2", Source(), use_bestindex_object=False)
        self.assertRaises(ValueError, self.db.create_module, "foo2", Source(), iVersion=77)

        self.db.execute("create virtual table bar using foo()")
        self.db.execute("create virtual table bar2 using foo2()")

        self.assertRaises(TypeError, self.db.execute, "select * from bar2 where c0>7")

        def basic(o):
            odict = apsw.ext.index_info_to_dict(o)
            self.assertEqual(expected, odict)
            exercise(o)
            return True

        # check out of bounds/setting etc
        def exercise(o):
            self.assertRaises(OverflowError, setattr, o, "idxNum", 0x7fff_ffff * 0x1_000)
            self.assertRaises(TypeError, setattr, o, "orderByConsumed", "not an int")
            self.assertRaises(OverflowError, setattr, o, "idxFlags", 0x7fff_ffff * 0x1_000)
            self.assertRaises(TypeError, setattr, o, "estimatedRows", "not an int")

            self.assertRaises(TypeError, o.get_aConstraint_iColumn, "not an int")
            self.assertRaises(TypeError, o.get_aConstraint_op, "not an int")
            self.assertRaises(TypeError, o.get_aConstraint_usable, "not an int")
            self.assertRaises(TypeError, o.get_aConstraint_collation, "not an int")
            self.assertRaises(TypeError, o.get_aConstraint_rhs, "not an int")
            self.assertRaises(TypeError, o.get_aOrderBy_iColumn, "not an int")
            self.assertRaises(TypeError, o.get_aOrderBy_desc, "not an int")
            self.assertRaises(TypeError, o.get_aConstraintUsage_in, "not an int")

            self.assertRaises(IndexError, o.set_aConstraintUsage_argvIndex, -1, 0)
            self.assertRaises(IndexError, o.set_aConstraintUsage_argvIndex, o.nConstraint, 0)
            self.assertRaises(OverflowError, o.set_aConstraintUsage_argvIndex, 2**(1 + sizeof_c_int), 0)
            self.assertRaises(IndexError, o.set_aConstraintUsage_omit, -1, 0)
            self.assertRaises(IndexError, o.set_aConstraintUsage_omit, o.nConstraint, 0)
            self.assertRaises(OverflowError, o.set_aConstraintUsage_omit, 2**(1 + sizeof_c_int), 0)

            self.assertRaises(IndexError, o.get_aConstraintUsage_argvIndex, -1)
            self.assertRaises(IndexError, o.get_aConstraintUsage_argvIndex, o.nConstraint)
            self.assertRaises(OverflowError, o.get_aConstraintUsage_argvIndex, 2**(1 + sizeof_c_int))
            self.assertRaises(IndexError, o.get_aConstraintUsage_omit, -1)
            self.assertRaises(IndexError, o.get_aConstraintUsage_omit, o.nConstraint)
            self.assertRaises(OverflowError, o.get_aConstraintUsage_omit, 2**(1 + sizeof_c_int))

            for n in dir(o):
                if n.startswith("_") or n.startswith("get_") or n.startswith("set_"):
                    continue
                ro = "(Read-only)" in inspect.getdoc(getattr(apsw.IndexInfo, n))
                if ro:
                    try:
                        setattr(o, n, getattr(o, n))
                    except AttributeError as e:
                        self.assertIn("is not writable", str(e))
                        continue
                # should be able to set to what it already is
                v = getattr(o, n)
                setattr(o, n, v)
                self.assertEqual(v, getattr(o, n))

                if v in (True, False):
                    setattr(o, n, not v)
                    self.assertEqual(getattr(o, n), not v)
                    self.assertRaises(TypeError, setattr, o, n, "not a bool")
                    self.assertRaises(TypeError, setattr, o, n, object())
                elif n == "estimatedCost":
                    for newval in (v + 1, -v / (v + 1), True):
                        setattr(o, "estimatedCost", newval)
                        self.assertEqual(o.estimatedCost, newval)
                    self.assertRaises(TypeError, setattr, o, "estimatedCost", "not a float")
                    self.assertRaises(TypeError, setattr, o, "estimatedCost", object())
                elif n == "idxStr":
                    self.assertRaises(TypeError, setattr, o, "idxStr", 3)
                    self.assertRaises(TypeError, setattr, o, "idxStr", b"aabbcc")
                    self.assertRaises(TypeError, setattr, o, "idxStr", object())
                    for i in range(50):
                        if i % 2:
                            o.idxStr = None
                            self.assertIsNone(o.idxStr)
                        t = f"stuff { i }"
                        o.idxStr = t
                        self.assertEqual(o.idxStr, t)
                elif n == "estimatedRows":
                    self.assertRaises(TypeError, setattr, o, n, 3.2)
                    self.assertRaises(TypeError, setattr, o, n, "seven")
                    self.assertRaises(TypeError, setattr, o, n, [])
                    for newval in (25, 77, 2 * 1024 * 1024 * 1024):
                        setattr(o, n, newval)
                        self.assertEqual(newval, getattr(o, n))
                else:
                    raise Exception(f"untested attribute { n }")

        Source.bio_callback = basic

        for query, expected in self.index_info_test_patterns:
            for row in self.db.execute(query, can_cache=False):
                pass

        # check we get failures outside of bestindex
        saved = Source.indexinfo_saved
        for name in dir(saved):
            if name.startswith("_"):
                continue
            if name.startswith("get_") or name.startswith("set_"):
                try:
                    getattr(saved, name)(0)
                    1 / 0
                except ValueError as e:
                    self.assertIn("IndexInfo is out of scope", str(e))
                continue
            doc = inspect.getdoc(getattr(apsw.IndexInfo, name))
            try:
                getattr(saved, name)
                1 / 0
            except ValueError as e:
                self.assertIn("IndexInfo is out of scope", str(e))

            if "(Read-only)" in doc:
                continue
            try:
                setattr(saved, name, 7)
                1 / 0
            except ValueError as e:
                self.assertIn("IndexInfo is out of scope", str(e))

        # error returns
        def bio1(o):
            return False

        def bio2(o):
            raise Exception("bio2 not playing")

        def bio3(o):
            return "not a bool"

        def bio4(o):
            return None

        for f in bio1, bio2, bio3, bio4:
            Source.bio_callback = f
            try:
                self.db.execute(self.index_info_test_patterns[0][0], can_cache=False)
            except Exception as e:
                if f is bio1:
                    self.assertIn("no query solution", str(e))
                elif f is bio2:
                    self.assertIn("bio2 not playing", str(e))
                elif f in (bio3, bio4):
                    self.assertIsInstance(e, TypeError)
                else:
                    1 / 0

        # how we get usable == False
        def bioUnusable(o):
            # we can get usable == False by having an IN which comes across as CONSTRAINT_EQ
            # and using it.  then the next call will have it as unusable
            for i in range(o.nConstraint):
                if o.get_aConstraint_op(i) == apsw.SQLITE_INDEX_CONSTRAINT_EQ \
                    and o.get_aConstraint_usable(i):
                    o.set_aConstraintUsage_argvIndex(i, 1)
                    return True
            else:
                self.assertTrue(any(o.get_aConstraint_usable(i) == False for i in range(o.nConstraint)))
            return True

        Source.bio_callback = bioUnusable
        for _ in self.db.execute("select * from bar where c1 in (1,2) and c2>6"):
            pass

        # check it actually works
        def bio(o):
            # we work on the second query in index_info_test_patterns
            self.assertEqual(apsw.ext.index_info_to_dict(o), self.index_info_test_patterns[1][1])
            o.set_aConstraintUsage_argvIndex(0, 1)
            o.set_aConstraintUsage_omit(0, True)
            self.assertNotEqual(apsw.ext.index_info_to_dict(o), self.index_info_test_patterns[1][1])
            o.idxStr = "some text"
            return True

        def bio_f(*args):
            self.assertEqual(args, (0, 'some text', ('foo', )))

        Source.bio_callback = bio
        Source.filter_callback = bio_f
        self.db.execute(self.index_info_test_patterns[1][0])

        # More than 63 columns affects colUsed
        manycol_names = [f'c{ n }' for n in range(100)]
        Source.schema = f"create table ignored({ ','.join(manycol_names) })"
        self.db.execute("create virtual table manycol using foo()")

        def check(o):
            d = apsw.ext.index_info_to_dict(o, column_names=manycol_names)
            self.assertIn("c61", d["colUsed_names"])
            self.assertNotIn("c62", d["colUsed_names"])
            for name in manycol_names[63:]:
                self.assertIn(name, d["colUsed_names"])
            return True

        Source.bio_callback = check
        Source.filter_callback = lambda *args: 0
        self.db.execute("select c61, c64 from manycol where c72>7")

        # in args
        def bio(o):
            d = apsw.ext.index_info_to_dict(o)
            self.assertTrue(d["aConstraintUsage"][0]["in"])
            self.assertTrue(d["aConstraintUsage"][1]["in"])
            self.assertFalse(d["aConstraintUsage"][2]["in"])
            self.assertRaises(TypeError, o.set_aConstraintUsage_in, 0, 'foo')
            self.assertRaises(IndexError, o.set_aConstraintUsage_in, 99, True)
            self.assertRaises(ValueError, o.set_aConstraintUsage_in, 2, True)
            o.set_aConstraintUsage_in(0, True)
            o.set_aConstraintUsage_in(1, True)
            for i in range(3):
                o.set_aConstraintUsage_argvIndex(i, i + 1)
            return True

        def bio_f(*args):
            self.assertEqual(args, (0, None, ({1, 'one', 1.1}, {b'aabbcc', None}, None)))

        Source.schema = "create table ignored(c0,c1,c2)"
        Source.bio_callback = bio
        Source.filter_callback = bio_f
        self.db.execute(
            "create virtual table in_filter using foo(); select * from in_filter where c0 in (1,1.1,'one') and c1 in (?,?,?) and c2 in (?)",
            (None, b'aabbcc', None, None))

        # vtab_config
        self.assertRaises(ValueError, self.db.vtab_config, apsw.SQLITE_VTAB_CONSTRAINT_SUPPORT)

        def check():
            self.assertRaises(TypeError, self.db.vtab_config, "three")
            self.assertRaises(TypeError, self.db.vtab_config, apsw.SQLITE_VTAB_CONSTRAINT_SUPPORT, "three")
            self.assertRaises(TypeError, self.db.vtab_config, apsw.SQLITE_VTAB_CONSTRAINT_SUPPORT,
                              apsw.SQLITE_VTAB_CONSTRAINT_SUPPORT, apsw.SQLITE_VTAB_CONSTRAINT_SUPPORT)
            self.assertRaises(ValueError, self.db.vtab_config, 97657)
            self.db.vtab_config(apsw.SQLITE_VTAB_CONSTRAINT_SUPPORT, 0)
            self.db.vtab_config(apsw.SQLITE_VTAB_INNOCUOUS)
            self.db.vtab_config(apsw.SQLITE_VTAB_DIRECTONLY, 0)

        Source.create_callback = check
        self.db.execute("create virtual table vtab_config using foo()")
        self.assertRaises(ValueError, self.db.vtab_config, apsw.SQLITE_VTAB_CONSTRAINT_SUPPORT)

        # vtab_on_conflict
        self.db.execute("create virtual table vtab_on_conflict using foo()")
        self.assertRaises(ValueError, self.db.vtab_on_conflict)
        Source.bio_callback = Source.filter_callback = lambda *args: True
        Source.Cursor.max_row = 1
        for mode in ("ROLLBACK", "FAIL", "ABORT", "IGNORE", "REPLACE"):
            Source.expected_conflict = "SQLITE_" + mode
            self.db.execute(f"update or { mode } vtab_on_conflict set c0=7 where rowid=0")
        self.assertRaises(ValueError, self.db.vtab_on_conflict)

        # savepoints
        Source.Cursor.max_row = 20
        Source.filter_callback = lambda *args: 0
        Source.bio_callback = lambda *args: True
        self.db.create_module("sptest", Source(), use_bestindex_object=True, iVersion=3)
        self.db.execute("create virtual table sptest using sptest()")

        def clear():
            while not self.db.get_autocommit():
                self.db.execute("rollback")

        query = """begin;
                    delete from sptest where rowid=7;
                    savepoint t0 ;
                    delete from sptest where rowid=8;
                    savepoint t1;
                    delete from sptest where rowid=9;
                    savepoint t2;
                    delete from sptest where rowid=10;
                    savepoint t3;
                    delete from sptest where rowid=10;
                    release t3; rollback to t1;
                    commit;
                   """
        # check it works
        clear()
        self.db.execute(query)
        Source.Table.Savepoint = Source.Table.Savepoint2
        clear()
        self.assertRaises(TypeError, self.db.execute, query)
        Source.Table.Savepoint = Source.Table.Savepoint1
        clear()
        self.db.execute(query)
        Source.Table.Release = Source.Table.Release2
        clear()
        self.assertRaises(TypeError, self.db.execute, query)
        Source.Table.Release = Source.Table.Release1
        clear()
        self.db.execute(query)
        Source.Table.RollbackTo = Source.Table.RollbackTo2
        clear()
        self.assertRaises(TypeError, self.db.execute, query)
        Source.Table.RollbackTo = Source.Table.RollbackTo1
        clear()
        self.db.execute(query)

        # shadowname
        self.assertEqual(len(Source.sn_called), 0)

        def make_shadow(bn):
            self.db.execute(f"create table { bn }_foo(x); create table { bn }_bar(x)")

        make_shadow("sptest")
        self.assertEqual(len(Source.sn_called), 2)

        Source.ShadowName = lambda *args: 1 / 0
        self.assertRaisesUnraisable(ZeroDivisionError, self.db.execute, "create table sptest_bam(x)")
        Source.ShadowName = lambda *args: "foo"
        self.assertRaisesUnraisable(TypeError, self.db.execute, "create table sptest_bam2(x)")
        Source.ShadowName = lambda *args: 3 + 4j
        self.assertRaisesUnraisable(TypeError, self.db.execute, "create table sptest_bam3(x)")
        Source.ShadowName = lambda *args: True
        self.db.execute("create table sptest_bam4(x)")

        def clear_all():
            self.db.close()
            self.db = apsw.Connection(":memory:")

        # run out of shadowname slots
        clear_all()
        for i in range(2000):
            try:
                self.db.create_module(f"sptest{ i }", Source(), use_bestindex_object=True, iVersion=3)
            except Exception as e:
                limit = i
                self.assertIn("No xShadowName slots are available", str(e))
                break

        # do some deliberate thrashing
        clear_all()

        registered = set()
        for i in range(limit * 20):
            if len(registered) == limit:
                victim = random.choice(list(registered))
                self.db.execute(f"drop table v{ victim }")
                self.db.create_module(victim, None)
                registered.remove(victim)
            name = f"sptest{ i }"
            self.db.create_module(f"sptest{ i }", Source(), use_bestindex_object=False, iVersion=3)
            self.db.execute(f"create virtual table v{ name } using { name }")
            make_shadow(f"v{ name }")
            registered.add(name)

    index_info_test_patterns = (("select * from bar where c7 is null", {
        'nConstraint':
        1,
        'aConstraint': [{
            'iColumn': 7,
            'op': 71,
            'op_str': 'SQLITE_INDEX_CONSTRAINT_ISNULL',
            'usable': True,
            'collation': 'BINARY',
            'rhs': None
        }],
        'nOrderBy':
        0,
        'aOrderBy': [],
        'aConstraintUsage': [{
            'argvIndex': 0,
            'in': False,
            'omit': False
        }],
        'idxNum':
        0,
        'idxStr':
        None,
        'orderByConsumed':
        False,
        'estimatedCost':
        5e+98,
        'estimatedRows':
        25,
        'idxFlags':
        0,
        'idxFlags_set':
        set(),
        'colUsed':
        {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29},
        'distinct':
        0
    }), (
        "select c2, c4, c6 from bar where c3 is not null and c10>'foo' and c12!=c14 group by c8 order by c10 desc",
        {
            'nConstraint':
            2,
            'aConstraint': [{
                'iColumn': 10,
                'op': 4,
                'op_str': 'SQLITE_INDEX_CONSTRAINT_GT',
                'usable': True,
                'collation': 'BINARY',
                'rhs': 'foo'
            }, {
                'iColumn': 3,
                'op': 70,
                'op_str': 'SQLITE_INDEX_CONSTRAINT_ISNOTNULL',
                'usable': True,
                'collation': 'BINARY',
                'rhs': None
            }],
            'nOrderBy':
            1,
            'aOrderBy': [{
                'iColumn': 8,
                'desc': True
            }],
            'aConstraintUsage': [{
                'argvIndex': 0,
                'omit': False,
                'in': False,
            }, {
                'argvIndex': 0,
                'omit': False,
                'in': False,
            }],
            'idxNum':
            0,
            'idxStr':
            None,
            'orderByConsumed':
            False,
            'estimatedCost':
            5e+98,
            'estimatedRows':
            25,
            'idxFlags':
            0,
            'idxFlags_set':
            set(),
            'colUsed': {2, 3, 4, 6, 8, 10, 12, 14},
            'distinct':
            1
        },
    ))

    def testVTableNoChange(self):
        "Test virtual table no change values on update"

        class Source:
            data = [(i, 2, i, 4, i) for i in range(10)]

            def Create(self, *args):
                return "create table ignored(c0, c1, c2, c3, c4)", Source.Table()

            Connect = Create

            class Table:

                def BestIndex(self, *args):
                    return None

                def Open(self):
                    return Source.Cursor()

                def Disconnect(self):
                    pass

                Destroy = Disconnect

                def UpdateChangeRow(tself, rowid, newrowid, fields):
                    d = Source.data
                    expected = (apsw.no_change, d[rowid][2] + 1, apsw.no_change, 4, apsw.no_change)
                    self.assertEqual(fields, expected)

            class Cursor:

                def Filter(self, *args):
                    self.pos = 0

                def Eof(self):
                    return self.pos >= len(Source.data)

                def Next(self):
                    self.pos += 1

                def Column(self, n):
                    return Source.data[self.pos][n]

                def ColumnNoChange(self, n):
                    if n % 2:
                        return self.Column(n)
                    return apsw.no_change

                def Close(self):
                    pass

                def Rowid(self):
                    return self.pos

        self.db.create_module("testing", Source(), eponymous=True, use_no_change=True)
        self.db.execute("update testing set c1=c2+1")

    def testWAL(self):
        "Test WAL functions"
        # note that it is harmless calling wal functions on a db not in wal mode
        self.assertRaises(TypeError, self.db.wal_autocheckpoint)
        self.assertRaises(TypeError, self.db.wal_autocheckpoint, "a strinbg")
        self.db.wal_autocheckpoint(8912)
        self.assertRaises(TypeError, self.db.wal_checkpoint, -1)
        self.db.wal_checkpoint()
        self.db.wal_checkpoint("main")
        v = self.db.wal_checkpoint(mode=apsw.SQLITE_CHECKPOINT_PASSIVE)
        self.assertTrue(isinstance(v, tuple) and len(v) == 2 and isinstance(v[0], int) and isinstance(v[1], int))
        self.assertRaises(apsw.MisuseError, self.db.wal_checkpoint, mode=876786)
        self.assertRaises(TypeError, self.db.set_wal_hook)
        self.assertRaises(TypeError, self.db.set_wal_hook, 12)
        self.db.set_wal_hook(None)
        # check we can set wal mode
        self.assertEqual("wal", self.db.cursor().execute("pragma journal_mode=wal").fetchall()[0][0])

        # errors in wal callback
        def zerodiv(*args):
            1 / 0

        self.db.set_wal_hook(zerodiv)
        self.assertRaises(ZeroDivisionError, self.db.cursor().execute, "create table one(x)")
        # the error happens after the wal commit so the table should exist
        self.assertTableExists("one")

        def badreturn(*args):
            return "three"

        self.db.set_wal_hook(badreturn)
        self.assertRaises(TypeError, self.db.cursor().execute, "create table two(x)")
        self.assertTableExists("two")

        expectdbname = ""

        def walhook(conn, dbname, pages):
            self.assertTrue(conn is self.db)
            self.assertTrue(pages > 0)
            self.assertEqual(dbname, expectdbname)
            return apsw.SQLITE_OK

        expectdbname = "main"
        self.db.set_wal_hook(walhook)
        self.db.cursor().execute("create table three(x)")
        self.db.cursor().execute("attach '%stestdb2?psow=0' as fred" % ("file:" + TESTFILEPREFIX, ))
        self.assertEqual("wal", self.db.cursor().execute("pragma fred.journal_mode=wal").fetchall()[0][0])
        expectdbname = "fred"
        self.db.cursor().execute("create table fred.three(x)")

    def testAuthorizer(self):
        "Verify the authorizer works"
        retval = apsw.SQLITE_DENY

        def authorizer(operation, paramone, paramtwo, databasename, triggerorview):
            # we fail creates of tables starting with "private"
            if operation == apsw.SQLITE_CREATE_TABLE and paramone.startswith("private"):
                return retval
            return apsw.SQLITE_OK

        c = self.db.cursor()
        # this should succeed
        c.execute("create table privateone(x)")
        # this should fail
        self.assertRaises(TypeError, self.db.set_authorizer, 12)  # must be callable
        self.assertRaises(TypeError, setattr, self.db, "authorizer", 12)
        self.db.set_authorizer(authorizer)
        self.assertIs(self.db.authorizer, authorizer)
        for val in apsw.SQLITE_DENY, apsw.SQLITE_DENY, 0x800276889000212112:
            retval = val
            if val < 100:
                self.assertRaises(apsw.AuthError, c.execute, "create table privatetwo(x)")
            else:
                self.assertRaises(OverflowError, c.execute, "create table privatetwo(x)")
        # this should succeed
        self.db.set_authorizer(None)
        self.assertIsNone(self.db.authorizer)
        c.execute("create table privatethree(x)")

        self.assertTableExists("privateone")
        self.assertTableNotExists("privatetwo")
        self.assertTableExists("privatethree")

        # error in callback
        def authorizer(operation, *args):
            if operation == apsw.SQLITE_CREATE_TABLE:
                1 / 0
            return apsw.SQLITE_OK

        self.db.authorizer = authorizer
        self.assertRaises(ZeroDivisionError, c.execute, "create table shouldfail(x)")
        self.assertTableNotExists("shouldfail")

        # bad return type in callback
        def authorizer(operation, *args):
            return "a silly string"

        self.db.set_authorizer(authorizer)
        self.assertRaises(TypeError, c.execute, "create table shouldfail(x); select 3+5")
        self.db.authorizer = None  # otherwise next line will fail!
        self.assertTableNotExists("shouldfail")

        # back to normal
        self.db.authorizer = None
        c.execute("create table shouldsucceed(x)")
        self.assertTableExists("shouldsucceed")

    def testExecTracing(self):
        "Verify tracing of executed statements and bindings"
        self.db.set_exec_trace(None)
        self.assertIsNone(self.db.exec_trace)
        self.db.exec_trace = None
        self.assertIsNone(self.db.exec_trace)
        c = self.db.cursor()
        cmds = []  # this is maniulated in tracefunc

        def tracefunc(cursor, cmd, bindings):
            cmds.append((cmd, bindings))
            return True

        c.execute("create table one(x,y,z)")
        self.assertEqual(len(cmds), 0)
        self.assertRaises(TypeError, c.set_exec_trace, 12)  # must be callable
        self.assertRaises(TypeError, self.db.set_exec_trace, 12)  # must be callable
        c.set_exec_trace(tracefunc)
        self.assertIs(c.exec_trace, tracefunc)
        statements = [
            ("insert into one values(?,?,?)", (1, 2, 3)),
            ("insert into one values(:a,$b,$c)", {
                'a': 1,
                'b': "string",
                'c': None
            }),
        ]
        for cmd, values in statements:
            c.execute(cmd, values)
        self.assertEqual(cmds, statements)
        self.assertTrue(c.get_exec_trace() is tracefunc)
        c.exec_trace = None
        self.assertTrue(c.get_exec_trace() is None)
        c.execute("create table bar(x,y,z)")
        # cmds should be unchanged
        self.assertEqual(cmds, statements)
        # tracefunc can abort execution
        count = curnext(c.execute("select count(*) from one"))[0]

        def tracefunc(cursor, cmd, bindings):
            return False  # abort

        c.set_exec_trace(tracefunc)
        self.assertRaises(apsw.ExecTraceAbort, c.execute, "insert into one values(1,2,3)")
        # table should not have been modified
        c.set_exec_trace(None)
        self.assertEqual(count, curnext(c.execute("select count(*) from one"))[0])

        # error in tracefunc
        def tracefunc(cursor, cmd, bindings):
            1 / 0

        c.set_exec_trace(tracefunc)
        self.assertRaises(ZeroDivisionError, c.execute, "insert into one values(1,2,3)")
        c.set_exec_trace(None)
        self.assertEqual(count, curnext(c.execute("select count(*) from one"))[0])
        # test across executemany and multiple statements
        counter = [0]

        def tracefunc(cursor, cmd, bindings):
            counter[0] = counter[0] + 1
            return True

        c.set_exec_trace(tracefunc)
        c.execute(
            "create table two(x);insert into two values(1); insert into two values(2); insert into two values(?); insert into two values(?)",
            (3, 4))
        self.assertEqual(counter[0], 5)
        counter[0] = 0
        c.executemany("insert into two values(?); insert into two values(?)", [[n, n + 1] for n in range(5)])
        self.assertEqual(counter[0], 10)
        # error in func but only after a while
        c.execute("delete from two")
        counter[0] = 0

        def tracefunc(cursor, cmd, bindings):
            counter[0] = counter[0] + 1
            if counter[0] > 3:
                1 / 0
            return True

        c.set_exec_trace(tracefunc)
        self.assertRaises(
            ZeroDivisionError, c.execute,
            "insert into two values(1); insert into two values(2); insert into two values(?); insert into two values(?)",
            (3, 4))
        self.assertEqual(counter[0], 4)
        c.set_exec_trace(None)
        # check the first statements got executed
        self.assertEqual(3, curnext(c.execute("select max(x) from two"))[0])

        # executemany
        def tracefunc(cursor, cmd, bindings):
            1 / 0

        c.set_exec_trace(tracefunc)
        self.assertRaises(ZeroDivisionError, c.executemany, "select ?", [(1, )])
        c.set_exec_trace(None)

        # tracefunc with wrong number of arguments
        def tracefunc(a, b, c, d, e, f):
            1 / 0

        c.set_exec_trace(tracefunc)
        self.assertRaises(TypeError, c.execute, "select max(x) from two")

        def tracefunc(*args):
            return BadIsTrue()

        c.set_exec_trace(tracefunc)
        self.assertRaises(ZeroDivisionError, c.execute, "select max(x) from two")
        # connection based tracing
        self.assertEqual(self.db.get_exec_trace(), None)
        traced = [False, False]

        def contrace(*args):
            traced[0] = True
            return True

        def curtrace(*args):
            traced[1] = True
            return True

        c.set_exec_trace(curtrace)
        c.execute("select 3")
        self.assertEqual(traced, [False, True])
        traced = [False, False]
        self.db.set_exec_trace(contrace)
        c.execute("select 3")
        self.assertEqual(traced, [False, True])
        traced = [False, False]
        c.set_exec_trace(None)
        c.execute("select 3")
        self.assertEqual(traced, [True, False])
        traced = [False, False]
        self.db.cursor().execute("select 3")
        self.assertEqual(traced, [True, False])
        self.assertEqual(self.db.get_exec_trace(), contrace)
        self.assertEqual(c.get_exec_trace(), None)
        self.assertEqual(self.db.cursor().get_exec_trace(), None)
        c.set_exec_trace(curtrace)
        self.assertEqual(c.get_exec_trace(), curtrace)

    def testRowTracing(self):
        "Verify row tracing"
        self.db.set_row_trace(None)
        c = self.db.cursor()
        c.execute("create table foo(x,y,z)")
        vals = (1, 2, 3)
        c.execute("insert into foo values(?,?,?)", vals)

        def tracefunc(cursor, row):
            return tuple([7 for i in row])

        # should get original row back
        self.assertEqual(curnext(c.execute("select * from foo")), vals)
        self.assertRaises(TypeError, c.set_row_trace, 12)  # must be callable
        c.set_row_trace(tracefunc)
        self.assertTrue(c.get_row_trace() is tracefunc)
        # all values replaced with 7
        self.assertEqual(curnext(c.execute("select * from foo")), tuple([7] * len(vals)))

        def tracefunc(cursor, row):
            return (7, )

        # a single 7
        c.set_row_trace(tracefunc)
        self.assertEqual(curnext(c.execute("select * from foo")), (7, ))
        # no alteration again
        c.set_row_trace(None)
        self.assertEqual(curnext(c.execute("select * from foo")), vals)

        # error in function
        def tracefunc(*result):
            1 / 0

        c.set_row_trace(tracefunc)
        try:
            for row in c.execute("select * from foo"):
                self.fail("Should have had exception")
                break
        except ZeroDivisionError:
            pass
        c.set_row_trace(None)
        self.assertEqual(curnext(c.execute("select * from foo")), vals)
        # returning null
        c.execute("create table bar(x)")
        c.executemany("insert into bar values(?)", [[x] for x in range(10)])
        counter = [0]

        def tracefunc(cursor, args):
            counter[0] = counter[0] + 1
            if counter[0] % 2:
                return None
            return args

        c.set_row_trace(tracefunc)
        countertoo = 0
        for row in c.execute("select * from bar"):
            countertoo += 1
        c.set_row_trace(None)
        self.assertEqual(countertoo, 5)  # half the rows should be skipped
        # connection based
        self.assertRaises(TypeError, self.db.set_row_trace, 12)
        self.assertEqual(self.db.get_row_trace(), None)
        traced = [False, False]

        def contrace(cursor, row):
            traced[0] = True
            return row

        def curtrace(cursor, row):
            traced[1] = True
            return row

        for row in c.execute("select 3,3"):
            pass
        self.assertEqual(traced, [False, False])
        traced = [False, False]
        self.db.set_row_trace(contrace)
        for row in self.db.cursor().execute("select 3,3"):
            pass
        self.assertEqual(traced, [True, False])
        traced = [False, False]
        c.set_row_trace(curtrace)
        for row in c.execute("select 3,3"):
            pass
        self.assertEqual(traced, [False, True])
        traced = [False, False]
        c.set_row_trace(None)
        for row in c.execute("select 3"):
            pass
        self.assertEqual(traced, [True, False])
        self.assertEqual(self.db.get_row_trace(), contrace)

    def testScalarFunctions(self):
        "Verify scalar functions"
        c = self.db.cursor()

        def ilove7(*args):
            return 7

        self.assertRaises(TypeError, self.db.create_scalar_function, "twelve", 12)  # must be callable
        self.assertRaises(TypeError, self.db.create_scalar_function, "twelve", 12, 27, 28)  # too many params
        try:
            self.db.create_scalar_function("twelve", ilove7, 900)  # too many args
        except (apsw.SQLError, apsw.MisuseError):
            # misuse can be returned for too many args
            pass
        # some unicode fun
        self.db.create_scalar_function, "twelve\N{BLACK STAR}", ilove7
        try:
            # SQLite happily registers the function, but you can't
            # call it
            self.assertEqual(c.execute("select " + "twelve\N{BLACK STAR}" + "(3)").fetchall(), [[7]])
        except apsw.SQLError:
            pass

        self.db.create_scalar_function("seven", ilove7)
        c.execute("create table foo(x,y,z)")
        for i in range(10):
            c.execute("insert into foo values(?,?,?)", (i, i, i))
        for i in range(10):
            self.assertEqual((7, ), curnext(c.execute("select seven(x,y,z) from foo where x=?", (i, ))))
        # clear func
        self.assertRaises(apsw.BusyError, self.db.create_scalar_function, "seven",
                          None)  # active select above so no funcs can be changed
        for row in c.execute("select null"):
            pass  # no active sql now
        self.db.create_scalar_function("seven", None)
        # function names are limited to 255 characters - SQLerror is the rather unintuitive error return
        try:
            self.db.create_scalar_function("a" * 300, ilove7)
        except (apsw.SQLError, apsw.MisuseError):
            pass  # see sqlite ticket #3875
        # have an error in a function
        def badfunc(*args):
            return 1 / 0

        self.db.create_scalar_function("badscalarfunc", badfunc)
        self.assertRaises(ZeroDivisionError, c.execute, "select badscalarfunc(*) from foo")
        # return non-allowed types
        for v in ({'a': 'dict'}, ['a', 'list'], self):

            def badtype(*args):
                return v

            self.db.create_scalar_function("badtype", badtype)
            self.assertRaises(TypeError, c.execute, "select badtype(*) from foo")
        # return non-unicode string
        def ilove8bit(*args):
            return "\x99\xaa\xbb\xcc"

        self.db.create_scalar_function("ilove8bit", ilove8bit)

        # coverage
        def bad(*args):
            1 / 0

        self.db.create_scalar_function("bad", bad)
        self.assertRaises(ZeroDivisionError, c.execute, "select bad(3)+bad(4)")
        # turn a blob into a string to fail python utf8 conversion
        self.assertRaises(UnicodeDecodeError, c.execute, "select bad(cast (x'fffffcfb9208' as TEXT))")

        # register same named function taking different number of arguments
        for i in range(-1, 4):
            self.db.create_scalar_function("multi", lambda *args: len(args), i)
        gc.collect()
        for row in c.execute("select multi(), multi(1), multi(1,2), multi(1,2,3), multi(1,2,3,4), multi(1,2,3,4,5)"):
            self.assertEqual(row, (0, 1, 2, 3, 4, 5))

        # deterministic flag

        # check error handling
        self.assertRaises(TypeError, self.db.create_scalar_function, "twelve", deterministic="324")
        self.assertRaises(TypeError, self.db.create_scalar_function, "twelve", deterministic=324)

        # check it has an effect
        class Counter:  # on calling returns how many times this instance has been called
            num_calls = 0

            def __call__(self):
                self.num_calls += 1
                return self.num_calls

        self.db.create_scalar_function("deterministic", Counter(), deterministic=True)
        self.db.create_scalar_function("nondeterministic", Counter(), deterministic=False)
        self.db.create_scalar_function("unspecdeterministic", Counter())

        # only deterministic can be used for indices
        c.execute("create table td(a,b); create index tda on td(a) where deterministic()")
        self.assertEqual(c.execute("select nondeterministic()=nondeterministic()").fetchall()[0][0], 0)
        self.assertEqual(c.execute("select unspecdeterministic()=unspecdeterministic()").fetchall()[0][0], 0)
        self.assertRaises(apsw.SQLError, c.execute, "create index tdb on td(b) where nondeterministic()")

    def testAggregateFunctions(self):
        "Verify aggregate functions"
        c = self.db
        c.execute("create table foo(x,y,z)")

        # aggregate function
        class longest:

            def __init__(self):
                self.result = ""

            def step(self, context, *args):
                for i in args:
                    if len(str(i)) > len(self.result):
                        self.result = str(i)

            def final(self, context):
                return self.result

            def factory():
                v = longest()
                return None, v.step, v.final

            factory = staticmethod(factory)

        self.assertRaises(TypeError, self.db.create_aggregate_function, True, True, True,
                          True)  # wrong number/type of params
        self.assertRaises(TypeError, self.db.create_aggregate_function, "twelve", 12)  # must be callable

        if "DEBUG" not in apsw.compile_options:
            # these cause assertion failures in sqlite
            try:
                self.db.create_aggregate_function("twelve", longest.factory, 923)  # max args is 127
            except (apsw.SQLError, apsw.MisuseError):
                # misuse is returned for too many args
                pass
            self.db.create_aggregate_function("twelve", None)

        self.assertRaises(TypeError, self.db.create_aggregate_function, "twelve\N{BLACK STAR}", 12)  # must be ascii
        self.db.create_aggregate_function("longest", longest.factory)

        vals = (
            ("kjfhgk", "gkjlfdhgjkhsdfkjg",
             "gklsdfjgkldfjhnbnvc,mnxb,mnxcv,mbncv,mnbm,ncvx,mbncv,mxnbcv,"),  # last one is deliberately the longest
            ("gdfklhj", ":gjkhgfdsgfd", "gjkfhgjkhdfkjh"),
            ("gdfjkhg", "gkjlfd", ""),
            (1, 2, 30),
        )

        for v in vals:
            c.execute("insert into foo values(?,?,?)", v)

        v = curnext(c.execute("select longest(x,y,z) from foo"))[0]
        self.assertEqual(v, vals[0][2])

        # SQLite doesn't allow step functions to return an error, so we have to defer to the final
        def badfactory():

            def badfunc(*args):
                1 / 0

            def final(*args):
                self.fail("This should not be executed")
                return 1

            return None, badfunc, final

        self.db.create_aggregate_function("badfunc", badfactory)
        self.assertRaises(ZeroDivisionError, c.execute, "select badfunc(x) from foo")

        # error in final
        def badfactory():

            def badfunc(*args):
                pass

            def final(*args):
                1 / 0

            return None, badfunc, final

        self.db.create_aggregate_function("badfunc", badfactory)
        self.assertRaises(ZeroDivisionError, c.execute, "select badfunc(x) from foo")

        # error in step and final
        def badfactory():

            def badfunc(*args):
                1 / 0

            def final(*args):
                raise ImportError()  # zero div from above is what should be returned

            return None, badfunc, final

        self.db.create_aggregate_function("badfunc2", badfactory)
        self.assertRaises(ZeroDivisionError, c.execute, "select badfunc2(x) from foo")

        # bad return from factory
        def badfactory():

            def badfunc(*args):
                pass

            def final(*args):
                return 0

            return {}

        self.db.create_aggregate_function("badfunc", badfactory)
        self.assertRaises(AttributeError, c.execute, "select badfunc(x) from foo")

        # incorrect number of items returned
        def badfactory():

            def badfunc(*args):
                pass

            def final(*args):
                return 0

            return (None, badfunc, final, badfactory)

        self.db.create_aggregate_function("badfunc3", badfactory)
        self.assertRaises(TypeError, c.execute, "select badfunc3(x) from foo")

        # step not callable
        def badfactory():

            def badfunc(*args):
                pass

            def final(*args):
                return 0

            return (None, True, final)

        self.db.create_aggregate_function("badfunc4", badfactory)
        self.assertRaises(TypeError, c.execute, "select badfunc4(x) from foo")

        # final not callable
        def badfactory():

            def badfunc(*args):
                pass

            def final(*args):
                return 0

            return (None, badfunc, True)

        self.db.create_aggregate_function("badfunc5", badfactory)
        self.assertRaises(TypeError, c.execute, "select badfunc5(x) from foo")

        # error in factory method
        def badfactory():
            1 / 0

        self.db.create_aggregate_function("badfunc6", badfactory)
        self.assertRaises(ZeroDivisionError, c.execute, "select badfunc6(x) from foo")

        # class based aggregate
        class summer:

            def __init__(self):
                self.sum = 0

            def step(self, v):
                self.sum += v

            def final(self):
                return self.sum

        c.execute("create table xyz(val)")
        c.executemany("insert into xyz values(?)", ((i, ) for i in range(20)))
        self.db.create_aggregate_function("summer", summer, -1)
        self.assertEqual(sum(range(20)), c.execute("select summer(val) from xyz").get)

        del summer.final
        self.assertRaises(AttributeError, c.execute, "select summer(val) from xyz")
        del summer.step
        self.assertRaises(AttributeError, c.execute, "select summer(val) from xyz")

        summer.step = summer.final = 3
        self.db.create_aggregate_function("summer1", summer, -1)
        self.assertRaisesRegex(TypeError, "step function must be callable", c.execute, "select summer1(val) from xyz")

        summer.step = lambda v: 0
        self.db.create_aggregate_function("summer2", summer, -1)
        self.assertRaisesRegex(TypeError, "final function must be callable", c.execute, "select summer2(val) from xyz")

    def testWindowFunctions(self):
        "Verify window functions"

        # check the sqlite example works
        class windowfunc:

            def __init__(self):
                self.v = 0

            def step(self, arg):
                self.v += arg

            def inverse(self, arg):
                self.v -= arg

            def final(self):
                return self.v

            def value(self):
                return self.v

        self.assertRaises(TypeError, self.db.create_window_function, 3, 3)
        self.db.create_window_function("sumint", None)
        self.db.create_window_function("sumint", windowfunc)
        self.db.execute("""CREATE TABLE t3(x, y);
                INSERT INTO t3 VALUES('a', 4),('b', 5),('c', 3),('d', 8),('e', 1);""")
        query = """SELECT x, sumint(y) OVER (ORDER BY x ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS sum_y
            FROM t3 ORDER BY x;"""
        expected = [('a', 9), ('b', 12), ('c', 16), ('d', 12), ('e', 9)]
        self.assertEqual(self.db.execute(query).fetchall(), expected)

        self.db.create_window_function("sumint", None)
        try:
            self.db.execute(query)
            1 / 0  # should not be reached
        except apsw.SQLError as e:
            self.assertIn("no such function: sumint", str(e))

        def factory():
            return windowfunc(), windowfunc.step, windowfunc.value, windowfunc.final, windowfunc.inverse

        self.db.create_window_function("sumint", factory)
        self.assertEqual(self.db.execute(query).fetchall(), expected)

        # now all the errors
        for factory, exc in (
            (lambda: 3, AttributeError),
            (lambda x: 3, TypeError),
            (lambda: 1 / 0, ZeroDivisionError),
            (lambda: [
                object,
            ] + [99] * 3, TypeError),
            (lambda: [
                object,
            ] + [99] * 5, TypeError),
        ):
            self.db.create_window_function("sumint", factory)
            self.assertRaises(exc, self.db.execute, query)

        args = [lambda: 3] * 4
        names = "step", "final", "value", "inverse"

        for can_cache in (False, True):
            for counter, n in enumerate(names):
                self.db.create_window_function("sumint", None)
                a = args[:]
                a[counter] = "a string"
                self.db.create_window_function("sumint", lambda: [object] + a)
                try:
                    self.db.execute(query)
                    1 / 0
                except TypeError as e:
                    self.assertIn(n, str(e))
                setattr(windowfunc, n + "orig", getattr(windowfunc, n))
                setattr(windowfunc, n, lambda *args: 1 / 0)
                self.db.create_window_function("sumint", windowfunc)
                try:
                    self.db.execute(query, can_cache=can_cache).fetchall()
                    raise Exception("should not be reached")
                except ZeroDivisionError:
                    pass
                setattr(windowfunc, n, getattr(windowfunc, n + "orig"))

        # cause final to run while the statement is being closed
        # https://sqlite.org/forum/forumpost/d72cba6ff7
        class windowfunc2:

            def step(*args):
                return 3

            inverse = value = step

            def final(*args):
                1 / 0

        self.db.create_window_function("sumint", windowfunc2)
        with self.assertRaises(ZeroDivisionError):
            self.db.execute(query, can_cache=False).close()
        with self.assertRaises(ZeroDivisionError):
            self.db.execute(query, can_cache=True).close()
        self.db.close()

    def testFastcall(self):
        "fastcall argument processing"
        # function that takes one argument
        self.assertRaisesRegex(TypeError, "Missing required parameter", apsw.sleep)
        self.assertRaisesRegex(TypeError, "'fred' is an invalid keyword argument", apsw.sleep, fred=3)
        self.assertRaisesRegex(TypeError,
                               "argument 'milliseconds' given by name and position",
                               apsw.sleep,
                               10,
                               milliseconds=20)
        self.assertRaisesRegex(TypeError, "Too many positional arguments", apsw.sleep, 10, 20)

        # many args where varargs had to be converted to fastcall
        self.assertRaisesRegex(TypeError, "Missing required parameter", apsw.Connection)
        apsw.Connection("")
        apsw.Connection("", vfs=None)
        self.assertRaisesRegex(apsw.SQLError, "no such vfs", apsw.Connection, "", vfs="fred")
        c = apsw.Connection(statementcachesize=22, flags=apsw.SQLITE_OPEN_READONLY, vfs=None, filename="")
        self.assertEqual(22, c.cache_stats()["size"])
        self.assertRaisesRegex(TypeError,
                               "Missing required parameter",
                               apsw.Connection,
                               statementcachesize=22,
                               flags=apsw.SQLITE_OPEN_READONLY,
                               vfs=None)

        # keyword only args
        self.assertRaisesRegex(TypeError, "Too many positional arguments", c.execute, "select 3", None, False)
        self.assertEqual(3,
                         c.execute(explain=0, prepare_flags=1, can_cache=1, bindings=None, statements="select 3").get)
        self.assertRaisesRegex(TypeError,
                               "argument 'statements' given by name and position",
                               c.execute,
                               "select 4",
                               explain=0,
                               prepare_flags=1,
                               can_cache=1,
                               bindings=None,
                               statements="select 3")

    def testCollation(self):
        "Verify collations"
        # create a whole bunch to check they are freed
        for i in range(1024):
            self.db.create_collation("x" * i, lambda x, y: i)
        for ii in range(1024):
            self.db.create_collation("x" * ii, lambda x, y: ii)

        c = self.db.cursor()

        def strnumcollate(s1, s2):
            "return -1 if s1<s2, +1 if s1>s2 else 0.  Items are string head and numeric tail"
            # split values into two parts - the head and the numeric tail
            values = [s1, s2]
            for vn, v in enumerate(values):
                for i in range(len(v), 0, -1):
                    if v[i - 1] not in "01234567890":
                        break
                try:
                    v = v[:i], int(v[i:])
                except ValueError:
                    v = v[:i], None
                values[vn] = v
            # compare
            if values[0] < values[1]:
                return -1  # return an int
            if values[0] > values[1]:
                return 1  # and a long
            return 0

        self.assertRaises(TypeError, self.db.create_collation, "twelve", strnumcollate, 12)  # wrong # params
        self.assertRaises(TypeError, self.db.create_collation, "twelve", 12)  # must be callable
        self.db.create_collation("strnum", strnumcollate)
        c.execute("create table foo(x)")
        # adding this unicode in front improves coverage
        uni = "\N{LATIN SMALL LETTER E WITH CIRCUMFLEX}"
        vals = (uni + "file1", uni + "file7", uni + "file9", uni + "file17", uni + "file20")
        valsrev = list(vals)
        valsrev.reverse()  # put them into table in reverse order
        valsrev = valsrev[1:] + valsrev[:1]  # except one out of order
        c.executemany("insert into foo values(?)", [(x, ) for x in valsrev])
        for i, row in enumerate(c.execute("select x from foo order by x collate strnum")):
            self.assertEqual(vals[i], row[0])

        # collation function with an error
        def collerror(*args):
            return 1 / 0

        self.db.create_collation("collerror", collerror)
        self.assertRaises(ZeroDivisionError, c.execute, "select x from foo order by x collate collerror")

        # collation function that returns bad value
        def collerror(*args):
            return {}

        self.db.create_collation("collbadtype", collerror)
        self.assertRaises(TypeError, c.execute, "select x from foo order by x collate collbadtype")

        # get error when registering
        c.execute("select x from foo order by x collate strnum")  # nb we don't read so cursor is still active
        self.assertRaises(apsw.BusyError, self.db.create_collation, "strnum", strnumcollate)

        # unregister
        for row in c:
            pass
        self.db.create_collation("strnum", None)
        # check it really has gone
        try:
            c.execute("select x from foo order by x collate strnum")
        except apsw.SQLError:
            pass
        # check statement still works
        for _ in c.execute("select x from foo"):
            pass

        # collation needed testing
        self.assertRaises(TypeError, self.db.collation_needed, 12)

        def cn1():
            pass

        def cn2(x, y):
            1 / 0

        def cn3(x, y):
            self.assertTrue(x is self.db)
            self.assertEqual(y, "strnum")
            self.db.create_collation("strnum", strnumcollate)

        self.db.collation_needed(cn1)
        try:
            for _ in c.execute("select x from foo order by x collate strnum"):
                pass
        except TypeError:
            pass
        self.db.collation_needed(cn2)
        try:
            for _ in c.execute("select x from foo order by x collate strnum"):
                pass
        except ZeroDivisionError:
            pass
        self.db.collation_needed(cn3)
        for _ in c.execute("select x from foo order by x collate strnum"):
            pass
        self.db.collation_needed(None)
        self.db.create_collation("strnum", None)

        # check it really has gone
        try:
            c.execute("select x from foo order by x collate strnum")
        except apsw.SQLError:
            pass

    def testProgressHandler(self):
        "Verify progress handler"
        c = self.db.cursor()
        phcalledcount = [0]

        def ph():
            phcalledcount[0] = phcalledcount[0] + 1
            return 0

        # make 400 rows of random numbers
        c.execute("begin ; create table foo(x)")
        c.executemany("insert into foo values(?)", randomintegers(400))
        c.execute("commit")

        self.assertRaises(TypeError, self.db.set_progress_handler, 12)  # must be callable
        self.assertRaises(TypeError, self.db.set_progress_handler, ph, "foo")  # second param is steps
        self.db.set_progress_handler(ph, -17)  # SQLite doesn't complain about negative numbers
        self.db.set_progress_handler(ph, 20)
        curnext(c.execute("select max(x) from foo"))

        self.assertNotEqual(phcalledcount[0], 0)
        saved = phcalledcount[0]

        # put an error in the progress handler
        def ph():
            return 1 / 0

        self.db.set_progress_handler(ph, 1)
        self.assertRaises(ZeroDivisionError, c.execute, "update foo set x=-10")
        self.db.set_progress_handler(None)  # clear ph so next line runs
        # none should have taken
        self.assertEqual(0, curnext(c.execute("select count(*) from foo where x=-10"))[0])
        # and previous ph should not have been called
        self.assertEqual(saved, phcalledcount[0])

        def ph():
            return BadIsTrue()

        self.db.set_progress_handler(ph, 1)
        self.assertRaises(ZeroDivisionError, c.execute, "update foo set x=-10")

    def testChanges(self):
        "Verify reporting of changes"
        c = self.db.cursor()
        c.execute("create table foo (x);begin")
        for i in range(100):
            c.execute("insert into foo values(?)", (i + 1000, ))
        c.execute("commit")
        c.execute("update foo set x=0 where x>=1000")
        self.assertEqual(100, self.db.changes())
        c.execute("begin")
        for i in range(100):
            c.execute("insert into foo values(?)", (i + 1000, ))

        # for coverage
        self.db.cache_flush()
        getattr(self.db, "release_" "memory")() # avoids old name testing false positive

        c.execute("commit")
        self.assertEqual(300, self.db.total_changes())
        if hasattr(apsw, "faultdict"):
            # check 64 bit conversion works
            apsw.faultdict["ConnectionChanges64"] = True
            self.assertEqual(1000000000 * 7 * 3, self.db.changes())

    def testLastInsertRowId(self):
        "Check last insert row id"
        c = self.db.cursor()
        c.execute("create table foo (x integer primary key)")
        for i in range(10):
            c.execute("insert into foo values(?)", (i, ))
            self.assertEqual(i, self.db.last_insert_rowid())
        # get a 64 bit value
        v = 2**40
        c.execute("insert into foo values(?)", (v, ))
        self.assertEqual(v, self.db.last_insert_rowid())
        # try setting it
        self.assertRaises(
            TypeError,
            self.db.set_last_insert_rowid,
        )
        self.assertRaises(TypeError, self.db.set_last_insert_rowid, "3")
        self.assertRaises(TypeError, self.db.set_last_insert_rowid, "3", 3)
        self.assertRaises(OverflowError, self.db.set_last_insert_rowid, 2**40 * 2**40)
        for v in -20, 0, 20, 2**32 - 1, -2**32 - 1, 2**60, -2**60:
            c.execute("insert into foo values(?)", (v - 3, ))
            self.assertNotEqual(v, self.db.last_insert_rowid())
            self.db.set_last_insert_rowid(v)
            self.assertEqual(v, self.db.last_insert_rowid())

    def testComplete(self):
        "Completeness of SQL statement checking"
        # the actual underlying routine just checks that there is a semi-colon
        # at the end, not inside any quotes etc
        self.assertEqual(False, apsw.complete("select * from"))
        self.assertEqual(False, apsw.complete("select * from \";\""))
        self.assertEqual(False, apsw.complete("select * from \";"))
        self.assertEqual(True, apsw.complete("select * from foo; select *;"))
        self.assertEqual(False, apsw.complete("select * from foo where x=1"))
        self.assertEqual(True, apsw.complete("select * from foo;"))
        self.assertEqual(True, apsw.complete("select '\u9494\ua7a7';"))
        self.assertRaises(TypeError, apsw.complete, 12)  # wrong type
        self.assertRaises(TypeError, apsw.complete)  # not enough args
        self.assertRaises(TypeError, apsw.complete, "foo", "bar")  # too many args

    def testBusyHandling(self):
        "Verify busy handling"
        c = self.db.cursor()
        c.execute("create table foo(x); begin")
        c.executemany("insert into foo values(?)", randomintegers(400))
        c.execute("commit")
        # verify it is blocked
        db2 = apsw.Connection(TESTFILEPREFIX + "testdb")
        c2 = db2.cursor()
        c2.execute("begin exclusive")
        try:
            self.assertRaises(apsw.BusyError, c.execute, "begin immediate ; select * from foo")
        finally:
            del c2
            db2.close()
            del db2

        # close and reopen databases - sqlite will return Busy immediately to a connection
        # it previously returned busy to
        del c
        self.db.close()
        del self.db
        self.db = apsw.Connection(TESTFILEPREFIX + "testdb")
        db2 = apsw.Connection(TESTFILEPREFIX + "testdb")
        c = self.db.cursor()
        c2 = db2.cursor()

        # Put in busy handler
        bhcalled = [0]

        def bh(*args):
            bhcalled[0] = bhcalled[0] + 1
            if bhcalled[0] == 4:
                return False
            return True

        self.assertRaises(TypeError, db2.set_busy_handler, 12)  # must be callable
        self.assertRaises(TypeError, db2.set_busy_timeout, "12")  # must be int
        db2.set_busy_timeout(
            -77)  # SQLite doesn't complain about negative numbers, but if it ever does this will catch it
        self.assertRaises(TypeError, db2.set_busy_timeout, 77, 88)  # too many args
        self.db.set_busy_handler(bh)

        c2.execute("begin exclusive")

        try:
            for row in c.execute("begin immediate ; select * from foo"):
                self.fail("Transaction wasn't exclusive")
        except apsw.BusyError:
            pass
        self.assertEqual(bhcalled[0], 4)

        # Close and reopen again
        del c
        del c2
        db2.close()
        self.db.close()
        del db2
        del self.db
        self.db = apsw.Connection(TESTFILEPREFIX + "testdb")
        db2 = apsw.Connection(TESTFILEPREFIX + "testdb")
        c = self.db.cursor()
        c2 = db2.cursor()

        # Put in busy timeout
        TIMEOUT = 3  # seconds, must be integer as sqlite can round down to nearest second anyway
        c2.execute("begin exclusive")
        self.assertRaises(TypeError, self.db.set_busy_handler, "foo")
        self.db.set_busy_timeout(int(TIMEOUT * 1000))
        b4 = time.time()
        try:
            c.execute("begin immediate ; select * from foo")
        except apsw.BusyError:
            pass
        after = time.time()
        took = after - b4
        # this sometimes fails in virtualized environments due to time
        # going backwards or not going forwards consistently.
        if took + 1 < TIMEOUT:
            print(f"Timeout was { TIMEOUT } seconds but only { took } seconds elapsed!")
            self.assertTrue(took >= TIMEOUT)

        # check clearing of handler
        c2.execute("rollback")
        self.db.set_busy_handler(None)
        b4 = time.time()
        c2.execute("begin exclusive")
        try:
            c.execute("begin immediate ; select * from foo")
        except apsw.BusyError:
            pass
        after = time.time()
        self.assertTrue(after - b4 < TIMEOUT)

        # Close and reopen again
        del c
        del c2
        db2.close()
        self.db.close()
        del db2
        del self.db
        self.db = apsw.Connection(TESTFILEPREFIX + "testdb")
        db2 = apsw.Connection(TESTFILEPREFIX + "testdb")
        c = self.db.cursor()
        c2 = db2.cursor()

        # error in busyhandler
        def bh(*args):
            1 / 0

        c2.execute("begin exclusive")
        self.db.set_busy_handler(bh)
        self.assertRaises(ZeroDivisionError, c.execute, "begin immediate ; select * from foo")
        del c
        del c2
        db2.close()

        def bh(*args):
            return BadIsTrue()

        db2 = apsw.Connection(TESTFILEPREFIX + "testdb")
        c = self.db.cursor()
        c2 = db2.cursor()
        c2.execute("begin exclusive")
        self.db.set_busy_handler(bh)
        self.assertRaises(ZeroDivisionError, c.execute, "begin immediate ; select * from foo")
        del c
        del c2
        db2.close()

    def testBusyHandling2(self):
        "Another busy handling test"

        # Based on an issue in 3.3.10 and before
        con2 = apsw.Connection(TESTFILEPREFIX + "testdb")
        cur = self.db.cursor()
        cur2 = con2.cursor()
        cur.execute("create table test(x,y)")
        cur.execute("begin")
        cur.execute("insert into test values(123,'abc')")
        self.assertRaises(apsw.BusyError, cur2.execute, "insert into test values(456, 'def')")
        cur.execute("commit")
        self.assertEqual(1, curnext(cur2.execute("select count(*) from test where x=123"))[0])
        con2.close()

    def testInterruptHandling(self):
        "Verify interrupt function"
        # this is tested by having a user defined function make the interrupt
        self.assertFalse(self.db.is_interrupted)
        c = self.db.cursor()
        c.execute("create table foo(x);begin")
        c.executemany("insert into foo values(?)", randomintegers(400))
        c.execute("commit")

        def ih(*args):
            self.db.interrupt()
            self.assertTrue(self.db.is_interrupted)
            return 7

        self.db.create_scalar_function("seven", ih)
        try:
            for row in c.execute("select seven(x) from foo"):
                pass
        except apsw.InterruptError:
            pass
        self.db.close(True)

    def testCommitHook(self):
        "Verify commit hooks"
        c = self.db.cursor()
        c.execute("create table foo(x)")
        c.executemany("insert into foo values(?)", randomintegers(10))
        chcalled = [0]

        def ch():
            chcalled[0] = chcalled[0] + 1
            if chcalled[0] == 4:
                return 1  # abort
            return 0  # continue

        self.assertRaises(TypeError, self.db.set_commit_hook, 12)  # not callable
        self.db.set_commit_hook(ch)
        self.assertRaises(apsw.ConstraintError, c.executemany, "insert into foo values(?)", randomintegers(10))
        self.assertEqual(4, chcalled[0])
        self.db.set_commit_hook(None)

        def ch():
            chcalled[0] = 99
            return 1

        self.db.set_commit_hook(ch)
        self.assertRaises(apsw.ConstraintError, c.executemany, "insert into foo values(?)", randomintegers(10))
        # verify it was the second one that was called
        self.assertEqual(99, chcalled[0])

        # error in commit hook
        def ch():
            return 1 / 0

        self.db.set_commit_hook(ch)
        self.assertRaises(ZeroDivisionError, c.execute, "insert into foo values(?)", (1, ))

        def ch():
            return BadIsTrue()

        self.db.set_commit_hook(ch)
        self.assertRaises(ZeroDivisionError, c.execute, "insert into foo values(?)", (1, ))

    def testRollbackHook(self):
        "Verify rollback hooks"
        c = self.db.cursor()
        c.execute("create table foo(x)")
        rhcalled = [0]

        def rh():
            rhcalled[0] = rhcalled[0] + 1
            return 1

        self.assertRaises(TypeError, self.db.set_rollback_hook, 12)  # must be callable
        self.db.set_rollback_hook(rh)
        c.execute("begin ; insert into foo values(10); rollback")
        self.assertEqual(1, rhcalled[0])
        self.db.set_rollback_hook(None)
        c.execute("begin ; insert into foo values(10); rollback")
        self.assertEqual(1, rhcalled[0])

        def rh():
            1 / 0

        self.db.set_rollback_hook(rh)
        # SQLite doesn't allow reporting an error from a rollback hook, so it will be seen
        # in the next command (eg the select in this case)
        self.assertRaises(ZeroDivisionError, c.execute,
                          "begin ; insert into foo values(10); rollback; select * from foo")
        # check cursor still works
        for row in c.execute("select * from foo"):
            pass

    def testUpdateHook(self):
        "Verify update hooks"
        c = self.db.cursor()
        c.execute("create table foo(x integer primary key, y)")
        uhcalled = []

        def uh(type, databasename, tablename, rowid):
            uhcalled.append((type, databasename, tablename, rowid))

        self.assertRaises(TypeError, self.db.set_update_hook, 12)  # must be callable
        self.db.set_update_hook(uh)
        statements = (
            ("insert into foo values(3,4)", (apsw.SQLITE_INSERT, 3)),
            ("insert into foo values(30,40)", (apsw.SQLITE_INSERT, 30)),
            (
                "update foo set y=47 where x=3",
                (apsw.SQLITE_UPDATE, 3),
            ),
            (
                "delete from foo where y=47",
                (apsw.SQLITE_DELETE, 3),
            ),
        )
        for sql, res in statements:
            c.execute(sql)
        results = [(type, "main", "foo", rowid) for sql, (type, rowid) in statements]
        self.assertEqual(uhcalled, results)
        self.db.set_update_hook(None)
        c.execute("insert into foo values(99,99)")
        self.assertEqual(len(uhcalled), len(statements))  # length should have remained the same

        def uh(*args):
            1 / 0

        self.db.set_update_hook(uh)
        self.assertRaises(ZeroDivisionError, c.execute, "insert into foo values(100,100)")
        self.db.set_update_hook(None)
        # improve code coverage
        c.execute("create table bar(x,y); insert into bar values(1,2); insert into bar values(3,4)")

        def uh(*args):
            1 / 0

        self.db.set_update_hook(uh)
        self.assertRaises(ZeroDivisionError, c.execute, "insert into foo select * from bar")
        self.db.set_update_hook(None)

        # check cursor still works
        c.execute("insert into foo values(1000,1000)")
        self.assertEqual(1, curnext(c.execute("select count(*) from foo where x=1000"))[0])

    def testProfile(self):
        "Verify profiling"
        # we do the test by looking for the maximum of PROFILESTEPS random
        # numbers with an index present and without.  The former
        # should be way quicker.
        c = self.db.cursor()
        c.execute("create table foo(x); begin")
        c.executemany("insert into foo values(?)", randomintegers(PROFILESTEPS))
        profileinfo = []

        def profile(statement, timing):
            profileinfo.append((statement, timing))

        c.execute("commit; create index foo_x on foo(x)")
        self.assertRaises(TypeError, self.db.set_profile, 12)  # must be callable
        self.db.set_profile(profile)
        for val1 in c.execute("select max(x) from foo"):
            pass  # profile is only run when results are exhausted
        self.db.set_profile(None)
        c.execute("drop index foo_x")
        self.db.set_profile(profile)
        for val2 in c.execute("select max(x) from foo"):
            pass
        self.assertEqual(val1, val2)
        self.assertTrue(len(profileinfo) >= 2)  # see SQLite ticket 2157
        self.assertEqual(profileinfo[0][0], profileinfo[-1][0])
        self.assertEqual("select max(x) from foo", profileinfo[0][0])
        self.assertEqual("select max(x) from foo", profileinfo[-1][0])
        # the query using the index should take way less time
        self.assertTrue(profileinfo[0][1] <= profileinfo[-1][1])

        def profile(*args):
            1 / 0

        self.db.set_profile(profile)
        self.assertRaises(ZeroDivisionError, c.execute, "create table bar(y)")
        # coverage
        wasrun = [False]

        def profile(*args):
            wasrun[0] = True

        def uh(*args):
            1 / 0

        self.db.set_profile(profile)
        self.db.set_update_hook(uh)
        self.assertRaises(ZeroDivisionError, c.execute, "insert into foo values(3)")
        self.assertEqual(wasrun[0], False)
        self.db.set_profile(None)
        self.db.set_update_hook(None)

    def testThreading(self):
        "Verify threading behaviour"
        # We used to require all operations on a connection happen in
        # the same thread.  Now they can happen in any thread, so we
        # ensure that inuse errors are detected by doing a long
        # running operation in one thread.
        c = self.db.cursor()
        c.execute("create table foo(x);begin;")
        c.executemany("insert into foo values(?)", randomintegers(10000))
        c.execute("commit")

        vals = {"stop": False, "raised": False}

        def wt():
            try:
                while not vals["stop"]:
                    c.execute("select min(max(x-1+x),min(x-1+x)) from foo")
            except apsw.ThreadingViolationError:
                vals["raised"] = True
                vals["stop"] = True

        t = ThreadRunner(wt)
        t.start()
        # ensure thread t has started
        time.sleep(0.1)
        b4 = time.time()
        # try to get a threadingviolation for 30 seconds
        try:
            try:
                while not vals["stop"] and time.time() - b4 < 30:
                    c.execute("select * from foo")
            except apsw.ThreadingViolationError:
                vals["stop"] = True
                vals["raised"] = True
        finally:
            vals["stop"] = True
        t.go()
        self.assertEqual(vals["raised"], True)

    def testStringsWithNulls(self):
        "Verify that strings with nulls in them are handled correctly"

        c = self.db.cursor()
        c.execute("create table foo(row,str)")
        vals = ("\0a simple string", "a simple string\0with a null", "a string\0with two\0nulls",
                "or even a \0\0\0\0\0\0sequence\0\0\0\0of them", "a \u1234 unic\0de \ufe54 string \u0089",
                "a \u1234 unicode \ufe54 string \u0089\0and some text",
                "\N{BLACK STAR} \N{WHITE STAR} \N{LIGHTNING} \N{COMET}\0more\0than you\0can handle",
                "\N{BLACK STAR} \N{WHITE STAR} \N{LIGHTNING} \N{COMET}\0\0\0\0\0sequences\0\0\0of them")

        vals = vals + (
            "a simple string\0",
            "a \u1234 unicode \ufe54 string \u0089\0",
        )

        for i, v in enumerate(vals):
            c.execute("insert into foo values(?,?)", (i, v))

        # add function to test conversion back as well
        def snap(*args):
            return args[0]

        self.db.create_scalar_function("snap", snap)

        # now see what we got out
        count = 0
        for row, v, fv in c.execute("select row,str,snap(str) from foo"):
            count += 1
            self.assertEqual(vals[row], v)
            self.assertEqual(vals[row], fv)
        self.assertEqual(count, len(vals))

        # check execute
        for v in vals:
            self.assertEqual(v, curnext(c.execute("select ?", (v, )))[0])
            # nulls not allowed in main query string, so lets check the other bits (unicode etc)
            v2 = v.replace("\0", " zero ")
            self.assertEqual(v2, curnext(c.execute("select '%s'" % (v2, )))[0])

        # collation
        def colcmp(l, r):
            self.assertTrue("\0" in l and "\0" in r)
            if l < r: return -1
            if l > r: return 1
            return 0

        self.db.create_collation("colcmp", colcmp)
        self.db.execute("select str from foo order by str collate colcmp").get

    def testSharedCache(self):
        "Verify setting of shared cache"

        # check parameters - wrong # or type of args
        self.assertRaises(TypeError, apsw.enable_shared_cache)
        self.assertRaises(TypeError, apsw.enable_shared_cache, "foo")
        self.assertRaises(TypeError, apsw.enable_shared_cache, True, None)

        # the setting can be changed at almost any time
        apsw.enable_shared_cache(True)
        apsw.enable_shared_cache(False)

    def testSerialize(self):
        "Verify serialize/deserialize calls"
        # check param types
        self.assertRaises(TypeError, self.db.serialize)
        self.assertRaises(TypeError, self.db.serialize, "a", "b")
        self.assertRaises(TypeError, self.db.serialize, 3)
        self.assertRaises(TypeError, self.db.deserialize, 3)
        self.assertRaises(TypeError, self.db.deserialize, "main", "main")

        # SQLite implementation detail: empty temp db gives back None
        self.assertEqual(None, self.db.serialize("temp"))

        # SQLite implementation detail: unknown name gives back None instead of error
        self.assertEqual(None, self.db.serialize("nosuchdbname"))

        # populate with real content
        self.db.cursor().execute("create table temp.foo(x); insert into temp.foo values(3), (4), (5)")
        # must have content now
        self.assertNotEqual(None, self.db.serialize("temp"))
        self.assertTableNotExists("main.foo")
        self.db.deserialize("main", self.db.serialize("temp"))
        # without this renaming, things get confused between identical tables in main and temp
        self.db.cursor().execute("alter table main.foo rename to bar")
        self.assertTablesEqual(self.db, "bar", self.db, "foo")
        # check we can modify deserialized
        self.db.cursor().execute("insert into bar values(3)")
        self.db.deserialize("main", self.db.serialize("temp"))
        self.db.cursor().execute("alter table temp.foo rename to bar")
        self.assertTablesEqual(self.db, "foo", self.db, "bar")
        # add a megabyte to table
        self.db.cursor().execute("insert into foo values(zeroblob(1024024))")

    # A check that various extensions (such as fts3, rtree, icu)
    # actually work.  We don't know if they were supposed to be
    # compiled in or not so the assumption is that they aren't.
    # However setup.py is being run then it sets environment variables
    # saying the extensions *must* be present if they were enabled.
    # See https://github.com/rogerbinns/apsw/issues/55 for what
    # led to this.
    def checkOptionalExtension(self, name, testquery):
        try:
            present = False
            apsw.Connection(":memory:").cursor().execute(testquery)
            present = True
        except apsw.Error:
            pass
        if "APSW_TEST_" + name.upper() in os.environ:
            self.assertEqual(present, True)
        return present

    def testFTSExtension(self):
        "Check FTS extensions (if present)"
        for v in 3, 4, 5:
            self.checkFTSExtension(v)

    def checkFTSExtension(self, v):
        self.db.cursor().execute("drop table if exists foo; drop table if exists test")
        if not self.checkOptionalExtension("fts" + str(v), "create virtual table foo using fts%d()" % v):
            return
        c = self.db.cursor()
        data = {
            'cake': 'flour, eggs, milk',
            'bbq ribs': 'ribs, hot sauce',
            'mayo': 'oil, Eggs',
            'glue': 'Egg',
            'salmon': 'Fish',
            'burger': 'Mechanically recovered meat',
            'broccoli stew': 'broccoli peppers cheese tomatoes',
            'pumpkin stew': 'pumpkin onions garlic celery',
            'broccoli pie': 'broccoli cheese onions flour',
            'pumpkin pie': 'pumpkin sugar flour butter'
        }

        c.execute("create virtual table test using fts%d(name, ingredients)" % v)
        c.executemany("insert into test values(?,?)", data.items())

        def check(pattern, expectednames):
            names = [n[0] for n in c.execute("select name from test where ingredients match ?", (pattern, ))]
            names.sort()
            expectednames = list(expectednames)
            expectednames.sort()
            self.assertEqual(names, expectednames)

        check('onions cheese', ['broccoli pie'])
        check('eggs OR oil', ['cake', 'mayo'])
        check('"pumpkin onions"', ['pumpkin stew'])

    def testRTreeExtension(self):
        "Check RTree extension if present"
        if not self.checkOptionalExtension("rtree",
                                           "create virtual table foo using rtree(one, two, three, four, five)"):
            return
        c = self.db.cursor()
        data = (
            (1, 2, 3, 4),
            (5.1, 6, 7.2, 8),
            (1, 4, 9, 12),
            (77, 77.1, 3, 9),
        )
        c.execute("create virtual table test using rtree(ii, x1, x2, y1, y2)")
        for i, row in enumerate(data):
            c.execute("insert into test values(?,?,?,?,?)", (i, row[0], row[1], row[2], row[3]))

        def check(pattern, expectedrows):
            rows = [n[0] for n in c.execute("select ii from test where " + pattern)]
            rows.sort()
            expectedrows = list(expectedrows)
            expectedrows.sort()
            self.assertEqual(rows, expectedrows)

        check("x1>2 AND x2<7 AND y1>17.2 AND y2<=8", [])
        check("x1>5 AND x2<=6 AND y1>-11 AND y2<=8", [1])

    def testGeopolyExtenstion(self):
        "Check geopoly extension if present"
        if not self.checkOptionalExtension("geopoly", "CREATE VIRTUAL TABLE newtab USING geopoly()"):
            return
        found = 0
        for row in self.db.cursor().execute(
                "CREATE VIRTUAL TABLE newtab USING geopoly();"
                "INSERT INTO newtab(_shape) VALUES('[[0,0],[1,0],[0.5,1],[0,0]]');"
                "SELECT * FROM newtab WHERE geopoly_overlap(_shape, $1);", ("[[0,0],[1,0],[0.5,1],[0,0]]", )):
            found += 1
        self.assertEqual(found, 1)

    def testICUExtension(self):
        "Check ICU extension if present"
        if not self.checkOptionalExtension("icu", "select lower('I', 'tr_tr')"):
            return

        c = self.db.cursor()

        # we compare SQLite standard vs icu
        def check(text, locale, func="lower", equal=False):
            q = "select " + func + "(?%s)"
            sqlite = c.execute(q % ("", ), (text, )).fetchall()
            icu = c.execute(q % (",'" + locale + "'", ), (text, )).fetchall()
            if equal:
                self.assertEqual(sqlite, icu)
            else:
                self.assertNotEqual(sqlite, icu)

        check("I", "tr_tr")
        check("I", "en_us", equal=True)

    def testJSON1Extension(self):
        # some sanity checks that it is working
        l = self.db.cursor().execute("select json_array_length('[1,2,3,4]')").fetchall()[0][0]
        self.assertEqual(l, 4)
        l = self.db.cursor().execute(
            """select json_extract('{"a":2,"c":[4,5,{"f":7}]}', '$.c[2].f')""").fetchall()[0][0]
        self.assertEqual(l, 7)

    def testTracebacks(self):
        "Verify augmented tracebacks"

        def badfunc(*args):
            zebra = 3
            1 / 0

        self.db.create_scalar_function("badfunc", badfunc)
        try:
            c = self.db.cursor()
            c.execute("select badfunc(1,'two',3.14)")
            self.fail("Exception should have occurred")
        except ZeroDivisionError:
            tb = sys.exc_info()[2]
            frames = []
            while tb:
                frames.append(tb.tb_frame)
                tb = tb.tb_next
        except:
            self.fail("Wrong exception type")

        frames.reverse()
        frame = frames[1]  # frame[0] is badfunc above
        self.assertTrue(frame.f_code.co_filename.endswith(".c"))
        self.assertTrue(frame.f_lineno > 100)
        self.assertTrue(frame.f_code.co_name.endswith("-badfunc"))
        # check local variables
        if platform.python_implementation() != "PyPy":
            l = frame.f_locals
            self.assertIn("NumberOfArguments", l)
            self.assertEqual(l["NumberOfArguments"], 3)

    def testLoadExtension(self):
        "Check loading of extensions"
        # unicode issues
        # they need to be enabled first (off by default)
        if self.db.config(apsw.SQLITE_DBCONFIG_ENABLE_LOAD_EXTENSION, -1):
            # someone wanted extension loading on by default!  Turn it back off
            self.db.config(apsw.SQLITE_DBCONFIG_ENABLE_LOAD_EXTENSION, 0)
        self.assertRaises(apsw.ExtensionLoadingError, self.db.loadextension, LOADEXTENSIONFILENAME)
        self.assertEqual(self.db.config(apsw.SQLITE_DBCONFIG_ENABLE_LOAD_EXTENSION, -1), 0)
        self.db.enableloadextension(False)
        self.assertRaises(ZeroDivisionError, self.db.enableloadextension, BadIsTrue())
        # should still be disabled
        self.assertEqual(self.db.config(apsw.SQLITE_DBCONFIG_ENABLE_LOAD_EXTENSION, 0), 0)
        self.assertRaises(apsw.ExtensionLoadingError, self.db.loadextension, LOADEXTENSIONFILENAME)
        self.assertEqual(self.db.config(apsw.SQLITE_DBCONFIG_ENABLE_LOAD_EXTENSION, 1), 1)
        self.db.loadextension(LOADEXTENSIONFILENAME)
        self.assertEqual(self.db.config(apsw.SQLITE_DBCONFIG_ENABLE_LOAD_EXTENSION, 0), 0)
        self.db.enableloadextension(True)
        # make sure it checks args
        self.assertRaises(TypeError, self.db.loadextension)
        self.assertRaises(TypeError, self.db.loadextension, 12)
        self.assertRaises(TypeError, self.db.loadextension, "foo", 12)
        self.assertRaises(TypeError, self.db.loadextension, "foo", "bar", 12)
        self.db.loadextension(LOADEXTENSIONFILENAME)
        c = self.db.cursor()
        self.assertEqual(1, curnext(c.execute("select half(2)"))[0])
        # second entry point hasn't been called yet
        self.assertRaises(apsw.SQLError, c.execute, "select doubleup(2)")
        # load using other entry point
        self.assertRaises(apsw.ExtensionLoadingError, self.db.loadextension, LOADEXTENSIONFILENAME, "doesntexist")
        self.db.loadextension(LOADEXTENSIONFILENAME, "alternate_sqlite3_extension_init")
        self.assertEqual(4, curnext(c.execute("select doubleup(2)"))[0])

    def testMakeSqliteMsgFromException(self):
        "Test C function that converts exception into SQLite error code"

        class Source:

            def Create1(self, *args):
                e = apsw.IOError()
                e.extendedresult = apsw.SQLITE_IOERR_ACCESS
                raise e

            def Create2(self, *args):
                e = apsw.IOError()
                e.extendedresult = (0x80 << 32) + apsw.SQLITE_IOERR_ACCESS  # bigger than 32 bits
                raise e

        self.db.create_module("foo", Source())
        for i in "1", "2":
            Source.Create = getattr(Source, "Create" + i)
            try:
                self.db.cursor().execute("create virtual table vt using foo()")
                1 / 0
            except:
                klass, value, tb = sys.exc_info()

            self.assertEqual(klass, apsw.IOError)
            self.assertTrue(isinstance(value, apsw.IOError))
            self.assertEqual(value.extendedresult & ((0xffff << 16) | 0xffff), apsw.SQLITE_IOERR_ACCESS)

    def testVtables(self):
        "Test virtual table functionality"

        data = (  # row 0 is headers, column 0 is rowid
            ("rowid", "name", "number", "item", "description"),
            (1, "Joe Smith", 1.1, "\u00f6\u1234", "foo"),
            (6000000000, "Road Runner", -7.3, "\u00f6\u1235", "foo"),
            (77, "Fred", 0, "\u00f6\u1236", "foo"),
        )

        dataschema = "create table this_should_be_ignored" + str(data[0][1:])
        # a query that will get constraints on every column
        allconstraints = "select rowid,* from foo where rowid>-1000 and name>='A' and number<=12.4 and item>'A' and description=='foo' order by item"
        allconstraintsl = [
            (-1, apsw.SQLITE_INDEX_CONSTRAINT_GT),  # rowid >
            (0, apsw.SQLITE_INDEX_CONSTRAINT_GE),  # name >=
            (1, apsw.SQLITE_INDEX_CONSTRAINT_LE),  # number <=
            (2, apsw.SQLITE_INDEX_CONSTRAINT_GT),  # item >
            (3, apsw.SQLITE_INDEX_CONSTRAINT_EQ),  # description ==
        ]

        for i in range(20):
            self.db.create_module("x" * i, lambda x: i)

        # If shared cache is enabled then vtable creation is supposed to fail
        try:
            apsw.enable_shared_cache(True)
            db = apsw.Connection(TESTFILEPREFIX + "testdb2")
            db.create_module("y", lambda x: 2)
        finally:
            apsw.enable_shared_cache(False)

        # The testing uses a different module name each time.  SQLite
        # doc doesn't define the semantics if a 2nd module is
        # registered with the same name as an existing one and I was
        # getting coredumps.  It looks like issues inside SQLite.

        cur = self.db.cursor()
        # should fail since module isn't registered
        self.assertRaises(apsw.SQLError, cur.execute, "create virtual table vt using testmod(x,y,z)")
        # wrong args
        self.assertRaises(TypeError, self.db.create_module, 1, 2, 3)
        # give a bad object
        self.db.create_module("testmod", 12)  # next line fails due to lack of Create method
        self.assertRaises(AttributeError, cur.execute, "create virtual table xyzzy using testmod(x,y,z)")

        class Source:

            def __init__(self, *expectargs):
                self.expectargs = expectargs

            def Create(self, *args):  # db, modname, dbname, tablename, args
                if self.expectargs != args[1:]:
                    raise ValueError("Create arguments are not correct.  Expected " + str(self.expectargs) +
                                     " but got " + str(args[1:]))
                1 / 0

            def CreateErrorCode(self, *args):
                # This makes sure that sqlite error codes happen.  The coverage checker
                # is what verifies the code actually works.
                raise apsw.BusyError("foo")

            def CreateUnicodeException(self, *args):
                raise Exception(
                    "\N{LATIN SMALL LETTER E WITH CIRCUMFLEX}\N{LATIN SMALL LETTER A WITH TILDE}\N{LATIN SMALL LETTER O WITH DIAERESIS}"
                )

            def CreateBadSchemaType(self, *args):
                return 12, None

            def CreateBadSchema(self, *args):
                return "this isn't remotely valid sql", None

            def CreateWrongNumReturns(self, *args):
                return "way", "too", "many", "items", 3

            def CreateBadSequence(self, *args):

                class badseq(object):

                    def __getitem__(self, which):
                        if which != 0:
                            1 / 0
                        return 12

                    def __len__(self):
                        return 2

                return badseq()

        # check Create does the right thing - we don't include db since it creates a circular reference
        self.db.create_module("testmod1", Source("testmod1", "main", "xyzzy", "1", '"one"'))
        self.assertRaises(ZeroDivisionError, cur.execute, 'create virtual table xyzzy using testmod1(1,"one")')
        # unicode
        uni = "\N{LATIN SMALL LETTER E WITH CIRCUMFLEX}\N{LATIN SMALL LETTER A WITH TILDE}\N{LATIN SMALL LETTER O WITH DIAERESIS}"

        self.db.create_module("testmod1dash1", Source("testmod1dash1", "main", uni, "1", '"' + uni + '"'))
        self.assertRaises(ZeroDivisionError, cur.execute,
                          'create virtual table %s using testmod1dash1(1,"%s")' % (uni, uni))
        Source.Create = Source.CreateErrorCode
        self.assertRaises(apsw.BusyError, cur.execute, 'create virtual table xyzzz using testmod1(2, "two")')
        Source.Create = Source.CreateUnicodeException
        self.assertRaises(Exception, cur.execute, 'create virtual table xyzzz using testmod1(2, "two")')
        Source.Create = Source.CreateBadSchemaType
        self.assertRaises(TypeError, cur.execute, 'create virtual table xyzzz using testmod1(2, "two")')
        Source.Create = Source.CreateBadSchema
        self.assertRaises(apsw.SQLError, cur.execute, 'create virtual table xyzzz2 using testmod1(2, "two")')
        Source.Create = Source.CreateWrongNumReturns
        self.assertRaises(TypeError, cur.execute, 'create virtual table xyzzz2 using testmod1(2, "two")')
        Source.Create = Source.CreateBadSequence
        self.assertRaises(ZeroDivisionError, cur.execute, 'create virtual table xyzzz2 using testmod1(2, "two")')

        # a good version of Source
        class Source:

            def Create(self, *args):
                return dataschema, VTable(list(data))

            Connect = Create

        class VTable:

            # A set of results from bestindex which should all generate TypeError.
            # Coverage checking will ensure all the code is appropriately tickled
            badbestindex = (
                12,
                (12, ),
                ((), ),
                (((), ), ),
                ((((), ), ), ),
                (((((), ), ), ), ),
                ((None, None, None, None, "bad"), ),
                ((0, None, (0, ), None, None), ),
                ((("bad", True), None, None, None, None), ),
                (((0, True), "bad", None, None, None), ),
                (None, "bad"),
                [4, (3, True), [2, False], 1, [0]],
            )
            numbadbextindex = len(badbestindex)

            def __init__(self, data):
                self.data = data
                self.bestindex3val = 0

            def BestIndex1(self, wrong, number, of, arguments):
                1 / 0

            def BestIndex2(self, *args):
                1 / 0

            def BestIndex3(self, constraints, orderbys):
                retval = self.badbestindex[self.bestindex3val]
                self.bestindex3val += 1
                if self.bestindex3val >= self.numbadbextindex:
                    self.bestindex3val = 0
                return retval

            def BestIndex4(self, constraints, orderbys):
                return (None, 12, "\N{LATIN SMALL LETTER E WITH CIRCUMFLEX}", "anything", "bad")

            def BestIndex4_1(self, constraints, orderbys):
                return (None, 12, "\N{LATIN SMALL LETTER E WITH CIRCUMFLEX}", True, "bad")

            def BestIndex5(self, constraints, orderbys):
                # unicode error
                return (None, None, "\xde\xad\xbe\xef")

            def BestIndex6(self, constraints, orderbys):
                return ((0, 1, (2, BadIsTrue()), 3, 4), )

            def BestIndex7(self, constraints, orderbys):
                return (None, 77, "foo", BadIsTrue(), 99)

            _bestindexreturn = 99

            def BestIndex99(self, constraints, orderbys):
                cl = list(constraints)
                cl.sort()
                assert allconstraintsl == cl
                assert orderbys == ((2, False), )
                retval = ([4, (3, True), [2, False], 1, (0, False)], 997, "\N{LATIN SMALL LETTER E WITH CIRCUMFLEX}",
                          False, 99)[:self._bestindexreturn]
                return retval

            def BestIndexGood(self, constraints, orderbys):
                return None

            def BestIndexGood2(self, constraints, orderbys):
                return []  # empty list is same as None

            def Open(self):
                return Cursor(self)

            def Open1(self, wrong, number, of, arguments):
                1 / 0

            def Open2(self):
                1 / 0

            def Open3(self):
                return None

            def Open99(self):
                return Cursor(self)

            UpdateInsertRow1 = None

            def UpdateInsertRow2(self, too, many, args):
                1 / 0

            def UpdateInsertRow3(self, rowid, fields):
                1 / 0

            def UpdateInsertRow4(self, rowid, fields):
                assert rowid is None
                return None

            def UpdateInsertRow5(self, rowid, fields):
                assert rowid is None
                return "this is not a number"

            def UpdateInsertRow6(self, rowid, fields):
                assert rowid is None
                return -922337203685477580799  # too big

            def UpdateInsertRow7(self, rowid, fields):
                assert rowid is None
                return 9223372036854775807  # ok

            def UpdateInsertRow8(self, rowid, fields):
                assert rowid is not None
                assert rowid == -12
                return "this should be ignored since rowid was supplied"

            def UpdateChangeRow1(self, too, many, args, methinks):
                1 / 0

            def UpdateChangeRow2(self, rowid, newrowid, fields):
                1 / 0

            def UpdateChangeRow3(self, rowid, newrowid, fields):
                assert newrowid == rowid

            def UpdateChangeRow4(self, rowid, newrowid, fields):
                assert newrowid == rowid + 20

            def UpdateDeleteRow1(self, too, many, args):
                1 / 0

            def UpdateDeleteRow2(self, rowid):
                1 / 0

            def UpdateDeleteRow3(self, rowid):
                assert rowid == 77

            def Disconnect1(self, too, many, args):
                1 / 0

            def Disconnect2(self):
                1 / 0

            def Disconnect3(self):
                pass

            def Destroy1(self, too, many, args):
                1 / 0

            def Destroy2(self):
                1 / 0

            def Destroy3(self):
                pass

            def Begin1(self, too, many, args):
                1 / 0

            def Begin2(self):
                1 / 0

            def Begin3(self):
                pass

            def Sync(self):
                pass

            def Commit(self):
                pass

            def Rollback(self):
                pass

            def Rename1(self, too, many, args):
                1 / 0

            def Rename2(self, x):
                1 / 0

            def Rename3(self, x):
                return ["thisshouldbeignored" * 25, [1]]

            def FindFunction1(self, too, many, args):
                1 / 0

            def FindFunction2(self, name, nargs):
                1 / 0

            def FindFunction3(self, name, nargs):
                return "this isn't a function"

            def FindFunction4(self, name, nargs):
                if nargs == 2:
                    return lambda x, y: x + y
                return None

            def FindFunction5(self, name, nargs):
                return (1, 2, 3, 4)

            def FindFunction6(self, name, nargs):
                return (1, 2)

            def FindFunction7(self, name, nargs):
                return (1, lambda x: x)

            def Integrity1(Self, schema, table, is_quick):
                self.assertEqual(schema, "main")
                self.assertEqual(table, "foo")
                self.assertEqual(is_quick, 0)
                return "a message"

            def Integrity2(Self, schema, table, is_quick):
                self.assertEqual(schema, "main")
                self.assertEqual(table, "foo")
                self.assertEqual(is_quick, 1)
                return 3

            def Integrity99(self, schema, table, is_quick):
                return None



        class Cursor:

            _bestindexreturn = 99

            def __init__(self, table):
                self.table = table

            def Filter1(self, toofewargs):
                1 / 0

            def Filter2(self, *args):
                1 / 0

            def Filter99(self, idxnum, idxstr, constraintargs):
                self.pos = 1  # row 0 is headers
                if self._bestindexreturn == 0:
                    assert idxnum == 0
                    assert idxstr == None
                    assert constraintargs == ()
                    return
                if self._bestindexreturn == 1:
                    assert idxnum == 0
                    assert idxstr == None
                    assert constraintargs == ('foo', 'A', 12.4, 'A', -1000)
                    return
                if self._bestindexreturn == 2:
                    assert idxnum == 997
                    assert idxstr == None
                    assert constraintargs == ('foo', 'A', 12.4, 'A', -1000)
                    return
                # 3 or more
                assert idxnum == 997
                assert idxstr == "\N{LATIN SMALL LETTER E WITH CIRCUMFLEX}"
                assert constraintargs == ('foo', 'A', 12.4, 'A', -1000)

            def Filter(self, *args):
                self.Filter99(*args)
                1 / 0

            def FilterGood(self, *args):
                self.pos = 1  # row 0 is headers

            def Eof1(self, toomany, args):
                1 / 0

            def Eof2(self):
                1 / 0

            def Eof3(self):
                return BadIsTrue()

            def Eof99(self):
                return not (self.pos < len(self.table.data))

            def Rowid1(self, too, many, args):
                1 / 0

            def Rowid2(self):
                1 / 0

            def Rowid3(self):
                return "cdrom"

            def Rowid99(self):
                return self.table.data[self.pos][0]

            def Column1(self):
                1 / 0

            def Column2(self, too, many, args):
                1 / 0

            def Column3(self, col):
                1 / 0

            def Column4(self, col):
                return self  # bad type

            def Column99(self, col):
                return self.table.data[self.pos][col + 1]  # col 0 is row id

            def Close1(self, too, many, args):
                1 / 0

            def Close2(self):
                1 / 0

            def Close99(self):
                del self.table  # deliberately break ourselves

            def Next1(self, too, many, args):
                1 / 0

            def Next2(self):
                1 / 0

            def Next99(self):
                self.pos += 1

        # use our more complete version
        self.db.create_module("testmod2", Source(), iVersion=4)
        cur.execute("create virtual table foo using testmod2(2,two)")
        # are missing/mangled methods detected correctly?
        self.assertRaises(AttributeError, cur.execute, "select rowid,* from foo order by number")
        VTable.BestIndex = VTable.BestIndex1
        self.assertRaises(TypeError, cur.execute, "select rowid,* from foo order by number")
        VTable.BestIndex = VTable.BestIndex2
        self.assertRaises(ZeroDivisionError, cur.execute, "select rowid,* from foo order by number")
        # check bestindex results
        VTable.BestIndex = VTable.BestIndex3
        for i in range(VTable.numbadbextindex):
            self.assertRaises(TypeError, cur.execute, allconstraints)
        VTable.BestIndex = VTable.BestIndex4
        self.assertRaises(TypeError, cur.execute, allconstraints)
        VTable.BestIndex = VTable.BestIndex4_1
        self.assertRaises(ValueError, cur.execute, allconstraints)
        VTable.BestIndex = VTable.BestIndex6
        self.assertRaises(ZeroDivisionError, cur.execute, allconstraints)
        VTable.BestIndex = VTable.BestIndex7
        self.assertRaises(ZeroDivisionError, cur.execute, allconstraints)

        # check varying number of return args from bestindex
        VTable.BestIndex = VTable.BestIndex99
        for i in range(6):
            VTable._bestindexreturn = i
            Cursor._bestindexreturn = i
            self.assertRaisesRoot(ZeroDivisionError, cur.execute, " " + allconstraints + " " * i)

        # error cases ok, return real values and move on to cursor methods
        del VTable.Open
        del Cursor.Filter
        self.assertRaises(AttributeError, cur.execute, allconstraints)  # missing open
        VTable.Open = VTable.Open1
        self.assertRaises(TypeError, cur.execute, allconstraints)
        VTable.Open = VTable.Open2
        self.assertRaises(ZeroDivisionError, cur.execute, allconstraints)
        VTable.Open = VTable.Open3
        self.assertRaises(AttributeError, cur.execute, allconstraints)
        VTable.Open = VTable.Open99
        self.assertRaises(AttributeError, cur.execute, allconstraints)
        # put in filter
        Cursor.Filter = Cursor.Filter1
        self.assertRaisesRoot(TypeError, cur.execute, allconstraints)
        Cursor.Filter = Cursor.Filter2
        self.assertRaisesRoot(ZeroDivisionError, cur.execute, allconstraints)
        Cursor.Filter = Cursor.Filter99
        self.assertRaisesRoot(AttributeError, cur.execute, allconstraints)
        Cursor.Eof = Cursor.Eof1
        self.assertRaisesRoot(TypeError, cur.execute, allconstraints)
        Cursor.Eof = Cursor.Eof2
        self.assertRaisesRoot(ZeroDivisionError, cur.execute, allconstraints)
        Cursor.Eof = Cursor.Eof3
        self.assertRaisesRoot(ZeroDivisionError, cur.execute, allconstraints)
        Cursor.Eof = Cursor.Eof99
        self.assertRaisesRoot(AttributeError, cur.execute, allconstraints)
        # now onto to rowid
        Cursor.Rowid = Cursor.Rowid1
        self.assertRaisesRoot(TypeError, cur.execute, allconstraints)
        Cursor.Rowid = Cursor.Rowid2
        self.assertRaisesRoot(ZeroDivisionError, cur.execute, allconstraints)
        Cursor.Rowid = Cursor.Rowid3
        self.assertRaisesRoot(ValueError, cur.execute, allconstraints)
        Cursor.Rowid = Cursor.Rowid99
        self.assertRaisesRoot(AttributeError, cur.execute, allconstraints)
        # column
        Cursor.Column = Cursor.Column1
        self.assertRaisesRoot(TypeError, cur.execute, allconstraints)
        Cursor.Column = Cursor.Column2
        self.assertRaisesRoot(TypeError, cur.execute, allconstraints)
        Cursor.Column = Cursor.Column3
        self.assertRaisesRoot(ZeroDivisionError, cur.execute, allconstraints)
        Cursor.Column = Cursor.Column4
        self.assertRaisesRoot(TypeError, cur.execute, allconstraints)
        Cursor.Column = Cursor.Column99
        try:
            for row in cur.execute(allconstraints):
                pass
        except AttributeError:
            pass
        # next
        Cursor.Next = Cursor.Next1
        with self.assertRaisesRoot(TypeError):
            for row in cur.execute(allconstraints):
                pass
        Cursor.Next = Cursor.Next2
        with self.assertRaisesRoot(ZeroDivisionError):
            for row in cur.execute(allconstraints):
                pass
        Cursor.Next = Cursor.Next99
        with self.assertRaisesRoot(AttributeError):
            for row in cur.execute(allconstraints):
                pass
        # close
        Cursor.Close = Cursor.Close1
        try:
            for row in cur.execute(allconstraints):
                pass
        except TypeError:
            pass
        Cursor.Close = Cursor.Close2
        try:
            for row in cur.execute(allconstraints):
                pass
        except ZeroDivisionError:
            pass
        Cursor.Close = Cursor.Close99

        # update (insert)
        sql = "insert into foo (name, description) values('gunk', 'foo')"
        self.assertRaises(AttributeError, cur.execute, sql)
        VTable.UpdateInsertRow = VTable.UpdateInsertRow1
        self.assertRaises(TypeError, cur.execute, sql)
        VTable.UpdateInsertRow = VTable.UpdateInsertRow2
        self.assertRaises(TypeError, cur.execute, sql)
        VTable.UpdateInsertRow = VTable.UpdateInsertRow3
        self.assertRaises(ZeroDivisionError, cur.execute, sql)
        VTable.UpdateInsertRow = VTable.UpdateInsertRow4
        self.assertRaises(TypeError, cur.execute, sql)
        VTable.UpdateInsertRow = VTable.UpdateInsertRow5
        self.assertRaises(TypeError, cur.execute, sql)
        VTable.UpdateInsertRow = VTable.UpdateInsertRow6
        self.assertRaises(OverflowError, cur.execute, sql)
        VTable.UpdateInsertRow = VTable.UpdateInsertRow7
        cur.execute(sql)
        self.assertEqual(self.db.last_insert_rowid(), 9223372036854775807)
        VTable.UpdateInsertRow = VTable.UpdateInsertRow8
        cur.execute("insert into foo (rowid,name, description) values(-12,'gunk', 'foo')")

        # update (change)
        VTable.BestIndex = VTable.BestIndexGood
        Cursor.Filter = Cursor.FilterGood
        sql = "update foo set description=='bar' where description=='foo'"
        self.assertRaises(AttributeError, cur.execute, sql)
        VTable.UpdateChangeRow = VTable.UpdateChangeRow1
        self.assertRaises(TypeError, cur.execute, sql)
        VTable.UpdateChangeRow = VTable.UpdateChangeRow2
        self.assertRaises(ZeroDivisionError, cur.execute, sql)
        VTable.UpdateChangeRow = VTable.UpdateChangeRow3
        cur.execute(sql)
        VTable.UpdateChangeRow = VTable.UpdateChangeRow4
        cur.execute("update foo set rowid=rowid+20 where 1")

        # update (delete)
        VTable.BestIndex = VTable.BestIndexGood2  # improves code coverage
        sql = "delete from foo where name=='Fred'"
        self.assertRaises(AttributeError, cur.execute, sql)
        VTable.UpdateDeleteRow = VTable.UpdateDeleteRow1
        self.assertRaises(TypeError, cur.execute, sql)
        VTable.UpdateDeleteRow = VTable.UpdateDeleteRow2
        self.assertRaises(ZeroDivisionError, cur.execute, sql)
        VTable.UpdateDeleteRow = VTable.UpdateDeleteRow3
        cur.execute(sql)

        # rename
        sql = "alter table foo rename to bar"
        VTable.Rename = VTable.Rename1
        self.assertRaises(TypeError, cur.execute, sql)
        VTable.Rename = VTable.Rename2
        self.assertRaises(ZeroDivisionError, cur.execute, sql)
        VTable.Rename = VTable.Rename3
        # this is to catch memory leaks
        cur.execute(sql)
        del VTable.Rename  # method is optional
        cur.execute("alter table bar rename to foo")  # put things back

        # findfunction
        # mess with overload function first
        self.assertRaises(TypeError, self.db.overload_function, 1, 1)
        self.assertRaises(apsw.MisuseError, self.db.overload_function, "a" * 1024, 1)
        self.db.overload_function("xyz", 2)
        self.assertRaises(apsw.SQLError, cur.execute, "select xyz(item,description) from foo", can_cache=False)
        VTable.FindFunction = VTable.FindFunction1
        self.assertRaises(apsw.SQLError,
                          self.assertRaisesUnraisable,
                          TypeError,
                          cur.execute,
                          "select xyz(item,description) from foo ",
                          can_cache=False)
        VTable.FindFunction = VTable.FindFunction2
        self.assertRaises(apsw.SQLError,
                          self.assertRaisesUnraisable,
                          ZeroDivisionError,
                          cur.execute,
                          "select xyz(item,description) from foo  ",
                          can_cache=False)
        VTable.FindFunction = VTable.FindFunction3

        def foo():
            for row in cur.execute("select xyz(item,description) from foo   ", can_cache=False):
                pass

        self.assertRaises(apsw.SQLError, self.assertRaisesUnraisable, TypeError, foo)
        # this should work
        VTable.FindFunction = VTable.FindFunction4
        for row in cur.execute("select xyz(item,description) from foo    ", can_cache=False):
            pass

        for ff, e in ((VTable.FindFunction5, TypeError), (VTable.FindFunction6, TypeError), (VTable.FindFunction7,
                                                                                             ValueError)):
            VTable.FindFunction = ff
            self.assertRaises(apsw.SQLError,
                              self.assertRaisesUnraisable,
                              e,
                              cur.execute,
                              "select xyz(item,description) from foo",
                              can_cache=False)

        # integrity check
        self.assertEqual("ok", self.db.pragma("integrity_check", "foo"))
        VTable.Integrity = VTable.Integrity1
        self.assertEqual("a message", self.db.pragma("integrity_check", "foo"))
        VTable.Integrity = VTable.Integrity2
        self.assertRaises(TypeError, self.db.pragma, "quick_check", "foo")
        VTable.Integrity = VTable.Integrity99
        self.assertEqual("ok", self.db.pragma("integrity_check", "foo"))

        # transaction control
        # Begin, Sync, Commit and rollback all use the same underlying code
        sql = "delete from foo where name=='Fred'"
        VTable.Begin = VTable.Begin1
        self.assertRaises(TypeError, cur.execute, sql)
        VTable.Begin = VTable.Begin2
        self.assertRaises(ZeroDivisionError, cur.execute, sql)
        VTable.Begin = VTable.Begin3
        cur.execute(sql)

        # disconnect - sqlite ignores any errors
        db = apsw.Connection(TESTFILEPREFIX + "testdb")
        db.create_module("testmod2", Source())
        cur2 = db.cursor()
        for _ in cur2.execute("select * from foo"):
            pass
        VTable.Disconnect = VTable.Disconnect1
        self.assertRaisesUnraisable(TypeError, db.close)  # nb close succeeds!
        self.assertRaises(apsw.CursorClosedError, cur2.execute, "select * from foo")
        del db
        db = apsw.Connection(TESTFILEPREFIX + "testdb")
        db.create_module("testmod2", Source())
        cur2 = db.cursor()
        for _ in cur2.execute("select * from foo"):
            pass
        VTable.Disconnect = VTable.Disconnect2
        self.assertRaisesUnraisable(ZeroDivisionError, db.close)  # nb close succeeds!
        self.assertRaises(apsw.CursorClosedError, cur2.execute, "select * from foo")
        del db
        db = apsw.Connection(TESTFILEPREFIX + "testdb")
        db.create_module("testmod2", Source())
        cur2 = db.cursor()
        for _ in cur2.execute("select * from foo"):
            pass
        VTable.Disconnect = VTable.Disconnect3
        db.close()
        del db

        # destroy
        VTable.Destroy = VTable.Destroy1
        self.assertRaises(apsw.SQLError, self.assertRaisesUnraisable, TypeError, cur.execute, "drop table foo")
        VTable.Destroy = VTable.Destroy2
        self.assertRaises(apsw.SQLError, self.assertRaisesUnraisable, ZeroDivisionError, cur.execute, "drop table foo")
        VTable.Destroy = VTable.Destroy3
        cur.execute("drop table foo")
        self.db.close()

    def testVTableUnusable(self):
        "Tests vtable bestindex with unusable constraints and other error handling"

        class Source:

            def Create(self, db, modulename, dbname, tablename, *args):
                return "create table ignored(a,b,c)", Table()

            Connect = Create

        class Table:

            last = None

            def BestIndex(tself, constraints, orderbys):
                if tself.last is None:
                    self.assertEqual(len(constraints), 3)
                    tself.last = constraints
                else:
                    # the in clause should have become unusable
                    self.assertEqual(len(constraints), 2)
                res = []
                estimated = 9999999999999999
                for c, op in constraints:
                    if op == apsw.SQLITE_INDEX_CONSTRAINT_EQ:  # actually IN
                        res.append(0)
                    else:
                        res.append(None)
                if all(r is None for r in res):
                    res[-1] = 0
                    estimated = 7  # make it prefer this
                return [res, 0, "fred", False, estimated]

            bifg_seqnum = 0

            def BestIndex_fail_getitem(self, *args):
                res = Table.bifg_sequence[Table.bifg_seqnum]
                Table.bifg_seqnum += 1
                return res

            def Open(self):
                return Cursor()

            def Disconnect(self):
                pass

            Destroy = Disconnect

        def bad(n, *args):

            class badgi(list):

                def __getitem__(self, n):
                    if n == self.failnum:
                        1 / 0
                    return super().__getitem__(n)

            res = badgi(*args)
            res.failnum = n
            return res

        all_none = (None, None, None, None, None)
        Table.bifg_sequence = (
            # outer list
            bad(0, all_none),
            bad(1, all_none),
            bad(2, all_none),
            bad(3, all_none),
            bad(4, all_none),
            # constraints
            [bad(0, [None])],
            [[bad(0, (1, 2))]],
            [[bad(1, (1, 2))]],
        )

        class Cursor:

            def Filter(cself, idxnum, idxstr, args):
                # if we correctly handled unusable then args should contain column c
                # bound value
                self.assertEqual(args, (100, ))

            def FilterIgnore(*args):
                pass

            def Eof(self):
                return True

            def Rowid(self):
                return 10

            def Column(self, col):
                return 10

            def Next(self):
                pass

            def Close(self):
                pass

        self.db.create_module("bestindex", Source())
        self.db.execute(
            "create virtual table foo using bestindex(); select * from foo where a in (66,77) and b>? and c>?",
            (99, 100))

        Table.BestIndex = Table.BestIndex_fail_getitem
        Cursor.Filter = Cursor.FilterIgnore
        for i in range(len(Table.bifg_sequence)):
            self.assertRaises(ZeroDivisionError, self.db.execute, "select * from foo where a > 7")

    def testClosingChecks(self):
        "Check closed connection/blob/cursor is correctly detected"
        cur = self.db.cursor()
        rowid = curnext(
            cur.execute("create table foo(x blob); insert into foo values(zeroblob(98765)); select rowid from foo"))[0]
        blob = self.db.blob_open("main", "foo", "x", rowid, True)
        blob.close()
        nargs = self.blob_nargs
        for func in [x for x in dir(blob) if not x.startswith("__") and not x in ("close", )]:
            args = ("one", "two", "three")[:nargs.get(func, 0)]
            try:
                getattr(blob, func)(*args)
                self.fail(f"blob method/attribute { func } didn't notice that the connection is closed")
            except ValueError:  # we issue ValueError to be consistent with file objects
                pass

        self.db.close()
        nargs = self.connection_nargs
        tested = 0
        for func in [x for x in dir(self.db) if x in nargs or (not x.startswith("__") and not x in ("close", ))]:
            tested += 1
            args = ("one", "two", "three")[:nargs.get(func, 0)]

            try:
                # attributes come back as None after a close
                func = getattr(self.db, func)
                if func:
                    func(*args)
                    self.fail(f"connection method/attribute { func } didn't notice that the connection is closed")
            except apsw.ConnectionClosedError:
                pass
        self.assertTrue(tested > len(nargs))

        # do the same thing, but for cursor
        nargs = self.cursor_nargs
        tested = 0
        for func in [x for x in dir(cur) if not x.startswith("__") and not x in ("close", )]:
            tested += 1
            args = ("one", "two", "three")[:nargs.get(func, 0)]
            try:
                getattr(cur, func)(*args)
                self.fail(f"cursor method/attribute { func } didn't notice that the connection is closed")
            except apsw.CursorClosedError:
                pass
        self.assertTrue(tested >= len(nargs))

    def testClosing(self):
        "Verify behaviour of close() functions"
        cur = self.db.cursor()
        cur.execute("select 3;select 4")
        self.assertRaises(apsw.IncompleteExecutionError, cur.close)
        # now force it
        self.assertRaises(TypeError, cur.close, sys)
        self.assertRaises(TypeError, cur.close, 1, 2, 3)
        cur.close(True)
        l = [self.db.cursor() for i in range(1234)]
        cur = self.db.cursor()
        cur.execute("select 3; select 4; select 5")
        l2 = [self.db.cursor() for i in range(1234)]
        self.assertRaises(apsw.IncompleteExecutionError, self.db.close)
        self.assertRaises(TypeError, self.db.close, sys)
        self.assertRaises(TypeError, self.db.close, 1, 2, 3)
        self.db.close(True)  # force it
        self.db.close()  # should be fine now
        # coverage - close cursor after closing db
        db = apsw.Connection(":memory:")
        cur = db.cursor()
        db.close()
        cur.close()

    def testLargeObjects(self):
        "Verify handling of large strings/blobs (>2GB) [requires 64 bit platform]"
        assert is64bit
        # For binary/blobs I use an anonymous area slightly larger than 2GB chunk of memory, but don't touch any of it
        f = mmap.mmap(-1, 2 * 1024 * 1024 * 1024 + 25000)
        c = self.db.cursor()
        c.execute("create table foo(theblob)")
        self.assertRaises(apsw.TooBigError, c.execute, "insert into foo values(?)", (f, ))
        c.execute("insert into foo values(?)", ("jkghjk" * 1024, ))
        b = self.db.blob_open("main", "foo", "theblob", self.db.last_insert_rowid(), True)
        b.read(1)
        self.assertRaises(ValueError, b.write, f)

        def func():
            return f

        self.db.create_scalar_function("toobig", func)
        self.assertRaises(apsw.TooBigError, c.execute, "select toobig()")
        f.close()

    def testErrorCodes(self):
        "Verify setting of result codes on error/exception"
        fname = TESTFILEPREFIX + "gunk-errcode-test"
        write_whole_file(fname, "wb", b"A" * 8192)
        db = None
        try:
            # The exception could be thrown on either of these lines
            # depending on several factors
            db = apsw.Connection(fname)
            db.cursor().execute("select * from sqlite_schema")
            1 / 0  # should not be reachable
        except:
            klass, e, tb = sys.exc_info()
            self.assertTrue(isinstance(e, apsw.NotADBError))
            self.assertEqual(e.result, apsw.SQLITE_NOTADB)
            self.assertEqual(e.extendedresult & 0xff, apsw.SQLITE_NOTADB)
        if db is not None:
            db.close(True)

        try:
            deletefile(fname)
        except:
            pass

        try:
            self.db.execute("select a from XYZ banana 4")
        except apsw.SQLError as e:
            self.assertEqual(25, e.error_offset)

    def testLimits(self):
        "Verify setting and getting limits"
        self.assertRaises(TypeError, self.db.limit, "apollo", 11)
        c = self.db.cursor()
        c.execute("create table foo(x)")
        c.execute("insert into foo values(?)", ("x" * 1024, ))
        old = self.db.limit(apsw.SQLITE_LIMIT_LENGTH)
        self.db.limit(apsw.SQLITE_LIMIT_LENGTH, 1023)
        self.assertRaises(apsw.TooBigError, c.execute, "insert into foo values(?)", ("y" * 1024, ))
        self.assertEqual(1023, self.db.limit(apsw.SQLITE_LIMIT_LENGTH, 0))
        self.assertRaises(apsw.TooBigError, c.execute, "insert into foo values(?)", ("x" * 1024, ))

    def testConnectionHooks(self):
        "Verify connection hooks"
        del apsw.connection_hooks
        try:
            db = apsw.Connection(":memory:")
        except AttributeError:
            pass
        apsw.connection_hooks = sys  # bad type
        try:
            db = apsw.Connection(":memory:")
        except TypeError:
            pass
        apsw.connection_hooks = ("a", "tuple", "of", "non-callables")
        try:
            db = apsw.Connection(":memory:")
        except TypeError:
            pass
        apsw.connection_hooks = (dir, lambda x: 1 / 0)
        try:
            db = apsw.Connection(":memory:")
        except ZeroDivisionError:
            pass

        def delit(db):
            del db

        apsw.connection_hooks = [delit for _ in range(9000)]
        db = apsw.Connection(":memory:")
        db.close()
        apsw.connection_hooks = [lambda x: x]
        db = apsw.Connection(":memory:")
        db.close()

    def testCompileOptions(self):
        "Verify getting compile options"
        # We don't know what the right answers are, so just check
        # there are more than zero entries.
        v = apsw.compile_options
        self.assertEqual(type(v), tuple)
        self.assertTrue(len(v) > 1)

    def testKeywords(self):
        "Verify keywords"
        k = apsw.keywords
        self.assertTrue("INSERT" in k)

    def testIssue4(self):
        "Issue 4: Error messages and SQLite ticket 3063"
        connection = apsw.Connection(":memory:")
        cursor = connection.cursor()

        cursor.execute("CREATE TABLE A_TABLE (ID ABC PRIMARY KEY NOT NULL)")
        try:
            cursor.execute("INSERT INTO A_TABLE VALUES (NULL)")
        except:
            klass, e, tb = sys.exc_info()
            assert "A_TABLE.ID" in str(e)

        try:
            cursor.execute("INSERT INTO A_TABLE VALUES (?)", (None, ))
        except:
            klass, e, tb = sys.exc_info()
            assert "A_TABLE.ID" in str(e)

    def testIssue15(self):
        "Issue 15: Release GIL during calls to prepare"
        self.db.cursor().execute("create table foo(x)")
        self.db.cursor().execute("begin exclusive")
        db2 = apsw.Connection(TESTFILEPREFIX + "testdb")
        db2.set_busy_timeout(30000)
        t = ThreadRunner(db2.cursor().execute, "select * from foo")
        t.start()
        time.sleep(1)
        self.db.cursor().execute("commit")
        t.go()

    def testIssue19(self):
        "Issue 19: Incomplete cursor execution"
        c = self.db.cursor()
        c.execute("create table numbers(x)")
        for i in range(10):
            c.execute("insert into numbers values(?)", (i, ))
        c.execute("select * from numbers")
        curnext(c)
        curnext(c)
        curnext(c)
        self.db.cursor().execute("delete from numbers where x=5")
        curnext(c)
        curnext(c)

    def testIssue24(self):
        "Issue 24: Ints and Longs"
        c = self.db.cursor()
        for row in c.execute("select 3"):
            pass
        self.assertEqual(int, type(row[0]))
        for row in c.execute("select -2147483647-1"):
            pass
        self.assertEqual(int, type(row[0]))
        for row in c.execute("select 2147483647"):
            pass
        self.assertEqual(int, type(row[0]))
        # Depending on the platform, sizeof(long), 64 bitness etc we
        # may remain as python type int or type long. Check we are
        # getting the right numbers no matter what.  This duplicates
        # testTypes but you can never be too careful.
        for v in "2147483646", "2147483647", "2147483648", "2147483649", \
                "21474836460", "21474836470", "21474836480", "21474836490", \
                "147483646", "147483647", "147483648", "147483649":
            for neg in ("-", ""):
                val = c.execute("select " + neg + v).fetchall()[0][0]
                val = repr(val)
                if val.endswith("L"):
                    val = val[:-1]
                self.assertEqual(val, neg + v)

    def testIssue31(self):
        "Issue 31: GIL & SQLite mutexes with heavy threading, threadsafe errors from SQLite"
        randomnumbers = [random.randint(0, 10000) for _ in range(10000)]

        cursor = self.db.cursor()
        cursor.execute("create table foo(x)")
        cursor.execute("begin")
        for num in randomnumbers:
            cursor.execute("insert into foo values(?)", (num, ))
        cursor.execute("end")

        self.db.create_scalar_function("timesten", lambda x: x * 10)

        def dostuff(n):
            # spend n seconds doing stuff to the database
            c = self.db.cursor()
            b4 = time.time()
            while time.time() - b4 < n:
                i = random.choice(randomnumbers)
                if i % 5 == 0:
                    sql = "select timesten(x) from foo where x=%d order by x" % (i, )
                    c.execute(sql)
                elif i % 5 == 1:
                    sql = "select timesten(x) from foo where x=? order by x"
                    called = 0
                    for row in self.db.cursor().execute(sql, (i, )):
                        called += 1
                        self.assertEqual(row[0], 10 * i)
                    # same value could be present multiple times
                    self.assertTrue(called >= 1)
                elif i % 5 == 2:
                    try:
                        self.db.cursor().execute("deliberate syntax error")
                    except apsw.SQLError:
                        assert ("deliberate" in str(sys.exc_info()[1]))
                elif i % 5 == 3:
                    try:
                        self.db.cursor().execute("bogus syntax error")
                    except apsw.SQLError:
                        assert ("bogus" in str(sys.exc_info()[1]))
                else:
                    sql = "select timesten(x) from foo where x=? order by x"
                    self.db.cursor().execute(sql, (i, ))

        runtime = float(os.getenv("APSW_HEAVY_DURATION")) if os.getenv("APSW_HEAVY_DURATION") else 15
        threads = [ThreadRunner(dostuff, runtime) for _ in range(20)]
        for t in threads:
            t.start()

        for t in threads:
            # if there were any errors then exceptions would be raised here
            t.go()

    def testIssue50(self):
        "Issue 50: Check Blob.read return value on eof"
        # first get what the system returns on eof
        f = open(os.devnull, "rb")
        try:
            # deliberately hit eof
            f.read()
            # now try to read some more
            feof = f.read(10)
        finally:
            f.close()
        cur = self.db.cursor()
        # make a blob to play with
        rowid = curnext(
            cur.execute("create table foo(x blob); insert into foo values(zeroblob(98765)); select rowid from foo"))[0]
        blobro = self.db.blob_open("main", "foo", "x", rowid, False)
        try:
            blobro.read(98765)
            beof = blobro.read(10)
            self.assertEqual(type(beof), type(feof))
            self.assertEqual(beof, feof)
        finally:
            blobro.close()

    def testIssue98(self, runfrom106=None):
        "Issue 98: An error in context manager commit should do a rollback"
        self.db.cursor().execute("create table foo(x); insert into foo values(3); insert into foo values(4)")
        # We need the reader to block a writer, which requires non-WAL mode
        self.db.cursor().execute("pragma journal_mode=delete")
        db2 = apsw.Connection(TESTFILEPREFIX + "testdb")
        if runfrom106:
            db2.set_exec_trace(runfrom106)
        db2.cursor().execute("pragma journal_mode=delete")
        # deliberately don't read from cursor on connection 1 which will prevent a commit
        x = self.db.cursor().execute("select * from foo")
        db2.__enter__()
        db2.cursor().execute("insert into foo values(5)")  # transaction is buffered in memory by SQLite
        try:
            db2.__exit__(None, None, None)
        except apsw.BusyError:
            pass
        # Ensure transaction was rolled back
        x.fetchall()
        for row in db2.cursor().execute("select * from foo where x=5"):
            self.fail("Transaction was not rolled back")
        db2.close()
        if runfrom106: return
        # Verify that error in tracer results in rollback
        self.db.__enter__()

        def h(*args):
            1 / 0

        self.db.cursor().execute("insert into foo values(6)")
        self.db.set_exec_trace(h)
        try:
            self.db.__exit__(None, None, None)
        except ZeroDivisionError:
            self.db.set_exec_trace(None)
            pass
        for row in self.db.cursor().execute("select * from foo where x=6"):
            self.fail("Transaction was not rolled back")

    def testIssue103(self):
        "Issue 103: Error handling when sqlite3_declare_vtab fails"

        class Source:

            def Create(self, *args):
                return "create table x(delete)", None

        self.db.create_module("issue103", Source())
        try:
            self.db.cursor().execute("create virtual table foo using issue103()")
            1 / 0  # should not be reached
        except apsw.SQLError:
            assert "near \"delete\": syntax error" in str(sys.exc_info()[1])

    def testIssue106(self):
        "Issue 106: Profiling and tracing"
        traces = []

        def tracer(cur, sql, bindings):
            sql = sql.lower().split()[0]
            if sql in ("savepoint", "release", "rollback"):
                traces.append(sql)
            return True

        self.testIssue98(tracer)
        self.assertTrue(len(traces) >= 3)
        self.assertTrue("savepoint" in traces)
        self.assertTrue("release" in traces)
        self.assertTrue("rollback" in traces)

    def testIssue142(self):
        "Issue 142: bytes from system during dump"
        orig_strftime = time.strftime
        orig_getuser = getpass.getuser
        fh = []
        try:
            time.strftime = lambda arg: b"gjkTIMEJUNKhgjhg\xfe\xdf"
            getpass.getuser = lambda: b"\x81\x82\x83gjkhgUSERJUNKjhg\xfe\xdf"
            fh = [open(TESTFILEPREFIX + "test-shell-" + t, "w+", encoding="utf8") for t in ("in", "out", "err")]
            kwargs = {"stdin": fh[0], "stdout": fh[1], "stderr": fh[2]}

            rows = (["correct"], ["horse"], ["battery"], ["staple"])
            self.db.cursor().execute("create table foo(x)")
            self.db.cursor().executemany("insert into foo values(?)", rows)
            shell = apsw.shell.Shell(db=self.db, **kwargs)
            shell.command_dump([])

            fh[1].seek(0)
            out = fh[1].read()

            for row in rows:
                self.assertTrue(row[0] in out)

            self.assertTrue("TIMEJUNK" in out)
            self.assertTrue("USERJUNK" in out)

        finally:
            for f in fh:
                f.close()
            time.strftime = orig_strftime
            getpass.getuser = orig_getuser

    def testIssue186(self):
        "Issue 186: desription cache between statements"
        cur = self.db.cursor()

        for i, row in enumerate(cur.execute("select 1; select 1,2; select 1,2,3; select 1,2,3,4;")):
            # this catches if the order of getting them makes a difference
            if i % 2:
                self.assertEqual(len(cur.description), len(cur.get_description()))
            else:
                self.assertEqual(len(cur.get_description()), len(cur.description))
            self.assertEqual(len(cur.description), i + 1)

        # check executemany too
        for i, row in enumerate(
                cur.executemany("select ?; select ?,?; select ?,?,?; select ?,?,?,?;", [
                    (1, 1, 2, 1, 2, 3, 1, 2, 3, 4),
                    (1, 1, 2, 1, 2, 3, 1, 2, 3, 4),
                ])):
            i %= 4
            self.assertEqual(len(cur.get_description()), i + 1)

        # and the tracers
        def tracer(cursor, *args):
            self.assertEqual(len(cursor.get_description()), expect)
            return True

        expect = 1
        cur.set_exec_trace(tracer)
        cur.set_row_trace(tracer)
        for i, row in enumerate(cur.execute("select 1; select 1,2; select 1,2,3; select 1,2,3,4;")):
            expect += 1
        expect = 1
        for i, row in enumerate(
                cur.executemany("select ?; select ?,?; select ?,?,?; select ?,?,?,?;", [
                    (1, 1, 2, 1, 2, 3, 1, 2, 3, 4),
                    (1, 1, 2, 1, 2, 3, 1, 2, 3, 4),
                ])):
            expect += 1
            if expect > 4: expect = 1

    def testTicket2158(self):
        "Check we are not affected by SQLite ticket #2158"

        def dummy(x, y):
            if x < y: return -1
            if x > y: return 1
            return 0

        self.db.create_collation("dummy", dummy)
        cur = self.db.cursor()
        cur.execute("create table foo(x)")
        cur.executemany("insert into foo values(?)", randomintegers(20))
        for row in cur.execute("select * from foo order by x collate dummy"):
            pass
        self.db.create_collation("dummy", None)
        self.assertRaises(apsw.SQLError, cur.execute, "select * from foo order by x collate dummy")

    def testIssue199(self):
        "Backup API should accept Connection subclasses"

        # https://github.com/rogerbinns/apsw/issues/199
        class subclass(apsw.Connection):
            pass

        dbsub = subclass("")
        dbsub.cursor().execute("create table a(b);insert into a values(3);")

        b = self.db.backup("main", dbsub, "main")
        try:
            while not b.done:
                b.step(100)
        finally:
            b.finish()

    def testIssue311(self):
        "Indirect descendents of VFS should support WAL (in addition to direct subclasses)"

        class vfswrapped(apsw.VFS):

            def __init__(self):
                self.myname = "testIssue311"
                self.base = ""
                apsw.VFS.__init__(self, self.myname, self.base)

            def xOpen(self, name, flags):
                return vfsfilewrap(self.base, name, flags)

        class vfsfilewrap(apsw.VFSFile):

            def __init__(self, parent, name, flags):
                apsw.VFSFile.__init__(self, parent, name, flags)

        # we make testdb be wal and then try to work with it
        self.db.cursor().execute(
            "pragma journal_mode=wal; create table test(x,y); insert into test values(3,4)").fetchall()

        wrap = vfswrapped()

        con = apsw.Connection(TESTFILEPREFIX + "testdb", vfs=wrap.myname)

        for row in con.cursor().execute("select x+y from test"):
            self.assertEqual(row[0], 7)
            break
        else:
            self.fail("No rows seen")

    def testIssue314(self):
        "Reference cycles between instance, Connection and instance.method"
        cleared = []

        class SelfReferencer:

            def __del__(self):
                cleared.append(id(self))

            def __init__(self):
                self.db = apsw.Connection("")
                self.db.set_busy_handler(self.refme)
                self.cur = self.db.cursor()
                self.cur.set_row_trace(self.refme)

            def refme(self):
                pass

        for i in range(1000):
            SelfReferencer()
        gc.collect()
        self.assertEqual(1000, len(cleared))

    def testIssue394(self):
        "Default vfs manipulation"
        starting = apsw.vfs_names()
        if len(starting) > 1:
            apsw.set_default_vfs(starting[1])
            self.assertEqual(apsw.vfs_names()[0], starting[1])
            self.assertNotEqual(starting, apsw.vfs_names())
            apsw.set_default_vfs(starting[0])
            # we assume it is safe to remove the last listed vfs for testing
            apsw.unregister_vfs(starting[-1])
            self.assertEqual(apsw.vfs_names(), starting[:-1])
        self.assertRaises(TypeError, apsw.set_default_vfs)
        self.assertRaises(TypeError, apsw.set_default_vfs, 3)
        self.assertRaises(TypeError, apsw.set_default_vfs, "3", 3)
        self.assertRaises(ValueError, apsw.set_default_vfs, "4324324234")
        self.assertRaises(TypeError, apsw.unregister_vfs)
        self.assertRaises(TypeError, apsw.unregister_vfs, 3)
        self.assertRaises(TypeError, apsw.unregister_vfs, "3", 3)
        self.assertRaises(ValueError, apsw.unregister_vfs, "4342345324")

    def testIssue431(self):
        "All applicable constants for config/status calls"
        self.assertEqual(0, self.db.config(apsw.SQLITE_DBCONFIG_TRIGGER_EQP, -1))
        self.db.execute("create table foo(x)")
        self.assertTrue(self.db.table_exists("main", "foo"))
        self.db.config(apsw.SQLITE_DBCONFIG_RESET_DATABASE, 1)
        self.db.execute("VACUUM")
        self.db.config(apsw.SQLITE_DBCONFIG_RESET_DATABASE, 0)
        self.assertFalse(self.db.table_exists("main", "foo"))

    def testIssue433(self):
        "Comments and exec tracing"
        p = None

        def et(cursor, sql, bindings):
            nonlocal p
            p = (cursor, sql, bindings)
            return False

        cur = self.db.cursor()
        cur.exec_trace = et

        for query in ("-- foo", "select 3;;;; "):
            with contextlib.suppress(apsw.ExecTraceAbort):
                cur.execute(query)
            self.assertEqual(p[1], query)
            self.assertEqual(p[0].has_vdbe, "select" in query)

    def testIssue475(self):
        "Unused cursor attributes"
        # The cause of this issue was a new cursor attribute access, so we check
        # all objects to catch anything else
        objects = [
            apsw, apsw.Connection, apsw.Cursor, apsw.Blob, apsw.Error, apsw.VFS, apsw.VFSFile, self.db,
            self.db.cursor()
        ]
        self.db.execute("create table x(y); insert into x values(x'abcdef1012');select * from x")
        objects.append(self.db.blob_open("main", "x", "y", self.db.last_insert_rowid(), 0))
        objects.append(apsw.VFS("aname", ""))
        objects.append(
            apsw.VFSFile("", self.db.db_filename("main"),
                         [apsw.SQLITE_OPEN_MAIN_DB | apsw.SQLITE_OPEN_CREATE | apsw.SQLITE_OPEN_READWRITE, 0]))
        objects.append(apsw.VFSFcntlPragma)

        for o in objects:
            for n in dir(o):
                try:
                    getattr(o, n)
                except apsw.Error:
                    pass

    def testIssue488(self):
        "__init__ called multiple times"
        objects = [
            self.db,
            self.db.cursor(),
            apsw.zeroblob(3),
            apsw.VFS("aname", ""),
            apsw.VFSFile(
                "",
                self.db.db_filename("main"),
                [apsw.SQLITE_OPEN_MAIN_DB | apsw.SQLITE_OPEN_CREATE | apsw.SQLITE_OPEN_READWRITE, 0],
            )
        ]
        for o in objects:
            self.assertRaisesRegex(RuntimeError, "__init__ has already been called, and cannot be called again",
                                   o.__init__, "issue 488")

    def testIssue506(self):
        "VFS file with None/NULL filename"
        vfs_saw_none = False
        vfsfile_saw_none = False
        class TVFS(apsw.VFS):
            vfsname = "test"
            def __init__(self):
                super().__init__(self.vfsname, "")

            def xOpen(self, name, flags):
                nonlocal vfs_saw_none
                if name is None:
                    vfs_saw_none = True
                return TVFSFile(name, flags)

        class TVFSFile(apsw.VFSFile):
            def __init__(self, filename, flags):
                nonlocal vfsfile_saw_none
                if filename is None:
                    vfsfile_saw_none = True
                super().__init__("", filename, flags)

        tvfs = TVFS()
        con = apsw.Connection(self.db.filename, vfs=tvfs.vfsname)
        # put enough stuff in temp table that it spills to disk
        con.pragma("cache_size", 10)
        temp_store = con.pragma("temp_store")
        try:
            con.pragma("temp_store", 1)
            con.execute("create temp table testissue506(x UNIQUE,y UNIQUE, PRIMARY KEY(x,y))")
            con.executemany("insert into testissue506 values(?,?)", (("a"*i, "b"*i) for i in range(1000)))
        finally:
            con.pragma("temp_store", temp_store)

        # verify we saw the Nones
        self.assertTrue(vfs_saw_none)
        self.assertTrue(vfsfile_saw_none)

    def testIssue526(self):
        "Error in VFS xRandomness"
        class RandomVFS(apsw.VFS):
            def __init__(self):
                apsw.VFS.__init__(self, "issue526", "", 1)

        vfs = RandomVFS()

        self.db.pragma("journal_mode", "WAL")
        try:
            with self.db:
                RandomVFS.xRandomness = lambda *args: 1/0
                # this resets SQLite randomness so xRandonness of default VFS will be called again
                apsw.randomness(0)
                self.db.pragma("user_version", 1)
            raise Exception("Should not reach here")
        except ZeroDivisionError:
            pass

    def testCursorGet(self):
        "Cursor.get"
        for query, expected in (("select 3,4", (3, 4)), ("select 3; select 4", [3, 4]), ("select 3,4; select 4,5", [
            (3, 4), (4, 5)
        ]), ("select 3,4; select 5", [(3, 4), 5]), ("select 3", 3)):
            self.assertEqual(self.db.execute(query).get, expected)

        q = "select ?"
        b = [(i, ) for i in range(10)]
        self.assertEqual(self.db.executemany(q, b).get, [0, 1, 2, 3, 4, 5, 6, 7, 8, 9])

        self.assertEqual(self.db.execute("/* comment */").get, None)

        # second query is deliberate syntax error
        cur = self.db.execute("select 3 ; select 3 +")
        self.assertRaises(apsw.SQLError, getattr, cur, "get")

        # a query where all results are exhausted
        cur = self.db.cursor()
        cur.execute("select 3").fetchall()
        self.assertEqual(cur.get, None)

    def testConnectionPragma(self):
        "Connection.pragma"
        self.assertRaises(TypeError, self.db.pragma)
        self.assertRaises(TypeError, self.db.pragma, 3)
        self.assertRaises(TypeError, self.db.pragma, "three", "four", "five")
        self.assertRaises(TypeError, self.db.pragma, "three", 4 + 3j)
        self.assertEqual(self.db.pragma("journal_mode"), "delete")
        self.assertEqual(self.db.pragma("journal_mode", "wal"), "wal")
        self.assertEqual(self.db.pragma("user_version"), 0)
        self.db.pragma("user_version", 7)
        self.assertEqual(self.db.pragma("user_version"), 7)
        self.assertRaises(apsw.SQLError, self.db.pragma, "user_version", "abc\0def")

        # check not cached - #525
        self.db.pragma("should_not_cache", "should_not_cache")
        self.db.pragma("should_not_cache")
        for entry in self.db.cache_stats(include_entries=True)["entries"]:
            self.assertNotIn("should_not_cache", entry["query"])

        # schema - #524
        self.db.pragma("user_version", 7, schema="temp")
        self.assertEqual(7, self.db.execute("pragma temp.user_version").get)
        self.assertRaisesRegex(apsw.SQLError, ".*unknown database.*",  self.db.pragma, "quack", schema='"')
        self.db.execute(f"""attach '{self.db.filename}' as '"' """)
        self.db.pragma("quack", schema='"')
        # check no syntax error because quoting is correct
        self.db.pragma(r"""qu\'"`ack""", schema='"')


    def testSleep(self):
        "apsw.sleep"
        apsw.sleep(1)
        apsw.sleep(-1)
        self.assertRaises(OverflowError, apsw.sleep, 2_500_000_000)
        self.assertRaises(TypeError, apsw.sleep, "2_500_000_000")

    def testPysqliteRecursiveIssue(self):
        "Check an issue that affected pysqlite"
        # https://code.google.com/p/pysqlite/source/detail?r=260ee266d6686e0f87b0547c36b68a911e6c6cdb
        cur = self.db.cursor()
        cur.execute("create table a(x); create table b(y);")

        def foo():
            yield (1, )
            cur.execute("insert into a values(?)", (1, ))
            yield (2, )

        self.assertRaises(apsw.ThreadingViolationError, cur.executemany, "insert into b values(?)", foo())

    def testWriteUnraisable(self):
        "Verify writeunraisable replacement function"

        def unraise():
            # We cause an unraisable error to happen by writing to a
            # blob open for reading.  The close method called in the
            # destructor will then also give the error
            db = apsw.Connection(":memory:")
            rowid = curnext(db.cursor().execute(
                "create table foo(x); insert into foo values(x'aabbccdd'); select rowid from foo"))[0]
            blob = db.blob_open("main", "foo", "x", rowid, False)
            try:
                blob.write(b"badd")
            except apsw.ReadOnlyError:
                pass
            del db
            del blob
            gc.collect()

        # Normal excepthook
        self.assertRaisesUnraisable(apsw.ReadOnlyError, unraise)
        # excepthook with error to check PyErr_Display is called
        xx = sys.excepthook
        yy = sys.stderr
        sys.stderr = open(TESTFILEPREFIX + "errout.txt", "wt", encoding="utf8")

        def ehook(blah):
            1 / 0

        sys.excepthook = ehook
        unraise()
        sys.stderr.close()
        v = open(TESTFILEPREFIX + "errout.txt", "rt", encoding="utf8").read()
        deletefile(TESTFILEPREFIX + "errout.txt")
        self.assertTrue(len(v))
        sys.excepthook = xx
        sys.stderr = yy

    def testStatementCache(self, scsize=17):
        "Verify statement cache integrity"
        self.db.close()
        self.db = apsw.Connection(TESTFILEPREFIX + "testdb", statementcachesize=scsize)
        self.assertRaises(TypeError, self.db.cache_stats, include_entries="orange")
        actual = self.db.cache_stats()["size"]
        self.assertTrue(actual == scsize or (scsize > 0 and actual > 0))
        cur = self.db.cursor()
        cur.execute("create table foo(x,y)")
        cur.execute("create index foo_x on foo(x)")
        cur.execute("insert into foo values(1,2)")
        cur.execute("drop index foo_x")
        cur.execute("insert into foo values(1,2)")  # cache hit, but needs reprepare
        cur.execute("drop table foo; create table foo(x)")
        try:
            cur.execute("insert into foo values(1,2)")  # cache hit, but invalid sql
        except apsw.SQLError:
            pass
        cur.executemany("insert into foo values(?)", [[1], [2]])
        # overflow the statement cache
        l = [self.db.cursor().execute("select x from foo" + " " * i) for i in range(actual + 200)]
        del l
        gc.collect()
        # coverage
        l = []
        for i in range(actual * 2 + 10):
            l.append(self.db.cursor().execute("select x from foo" + " " * i))
            for row in self.db.cursor().execute("select * from foo"):
                pass
        # other wrangling
        l = [self.db.cursor().execute("select x from foo") for i in range(actual + 200)]
        for i in range(actual * 2 + 200):
            for row in self.db.cursor().execute("select * from foo" + " " * i):
                pass
        del l
        gc.collect()
        db2 = apsw.Connection(TESTFILEPREFIX + "testdb", statementcachesize=scsize)
        self.assertEqual(db2.cache_stats()["size"], actual)
        cur2 = db2.cursor()
        cur2.execute("create table bar(x,y)")
        for _ in cur.execute("select * from foo"):
            pass
        db2.close()
        # Get some coverage - overflow cache and recycling
        l = [self.db.cursor().execute("select 3" + " " * i) for i in range(100 + 256 + 17)]
        while l:
            l.pop().fetchall()
        # embedded nulls
        got = []
        try:
            for row in cur.execute("select 3;select 4\0;select 5"):
                got.append(row[0])
        except ValueError:
            self.assertEqual(got, [3])
        # these compile to null vdbe
        for _ in range(5):  # ensure statement cache is used
            for query in (
                    "",
                    "-- foo",
                    ";",
                    "\n",
            ):
                for row in cur.execute(query):
                    self.fail("Query is empty")
        # check with stats
        s = self.db.cache_stats()
        self.assertEqual(s["size"], min(scsize, 512))  # 512 is current max
        s2 = self.db.cache_stats(True)
        s2.pop("entries")
        self.assertEqual(s, s2)
        self.assertEqual(self.db.execute("select 3" + " " * s["max_cacheable_bytes"] + "+1").fetchall(), [(4, )])
        self.assertEqual(s["too_big"] + 1, self.db.cache_stats().pop("too_big"))

        s = self.db.cache_stats()
        self.db.execute("select 997", can_cache=False).fetchall()
        self.assertEqual(s["no_cache"] + 1, self.db.cache_stats().pop("no_cache"))
        self.db.execute("select 997", can_cache=True).fetchall()
        self.assertEqual(s["misses"] + 2, self.db.cache_stats().pop("misses"))
        self.db.execute("select 997", can_cache=True).fetchall()
        self.assertEqual(s["misses"] + 2 + (1 if not scsize else 0), self.db.cache_stats().pop("misses"))

        # prepare_flags
        class VTModule:

            def Create(self, *args):
                return ("create table dontcare(x int)", VTTable())

            Connect = Create

        class VTTable:

            def Open(self):
                return VTCursor()

            def BestIndex(self, *args):
                return None

        class VTCursor:
            rows = [[99], [100]]

            def __init__(self):
                self.pos = 0

            def Filter(self, *args):
                self.pos = 0

            def Eof(self):
                return self.pos >= len(self.rows)

            def Column(self, num):
                if num < 0:
                    return self.pos + 1_000_000
                return self.rows[self.pos][num]

            def Next(self):
                self.pos += 1

            def Close(self):
                pass

        vt = VTModule()
        self.db.create_module("fortestingonly", vt)
        # no_vtab doesn't block creating a vtab
        self.db.execute("create VIRTUAL table fred USING fortestingonly()", prepare_flags=apsw.SQLITE_PREPARE_NO_VTAB)
        # make sure query using vtab is identical so cache would be hit
        query = "select * from fred"
        self.assertEqual(self.db.execute(query).fetchall(), [(99, ), (100, )])
        # this should fail (sqlite pretends the vtabs don't exist rather than giving specific error)
        self.assertRaises(apsw.SQLError,
                          self.db.execute,
                          "select * from fred",
                          prepare_flags=apsw.SQLITE_PREPARE_NO_VTAB)

    def testStatementCacheZeroSize(self):
        "Rerun statement cache tests with a zero sized/disabled cache"
        self.db = apsw.Connection(TESTFILEPREFIX + "testdb", statementcachesize=-1)
        self.testStatementCache(0)

    def testStatementCacheLargeSize(self):
        "Rerun statement cache tests with a large cache"
        self.db = apsw.Connection(TESTFILEPREFIX + "testdb", statementcachesize=17000)
        self.testStatementCache(170000)

    def testStmtExplain(self):
        "Verify sqlite3_stmt_explain operation"

        # sanity check
        for row in self.db.execute("explain select 3", explain=0):
            self.assertEqual(row, (3, ))
        for row in self.db.execute("explain query plan select 3", explain=0):
            self.assertEqual(row, (3, ))
        for row in self.db.execute("explain select 3", explain=1):
            self.assertNotEqual(row, (3, ))

        is_explain = -99

        def ehook(cur, sql, bindings):
            nonlocal is_explain
            is_explain = cur.is_explain
            return True

        self.db.exec_trace = ehook
        cur = self.db.cursor()
        cur.exec_trace = ehook
        queries = {"select 3": 0, "explain select 4": 1, "explain query plan select 5": 2}
        for target in self.db, cur:
            for explain in (-1, 0, 1, 2):
                for meth in "execute", "executemany":
                    bindings = None if meth == "execute" else (tuple(), )
                    for q, v in queries.items():
                        is_explain = -99
                        for row in getattr(target, meth)(q, bindings, explain=explain):
                            pass
                        if explain < 0:
                            self.assertEqual(is_explain, v)
                        else:
                            self.assertEqual(is_explain, explain)

        self.assertRaises(apsw.SQLError, self.db.execute, "select 6", explain=7)

    # the text also includes characters that can't be represented in 16 bits (BMP)
    wikipedia_text = """Wikipedia\nThe Free Encyclopedia\nEnglish\n6 383 000+ articles\n日本語\n1 292 000+ 記事\nРусский\n1 756 000+ статей\nDeutsch\n2 617 000+ Artikel\nEspañol\n1 717 000+ artículos\nFrançais\n2 362 000+ articles\nItaliano\n1 718 000+ voci\n中文\n1 231 000+ 條目\nPolski\n1 490 000+ haseł\nPortuguês\n1 074 000+ artigos\nSearch Wikipedia\nEN\nEnglish\n\n Read Wikipedia in your language\n1 000 000+ articles\nPolski\nالعربية\nDeutsch\nEnglish\nEspañol\nFrançais\nItaliano\nمصرى\nNederlands\n日本語\nPortuguês\nРусский\nSinugboanong Binisaya\nSvenska\nУкраїнська\nTiếng Việt\nWinaray\n中文\n100 000+ articles\nAfrikaans\nSlovenčina\nAsturianu\nAzərbaycanca\nБългарски\nBân-lâm-gú / Hō-ló-oē\nবাংলা\nБеларуская\nCatalà\nČeština\nCymraeg\nDansk\nEesti\nΕλληνικά\nEsperanto\nEuskara\nفارسی\nGalego\n한국어\nՀայերեն\nहिन्दी\nHrvatski\nBahasa Indonesia\nעברית\nქართული\nLatina\nLatviešu\nLietuvių\nMagyar\nМакедонски\nBahasa Melayu\nBahaso Minangkabau\nNorskbokmålnynorsk\nНохчийн\nOʻzbekcha / Ўзбекча\nҚазақша / Qazaqşa / قازاقشا\nRomână\nSimple English\nSlovenščina\nСрпски / Srpski\nSrpskohrvatski / Српскохрватски\nSuomi\nதமிழ்\nТатарча / Tatarça\nภาษาไทย\nТоҷикӣ\nتۆرکجه\nTürkçe\nاردو\nVolapük\n粵語\nမြန်မာဘာသာ\n10 000+ articles\nBahsa Acèh\nAlemannisch\nአማርኛ\nAragonés\nBasa Banyumasan\nБашҡортса\nБеларуская (Тарашкевіца)\nBikol Central\nবিষ্ণুপ্রিয়া মণিপুরী\nBoarisch\nBosanski\nBrezhoneg\nЧӑвашла\nDiné Bizaad\nEmigliàn–Rumagnòl\nFøroyskt\nFrysk\nGaeilge\nGàidhlig\nગુજરાતી\nHausa\nHornjoserbsce\nIdo\nIlokano\nInterlingua\nИрон æвзаг\nÍslenska\nJawa\nಕನ್ನಡ\nKreyòl Ayisyen\nKurdî / كوردی\nکوردیی ناوەندی\nКыргызча\nКырык Мары\nLëtzebuergesch\nLimburgs\nLombard\nLìgure\nमैथिली\nMalagasy\nമലയാളം\n文言\nमराठी\nმარგალური\nمازِرونی\nMìng-dĕ̤ng-ngṳ̄ / 閩東語\nМонгол\nनेपाल भाषा\nनेपाली\nNnapulitano\nNordfriisk\nOccitan\nМарий\nଓଡି଼ଆ\nਪੰਜਾਬੀ (ਗੁਰਮੁਖੀ)\nپنجابی (شاہ مکھی)\nپښتو\nPiemontèis\nPlattdüütsch\nQırımtatarca\nRuna Simi\nसंस्कृतम्\nСаха Тыла\nScots\nShqip\nSicilianu\nසිංහල\nسنڌي\nŚlůnski\nBasa Sunda\nKiswahili\nTagalog\nతెలుగు\nᨅᨔ ᨕᨙᨁᨗ / Basa Ugi\nVèneto\nWalon\n吳語\nייִדיש\nYorùbá\nZazaki\nŽemaitėška\nisiZulu\n1 000+ articles\nАдыгэбзэ\nÆnglisc\nAkan\nаԥсшәа\nԱրեւմտահայերէն\nArmãneashce\nArpitan\nܐܬܘܪܝܐ\nAvañe’ẽ\nАвар\nAymar\nBasa Bali\nBahasa Banjar\nभोजपुरी\nBislama\nབོད་ཡིག\nБуряад\nChavacano de Zamboanga\nCorsu\nVahcuengh / 話僮\nDavvisámegiella\nDeitsch\nދިވެހިބަސް\nDolnoserbski\nЭрзянь\nEstremeñu\nFiji Hindi\nFurlan\nGaelg\nGagauz\nGĩkũyũ\nگیلکی\n贛語\nHak-kâ-ngî / 客家語\nХальмг\nʻŌlelo Hawaiʻi\nIgbo\nInterlingue\nKabɩyɛ\nKapampangan\nKaszëbsczi\nKernewek\nភាសាខ្មែរ\nKinyarwanda\nКоми\nKongo\nकोंकणी / Konknni\nKriyòl Gwiyannen\nພາສາລາວ\nDzhudezmo / לאדינו\nЛакку\nLatgaļu\nЛезги\nLingála\nlojban\nLuganda\nMalti\nReo Mā’ohi\nMāori\nMirandés\nМокшень\nߒߞߏ\nNa Vosa Vaka-Viti\nNāhuatlahtōlli\nDorerin Naoero\nNedersaksisch\nNouormand / Normaund\nNovial\nAfaan Oromoo\nঅসমীযা়\nपालि\nPangasinán\nPapiamentu\nПерем Коми\nPfälzisch\nPicard\nКъарачай–Малкъар\nQaraqalpaqsha\nRipoarisch\nRumantsch\nРусиньскый Язык\nGagana Sāmoa\nSardu\nSeeltersk\nSesotho sa Leboa\nChiShona\nSoomaaliga\nSranantongo\nTaqbaylit\nTarandíne\nTetun\nTok Pisin\nfaka Tonga\nTürkmençe\nТыва дыл\nУдмурт\nئۇيغۇرچه\nVepsän\nVõro\nWest-Vlams\nWolof\nisiXhosa\nZeêuws\n100+ articles\nBamanankan\nChamoru\nChichewa\nEʋegbe\nFulfulde\n𐌲𐌿𐍄𐌹𐍃𐌺\nᐃᓄᒃᑎᑐᑦ / Inuktitut\nIñupiak\nKalaallisut\nكٲشُر\nLi Niha\nNēhiyawēwin / ᓀᐦᐃᔭᐍᐏᐣ\nNorfuk / Pitkern\nΠοντιακά\nརྫོང་ཁ\nRomani\nKirundi\nSängö\nSesotho\nSetswana\nСловѣ́ньскъ / ⰔⰎⰑⰂⰡⰐⰠⰔⰍⰟ\nSiSwati\nThuɔŋjäŋ\nᏣᎳᎩ\nTsėhesenėstsestotse\nTshivenḓa\nXitsonga\nchiTumbuka\nTwi\nትግርኛ\nဘာသာ မန်\n"""
    assert (any(ord(c) > 65536 for c in wikipedia_text))

    def testWikipedia(self):
        "Use front page of wikipedia to check unicode handling"
        self.db.close()
        text = APSW.wikipedia_text
        for encoding in "UTF-16", "UTF-16le", "UTF-16be", "UTF-8":
            if os.path.exists(TESTFILEPREFIX + "testdb"):
                deletefile(TESTFILEPREFIX + "testdb")
            db = apsw.Connection(TESTFILEPREFIX + "testdb")
            c = db.cursor()
            c.execute("pragma encoding=\"%s\"" % (encoding, ))
            for row in c.execute("pragma encoding"):
                # we use startswith as UTF-16 will be returned with le/be suffix
                self.assertTrue(row[0].startswith(encoding))
            c.execute("create table foo(x); insert into foo values(?)", (text, ))
            for row in c.execute("select * from foo"):
                self.assertEqual(row[0], text)
            db.close()

    # calls that need protection
    calls={
        'sqlite3api': { # items of interest - sqlite3 calls
                        'match': re.compile(r"(sqlite3_[A-Za-z0-9_]+)\s*\("),
                        # what must also be on same or preceding line
                        'needs': re.compile("PYSQLITE(_|_BLOB_|_CON_|_CUR_|_SC_|_VOID_|_BACKUP_)CALL"),

           # except if match.group(1) matches this - these don't
           # acquire db mutex so no need to wrap (determined by
           # examining sqlite3.c).  If they acquire non-database
           # mutexes then that is ok.

           # In the case of sqlite3_result_*|declare_vtab, the mutex
           # is already held by enclosing sqlite3_step and the
           # methods will only be called from that same thread so it
           # isn't a problem.
                        'skipcalls': re.compile("^sqlite3_(blob_bytes|column_count|bind_parameter_count|data_count|vfs_.+|changes64|total_changes64"
                                                "|bind_parameter_name"
                                                "|get_" "autocommit|last_insert_rowid|complete|interrupt|limit|malloc64|free|threadsafe|value_.+"
                                                "|libversion|enable_" "shared_cache|initialize|shutdown|config|memory_.+|soft_heap_limit64|hard_heap_limit64"
                                                "|randomness|db_readonly|db_filename|release_" "memory|status64|result_.+|user_data|mprintf|aggregate_context"
                                                "|declare_vtab|backup_remaining|backup_pagecount|mutex_enter|mutex_leave|sourceid|uri_.+"
                                                "|column_name|column_decltype|column_database_name|column_table_name|column_origin_name"
                                                "|stmt_isexplain|stmt_readonly|filename_journal|filename_wal|stmt_status|sql|log|vtab_collation"
                                                "|vtab_rhs_value|vtab_distinct|vtab_config|vtab_on_conflict|vtab_in_first|vtab_in_next|vtab_in"
                                                "|vtab_nochange|is_interrupted|extended_errcode)$"),
                        # error message
                        'desc': "sqlite3_ calls must wrap with PYSQLITE_CALL",
                        },
        'inuse':        {
                        'match': re.compile(r"(convert_column_to_pyobject|statementcache_prepare|statementcache_finalize|statementcache_next)\s*\("),
                        'needs': re.compile("INUSE_CALL"),
                        'desc': "call needs INUSE wrapper",
                        "skipfiles": re.compile(r".*[/\\]statementcache.c$"),
                        },
        }

    def sourceCheckMutexCall(self, filename, name, lines):
        # we check that various calls are wrapped with various macros
        for i, line in enumerate(lines):
            if "PYSQLITE_CALL" in line and "Py" in line:
                self.fail("%s: %s() line %d - Py call while GIL released - %s" % (filename, name, i, line.strip()))
            for k, v in self.calls.items():
                if v.get('skipfiles', None) and v['skipfiles'].match(filename):
                    continue
                mo = v['match'].search(line)
                if mo:
                    func = mo.group(1)
                    if v.get('skipcalls', None) and v['skipcalls'].match(func):
                        continue
                    if not v["needs"].search(line) and not v["needs"].search(lines[i - 1]):
                        self.fail("%s: %s() line %d call to %s(): %s - %s\n" %
                                  (filename, name, i, func, v['desc'], line.strip()))

    def sourceCheckFunction(self, filename, name, lines):
        # existing exception in callbacks
        if any("PyGILState_Ensure" in line for line in lines):
            if not any("MakeExistingException" in line for line in lines):
                self.fail(
                    f"file { filename } function { name } calls PyGILState_Ensure but does not have MakeExistingException"
                )
        # not further checked
        if name.split("_")[0] in ("ZeroBlobBind", "APSWVFS", "APSWVFSFile", "APSWBuffer", "FunctionCBInfo"):
            return

        checks = {
            "APSWCursor": {
                "skip": ("dealloc", "init", "dobinding", "dobindings", "do_exec_trace", "do_row_trace", "step", "close",
                         "close_internal", "tp_traverse", "tp_str"),
                "req": {
                    "use": "CHECK_USE",
                    "closed": "CHECK_CURSOR_CLOSED",
                },
                "order": ("use", "closed")
            },
            "Connection": {
                "skip": ("internal_cleanup", "dealloc", "init", "close", "interrupt", "close_internal",
                         "remove_dependent", "readonly", "getmainfilename", "db_filename", "traverse", "clear",
                         "tp_traverse", "get_cursor_factory", "set_cursor_factory", "tp_str"),
                "req": {
                    "use": "CHECK_USE",
                    "closed": "CHECK_CLOSED",
                },
                "order": ("use", "closed")
            },
            "APSWBlob": {
                "skip": ("dealloc", "init", "close", "close_internal", "tp_str"),
                "req": {
                    "use": "CHECK_USE",
                    "closed": "CHECK_BLOB_CLOSED"
                },
                "order": ("use", "closed")
            },
            "APSWBackup": {
                "skip": ("dealloc", "init", "close_internal", "get_remaining", "get_page_count", "tp_str"),
                "req": {
                    "use": "CHECK_USE",
                    "closed": "CHECK_BACKUP_CLOSED"
                },
                "order": ("use", "closed")
            },
            "apswvfs": {
                "req": {
                    "preamble": "VFSPREAMBLE",
                    "tb": "AddTraceBackHere",
                    "postamble": "VFSPOSTAMBLE"
                },
                "order": ("preamble", "tb", "postamble")
            },
            "apswvfspy": {
                "req": {
                    "check": "CHECKVFSPY",
                    "notimpl": "VFSNOTIMPLEMENTED(%(base)s,"
                },
                "order": ("check", "notimpl"),
            },
            "apswvfspy_unregister": {
                "req": {
                    "check": "CHECKVFSPY",
                },
            },
            "apswvfsfile": {
                "req": {
                    "preamble": "FILEPREAMBLE",
                    "postamble": "FILEPOSTAMBLE",
                },
                "order": ("preamble", "postamble")
            },
            "apswvfsfilepy": {
                "skip": ("xClose", ),
                "req": {
                    "check": "CHECKVFSFILEPY",
                    "notimpl": "VFSFILENOTIMPLEMENTED(%(base)s,"
                },
                "order": ("check", "notimpl"),
            },
            "SqliteIndexInfo": {
                "req": {
                    "check": "CHECK_INDEX",
                },
            },
            "apswfcntl": {
                "req": {}
            },
            "apswurifilename": {
                "req": {
                    "check": "CHECK_SCOPE"
                }
            }
        }

        prefix, base = name.split("_", 1)
        if name in checks:
            checker = checks[name]
        elif prefix in checks:
            checker = checks[prefix]
        else:
            self.fail(filename + ": " + prefix + " not in checks (" + name + ")")

        if base in checker.get("skip", ()):
            return

        format = {"base": base, "prefix": prefix}

        found = {}
        for k in checker["req"]:
            found[k] = None

        # check the lines
        for i, line in enumerate(lines):
            for k, v in checker["req"].items():
                v = v % format
                if v in line and found[k] is None:
                    found[k] = i

        # check they are present
        for k, v in checker["req"].items():
            if found[k] is None:
                v = v % format
                self.fail(filename + ": " + k + " " + v + " missing in " + name)

        # check order
        order = checker.get("order", ())
        for i in range(len(order) - 2):
            b4 = order[i]
            after = order[i + 1]
            if found[b4] > found[after]:
                self.fail(filename + ": " + checker["req"][b4] % format + " should be before " +
                          checker["req"][after] % format + " in " + name)

        return

    should_use_compat = ("PyObject_CheckReadBuffer", "PyObject_AsReadBuffer")

    def testSourceChecks(self):
        "Check various source code issues"
        # We expect a coding style where the functions are named
        # Object_method, are at the start of the line and have a first
        # parameter named self.
        for filename in glob.glob("src/*.c"):
            if filename.endswith("testextension.c") or filename.endswith("constants.c"):
                continue
            # check not using C++ style comments
            code = read_whole_file(filename, "rt").replace("http://", "http:__").replace("https://", "https:__")
            if "//" in code:
                self.fail("// style comment in " + filename)

            if filename.replace("\\", "/") != "src/pyutil.c":
                for n in self.should_use_compat:
                    if n in code:
                        self.fail("Should be using compat function for %s in file %s" % (n, filename))

            # not allowed PyObject_New because we can't faultinject it
            if re.search(r"\bPyObject_New\b", code):
                self.fail(f"In { filename } you must use _PyObject_New (leading underscore)")

            # check check funcs
            funcpat1 = re.compile(r"^(\w+_\w+)\s*\(\s*\w+\s*\*\s*self")
            funcpat2 = re.compile(r"^(\w+)\s*\(")
            name1 = None
            name2 = None
            lines = []
            infunc = 0
            for line in read_whole_file(filename, "rt").split("\n"):
                if line.startswith("}") and infunc:
                    if infunc == 1:
                        self.sourceCheckMutexCall(filename, name1, lines)
                        self.sourceCheckFunction(filename, name1, lines)
                    elif infunc == 2:
                        self.sourceCheckMutexCall(filename, name2, lines)
                    else:
                        assert False
                    infunc = 0
                    lines = []
                    name1 = None
                    name2 = None
                    continue
                if name1 and line.startswith("{"):
                    infunc = 1
                    continue
                if name2 and line.startswith("{"):
                    infunc = 2
                    continue
                if infunc:
                    lines.append(line)
                    continue
                m = funcpat1.match(line)
                if m:
                    name1 = m.group(1)
                    continue
                m = funcpat2.match(line)
                if m:
                    name2 = m.group(1)
                    continue

    def testConfig(self):
        "Verify sqlite3_config wrapper"
        # we need to ensure there are no outstanding sqlite objects
        self.db = None
        gc.collect()
        self.assertRaises(apsw.MisuseError, apsw.config, apsw.SQLITE_CONFIG_MEMSTATUS, True)
        self.assertEqual(0, len(apsw.connections()))
        apsw.shutdown()
        try:
            self.assertRaises(TypeError, apsw.config)
            self.assertRaises(TypeError, apsw.config, "chicken")
            apsw.config(apsw.SQLITE_CONFIG_SINGLETHREAD)
            self.assertRaises(TypeError, apsw.config, apsw.SQLITE_CONFIG_SINGLETHREAD, 2)
            self.assertRaises(TypeError, apsw.config, apsw.SQLITE_CONFIG_MEMSTATUS)
            apsw.config(apsw.SQLITE_CONFIG_MEMSTATUS, True)
            apsw.config(apsw.SQLITE_CONFIG_MEMSTATUS, False)
            self.assertRaises(TypeError, apsw.config, 89748937)
            x = 0x7fffffff
            self.assertRaises(OverflowError, apsw.config, x * x * x * x)
            self.assertTrue(apsw.config(apsw.SQLITE_CONFIG_PCACHE_HDRSZ) >= 0)
            self.assertRaises(TypeError, apsw.config, apsw.SQLITE_CONFIG_PCACHE_HDRSZ, "extra")
            apsw.config(apsw.SQLITE_CONFIG_PMASZ, -1)
            self.assertRaises(TypeError, apsw.config, apsw.SQLITE_CONFIG_MMAP_SIZE, "3")
            self.assertRaises(TypeError, apsw.config, apsw.SQLITE_CONFIG_MMAP_SIZE, 3, "3")
            self.assertRaises(OverflowError, apsw.config, apsw.SQLITE_CONFIG_MMAP_SIZE, 2**65, 2**65)
            apsw.config(apsw.SQLITE_CONFIG_MMAP_SIZE, 2**63 - 1, 2**63 - 1)
            self.assertRaises(TypeError, apsw.config, apsw.SQLITE_CONFIG_MEMDB_MAXSIZE, "3")
            self.assertRaises(TypeError, apsw.config, apsw.SQLITE_CONFIG_MEMDB_MAXSIZE, 3, "3")
            self.assertRaises(OverflowError, apsw.config, apsw.SQLITE_CONFIG_MEMDB_MAXSIZE, 2**65)
            apsw.config(apsw.SQLITE_CONFIG_MEMDB_MAXSIZE, 2**63 - 1)
        finally:
            # put back to normal
            apsw.config(apsw.SQLITE_CONFIG_SERIALIZED)
            apsw.config(apsw.SQLITE_CONFIG_MEMSTATUS, True)
            apsw.initialize()

    def testFaultInjectionTested(self):
        "Make sure all fault injection is tested"
        faults = set()
        for filename in glob.glob("src/*.c"):
            with open(filename, "rt", encoding="utf8") as f:
                for line in f:
                    if "APSW_FAULT_INJECT" in line and "#define" not in line:
                        mo = re.match(r".*APSW_FAULT_INJECT\s*\(\s*(?P<name>\w+)\s*,.*", line)
                        assert mo, f"Failed to match line { line }"
                        name = mo.group("name")
                        assert name not in faults, f"fault inject name { name } found multiple times"
                        faults.add(name)

        testcode = read_whole_file(__file__, "rt", "utf8")

        for name in sorted(faults):
            self.assertTrue(re.search(f"\\b{ name }\\b", testcode), f"Couldn't find test for fault '{ name }'")

    def testMemory(self):
        "Verify memory tracking functions"
        self.assertNotEqual(apsw.memory_used(), 0)
        self.assertTrue(apsw.memory_high_water() >= apsw.memory_used())
        self.assertRaises(TypeError, apsw.memory_high_water, "eleven")
        apsw.memory_high_water(True)
        self.assertEqual(apsw.memory_high_water(), apsw.memory_used())
        self.assertRaises(TypeError, apsw.soft_heap_limit, 1, 2)
        self.assertRaises(TypeError, apsw.hard_heap_limit, 1, 2)
        apsw.soft_heap_limit(0)
        self.assertRaises(TypeError, apsw.release_memory, 1, 2)
        res = apsw.release_memory(0x7fffffff)
        self.assertTrue(type(res) in (int, ))
        apsw.soft_heap_limit(0x1234567890abc)
        self.assertEqual(0x1234567890abc, apsw.soft_heap_limit(0x1234567890abe))
        apsw.hard_heap_limit(0x1234567890abd)
        self.assertEqual(0x1234567890abd, apsw.hard_heap_limit(0x1234567890abe))

    def testRandomness(self):
        "Verify randomness routine"
        self.assertRaises(TypeError, apsw.randomness, "three")
        self.assertRaises(OverflowError, apsw.randomness, 0xffffffffee)
        self.assertRaises(ValueError, apsw.randomness, -2)
        self.assertEqual(0, len(apsw.randomness(0)))
        self.assertEqual(1, len(apsw.randomness(1)))
        self.assertEqual(16383, len(apsw.randomness(16383)))
        self.assertNotEqual(apsw.randomness(77), apsw.randomness(77))

    def testSqlite3Pointer(self):
        "Verify getting underlying sqlite3 pointer"
        self.assertRaises(TypeError, self.db.sqlite3_pointer, 7)
        self.assertTrue(type(self.db.sqlite3_pointer()) in (int, ))
        self.assertEqual(self.db.sqlite3_pointer(), self.db.sqlite3_pointer())
        self.assertNotEqual(self.db.sqlite3_pointer(), apsw.Connection(":memory:").sqlite3_pointer())

    def testPickle(self):
        "Verify data etc can be pickled"
        PicklingError = pickle.PicklingError

        # work out what protocol versions we can use
        versions = []
        for num in range(-1, 20):
            try:
                pickle.dumps(3, num)
                versions.append(num)
            except ValueError:
                pass

        # some objects to try pickling
        vals = test_types_vals
        cursor = self.db.cursor()
        cursor.execute("create table if not exists t(i,x)")

        def canpickle(val):
            return True

        cursor.execute("BEGIN")
        cursor.executemany("insert into t values(?,?)", [(i, v) for i, v in enumerate(vals) if canpickle(v)])
        cursor.execute("COMMIT")

        for ver in versions:
            for row in cursor.execute("select * from t"):
                self.assertEqual(row, pickle.loads(pickle.dumps(row, ver)))
                rownum, val = row
                if type(vals[rownum]) is float:
                    self.assertAlmostEqual(vals[rownum], val)
                else:
                    self.assertEqual(vals[rownum], val)
            # can't pickle cursors
            try:
                pickle.dumps(cursor, ver)
            except TypeError:
                pass
            except PicklingError:
                pass
            # some versions can pickle the db, but give a zeroed db back
            db = None
            try:
                db = pickle.loads(pickle.dumps(self.db, ver))
            except TypeError:
                pass
            if db is not None:
                self.assertRaises(apsw.ConnectionClosedError, db.db_filename, "main")
                self.assertRaises(apsw.ConnectionClosedError, db.cursor)
                self.assertRaises(apsw.ConnectionClosedError, db.get_autocommit)
                self.assertRaises(apsw.ConnectionClosedError, db.in_transaction)

    def testDropModules(self):
        "Verify dropping virtual table modules"

        # simplest implementation possible
        class Source:

            def Create(self, db, modulename, dbname, tablename, *args):
                return "create table placeholder(x)", Source.Table()

            Connect = Create

            class Table:

                def BestIndex(*args):
                    return

                def BestIndexObject(*args):
                    return True

                def Open(*args):
                    return Source.Cursor()

            class Cursor:

                def Eof(*args):
                    return True

                def Filter(*args):
                    pass

                Close = Filter

        counter = 0

        def check_module(name: str, shouldfail: bool) -> None:
            nonlocal counter
            counter += 1
            errs = []

            # eponymous only will have no_module even when they exist
            no_module = False
            no_table = False

            try:
                self.db.execute(f"create virtual table ex{ counter } using { name }()")
            except apsw.SQLError as e:
                no_module = f"no such module: { name }" in str(e)
            try:
                self.db.execute(f"select * from  { name }()")
            except apsw.SQLError as e:
                no_table = f"no such table: { name }" in str(e)
            if shouldfail:
                self.assertTrue(no_module and no_table)
            else:
                self.assertFalse(no_module and no_table)

        self.db.create_module("abc", Source())
        check_module("abc", False)
        self.db.create_module("abc", None)  # should drop the table
        check_module("abc", True)

        # we register a whole bunch, and then unregister subsets
        args = []
        for use_bestindex_object in (False, True):
            for iVersion in (1, 2, 3):
                for eponymous in (False, True):
                    for eponymous_only in (False, True):
                        for read_only in (False, True):
                            args.append({
                                "use_bestindex_object": use_bestindex_object,
                                "iVersion": iVersion,
                                "eponymous": eponymous,
                                "eponymous_only": eponymous_only,
                                "read_only": read_only
                            })

        names = list("".join(x) for x in itertools.permutations("abcd"))
        for n in names:
            try:
                self.db.create_module(n, Source(), **random.choice(args))
            except Exception as e:
                self.assertIn("No xShadowName slots are available.", str(e))
                continue
            check_module(n, False)

        random.shuffle(names)
        for i in range(len(names), -1, -1):
            if not names:
                break
            keep = random.sample(names, i)
            self.db.drop_modules(keep)
            for n in names:
                check_module(n, n not in keep)
            check_module("madeup", True)
            names = keep
        self.assertRaises(TypeError, self.db.drop_modules)
        self.assertRaises(TypeError, self.db.drop_modules, ["one", 2, "three"])

    def testStatus(self):
        "Verify status function"
        self.assertRaises(TypeError, apsw.status, "zebra")
        self.assertRaises(apsw.MisuseError, apsw.status, 2323)
        for i in apsw.mapping_status:
            if type(i) != type(""): continue
            res = apsw.status(getattr(apsw, i))
            self.assertEqual(len(res), 2)
            self.assertEqual(type(res), tuple)
            self.assertTrue(res[0] <= res[1])

    def testDBStatus(self):
        "Verify db status function"
        self.assertRaises(TypeError, self.db.status, "zebra")
        self.assertRaises(apsw.SQLError, self.db.status, 2323)
        for i in apsw.mapping_db_status:
            if type(i) != type(""): continue
            res = self.db.status(getattr(apsw, i))
            self.assertEqual(len(res), 2)
            self.assertEqual(type(res), tuple)
            self.assertTrue(res[1] == 0 or res[0] <= res[1])

    def testTxnState(self):
        "Verify db.txn_state"
        n = "\u1234\u3454324"
        self.assertRaises(TypeError, self.db.txn_state, 3)
        self.assertEqual(apsw.mapping_txn_state["SQLITE_TXN_NONE"], self.db.txn_state())
        self.db.cursor().execute("BEGIN EXCLUSIVE")
        self.assertEqual(apsw.mapping_txn_state["SQLITE_TXN_WRITE"], self.db.txn_state())
        self.db.cursor().execute("END")
        self.assertEqual(apsw.mapping_txn_state["SQLITE_TXN_NONE"], self.db.txn_state())
        self.assertRaises(ValueError, self.db.txn_state, n)
        self.assertEqual(apsw.mapping_txn_state["SQLITE_TXN_NONE"], self.db.txn_state("main"))

    def testZeroBlob(self):
        "Verify handling of zero blobs"
        self.assertRaises(TypeError, apsw.zeroblob)
        self.assertRaises(TypeError, apsw.zeroblob, "foo")
        self.assertRaises(TypeError, apsw.zeroblob, -7)
        self.assertRaises(apsw.TooBigError, self.db.execute, "select ?", (apsw.zeroblob(4000000000), ))
        cur = self.db.cursor()
        cur.execute("create table foo(x)")
        cur.execute("insert into foo values(?)", (apsw.zeroblob(27), ))
        v = curnext(cur.execute("select * from foo"))[0]
        self.assertEqual(v, b"\x00" * 27)

        # Make sure inheritance works
        class multi:

            def __init__(self, *args):
                self.foo = 3

        class derived(apsw.zeroblob):

            def __init__(self, num):
                #multi.__init__(self)
                apsw.zeroblob.__init__(self, num)

        cur.execute("delete from foo; insert into foo values(?)", (derived(28), ))
        v = curnext(cur.execute("select * from foo"))[0]
        self.assertEqual(v, b"\x00" * 28)
        self.assertEqual(apsw.zeroblob(91210).length(), 91210)

    def testBlobIO(self):
        "Verify Blob input/output"
        cur = self.db.cursor()
        rowid = curnext(
            cur.execute("create table foo(x blob); insert into foo values(zeroblob(98765)); select rowid from foo"))[0]
        self.assertRaises(TypeError, self.db.blob_open, 1)
        self.assertRaises(TypeError, self.db.blob_open, "main", "foo\xf3")
        self.assertRaises(TypeError, self.db.blob_open, "main", "foo", "x", complex(-1, -1), True)
        self.assertRaises(TypeError, self.db.blob_open, "main", "foo", "x", rowid, True, False)
        self.assertRaises(apsw.SQLError, self.db.blob_open, "main", "foo", "x", rowid + 27, False)
        self.assertRaises(apsw.SQLError, self.db.blob_open, "foo", "foo", "x", rowid, False)
        self.assertRaises(apsw.SQLError, self.db.blob_open, "main", "x", "x", rowid, False)
        self.assertRaises(apsw.SQLError, self.db.blob_open, "main", "foo", "y", rowid, False)
        blobro = self.db.blob_open("main", "foo", "x", rowid, False)
        # sidebar: check they can't be manually created
        self.assertRaises(TypeError, type(blobro))
        # check vals
        self.assertEqual(blobro.length(), 98765)
        self.assertEqual(blobro.length(), 98765)
        self.assertEqual(blobro.read(0), b"")
        zero = b"\x00"
        step = 5  # must be exact multiple of size
        assert (blobro.length() % step == 0)
        for i in range(0, 98765, step):
            x = blobro.read(step)
            self.assertEqual(zero * step, x)
        x = blobro.read(10)
        self.assertEqual(x, b"")
        blobro.seek(0, 1)
        self.assertEqual(blobro.tell(), 98765)
        blobro.seek(0)
        self.assertEqual(blobro.tell(), 0)
        self.assertEqual(len(blobro.read(11119999)), 98765)
        blobro.seek(2222)
        self.assertEqual(blobro.tell(), 2222)
        blobro.seek(0, 0)
        self.assertEqual(blobro.tell(), 0)
        self.assertEqual(blobro.read(), b"\x00" * 98765)
        blobro.seek(-3, 2)
        self.assertEqual(blobro.read(), b"\x00" * 3)
        # check types
        self.assertRaises(TypeError, blobro.read, "foo")
        self.assertRaises(TypeError, blobro.tell, "foo")
        self.assertRaises(TypeError, blobro.seek)
        self.assertRaises(TypeError, blobro.seek, "foo", 1)
        self.assertRaises(TypeError, blobro.seek, 0, 1, 2)
        self.assertRaises(ValueError, blobro.seek, 0, -3)
        self.assertRaises(ValueError, blobro.seek, 0, 3)
        # can't seek before beginning or after end of file
        self.assertRaises(ValueError, blobro.seek, -1, 0)
        self.assertRaises(ValueError, blobro.seek, 25, 1)
        self.assertRaises(ValueError, blobro.seek, 25, 2)
        self.assertRaises(ValueError, blobro.seek, 100000, 0)
        self.assertRaises(ValueError, blobro.seek, -100000, 1)
        self.assertRaises(ValueError, blobro.seek, -100000, 2)
        # close testing
        blobro.seek(0, 0)
        self.assertRaises(apsw.ReadOnlyError, blobro.write, b"kermit was here")
        # you get the error on the close too, and blob is always closed - sqlite ticket #2815
        self.assertRaises(apsw.ReadOnlyError, blobro.close)
        # check can't work on closed blob
        self.assertRaises(ValueError, blobro.read)
        self.assertRaises(ValueError, blobro.read_into, b"ab")
        self.assertRaises(ValueError, blobro.seek, 0, 0)
        self.assertRaises(ValueError, blobro.tell)
        self.assertRaises(ValueError, blobro.write, "abc")
        # read_into tests
        rowidri = self.db.cursor().execute(
            "insert into foo values(x'112233445566778899aabbccddeeff'); select last_insert_rowid()").fetchall()[0][0]
        blobro = self.db.blob_open("main", "foo", "x", rowidri, False)
        self.assertRaises(TypeError, blobro.read_into)
        self.assertRaises(TypeError, blobro.read_into, 3)
        buffers = []
        buffers.append(array.array("b", b"\0\0\0\0"))
        buffers.append(bytearray(b"\0\0\0\0"))

        # bytearray returns ints rather than chars so a fixup
        def _fixup(c):
            if type(c) == int:
                return bytes([c])
            return c

        for buf in buffers:
            self.assertRaises(TypeError, blobro.read_into)
            self.assertRaises(TypeError, blobro.read_into, buf, buf)
            self.assertRaises(TypeError, blobro.read_into, buf, 1, buf)
            self.assertRaises(TypeError, blobro.read_into, buf, 1, 1, buf)
            blobro.seek(0)
            blobro.read_into(buf, 1, 1)
            self.assertEqual(_fixup(buf[0]), b"\x00")
            self.assertEqual(_fixup(buf[1]), b"\x11")
            self.assertEqual(_fixup(buf[2]), b"\x00")
            self.assertEqual(_fixup(buf[3]), b"\x00")
            self.assertEqual(len(buf), 4)
            blobro.seek(3)
            blobro.read_into(buf)

            def check_unchanged():
                self.assertEqual(_fixup(buf[0]), b"\x44")
                self.assertEqual(_fixup(buf[1]), b"\x55")
                self.assertEqual(_fixup(buf[2]), b"\x66")
                self.assertEqual(_fixup(buf[3]), b"\x77")
                self.assertEqual(len(buf), 4)

            check_unchanged()
            blobro.seek(14)
            # too much requested
            self.assertRaises(ValueError, blobro.read_into, buf, 1)
            check_unchanged()
            # bounds errors
            self.assertRaises(ValueError, blobro.read_into, buf, 1, -1)
            self.assertRaises(ValueError, blobro.read_into, buf, 1, 7)
            self.assertRaises(ValueError, blobro.read_into, buf, -1, 2)
            self.assertRaises(ValueError, blobro.read_into, buf, 10000, 2)
            self.assertRaises(OverflowError, blobro.read_into, buf, 1, 45236748972389749283)
            check_unchanged()
        # get a read error
        blobro.seek(0)
        self.db.cursor().execute("update foo set x=x'112233445566' where rowid=?", (rowidri, ))
        self.assertRaises(apsw.AbortError, blobro.read_into, buf)
        # should fail with buffer being a string
        self.assertRaises(TypeError, blobro.read_into, "abcd", 1, 1)
        self.assertRaises(TypeError, blobro.read_into, "abcd", 1, 1)
        # write tests
        blobrw = self.db.blob_open("main", "foo", "x", rowid, True)
        self.assertEqual(blobrw.length(), 98765)
        blobrw.write(b"abcd")
        blobrw.seek(0, 0)
        self.assertEqual(blobrw.read(4), b"abcd")
        blobrw.write(b"efg")
        blobrw.seek(0, 0)
        self.assertEqual(blobrw.read(7), b"abcdefg")
        blobrw.seek(50, 0)
        blobrw.write(b"hijkl")
        blobrw.seek(-98765, 2)
        self.assertEqual(blobrw.read(55), b"abcdefg" + b"\x00" * 43 + b"hijkl")
        self.assertRaises(TypeError, blobrw.write, 12)
        self.assertRaises(TypeError, blobrw.write)
        self.assertRaises(TypeError, blobrw.write, "foo")
        # try to go beyond end
        self.assertRaises(ValueError, blobrw.write, b" " * 100000)
        self.assertRaises(TypeError, blobrw.close, "elephant")
        # coverage
        blobro = self.db.blob_open("main", "foo", "x", rowid, False)
        self.assertRaises(apsw.ReadOnlyError, blobro.write, b"abcd")
        blobro.close(True)
        self.db.cursor().execute("insert into foo(_rowid_, x) values(99, 1)")
        blobro = self.db.blob_open("main", "foo", "x", rowid, False)
        self.assertRaises(TypeError, blobro.reopen)
        self.assertRaises(TypeError, blobro.reopen, "banana")
        self.assertRaises(OverflowError, blobro.reopen, 45236748972389749283)
        first = blobro.read(2)
        # check position is reset
        blobro.reopen(rowid)
        self.assertEqual(blobro.tell(), 0)
        self.assertEqual(first, blobro.read(2))
        # invalid reopen
        self.assertRaises(apsw.SQLError, blobro.reopen, 0x1ffffffff)
        blobro.close()

    def testBlobReadError(self):
        "Ensure blob read errors are handled well"
        cur = self.db.cursor()
        cur.execute("create table ioerror (x, blob)")
        cur.execute("insert into ioerror (rowid,x,blob) values (2,3,x'deadbeef')")
        blob = self.db.blob_open("main", "ioerror", "blob", 2, False)
        blob.read(1)
        # Do a write which cause blob to become invalid
        cur.execute("update ioerror set blob='fsdfdsfasd' where x=3")
        try:
            blob.read(1)
            1 / 0
        except:
            klass, value = sys.exc_info()[:2]
            self.assertTrue(klass is apsw.AbortError)

    def testAutovacuumPages(self):
        self.assertRaises(TypeError, self.db.autovacuum_pages)
        self.assertRaises(TypeError, self.db.autovacuum_pages, 3)
        for stmt in ("pragma page_size=512", "pragma auto_vacuum=FULL", "create table foo(x)", "begin"):
            self.db.cursor().execute(stmt)
        self.db.cursor().executemany("insert into foo values(zeroblob(1023))", [tuple() for _ in range(500)])

        self.db.cursor().execute("commit")

        rowids = [row[0] for row in self.db.cursor().execute("select ROWID from foo")]

        last_free = [0]

        def avpcb(schema, nPages, nFreePages, nBytesPerPage):
            self.assertEqual(schema, "main")
            self.assertTrue(nFreePages < nPages)
            self.assertTrue(nFreePages >= 2)
            self.assertEqual(nBytesPerPage, 512)
            # we always return 1, so second call must have more free pages than first
            if last_free[0]:
                self.assertTrue(nFreePages > last_free[0])
            else:
                last_free[0] = nFreePages
            return 1

        def noparams():
            pass

        def badreturn(*args):
            return "seven"

        self.db.cursor().execute("delete from foo where rowid=?", (rowids.pop(), ))

        self.db.autovacuum_pages(noparams)
        self.assertRaises(TypeError, self.db.cursor().execute, "delete from foo where rowid=?", (rowids.pop(), ))
        self.db.autovacuum_pages(None)
        self.db.cursor().execute("delete from foo where rowid=?", (rowids.pop(), ))
        self.db.autovacuum_pages(avpcb)
        self.db.cursor().execute("delete from foo where rowid=?", (rowids.pop(), ))
        self.db.autovacuum_pages(badreturn)
        self.assertRaises(TypeError, self.db.cursor().execute, "delete from foo where rowid=?", (rowids.pop(), ))
        self.db.autovacuum_pages(None)
        self.db.cursor().execute("delete from foo where rowid=?", (rowids.pop(), ))

    def testTraceV2(self):
        "Connection.trace_v2"
        self.assertRaises(TypeError, self.db.trace_v2, "abc", "abc")
        self.assertRaises(TypeError, self.db.trace_v2, 1, "abc")

        self.assertRaises(ValueError, self.db.trace_v2, 1, None)
        self.assertRaises(ValueError, self.db.trace_v2, 0, lambda x: x)

        self.assertRaises(ValueError, self.db.trace_v2, 0xffffff, lambda x: x)

        results = []

        def tracecb(data):
            results.append(data)

        query = "select 3"

        self.db.trace_v2(apsw.SQLITE_TRACE_STMT, tracecb)

        for _ in self.db.execute(query):
            pass

        self.assertEqual(1, len(results))
        x = results.pop()
        self.assertEqual(apsw.SQLITE_TRACE_STMT, x["code"])
        self.assertIs(self.db, x["connection"])
        self.assertEqual(query, x["sql"])

        self.db.trace_v2(apsw.SQLITE_TRACE_ROW, tracecb)

        for _ in self.db.execute(query):
            pass

        self.assertEqual(1, len(results))
        x = results.pop()
        self.assertEqual(apsw.SQLITE_TRACE_ROW, x["code"])
        self.assertIs(self.db, x["connection"])
        self.assertEqual(query, x["sql"])

        self.db.trace_v2(apsw.SQLITE_TRACE_PROFILE, tracecb)

        for _ in self.db.execute(query):
            time.sleep(0.1)

        self.assertEqual(1, len(results))
        x = results.pop()
        self.assertEqual(apsw.SQLITE_TRACE_PROFILE, x["code"])
        self.assertIs(self.db, x["connection"])
        self.assertEqual(query, x["sql"])
        self.assertGreater(x["nanoseconds"], 0)
        self.assertIn("stmt_status", x)
        for n in apsw.mapping_statement_status:
            if isinstance(n, str):
                self.assertGreaterEqual(x["stmt_status"][n], 0)

        db2 = apsw.Connection("")
        db2.trace_v2(apsw.SQLITE_TRACE_CLOSE, tracecb)
        for _ in db2.execute(query):
            pass

        db2.close()
        self.assertEqual(1, len(results))
        x = results.pop()
        self.assertEqual(apsw.SQLITE_TRACE_CLOSE, x["code"])
        self.assertIs(db2, x["connection"])

        def tracehook(x):
            1 / 0

        self.db.trace_v2(apsw.SQLITE_TRACE_STMT, tracehook)
        self.assertRaisesUnraisable(ZeroDivisionError, self.db.execute, query)
        self.assertEqual(0, len(results))

    def testURIFilenames(self):
        assertRaises = self.assertRaises
        assertEqual = self.assertEqual

        class TVFS(apsw.VFS):

            def __init__(self):
                apsw.VFS.__init__(self, "uritest", "")

            def xOpen(self, name, flags):
                assert isinstance(name, apsw.URIFilename)
                # The various errors
                assertRaises(TypeError, name.uri_parameter)
                assertRaises(TypeError, name.uri_parameter, 2)
                assertRaises(TypeError, name.uri_int)
                assertRaises(TypeError, name.uri_int, 7)
                assertRaises(TypeError, name.uri_int, 7, 7)
                assertRaises(TypeError, name.uri_int, 7, 7, 7)
                assertRaises(TypeError, name.uri_int, "seven", "seven")
                assertRaises(TypeError, name.uri_boolean, "seven")
                assertRaises(TypeError, name.uri_boolean, "seven", "seven")
                assertRaises(TypeError, name.uri_boolean, "seven", None)
                # Check values
                assert name.filename().endswith("testdb2")
                assertEqual(name.uri_parameter("notexist"), None)
                assertEqual(name.uri_parameter("foo"), "1&2=3")
                assertEqual(name.uri_int("foo", -7), -7)
                assertEqual(name.uri_int("bar", -7), 43242342)
                # https://sqlite.org/src/info/5f41597f7c
                # assertEqual(name.uri_boolean("foo", False), False)
                assertEqual(name.uri_boolean("bam", False), True)
                assertEqual(name.uri_boolean("baz", True), False)
                assertRaises(AttributeError, setattr, name, "parameters", 3)
                assertEqual(name.parameters, ('foo', 'bar', 'bam', 'baz'))
                1 / 0

        testvfs = TVFS()

        self.assertRaises(ZeroDivisionError,
                          apsw.Connection,
                          "file:testdb2?foo=1%262%3D3&bar=43242342&bam=true&baz=fal%73%65",
                          flags=apsw.SQLITE_OPEN_READWRITE | apsw.SQLITE_OPEN_CREATE | apsw.SQLITE_OPEN_URI,
                          vfs="uritest")

    def testVFSWithWAL(self):
        "Verify VFS using WAL"
        apsw.connection_hooks.append(
            lambda c: c.cursor().execute("pragma journal_mode=WAL; PRAGMA wal_autocheckpoint=1").fetchall())
        try:
            self.testVFS()
        finally:
            apsw.connection_hooks.pop()

    def testVFS(self):
        "Verify VFS functionality"
        global testtimeout

        testdb = vfstestdb

        # some coverage related stuff
        self.assertRaises(TypeError, apsw.VFSFile, "", 3, [0, 0])
        self.assertRaises(TypeError, apsw.VFS, "ignored", base=7)
        self.assertRaises(TypeError, apsw.VFS, 7)
        self.assertRaises(TypeError, apsw.VFS, "ignored", maxpathname="seven")
        self.assertRaises(TypeError, apsw.VFS, "ignored", iVersion="seven")
        self.assertRaises(ValueError, apsw.VFS, "ignored", iVersion=-2)
        self.assertRaises(TypeError, apsw.VFS, "ignored", exclude=2)
        self.assertNotIn("ignored", apsw.vfs_names())

        # Check basic functionality and inheritance - make an obfuscated provider

        # obfusvfs code
        def encryptme(data):
            # An "encryption" scheme in honour of MAPI and SQL server passwords
            if not data: return data
            return bytes([x ^ 0xa5 for x in data])

        class ObfuscatedVFSFile(apsw.VFSFile):

            def __init__(self, inheritfromvfsname, filename, flags):
                apsw.VFSFile.__init__(self, inheritfromvfsname, filename, flags)

            def xRead(self, amount, offset):
                return encryptme(super().xRead(amount, offset))

            def xWrite(self, data, offset):
                super().xWrite(encryptme(data), offset)

        class ObfuscatedVFS(apsw.VFS):

            def __init__(self, vfsname="obfu", basevfs=""):
                self.vfsname = vfsname
                self.basevfs = basevfs
                apsw.VFS.__init__(self, self.vfsname, self.basevfs, exclude=None)

            def xOpen(self, name, flags):
                return ObfuscatedVFSFile(self.basevfs, name, flags)

        vfs = ObfuscatedVFS()

        query = "create table foo(x,y); insert into foo values(1,2); insert into foo values(3,4)"
        self.db.cursor().execute(query)

        db2 = apsw.Connection(TESTFILEPREFIX + "testdb2", vfs=vfs.vfsname)
        db2.cursor().execute(query)
        db2.close()
        self.db.cursor().execute("pragma journal_mode=delete").fetchall()
        self.db.close()  # flush

        # check the two databases are the same (modulo the XOR)
        orig = read_whole_file(TESTFILEPREFIX + "testdb", "rb")
        obfu = read_whole_file(TESTFILEPREFIX + "testdb2", "rb")
        self.assertEqual(len(orig), len(obfu))
        self.assertNotEqual(orig, obfu)

        # we ignore wal/non-wal differences
        def compare(one, two):
            self.assertEqual(one[0:18], two[:18])
            self.assertEqual(one[96:], two[96:])

        compare(orig, encryptme(obfu))

        # versioning and excludes
        meth_names = {}
        for ver in (1, 2, 3):
            name = f"foo{ ver }"
            v = apsw.VFS(name, iVersion=ver)
            registered = [vfs for vfs in apsw.vfs_details() if vfs["zName"] == name][0]
            assert registered["iVersion"] == ver
            for k in registered:
                if k.startswith("x") and k not in meth_names:
                    meth_names[k] = ver

        for ver in (1, 2, 3):
            name = f"bar{ ver }"
            exclude = set(random.sample(list(meth_names.keys()), 3))
            v = apsw.VFS(name, iVersion=ver, exclude=exclude)
            registered = [vfs for vfs in apsw.vfs_details() if vfs["zName"] == name][0]
            assert registered["iVersion"] == ver
            for name in registered:
                if not name.startswith("x"):
                    continue
                assert meth_names[name] <= ver
                if name in exclude:
                    assert registered[name] == 0
                else:
                    assert registered[name] != 0

        # helper routines
        self.assertRaises(TypeError, apsw.exception_for, "three")
        self.assertRaises(ValueError, apsw.exception_for, 8764324)
        self.assertRaises(OverflowError, apsw.exception_for, 0xffffffffffffffff10)

        # test raw file object
        f = ObfuscatedVFSFile("", os.path.abspath(TESTFILEPREFIX + "testdb"),
                              [apsw.SQLITE_OPEN_MAIN_DB | apsw.SQLITE_OPEN_READONLY, 0])
        del f  # check closes
        f = ObfuscatedVFSFile("", os.path.abspath(TESTFILEPREFIX + "testdb"),
                              [apsw.SQLITE_OPEN_MAIN_DB | apsw.SQLITE_OPEN_READONLY, 0])
        data = f.xRead(len(obfu), 0)  # will encrypt it
        compare(obfu, data)
        f.xClose()
        f.xClose()
        f2 = apsw.VFSFile("", os.path.abspath(TESTFILEPREFIX + "testdb"),
                          [apsw.SQLITE_OPEN_MAIN_DB | apsw.SQLITE_OPEN_READONLY, 0])
        del f2
        f2 = apsw.VFSFile("", os.path.abspath(TESTFILEPREFIX + "testdb2"),
                          [apsw.SQLITE_OPEN_MAIN_DB | apsw.SQLITE_OPEN_READONLY, 0])
        data = f2.xRead(len(obfu), 0)
        self.assertEqual(obfu, data)
        f2.xClose()
        f2.xClose()

        # cleanup so it doesn't interfere with following code using the same file
        del f
        del f2
        db2.close()
        del db2
        vfs.unregister()
        gc.collect()

        ### Detailed vfs testing

        self.db = None
        gc.collect()

        defvfs = apsw.vfs_names()[0]  # we want to inherit from this one

        def testrand():
            gc.collect()
            apsw.randomness(0)
            vfs = RandomVFS()
            db = apsw.Connection(TESTFILEPREFIX + "testdb")
            curnext(db.cursor().execute("select randomblob(10)"))

        class RandomVFSUpper(apsw.VFS):

            def __init__(self):
                apsw.VFS.__init__(self, "randomupper", defvfs)

            def xRandomness1(self, n):
                return b"\xaa\xbb"

        class RandomVFS(apsw.VFS):

            def __init__(self):
                apsw.VFS.__init__(self, "random", "randomupper", makedefault=True)

            def xRandomness1(self, bad, number, of, arguments):
                1 / 0

            def xRandomness2(self, n):
                1 / 0

            def xRandomness3(self, n):
                return b"abcd"

            def xRandomness4(self, n):
                return "abcd"

            def xRandomness5(self, n):
                return b"a" * (2 * n)

            def xRandomness6(self, n):
                return None

            def xRandomness7(self, n):
                return 3

            def xRandomness99(self, n):
                return super().xRandomness(n + 2049)

        vfsupper = RandomVFSUpper()
        vfs = RandomVFS()
        self.assertRaises(TypeError, vfs.xRandomness, "jksdhfsd")
        self.assertRaises(TypeError, vfs.xRandomness, 3, 3)
        self.assertRaises(ValueError, vfs.xRandomness, -88)

        RandomVFS.xRandomness = RandomVFS.xRandomness1
        self.assertRaises(TypeError, testrand)
        RandomVFS.xRandomness = RandomVFS.xRandomness2
        self.assertRaises(ZeroDivisionError, testrand)
        RandomVFS.xRandomness = RandomVFS.xRandomness3
        testrand()  # shouldn't have problems
        RandomVFS.xRandomness = RandomVFS.xRandomness4
        self.assertRaises(TypeError, testrand)
        RandomVFS.xRandomness = RandomVFS.xRandomness5
        testrand()  # shouldn't have problems
        RandomVFS.xRandomness = RandomVFS.xRandomness6
        testrand()  # shouldn't have problems
        RandomVFS.xRandomness = RandomVFS.xRandomness7
        self.assertRaises(TypeError, testrand)
        RandomVFS.xRandomness = RandomVFS.xRandomness99
        testrand()  # shouldn't have problems
        vfsupper.xRandomness = vfsupper.xRandomness1
        testrand()  # coverage
        vfsupper.unregister()
        vfs.unregister()

        class ErrorVFS(apsw.VFS):
            # A vfs that returns errors for all methods
            def __init__(self):
                apsw.VFS.__init__(self, "errorvfs", "")

            def errorme(self, *args):
                raise apsw.exception_for(apsw.SQLITE_IOERR)

        class TestVFS(apsw.VFS):

            def init1(self):
                super().__init__("apswtest")

            def init99(self, name="apswtest", base="", **kwargs):
                super().__init__(name, base, **kwargs)

            def xDelete1(self, name, syncdir):
                super().xDelete(".", False)

            def xDelete2(self, bad, number, of, args):
                1 / 0

            def xDelete3(self, name, syncdir):
                1 / 0

            def xDelete4(self, name, syncdir):
                super().xDelete("bad", "arguments")

            def xDelete99(self, name, syncdir):
                assert isinstance(name, str)
                assert isinstance(syncdir, bool)
                return super().xDelete(name, syncdir)

            def xAccess1(self, bad, number, of, args):
                1 / 0

            def xAccess2(self, name, flags):
                1 / 0

            def xAccess3(self, name, flags):
                return super().xAccess("bad", "arguments")

            def xAccess4(self, name, flags):
                return (3, )

            def xAccess99(self, name, flags):
                assert (type(name) == type(""))
                assert (type(flags) == type(1))
                return super().xAccess(name, flags)

            def xFullPathname1(self, bad, number, of, args):
                1 / 0

            def xFullPathname2(self, name):
                1 / 0

            def xFullPathname3(self, name):
                return super().xFullPathname("bad", "args")

            def xFullPathname4(self, name):
                # parameter is larger than default buffer sizes used by sqlite
                return super().xFullPathname(name * 10000)

            def xFullPathname5(self, name):
                # result is larger than default buffer sizes used by sqlite
                return "a" * 10000

            def xFullPathname6(self, name):
                return 12  # bad return type

            def xFullPathname99(self, name):
                assert (type(name) == type(""))
                return super().xFullPathname(name)

            def xOpen1(self, bad, number, of, arguments):
                1 / 0

            def xOpen2(self, name, flags):
                super().xOpen(name, 3)
                1 / 0

            def xOpen3(self, name, flags):
                v = super().xOpen(name, flags)
                flags.append(v)
                return v

            def xOpen4(self, name, flags):
                return None

            def xOpen99(self, name, flags):
                assert (isinstance(name, apsw.URIFilename) or name is None or type(name) == type(""))
                assert (type(flags) == type([]))
                assert (len(flags) == 2)
                assert (type(flags[0]) in (int, ))
                assert (type(flags[1]) in (int, ))
                return super().xOpen(name, flags)

            def xOpen100(self, name, flags):
                return TestFile(name, flags)

            def xDlOpen1(self, bad, number, of, arguments):
                1 / 0

            def xDlOpen2(self, name):
                1 / 0

            def xDlOpen3(self, name):
                return -1

            def xDlOpen4(self, name):
                return "fred"

            def xDlOpen5(self, name):
                return super().xDlOpen(3)

            # python 3 only test
            def xDlOpen6(self, name):
                return super().xDlOpen(b"abcd")  # bad string type

            def xDlOpen7(self, name):
                return 0xffffffffffffffff10

            def xDlOpen99(self, name):
                assert (type(name) == type(""))
                res = super().xDlOpen(name)
                if ctypes:
                    try:
                        cres = ctypes.cdll.LoadLibrary(name)._handle
                    except:
                        cres = 0
                    assert (res == cres)
                return res

            def xDlSym1(self, bad, number, of, arguments):
                1 / 0

            def xDlSym2(self, handle, name):
                1 / 0

            def xDlSym3(self, handle, name):
                return "fred"

            def xDlSym4(self, handle, name):
                super().xDlSym(3, 3)

            def xDlSym5(self, handle, name):
                return super().xDlSym(handle, b"abcd")

            def xDlSym6(self, handle, name):
                return 0xffffffffffffffff10

            def xDlSym99(self, handle, name):
                assert (type(handle) in (int, ))
                assert (type(name) == type(""))
                res = super().xDlSym(handle, name)
                # pypy doesn't have dlsym
                if not iswindows and hasattr(_ctypes, "dlsym"):
                    assert (_ctypes.dlsym(handle, name) == res)
                # windows has funky issues I don't want to deal with here
                return res

            def xDlClose1(self, bad, number, of, arguments):
                1 / 0

            def xDlClose2(self, handle):
                1 / 0

            def xDlClose3(self, handle):
                return super().xDlClose("three")

            def xDlClose99(self, handle):
                assert (type(handle) in (int, ))
                super().xDlClose(handle)

            def xDlError1(self, bad, number, of, arguments):
                1 / 0

            def xDlError2(self):
                1 / 0

            def xDlError3(self):
                return super().xDlError("three")

            def xDlError4(self):
                return 3

            def xDlError5(self):
                return b"abcd"

            def xDlError6(self):
                return None

            def xDlError99(self):
                return super().xDlError()

            def xSleep1(self, bad, number, of, arguments):
                1 / 0

            def xSleep2(self, microseconds):
                1 / 0

            def xSleep3(self, microseconds):
                return super().xSleep("three")

            def xSleep4(self, microseconds):
                return "three"

            def xSleep5(self, microseconds):
                return 0xffffffff0

            def xSleep6(self, microseconds):
                return 0xffffffffeeeeeeee0

            def xSleep99(self, microseconds):
                assert (type(microseconds) in (int, ))
                return super().xSleep(microseconds)

            def xCurrentTime1(self, bad, args):
                1 / 0

            def xCurrentTime2(self):
                1 / 0

            def xCurrentTime3(self):
                return super().xCurrentTime("three")

            def xCurrentTime4(self):
                return "three"

            def xCurrentTime5(self):
                return math.exp(math.pi) * 26000

            def xCurrentTimeCorrect(self):
                # actual correct implementation https://stackoverflow.com/questions/466321/convert-unix-timestamp-to-julian
                return time.time() / 86400.0 + 2440587.5

            def xCurrentTime99(self):
                return super().xCurrentTime()

            def xCurrentTimeInt641(self, bad, args):
                1 / 0

            def xCurrentTimeInt642(self):
                1 / 0

            def xCurrentTimeInt643(self):
                return super().xCurrentTimeInt64("three")

            def xCurrentTimeInt644(self):
                return "three"

            def xCurrentTimeInt645(self):
                return 2**66

            def xCurrentTimeCorrect(self):
                # actual correct implementation https://stackoverflow.com/questions/466321/convert-unix-timestamp-to-julian
                return time.time() / 86400.0 + 2440587.5

            def xCurrentTime99(self):
                return super().xCurrentTime()

            def xCurrentTimeInt6499(self):
                return super().xCurrentTimeInt64()

            def xGetLastError1(self, bad, args):
                1 / 0

            def xGetLastError2(self):
                1 / 0

            def xGetLastError3(self):
                return super().xGetLastError("three")

            def xGetLastError4(self):
                return 3

            def xGetLastError5(self):
                return -17, "a" * 1500

            def xGetLastError6(self):
                return -0x7fffffff - 200, None

            def xGetLastError7(self):

                class te(tuple):

                    def __getitem__(self, n):
                        if n == 0:
                            return 23
                        1 / 0

                return te((1, 2))

            def xGetLastError8(self):
                return 0, None

            def xGetLastError9(self):
                return 0, "Some sort of message"

            def xGetLastError10(self):
                return "banana", "Some sort of message"

            def xGetLastError99(self):
                return super().xGetLastError()

            def xNextSystemCall1(self, bad, args):
                1 / 0

            def xNextSystemCall2(self, name):
                return 3

            def xNextSystemCall3(self, name):
                return "foo\xf3"

            def xNextSystemCall4(self, name):
                1 / 0

            def xNextSystemCall99(self, name):
                return super().xNextSystemCall(name)

            def xGetSystemCall1(self, bad, args):
                1 / 0

            def xGetSystemCall2(self, name):
                1 / 0

            def xGetSystemCall3(self, name):
                return "fred"

            def xGetSystemCall4(self, name):
                return 3.7

            def xGetSystemCall99(self, name):
                return super().xGetSystemCall(name)

            def xSetSystemCall1(self, bad, args, args3):
                1 / 0

            def xSetSystemCall2(self, name, ptr):
                1 / 0

            def xSetSystemCall3(self, name, ptr):
                raise apsw.NotFoundError()

            def xSetSystemCall99(self, name, ptr):
                return super().xSetSystemCall(name, ptr)

        class TestFile(apsw.VFSFile):

            def init1(self, name, flags):
                super().__init__("bogus", "arguments")

            def init2(self, name, flags):
                super().__init__("bogus", 3, 4)

            def init3(self, name, flags):
                super().__init__("bogus", "4", 4)

            def init4(self, name, flags):
                super().__init__("bogus", "4", [4, 4, 4, 4])

            def init5(self, name, flags):
                super().__init__("", name, [0xffffffffeeeeeeee0, 0xffffffffeeeeeeee0])

            def init6(self, name, flags):
                super().__init__("", name, [0xffffffffa, 0])  # 64 bit int vs long overflow

            def init7(self, name, flags):
                super().__init__("", name, (6, 7))

            def init8(self, name, flags):
                super().__init__("bogus", name, flags)

            def init9(self, name, flags):
                super().__init__("", name, (6, "six"))

            def init99(self, name, flags):
                super().__init__("", name, flags)

            def xRead1(self, bad, number, of, arguments):
                1 / 0

            def xRead2(self, amount, offset):
                1 / 0

            def xRead3(self, amount, offset):
                return 3

            def xRead4(self, amount, offset):
                return "a" * amount

            def xRead5(self, amount, offset):
                return super().xRead(amount - 1, offset)

            def xRead99(self, amount, offset):
                return super().xRead(amount, offset)

            def xWrite1(self, bad, number, of, arguments):
                1 / 0

            def xWrite2(self, buffy, offset):
                1 / 0

            def xWrite99(self, buffy, offset):
                return super().xWrite(buffy, offset)

            def xUnlock1(self, bad, number, of, arguments):
                1 / 0

            def xUnlock2(self, level):
                1 / 0

            def xUnlock99(self, level):
                return super().xUnlock(level)

            def xLock1(self, bad, number, of, arguments):
                1 / 0

            def xLock2(self, level):
                1 / 0

            def xLock99(self, level):
                return super().xLock(level)

            def xTruncate1(self, bad, number, of, arguments):
                1 / 0

            def xTruncate2(self, size):
                1 / 0

            def xTruncate99(self, size):
                return super().xTruncate(size)

            def xSync1(self, bad, number, of, arguments):
                1 / 0

            def xSync2(self, flags):
                1 / 0

            def xSync99(self, flags):
                return super().xSync(flags)

            def xSectorSize1(self, bad, number, of, args):
                1 / 0

            def xSectorSize2(self):
                1 / 0

            def xSectorSize3(self):
                return "three"

            def xSectorSize4(self):
                return 0xffffffffeeeeeeee0

            def xSectorSize99(self):
                return super().xSectorSize()

            def xDeviceCharacteristics1(self, bad, number, of, args):
                1 / 0

            def xDeviceCharacteristics2(self):
                1 / 0

            def xDeviceCharacteristics3(self):
                return "three"

            def xDeviceCharacteristics4(self):
                return 0xffffffffeeeeeeee0

            def xDeviceCharacteristics99(self):
                return super().xDeviceCharacteristics()

            def xFileSize1(self, bad, number, of, args):
                1 / 0

            def xFileSize2(self):
                1 / 0

            def xFileSize3(self):
                return "three"

            def xFileSize4(self):
                return 0xffffffffeeeeeeee0

            def xFileSize99(self):
                res = super().xFileSize()
                if res < 100000:
                    return int(res)
                return res

            def xCheckReservedLock1(self, bad, number, of, args):
                1 / 0

            def xCheckReservedLock2(self):
                1 / 0

            def xCheckReservedLock3(self):
                return "three"

            def xCheckReservedLock4(self):
                return 0xffffffffeeeeeeee0

            def xCheckReservedLock99(self):
                return super().xCheckReservedLock()

            def xFileControl1(self, bad, number, of, args):
                1 / 0

            def xFileControl2(self, op, ptr):
                1 / 0

            def xFileControl3(self, op, ptr):
                return "banana"

            def xFileControl99(self, op, ptr):
                if op == 1027:
                    assert (ptr == 1027)
                elif op == 1028:
                    if ctypes:
                        assert (True is ctypes.py_object.from_address(ptr).value)
                else:
                    return super().xFileControl(op, ptr)
                return True

        TestVFS.xCurrentTime = TestVFS.xCurrentTimeCorrect

        # check initialization
        self.assertRaises(TypeError, apsw.VFS, "3", 3)
        self.assertRaises(ValueError, apsw.VFS, "never", "klgfkljdfsljgklfjdsglkdfs")
        self.assertTrue("never" not in apsw.vfs_names())
        TestVFS.__init__ = TestVFS.init1
        vfs = TestVFS()
        self.assertRaises(apsw.VFSNotImplementedError, testdb)
        del vfs
        gc.collect()
        TestVFS.__init__ = TestVFS.init99
        vfs = TestVFS()
        vfs_notime64_base = TestVFS(name="apswtest_notime64_base", exclude={"xCurrentTimeInt64"})
        vfs_notime64_base.xCurrentTime = lambda *args: 3.1415
        vfs_notime64 = TestVFS(name="apswtest_notime64", base="apswtest_notime64_base", exclude={"xCurrentTimeInt64"})
        self.assertIn("apswtest", apsw.vfs_names())
        self.assertIn("apswtest_notime64", apsw.vfs_names())
        # Should work without any overridden methods
        testdb()

        ## xDelete
        self.assertRaises(TypeError, vfs.xDelete, "bogus", "arguments")
        TestVFS.xDelete = TestVFS.xDelete1
        self.assertRaises(apsw.IOError, testdb)
        TestVFS.xDelete = TestVFS.xDelete2
        self.assertRaises(TypeError, testdb)
        TestVFS.xDelete = TestVFS.xDelete3
        self.assertRaises(ZeroDivisionError, testdb)
        TestVFS.xDelete = TestVFS.xDelete4
        self.assertRaises(TypeError, testdb)
        TestVFS.xDelete = TestVFS.xDelete99
        testdb()

        ## xAccess
        self.assertRaises(TypeError, vfs.xAccess, "bogus", "arguments")
        TestVFS.xAccess = TestVFS.xAccess1
        self.assertRaises(TypeError, testdb)
        TestVFS.xAccess = TestVFS.xAccess2
        self.assertRaises(ZeroDivisionError, testdb)
        TestVFS.xAccess = TestVFS.xAccess3
        self.assertRaises(TypeError, testdb)
        TestVFS.xAccess = TestVFS.xAccess4
        self.assertRaises(TypeError, testdb)
        TestVFS.xAccess = TestVFS.xAccess99
        if iswindows:
            self.assertRaises(apsw.IOError, vfs.xAccess, "<bad<filename:", apsw.SQLITE_ACCESS_READWRITE)
        else:
            self.assertEqual(False, vfs.xAccess("<bad<filename:", apsw.SQLITE_ACCESS_READWRITE))
            self.assertEqual(True, vfs.xAccess(".", apsw.SQLITE_ACCESS_EXISTS))
        # unix vfs doesn't ever return error so we have to indirect through one of ours
        errvfs = ErrorVFS()
        errvfs.xAccess = errvfs.errorme
        vfs2 = TestVFS("apswtest2", "errorvfs")
        self.assertRaises(apsw.IOError, testdb, vfsname="apswtest2")
        del vfs2
        del errvfs
        gc.collect()

        ## xFullPathname
        self.assertRaises(TypeError, vfs.xFullPathname, "bogus", "arguments")
        self.assertRaises(TypeError, vfs.xFullPathname, 3)
        TestVFS.xFullPathname = TestVFS.xFullPathname1
        self.assertRaises(TypeError, testdb)
        TestVFS.xFullPathname = TestVFS.xFullPathname2
        self.assertRaises(ZeroDivisionError, testdb)
        TestVFS.xFullPathname = TestVFS.xFullPathname3
        self.assertRaises(TypeError, testdb)
        TestVFS.xFullPathname = TestVFS.xFullPathname4
        if not iswindows:
            # ::TODO:: check this on windows
            # SQLite doesn't give an error even though the vfs is silently truncating
            # the full pathname.  See SQLite ticket 3373
            self.assertRaises(apsw.CantOpenError, testdb)  # we get cantopen on the truncated fullname
        TestVFS.xFullPathname = TestVFS.xFullPathname5
        self.assertRaises(apsw.TooBigError, testdb)
        TestVFS.xFullPathname = TestVFS.xFullPathname6
        self.assertRaises(TypeError, testdb)
        TestVFS.xFullPathname = TestVFS.xFullPathname99
        testdb()

        ## xOpen
        self.assertRaises(TypeError, vfs.xOpen, 3)
        self.assertRaises(TypeError, vfs.xOpen, 3, 3)
        self.assertRaises(TypeError, vfs.xOpen, None, (1, 2))
        self.assertRaises(TypeError, vfs.xOpen, None, [1, 2, 3])
        self.assertRaises(TypeError, vfs.xOpen, None, ["1", 2])
        self.assertRaises(TypeError, vfs.xOpen, None, [1, "2"])
        self.assertRaises(OverflowError, vfs.xOpen, None, [0xffffffffeeeeeeee0, 2])
        self.assertRaises(OverflowError, vfs.xOpen, None, [0xffffffff0, 2])
        self.assertRaises(OverflowError, vfs.xOpen, None, [1, 0xffffffff0])
        self.assertRaises(
            apsw.CantOpenError, testdb,
            filename="notadir/notexist/nochance")  # can't open due to intermediate directories not existing
        TestVFS.xOpen = TestVFS.xOpen1
        self.assertRaises(TypeError, testdb)
        TestVFS.xOpen = TestVFS.xOpen2
        self.assertRaises(TypeError, testdb)
        TestVFS.xOpen = TestVFS.xOpen3
        self.assertRaises(TypeError, testdb)
        TestVFS.xOpen = TestVFS.xOpen4
        self.assertRaises(AttributeError, testdb)
        TestVFS.xOpen = TestVFS.xOpen99
        testdb()

        if hasattr(apsw.Connection(":memory:"), "enableloadextension") and os.path.exists(LOADEXTENSIONFILENAME):
            ## xDlOpen
            self.assertRaises(TypeError, vfs.xDlOpen, 3)
            self.assertRaises(TypeError, vfs.xDlOpen, b"\xfb\xfc\xfd\xfe\xff\xff\xff\xff")
            TestVFS.xDlOpen = TestVFS.xDlOpen1
            self.assertRaises(TypeError, testdb)
            TestVFS.xDlOpen = TestVFS.xDlOpen2
            self.assertRaises(ZeroDivisionError, testdb)
            TestVFS.xDlOpen = TestVFS.xDlOpen3
            self.assertRaises(TypeError, testdb)
            TestVFS.xDlOpen = TestVFS.xDlOpen4
            self.assertRaises(TypeError, testdb)
            TestVFS.xDlOpen = TestVFS.xDlOpen5
            self.assertRaises(TypeError, testdb)
            TestVFS.xDlOpen = TestVFS.xDlOpen6
            self.assertRaises(TypeError, testdb)
            TestVFS.xDlOpen = TestVFS.xDlOpen7
            self.assertRaises(OverflowError, testdb)
            TestVFS.xDlOpen = TestVFS.xDlOpen99
            testdb()

            ## xDlSym
            self.assertRaises(TypeError, vfs.xDlSym, 3)
            self.assertRaises(TypeError, vfs.xDlSym, 3, 3)
            self.assertRaises(TypeError, vfs.xDlSym, "three", "three")
            TestVFS.xDlSym = TestVFS.xDlSym1
            self.assertRaises(TypeError, testdb)
            TestVFS.xDlSym = TestVFS.xDlSym2
            self.assertRaises(ZeroDivisionError, testdb)
            TestVFS.xDlSym = TestVFS.xDlSym3
            self.assertRaises(TypeError, testdb)
            TestVFS.xDlSym = TestVFS.xDlSym4
            self.assertRaises(TypeError, testdb)
            TestVFS.xDlSym = TestVFS.xDlSym5
            self.assertRaises(TypeError, testdb)
            TestVFS.xDlSym = TestVFS.xDlSym6
            self.assertRaises(OverflowError, testdb)
            TestVFS.xDlSym = TestVFS.xDlSym99
            testdb()

            ## xDlClose
            self.assertRaises(TypeError, vfs.xDlClose, "three")
            self.assertRaises(OverflowError, vfs.xDlClose, 0xffffffffffffffff10)
            TestVFS.xDlClose = TestVFS.xDlClose1
            self.assertRaises(TypeError, testdb)
            TestVFS.xDlClose = TestVFS.xDlClose2
            self.assertRaises(ZeroDivisionError, testdb)
            TestVFS.xDlClose = TestVFS.xDlClose3
            self.assertRaises(TypeError, testdb)
            TestVFS.xDlClose = TestVFS.xDlClose99
            testdb()

            ## xDlError
            self.assertRaises(TypeError, vfs.xDlError, "three")
            TestVFS.xDlError = TestVFS.xDlError1
            self.assertRaises(TypeError, testdb)
            TestVFS.xDlError = TestVFS.xDlError2
            self.assertRaises(ZeroDivisionError, testdb)
            TestVFS.xDlError = TestVFS.xDlError3
            self.assertRaises(TypeError, testdb)
            TestVFS.xDlError = TestVFS.xDlError4
            self.assertRaises(TypeError, testdb)
            TestVFS.xDlError = TestVFS.xDlError5
            self.assertRaises(TypeError, testdb)
            TestVFS.xDlError = TestVFS.xDlError6  # should not error
            testdb()
            TestVFS.xDlError = TestVFS.xDlError99
            testdb()

        ## xSleep
        testtimeout = True
        self.assertRaises(TypeError, vfs.xSleep, "three")
        self.assertRaises(TypeError, vfs.xSleep, 3, 3)
        TestVFS.xSleep = TestVFS.xSleep1
        self.assertRaises(TypeError, testdb, mode="delete")
        TestVFS.xSleep = TestVFS.xSleep2
        self.assertRaises(ZeroDivisionError, testdb, mode="delete")
        TestVFS.xSleep = TestVFS.xSleep3
        self.assertRaises(TypeError, testdb, mode="delete")
        TestVFS.xSleep = TestVFS.xSleep4
        self.assertRaises(TypeError, testdb, mode="delete")
        TestVFS.xSleep = TestVFS.xSleep5
        self.assertRaises(OverflowError, testdb, mode="delete")
        TestVFS.xSleep = TestVFS.xSleep6
        self.assertRaises(OverflowError, testdb, mode="delete")
        TestVFS.xSleep = TestVFS.xSleep99
        testdb(mode="delete")
        testtimeout = False

        ## xCurrentTime / xCurrentTimeInt64
        self.assertRaises(TypeError, vfs.xCurrentTime, "three")
        self.assertRaises(TypeError, vfs.xCurrentTimeInt64, "three")
        for vname in "apswtest", "apswtest_notime64":
            TestVFS.xCurrentTime = TestVFS.xCurrentTime1
            TestVFS.xCurrentTimeInt64 = TestVFS.xCurrentTimeInt641
            self.assertRaises(TypeError, testdb, vfsname=vname)
            TestVFS.xCurrentTime = TestVFS.xCurrentTime2
            TestVFS.xCurrentTimeInt64 = TestVFS.xCurrentTimeInt642
            self.assertRaises(ZeroDivisionError, testdb, vfsname=vname)
            TestVFS.xCurrentTime = TestVFS.xCurrentTime3
            TestVFS.xCurrentTimeInt64 = TestVFS.xCurrentTimeInt643
            self.assertRaises(TypeError, testdb, vfsname=vname)
            TestVFS.xCurrentTime = TestVFS.xCurrentTime4
            TestVFS.xCurrentTimeInt64 = TestVFS.xCurrentTimeInt644
            self.assertRaises(TypeError, testdb, vfsname=vname)
            TestVFS.xCurrentTime = TestVFS.xCurrentTime5
            TestVFS.xCurrentTimeInt64 = TestVFS.xCurrentTimeInt645
            self.assertRaisesRoot(OverflowError, testdb, vfsname=vname)
            TestVFS.xCurrentTime = TestVFS.xCurrentTime99
            TestVFS.xCurrentTimeInt64 = TestVFS.xCurrentTimeInt6499
            testdb(vfsname=vname)
            TestVFS.xCurrentTime = TestVFS.xCurrentTimeCorrect
            TestVFS.xCurrentTimeInt64 = TestVFS.xCurrentTimeInt6499
            testdb(vfsname=vname)

        ## xGetLastError
        # We can't directly test because the methods are called as side effects
        # of other errors.  However coverage shows we are exercising the code.
        def provoke_error():
            self.assertRaisesRoot(apsw.CantOpenError, testdb, attachdb='.')

        for n in range(1, 11):
            TestVFS.xGetLastError = getattr(TestVFS, "xGetLastError" + str(n))
            provoke_error()

        TestVFS.xGetLastError = TestVFS.xGetLastError99
        provoke_error()

        ## System call stuff
        if "unix" in apsw.vfs_names():

            class VFS2(apsw.VFS):

                def __init__(self):
                    apsw.VFS.__init__(self, "apswtest2", "apswtest")

            vfs2 = VFS2()

            ## xNextSystemCall
            self.assertRaises(TypeError, vfs.xNextSystemCall, 0)
            items = [None]
            while True:
                n = vfs.xNextSystemCall(items[-1])
                if n is None:
                    break
                items.append(n)
            items = items[1:]
            self.assertNotEqual(0, len(items))
            self.assertTrue("open" in items)

            TestVFS.xNextSystemCall = TestVFS.xNextSystemCall1
            self.assertRaisesRoot(TypeError, vfs2.xNextSystemCall, "open")
            TestVFS.xNextSystemCall = TestVFS.xNextSystemCall2
            self.assertRaisesRoot(TypeError, vfs2.xNextSystemCall, "open")
            TestVFS.xNextSystemCall = TestVFS.xNextSystemCall4
            self.assertEqual(None, self.assertRaisesRoot(ZeroDivisionError, vfs2.xNextSystemCall, "open"))
            TestVFS.xNextSystemCall = TestVFS.xNextSystemCall99
            vfs2.xNextSystemCall("open")

            ## xGetSystemCall
            self.assertRaises(TypeError, vfs.xGetSystemCall)
            self.assertRaises(TypeError, vfs.xGetSystemCall, 3)
            self.assertEqual(None, vfs.xGetSystemCall("a name that won't exist"))
            self.assertTrue(isinstance(vfs.xGetSystemCall("open"), (int, )))

            TestVFS.xGetSystemCall = TestVFS.xGetSystemCall1
            self.assertRaises(TypeError, vfs2.xGetSystemCall, "open")
            TestVFS.xGetSystemCall = TestVFS.xGetSystemCall2
            self.assertRaises(ZeroDivisionError, vfs2.xGetSystemCall, "open")
            TestVFS.xGetSystemCall = TestVFS.xGetSystemCall3
            self.assertRaises(TypeError, vfs2.xGetSystemCall, "open")
            TestVFS.xGetSystemCall = TestVFS.xGetSystemCall4
            self.assertRaises(TypeError, vfs2.xGetSystemCall, "open")
            TestVFS.xGetSystemCall = TestVFS.xGetSystemCall99
            self.assertTrue(vfs2.xGetSystemCall("open") > 0)

            ## xSetSystemCall
            fallback = apsw.VFS("fallback", base="")  # undo any damage we do
            try:
                self.assertRaises(TypeError, vfs.xSetSystemCall)
                self.assertRaises(TypeError, vfs.xSetSystemCall, 3, 4)
                self.assertRaises((TypeError, ValueError), vfs.xSetSystemCall, "a\0b", 4)
                self.assertRaises(TypeError, vfs.xSetSystemCall, "none", 3.7)
                realopen = vfs.xGetSystemCall("open")
                self.assertEqual(False, vfs.xSetSystemCall("doesn't exist", 0))
                self.assertEqual(True, vfs.xSetSystemCall("open", realopen + 1))
                self.assertEqual(realopen + 1, vfs.xGetSystemCall("open"))
                self.assertEqual(True, vfs.xSetSystemCall("open", realopen))
                TestVFS.xSetSystemCall = TestVFS.xSetSystemCall1
                self.assertRaises(TypeError, vfs2.xSetSystemCall, "open", realopen)
                TestVFS.xSetSystemCall = TestVFS.xSetSystemCall2
                self.assertRaises(ZeroDivisionError, vfs2.xSetSystemCall, "open", realopen)
                TestVFS.xSetSystemCall = TestVFS.xSetSystemCall3
                self.assertEqual(False, vfs2.xSetSystemCall("doesn't exist", 0))
                TestVFS.xSetSystemCall = TestVFS.xSetSystemCall99
                self.assertEqual(True, vfs2.xSetSystemCall("open", realopen))
            finally:
                # undocumented - this resets all calls to their defaults
                fallback.xSetSystemCall(None, 0)
                fallback.unregister()

        ##
        ## VFS file testing
        ##

        ## init
        TestVFS.xOpen = TestVFS.xOpen100

        TestFile.__init__ = TestFile.init1
        self.assertRaises(TypeError, testdb)
        TestFile.__init__ = TestFile.init2
        self.assertRaises(TypeError, testdb)
        TestFile.__init__ = TestFile.init3
        self.assertRaises(TypeError, testdb)
        TestFile.__init__ = TestFile.init4
        self.assertRaises(TypeError, testdb)
        TestFile.__init__ = TestFile.init5
        self.assertRaises(OverflowError, testdb)
        TestFile.__init__ = TestFile.init6
        self.assertRaises(OverflowError, testdb)
        TestFile.__init__ = TestFile.init7
        self.assertRaises(TypeError, testdb)
        TestFile.__init__ = TestFile.init8
        self.assertRaises(ValueError, testdb)
        TestFile.__init__ = TestFile.init9
        self.assertRaises(TypeError, testdb)
        TestFile.__init__ = TestFile.init99
        testdb()  # should work just fine

        # cause an open failure
        self.assertRaises(apsw.CantOpenError, TestFile, ".",
                          [apsw.SQLITE_OPEN_MAIN_DB | apsw.SQLITE_OPEN_CREATE | apsw.SQLITE_OPEN_READWRITE, 0])

        ## xRead
        t = TestFile(os.path.abspath(TESTFILEPREFIX + "testfile"),
                     [apsw.SQLITE_OPEN_MAIN_DB | apsw.SQLITE_OPEN_CREATE | apsw.SQLITE_OPEN_READWRITE, 0])
        self.assertRaises(TypeError, t.xRead, "three", "four")
        self.assertRaises(OverflowError, t.xRead, 0xffffffffeeeeeeee0, 1)
        self.assertRaises(OverflowError, t.xRead, 1, 0xffffffffeeeeeeee0)
        TestFile.xRead = TestFile.xRead1
        self.assertRaises(TypeError, testdb)
        TestFile.xRead = TestFile.xRead2
        self.assertRaises(ZeroDivisionError, testdb)
        TestFile.xRead = TestFile.xRead3
        self.assertRaises(TypeError, testdb)
        TestFile.xRead = TestFile.xRead4
        self.assertRaises(TypeError, testdb)
        TestFile.xRead = TestFile.xRead5
        self.assertRaises(apsw.IOError, testdb)
        TestFile.xRead = TestFile.xRead99
        testdb()

        ## xWrite
        self.assertRaises(TypeError, t.xWrite, "three", "four")
        self.assertRaises(OverflowError, t.xWrite, b"three", 0xffffffffeeeeeeee0)
        self.assertRaises(TypeError, t.xWrite, "foo", 0)
        TestFile.xWrite = TestFile.xWrite1
        self.assertRaises(TypeError, testdb)
        TestFile.xWrite = TestFile.xWrite2
        self.assertRaises(ZeroDivisionError, testdb)
        TestFile.xWrite = TestFile.xWrite99
        testdb()

        ## xUnlock
        self.assertRaises(TypeError, t.xUnlock, "three")
        self.assertRaises(OverflowError, t.xUnlock, 0xffffffffeeeeeeee0)
        # doesn't care about nonsensical levels - assert fails in debug build
        # t.xUnlock(-1)
        if not apsw.connection_hooks:
            TestFile.xUnlock = TestFile.xUnlock1
            self.assertRaises(TypeError, testdb)
            TestFile.xUnlock = TestFile.xUnlock2
            self.assertRaises(ZeroDivisionError, testdb)
        TestFile.xUnlock = TestFile.xUnlock99
        testdb()

        ## xLock
        self.assertRaises(TypeError, t.xLock, "three")
        self.assertRaises(OverflowError, t.xLock, 0xffffffffeeeeeeee0)
        # doesn't care about nonsensical levels - assert fails in debug build
        # t.xLock(0xffffff)
        TestFile.xLock = TestFile.xLock1
        self.assertRaises(TypeError, testdb)
        TestFile.xLock = TestFile.xLock2
        self.assertRaises(ZeroDivisionError, testdb)
        TestFile.xLock = TestFile.xLock99
        testdb()

        ## xTruncate
        self.assertRaises(TypeError, t.xTruncate, "three")
        self.assertRaises(OverflowError, t.xTruncate, 0xffffffffeeeeeeee0)
        if not iswindows:
            # windows is happy to truncate to -77 bytes
            self.assertRaises(apsw.IOError, t.xTruncate, -77)
        TestFile.xTruncate = TestFile.xTruncate1
        self.assertRaises(TypeError, testdb)
        TestFile.xTruncate = TestFile.xTruncate2
        self.assertRaises(ZeroDivisionError, testdb)
        TestFile.xTruncate = TestFile.xTruncate99
        testdb()

        ## xSync
        saved = apsw.connection_hooks
        apsw.connection_hooks = []
        try:
            self.assertRaises(TypeError, t.xSync, "three")
            self.assertRaises(OverflowError, t.xSync, 0xffffffffeeeeeeee0)
            TestFile.xSync = TestFile.xSync1
            self.assertRaises(TypeError, testdb)
            TestFile.xSync = TestFile.xSync2
            self.assertRaises(ZeroDivisionError, testdb)
            TestFile.xSync = TestFile.xSync99
            testdb()
        finally:
            apsw.connection_hooks = saved

        ## xSectorSize
        self.assertRaises(TypeError, t.xSectorSize, 3)
        TestFile.xSectorSize = TestFile.xSectorSize1
        self.assertRaises(TypeError, testdb)
        TestFile.xSectorSize = TestFile.xSectorSize2
        self.assertRaises(ZeroDivisionError, testdb)
        TestFile.xSectorSize = TestFile.xSectorSize3
        self.assertRaises(TypeError, testdb)
        TestFile.xSectorSize = TestFile.xSectorSize4
        self.assertRaises(OverflowError, testdb)
        TestFile.xSectorSize = TestFile.xSectorSize99
        testdb()

        ## xDeviceCharacteristics
        self.assertRaises(TypeError, t.xDeviceCharacteristics, 3)
        TestFile.xDeviceCharacteristics = TestFile.xDeviceCharacteristics1
        self.assertRaisesUnraisable(TypeError, testdb)
        TestFile.xDeviceCharacteristics = TestFile.xDeviceCharacteristics2
        self.assertRaisesUnraisable(ZeroDivisionError, testdb)
        TestFile.xDeviceCharacteristics = TestFile.xDeviceCharacteristics3
        self.assertRaisesUnraisable(TypeError, testdb)
        TestFile.xDeviceCharacteristics = TestFile.xDeviceCharacteristics4
        self.assertRaisesUnraisable(OverflowError, testdb)
        TestFile.xDeviceCharacteristics = TestFile.xDeviceCharacteristics99
        testdb()

        ## xFileSize
        self.assertRaises(TypeError, t.xFileSize, 3)
        TestFile.xFileSize = TestFile.xFileSize1
        self.assertRaises(TypeError, testdb)
        TestFile.xFileSize = TestFile.xFileSize2
        self.assertRaises(ZeroDivisionError, testdb)
        TestFile.xFileSize = TestFile.xFileSize3
        self.assertRaises(TypeError, testdb)
        TestFile.xFileSize = TestFile.xFileSize4
        self.assertRaises(OverflowError, testdb)
        TestFile.xFileSize = TestFile.xFileSize99
        testdb()

        ## xCheckReservedLock
        self.assertRaises(TypeError, t.xCheckReservedLock, 8)
        if not iswindows:
            # we don't do checkreservedlock test on windows as the
            # various files that need to be copied and finagled behind
            # the scenes are locked
            TestFile.xCheckReservedLock = TestFile.xCheckReservedLock1
            self.assertRaises(TypeError, testdb)
            TestFile.xCheckReservedLock = TestFile.xCheckReservedLock2
            self.assertRaises(ZeroDivisionError, testdb)
            TestFile.xCheckReservedLock = TestFile.xCheckReservedLock3
            self.assertRaises(TypeError, testdb)
            TestFile.xCheckReservedLock = TestFile.xCheckReservedLock4
            self.assertRaises(OverflowError, testdb)
        TestFile.xCheckReservedLock = TestFile.xCheckReservedLock99
        db = testdb()

        ## xFileControl
        self.assertRaises(TypeError, t.xFileControl, "three", "four")
        self.assertRaises(OverflowError, t.xFileControl, 10, 0xffffffffeeeeeeee0)
        self.assertRaises(TypeError, t.xFileControl, 10, "three")
        self.assertEqual(t.xFileControl(2000, 3000), False)
        fc1 = testdb(TESTFILEPREFIX + "testdb", closedb=False).file_control
        fc2 = testdb(TESTFILEPREFIX + "testdb2", closedb=False).file_control
        TestFile.xFileControl = TestFile.xFileControl1
        self.assertRaises(TypeError, fc1, "main", 1027, 1027)
        TestFile.xFileControl = TestFile.xFileControl2
        self.assertRaises(ZeroDivisionError, fc2, "main", 1027, 1027)
        TestFile.xFileControl = TestFile.xFileControl3
        self.assertRaises(TypeError, fc2, "main", 1027, 1027)
        TestFile.xFileControl = TestFile.xFileControl99
        del fc1
        del fc2
        # these should work
        testdb(closedb=False).file_control("main", 1027, 1027)
        if ctypes:
            objwrap = ctypes.py_object(True)
            testdb(closedb=False).file_control("main", 1028, ctypes.addressof(objwrap))
        # for coverage
        class VFSx(apsw.VFS):

            def __init__(self):
                apsw.VFS.__init__(self, "file_control", "apswtest")

        vfs2 = VFSx()
        testdb(vfsname="file_control", closedb=False).file_control("main", 1027, 1027)
        del vfs2

        ## xClose
        t.xClose()
        # make sure there is no problem closing twice
        t.xClose()
        del t
        gc.collect()

        t = apsw.VFSFile("", os.path.abspath(TESTFILEPREFIX + "testfile2"),
                         [apsw.SQLITE_OPEN_MAIN_DB | apsw.SQLITE_OPEN_CREATE | apsw.SQLITE_OPEN_READWRITE, 0])
        t.xClose()
        # check all functions detect closed file
        for n in dir(t):
            if n not in ('xClose', 'excepthook') and not n.startswith("__"):
                self.assertRaises(apsw.VFSFileClosedError, getattr(t, n))

    def testWith(self):
        "Context manager functionality"

        # Does it work?
        # the autocommit tests are to make sure we are not in a transaction
        self.assertEqual(True, self.db.get_autocommit())
        self.assertEqual(False, self.db.in_transaction)
        self.assertTableNotExists("foo1")
        with self.db as db:
            db.cursor().execute('create table foo1(x)')
        self.assertTableExists("foo1")
        self.assertEqual(True, self.db.get_autocommit())
        self.assertEqual(False, self.db.in_transaction)

        # with an error
        self.assertEqual(True, self.db.get_autocommit())
        self.assertEqual(False, self.db.in_transaction)
        self.assertTableNotExists("foo2")
        try:
            with self.db as db:
                db.cursor().execute('create table foo2(x)')
                1 / 0
        except ZeroDivisionError:
            pass
        self.assertTableNotExists("foo2")
        self.assertEqual(True, self.db.get_autocommit())

        # nested - simple - success
        with self.db as db:
            self.assertEqual(False, self.db.get_autocommit())
            self.assertEqual(True, self.db.in_transaction)
            db.cursor().execute('create table foo2(x)')
            with db as db2:
                self.assertEqual(False, self.db.get_autocommit())
                db.cursor().execute('create table foo3(x)')
                with db2 as db3:
                    self.assertEqual(False, self.db.get_autocommit())
                    db.cursor().execute('create table foo4(x)')
        self.assertEqual(True, self.db.get_autocommit())
        self.assertTableExists("foo2")
        self.assertTableExists("foo3")
        self.assertTableExists("foo4")

        # nested - simple - failure
        try:
            self.db.cursor().execute('begin; create table foo5(x)')
            with self.db as db:
                self.assertEqual(False, self.db.get_autocommit())
                db.cursor().execute('create table foo6(x)')
                with db as db2:
                    self.assertEqual(False, self.db.get_autocommit())
                    db.cursor().execute('create table foo7(x)')
                    with db2 as db3:
                        self.assertEqual(False, self.db.get_autocommit())
                        db.cursor().execute('create table foo8(x)')
                        1 / 0
        except ZeroDivisionError:
            pass
        self.assertEqual(False, self.db.get_autocommit())
        self.db.cursor().execute("commit")
        self.assertEqual(True, self.db.get_autocommit())
        self.assertTableExists("foo5")
        self.assertTableNotExists("foo6")
        self.assertTableNotExists("foo7")
        self.assertTableNotExists("foo8")

        # improve coverage and various corner cases
        self.db.__enter__()
        self.assertRaises(TypeError, self.db.__exit__, 1)
        for i in range(10):
            self.db.__exit__(None, None, None)

        # make an exit fail
        self.db.__enter__()
        self.db.cursor().execute("commit")
        # deliberately futz with the outstanding transaction
        self.assertRaises(apsw.SQLError, self.db.__exit__, None, None, None)
        self.db.__exit__(None, None, None)  # extra exit should be harmless

        # exectracing
        traces = []

        def et(con, sql, bindings):
            if con == self.db:
                traces.append(sql)
            return True

        self.db.set_exec_trace(et)
        try:
            with self.db as db:
                db.cursor().execute('create table foo2(x)')
        except apsw.SQLError:  # table already exists so we should get an error
            pass

        # check we saw the right things in the traces
        self.assertTrue(len(traces) == 3)
        for s in traces:
            self.assertTrue("SAVEPOINT" in s.upper())

        def et(*args):
            return BadIsTrue()

        self.db.set_exec_trace(et)
        try:
            with self.db as db:
                db.cursor().execute('create table etfoo2(x)')
        except ZeroDivisionError:
            pass
        self.assertTableNotExists("etfoo2")

        def et(*args):
            return False

        self.db.set_exec_trace(et)
        try:
            with self.db as db:
                db.cursor().execute('create table etfoo2(x)')
        except apsw.ExecTraceAbort:
            pass
        self.db.set_exec_trace(None)
        self.assertTableNotExists("etfoo2")

        # test blobs with context manager
        self.db.cursor().execute("create table blobby(x); insert into blobby values(x'aabbccddee')")
        rowid = self.db.last_insert_rowid()
        blob = self.db.blob_open('main', 'blobby', 'x', rowid, 0)
        with blob as b:
            self.assertEqual(id(blob), id(b))
            b.read(1)
        # blob gives ValueError if you do operations on closed blob
        self.assertRaises(ValueError, blob.read)

        self.db.cursor().execute("insert into blobby values(x'aabbccddee')")
        rowid = self.db.last_insert_rowid()
        blob = self.db.blob_open('main', 'blobby', 'x', rowid, 0)
        try:
            with blob as b:
                self.assertEqual(id(blob), id(b))
                1 / 0
                b.read(1)
        except ZeroDivisionError:
            # blob gives ValueError if you do operating on closed blob
            self.assertRaises(ValueError, blob.read)

        # backup code
        if not hasattr(self.db, "backup"): return  # experimental
        db2 = apsw.Connection(":memory:")
        with db2.backup("main", self.db, "main") as b:
            while not b.done:
                b.step(1)
        self.assertEqual(b.done, True)
        self.assertDbIdentical(self.db, db2)

    def fillWithRandomStuff(self, db, seed=1):
        "Fills a database with random content"
        db.cursor().execute("create table a(x)")
        for i in range(1, 11):
            db.cursor().execute("insert into a values(?)",
                                ("aaaaaaaaaaaaaaabbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb" * i * 8192, ))

    def assertDbIdentical(self, db1, db2):
        "Ensures databases are identical"
        c1 = db1.cursor()
        c2 = db2.cursor()
        self.assertEqual(list(c1.execute("select * from sqlite_schema order by _ROWID_")),
                         list(c2.execute("select * from sqlite_schema order by _ROWID_")))
        for table in db1.cursor().execute("select name from sqlite_schema where type='table'"):
            table = table[0]
            self.assertEqual(
                list(c1.execute("select * from [%s] order by _ROWID_" % (table, ))),
                list(c2.execute("select * from [%s] order by _ROWID_" % (table, ))),
            )
        for table in db2.cursor().execute("select name from sqlite_schema where type='table'"):
            table = table[0]
            self.assertEqual(
                list(c1.execute("select * from [%s] order by _ROWID_" % (table, ))),
                list(c2.execute("select * from [%s] order by _ROWID_" % (table, ))),
            )

    def testBackup(self):
        "Verify hot backup functionality"
        # bad calls
        self.assertRaises(TypeError, self.db.backup, "main", "main", "main", "main")
        self.assertRaises(TypeError, self.db.backup, "main", 3, "main")
        db2 = apsw.Connection(":memory:")
        db2.close()
        self.assertRaises(ValueError, self.db.backup, "main", db2, "main")
        # can't copy self
        self.assertRaises(ValueError, self.db.backup, "main", self.db, "it doesn't care what is here")

        # try and get inuse error
        if not os.getenv("APSW_VALGRIND"):
            # valgrind only runs one thread at a time (kinda like the GIL)
            # which makes this race condition almost impossible to cause
            # so we skip
            dbt = apsw.Connection(":memory:")
            vals = {"stop": False, "raised": False}

            def wt():
                # worker thread spins grabbing and releasing inuse flag
                while not vals["stop"]:
                    try:
                        dbt.set_busy_timeout(100)
                    except apsw.ThreadingViolationError:
                        # this means main thread grabbed inuse first
                        pass

            runtime = float(os.getenv("APSW_HEAVY_DURATION")) if os.getenv("APSW_HEAVY_DURATION") else 30
            t = ThreadRunner(wt)
            t.start()
            b4 = time.time()
            # try to get inuse error
            try:
                try:
                    while not vals["stop"] and time.time() - b4 < runtime:
                        self.db.backup("main", dbt, "main").close()
                except apsw.ThreadingViolationError:
                    vals["stop"] = True
                    vals["raised"] = True
            finally:
                vals["stop"] = True

        # standard usage
        db2 = apsw.Connection(":memory:")
        self.fillWithRandomStuff(db2)

        b = self.db.backup("main", db2, "main")
        self.assertRaises(TypeError, b.step, '3')
        try:
            b.step(1)
            self.assertTrue(b.remaining > 0)
            self.assertTrue(b.page_count > 0)
            while not b.done:
                b.step(1)
        finally:
            b.finish()
        self.assertDbIdentical(self.db, db2)
        self.db.cursor().execute("drop table a")

        # don't clean up
        b = self.db.backup("main", db2, "main")
        try:
            while not b.done:
                b.step(1)
        finally:
            b.finish()

        self.assertDbIdentical(self.db, db2)
        del b
        del db2
        fname = self.db.filename
        self.db = None
        gc.collect()

        # check dest db can't be used for anything else
        db2 = apsw.Connection(":memory:")
        c = db2.cursor()
        c.execute("create table x(y); insert into x values(3); select * from x")
        self.db = apsw.Connection(":memory:")
        self.fillWithRandomStuff(self.db)
        self.assertRaises(apsw.ThreadingViolationError, db2.backup, "main", self.db, "main")
        c.close()
        b = db2.backup("main", self.db, "main")
        # double check cursor really is dead
        self.assertRaises(apsw.CursorClosedError, c.execute, "select 3")
        # with the backup object existing, all operations on db2 should fail
        self.assertRaises(apsw.ThreadingViolationError, db2.cursor)
        # finish and then trying to step
        b.finish()
        self.assertRaises(apsw.ConnectionClosedError, b.step)

        # make step and finish fail with locked error
        self.db = apsw.Connection(fname)

        def lockerr():
            db2 = apsw.Connection(self.db.filename)
            db2.cursor().execute("begin exclusive")
            db3 = apsw.Connection(self.db.filename)
            b = db3.backup("main", self.db, "main")
            # if step gets busy then so does finish, but step has to be called at least once
            self.assertRaises(apsw.BusyError, b.step)
            return b

        b = lockerr()
        b.close(True)
        del b
        b = lockerr()
        self.assertRaises(apsw.BusyError, b.close, False)
        del b

        b = lockerr()
        self.assertRaises(apsw.BusyError, b.finish)
        b.finish()  # should be ok the second time
        del b

        b = lockerr()
        self.assertRaises(TypeError, b.close, "3")
        self.assertRaises(apsw.BusyError, b.close, False)
        b.close()  # should also be ok
        del b

        def f():
            b = lockerr()
            del b
            gc.collect()

        self.assertRaisesUnraisable(apsw.BusyError, f)

        # coverage
        b = lockerr()
        self.assertRaises(TypeError, b.__exit__, 3)
        self.assertRaises(apsw.BusyError, b.__exit__, None, None, None)
        b.__exit__(None, None, None)

    def testLog(self):
        "Verifies logging functions"
        self.assertRaises(TypeError, apsw.log)
        self.assertRaises(TypeError, apsw.log, 1)
        self.assertRaises(TypeError, apsw.log, 1, 2)
        self.assertRaises(TypeError, apsw.log, 1, 2, 3)
        self.assertRaises(TypeError, apsw.log, 1, None)
        apsw.log(apsw.SQLITE_MISUSE, "Hello world")  # nothing should happen
        self.assertRaises(TypeError, apsw.config, apsw.SQLITE_CONFIG_LOG, 2)
        self.assertRaises(TypeError, apsw.config, apsw.SQLITE_CONFIG_LOG)
        try:
            apsw.config(apsw.SQLITE_CONFIG_LOG, None)
            apsw.log(apsw.SQLITE_MISUSE, "Hello world")
            called = [0]

            def handler(code, message, called=called):
                called[0] += 1
                self.assertEqual(code, apsw.SQLITE_MISUSE)
                self.assertEqual(message, "a \u1234 unicode ' \ufe54 string \u0089")

            apsw.config(apsw.SQLITE_CONFIG_LOG, handler)
            apsw.log(apsw.SQLITE_MISUSE, "a \u1234 unicode ' \ufe54 string \u0089")
            self.assertEqual(called[0], 1)

            def badhandler(code, message, called=called):
                if message and "apsw_write_unraisable" in message:
                    return
                called[0] += 1
                self.assertEqual(code, apsw.SQLITE_NOMEM)
                self.assertEqual(message, "Xa \u1234 unicode ' \ufe54 string \u0089")
                1 / 0

            apsw.config(apsw.SQLITE_CONFIG_LOG, badhandler)
            self.assertRaisesUnraisable(ZeroDivisionError, apsw.log, apsw.SQLITE_NOMEM,
                                        "Xa \u1234 unicode ' \ufe54 string \u0089")
            self.assertEqual(called[0], 2)
        finally:
            gc.collect()
            apsw.config(apsw.SQLITE_CONFIG_LOG, None)

    def testReadonly(self):
        "Check Connection.readonly()"
        self.assertEqual(self.db.readonly("main"), False)
        c = apsw.Connection(TESTFILEPREFIX + "testdb", flags=apsw.SQLITE_OPEN_READONLY)
        self.assertEqual(c.readonly("main"), True)
        self.assertRaises(apsw.SQLError, self.db.readonly, "sdfsd")

        class foo:

            def __str__(self):
                1 / 0

        self.assertRaises(TypeError, self.db.readonly, foo())

    def testFilename(self):
        "Check connections and filenames"
        self.assertTrue(self.db.filename.endswith("testdb"))
        self.assertTrue(os.sep in self.db.filename)
        self.assertEqual(self.db.filename, self.db.db_filename("main"))
        self.assertRaises(TypeError, self.db.db_filename, 3)
        self.db.cursor().execute("attach '%s' as foo" % (TESTFILEPREFIX + "testdb2", ))
        self.assertEqual(self.db.filename + "2", self.db.db_filename("foo"))
        self.assertTrue(self.db.filename_wal.startswith(self.db.filename))
        self.assertTrue(self.db.filename_journal.startswith(self.db.filename))
        xdb = apsw.Connection("")
        # these all end up empty string
        self.assertTrue(xdb.filename == xdb.filename_wal and xdb.filename_wal == xdb.filename_journal)

    def testShell(self, shellclass=None):
        "Check Shell functionality"
        if shellclass is None:
            shellclass = apsw.shell.Shell

        fh = [open(TESTFILEPREFIX + "test-shell-" + t, "w+", encoding="utf8") for t in ("in", "out", "err")]
        kwargs = {"stdin": fh[0], "stdout": fh[1], "stderr": fh[2]}

        def reset():
            for i in fh:
                i.truncate(0)
                i.seek(0)

        def isempty(x):
            self.assertEqual(get(x), "")

        def isnotempty(x):
            self.assertNotEqual(len(get(x)), 0)

        def cmd(c):
            assert fh[0].tell() == 0
            fh[0].truncate(0)
            fh[0].seek(0)
            fh[0].write(c)
            fh[0].seek(0)

        def get(x):
            x.seek(0)
            return x.read()

        # Make one and ensure help works
        shellclass(stdin=fh[0], stdout=fh[1], stderr=fh[2], args=["", ".help"])
        self.assertNotIn("Traceback", get(fh[2]))
        reset()

        # Lets give it some harmless sql arguments and do a sanity check
        s = shellclass(args=[TESTFILEPREFIX + "testdb", "create table x(x)", "insert into x values(1)"], **kwargs)
        self.assertTrue(s.db.filename.endswith("testdb"))
        # do a dump and check our table is there with its values
        s.command_dump([])
        self.assertTrue("x(x)" in get(fh[1]))
        self.assertTrue("(1);" in get(fh[1]))

        # empty args
        self.assertEqual((None, [], []), s.process_args(None))

        # input description
        reset()
        write_whole_file(TESTFILEPREFIX + "test-shell-1", "wt", "syntax error")
        try:
            shellclass(args=[TESTFILEPREFIX + "testdb", ".read %stest-shell-1" % (TESTFILEPREFIX, )], **kwargs)
        except shellclass.Error:
            self.assertTrue("test-shell-1" in get(fh[2]))
            isempty(fh[1])

        # Check single and double dash behave the same
        reset()
        try:
            shellclass(args=["-init"], **kwargs)
        except shellclass.Error:
            isempty(fh[1])
            self.assertTrue("specify a filename" in get(fh[2]))

        reset()
        s = shellclass(**kwargs)
        try:
            s.process_args(["--init"])
        except shellclass.Error:
            self.assertTrue("specify a filename" in str(sys.exc_info()[1]))

        # various command line options
        # an invalid one
        reset()
        try:
            shellclass(args=["---tripledash"], **kwargs)
        except shellclass.Error:
            isempty(fh[1])
            self.assertTrue("-tripledash" in get(fh[2]))
            self.assertTrue("--tripledash" not in get(fh[2]))

        ###
        ### --init
        ###
        reset()
        write_whole_file(TESTFILEPREFIX + "test-shell-1", "wt", "syntax error")
        try:
            shellclass(args=["-init", TESTFILEPREFIX + "test-shell-1"], **kwargs)
        except shellclass.Error:
            # we want to make sure it read the file
            isempty(fh[1])
            self.assertTrue("syntax error" in get(fh[2]))
        reset()
        write_whole_file(TESTFILEPREFIX + "test-shell-1", "wt", "select 3;")
        shellclass(args=["-init", TESTFILEPREFIX + "test-shell-1"], **kwargs)
        # we want to make sure it read the file
        isempty(fh[2])
        self.assertTrue("3" in get(fh[1]))

        ###
        ### --header
        ###
        reset()
        s = shellclass(**kwargs)
        s.process_args(["--header"])
        self.assertEqual(s.header, True)
        s.process_args(["--noheader"])
        self.assertEqual(s.header, False)
        s.process_args(["--noheader", "-header", "-noheader", "--header"])
        self.assertEqual(s.header, True)
        s.process_args(["-no-colour", "--nocolor"])
        self.assertEqual(s.colour_scheme, "off")
        # did they actually turn on?
        isempty(fh[1])
        isempty(fh[2])
        s.process_args([TESTFILEPREFIX + "testdb", ".mode column", "select 3"])
        isempty(fh[2])
        self.assertTrue("3" in get(fh[1]))
        self.assertTrue("----" in get(fh[1]))

        ###
        ### --echo, --bail, --interactive
        ###
        reset()
        for v in ("echo", "bail", "interactive"):
            s = shellclass(**kwargs)
            b4 = getattr(s, v)
            s.process_args(["--" + v])
            # setting should have changed
            self.assertNotEqual(b4, getattr(s, v))
            isempty(fh[1])
            isempty(fh[2])

        ###
        ### --batch
        ###
        reset()
        s = shellclass(**kwargs)
        s.interactive = True
        s.process_args(["-batch"])
        self.assertEqual(s.interactive, False)
        isempty(fh[1])
        isempty(fh[2])

        ###
        ### --separator, --nullvalue, --encoding
        ###
        for v, val in ("separator", "\n"), ("nullvalue", "abcdef"), ("encoding", "iso8859-1"):
            reset()
            s = shellclass(args=["--" + v, val], **kwargs)
            # We need the eval because shell processes backslashes in
            # string.  After deliberating that is the right thing to
            # do
            if v == "encoding":
                self.assertEqual((val, None), getattr(s, v))
            else:
                self.assertEqual(val, getattr(s, v))
            isempty(fh[1])
            isempty(fh[2])
            self.assertRaises(shellclass.Error, shellclass, args=["-" + v, val, "--" + v], **kwargs)
            isempty(fh[1])
            self.assertTrue(v in get(fh[2]))

        ###
        ### --version
        ###
        reset()
        self.assertRaises(SystemExit, shellclass, args=["--version"], **kwargs)
        # it writes to stdout
        isempty(fh[2])
        self.assertTrue(apsw.sqlite_lib_version() in get(fh[1]))

        ###
        ### --help
        ###
        reset()
        self.assertRaises(SystemExit, shellclass, args=["--help"], **kwargs)
        # it writes to stderr
        isempty(fh[1])
        self.assertTrue("-version" in get(fh[2]))

        ###
        ### Items that correspond to output mode
        ###
        reset()
        shellclass(args=[
            "--python", "--column", "--python", ":memory:", "create table x(x)", "insert into x values(x'aa')",
            "select * from x;"
        ],
                   **kwargs)
        isempty(fh[2])
        self.assertTrue('b"' in get(fh[1]) or "buffer(" in get(fh[1]))

        ###
        ### Is process_unknown_args called as documented?
        ###
        reset()

        class s2(shellclass):

            def process_unknown_args(self, args):
                1 / 0

        self.assertRaises(ZeroDivisionError, s2, args=["--unknown"], **kwargs)
        isempty(fh[1])
        self.assertTrue("division" in get(fh[2]))  # py2 says "integer division", py3 says "int division"

        class s3(shellclass):

            def process_unknown_args(_, args):
                self.assertEqual(args[0:2], ["myoption", "myvalue"])
                return args[2:]

        reset()
        self.assertRaises(s3.Error, s3, args=["--python", "--myoption", "myvalue", "--init"], **kwargs)
        isempty(fh[1])
        self.assertTrue("-init" in get(fh[2]))

        ###
        ### .open
        ####
        reset()
        s = shellclass(**kwargs)
        self.assertTrue(s.db.filename == "")
        for n in "testdb", "testdb2", "testdb3":
            fn = TESTFILEPREFIX + n
            reset()
            cmd(".open " + fn)
            s.cmdloop()
            self.assertTrue(s.db.filename.endswith(fn))
        reset()
        fn = TESTFILEPREFIX + "testdb"
        cmd(".open " + fn)
        cmd("create table foo(x); insert into foo values(2);")
        s.cmdloop()
        for row in s.db.cursor().execute("select * from foo"):
            break
        else:
            self.fail("Table doesn't have any rows")
        reset()
        cmd(".open --wipe " + fn)
        s.cmdloop()
        for row in s.db.cursor().execute("select * from sqlite_schema"):
            self.fail("--wipe didn't wipe file")

        N = "sentinel-chidh-jklhfd"

        class vfstest(apsw.VFS):

            def __init__(self):
                super().__init__(name=N, base="")

        ref = vfstest()
        reset()
        cmd(f".open --vfs { N } { fn }\n.connection\n.close")
        s.cmdloop()
        isempty(fh[2])
        self.assertIn(f"({ N })", get(fh[1]))
        del ref

        ###
        ### Some test data
        ###
        reset()
        s = shellclass(**kwargs)
        s.cmdloop()

        def testnasty():
            reset()
            cmd("""create table if not exists nastydata(x,y);
                 insert into nastydata values(x'', 1e999); -- see issue 482 for zero sized blob
                 insert into nastydata values(null,'xxx\\u1234\\uabcdyyy\r\n\t\"this is nasty\u0001stuff!');
                """)
            s.cmdloop()
            isempty(fh[1])
            isempty(fh[2])
            reset()
            cmd(".bail on\n.header OFF\nselect * from nastydata;")
            s.cmdloop()
            isempty(fh[2])
            isnotempty(fh[1])

        ###
        ### Output formats - column
        ###
        reset()
        x = 'a' * 20
        cmd(".mode column\n.header ON\nselect '" + x + "';")
        s.cmdloop()
        isempty(fh[2])
        # colwidth should be 2 more
        sep = '-' * (len(x) + 2)  # apostrophes quoting string in column header
        out = get(fh[1]).replace("\n", "")
        self.assertEqual(len(out.split(sep)), 2)
        self.assertEqual(len(out.split(sep)[0]), len(x) + 2)  # plus two apostrophes
        self.assertEqual(len(out.split(sep)[1]), len(x) + 2)  # same
        self.assertTrue("  " in out.split(sep)[1])  # space padding
        # make sure truncation happens
        reset()
        cmd(".width 5\nselect '" + x + "';\n")
        s.cmdloop()
        isempty(fh[2])
        self.assertTrue("a" * 6 not in get(fh[1]))
        # right justification
        reset()
        cmd(".header off\n.width -3 -3\nselect 3,3;\n.width 3 3\nselect 3,3;")
        s.cmdloop()
        isempty(fh[2])
        v = get(fh[1])
        self.assertTrue(v.startswith("  3    3"))
        v = v.split("\n")
        self.assertNotEqual(v[0], v[1])
        self.assertEqual(len(v[0]), len(v[1]))
        # do not output blob as is
        self.assertTrue("\xaa" not in get(fh[1]))
        # undo explain
        reset()
        cmd(".explain OFF\n")
        s.cmdloop()
        testnasty()

        ###
        ### Output formats - csv
        ###
        reset()
        # mode change should reset separator
        cmd(".separator F\n.mode csv\nselect 3,3;\n")
        s.cmdloop()
        isempty(fh[2])
        self.assertTrue("3,3" in get(fh[1]))
        # tab sep
        reset()
        cmd(".separator '\\t'\nselect 3,3;\n")
        s.cmdloop()
        isempty(fh[2])
        self.assertTrue("3\t3" in get(fh[1]))
        # back to comma
        reset()
        cmd(".mode csv\nselect 3,3;\n")
        s.cmdloop()
        isempty(fh[2])
        self.assertTrue("3,3" in get(fh[1]))
        # quoting
        reset()
        cmd(".header ON\nselect 3 as [\"one\"], 4 as [\t];\n")
        s.cmdloop()
        isempty(fh[2])
        self.assertTrue('"""one""",\t' in get(fh[1]))
        # custom sep
        reset()
        cmd(".separator |\nselect 3 as [\"one\"], 4 as [\t];\n")
        s.cmdloop()
        isempty(fh[2])
        self.assertTrue("3|4\n" in get(fh[1]))
        self.assertTrue('"one"|\t\n' in get(fh[1]))
        # testnasty() - csv module is pretty much broken

        ###
        ### Output formats - html
        ###
        reset()
        cmd(".mode html\n.header OFF\nselect 3,4;\n")
        s.cmdloop()
        isempty(fh[2])
        # should be no header
        self.assertTrue("<th>" not in get(fh[1]).lower())
        # does it actually work?
        self.assertTrue("<td>3</td>" in get(fh[1]).lower())
        # check quoting works
        reset()
        cmd(".header ON\nselect 3 as [<>&];\n")
        s.cmdloop()
        isempty(fh[2])
        self.assertTrue("<th>&lt;&gt;&amp;</th>" in get(fh[1]).lower())
        # do we output rows?
        self.assertTrue("<tr>" in get(fh[1]).lower())
        self.assertTrue("</tr>" in get(fh[1]).lower())
        testnasty()

        ###
        ### Output formats - insert
        ###
        reset()
        all = "3,3.1,'3.11',null,x'0311'"
        cmd(".mode insert\n.header OFF\nselect " + all + ";\n")
        s.cmdloop()
        isempty(fh[2])
        self.assertTrue(all in get(fh[1]).lower())
        # empty values
        reset()
        all = "0,0.0,'',null,x''"
        cmd("select " + all + ";\n")
        s.cmdloop()
        isempty(fh[2])
        self.assertTrue(all in get(fh[1]).lower())
        # header, separator and nullvalue should make no difference
        save = get(fh[1])
        reset()
        cmd(".header ON\n.separator %\n.nullvalue +\nselect " + all + ";\n")
        s.cmdloop()
        isempty(fh[2])
        self.assertEqual(save, get(fh[1]))
        # check the table name
        self.assertTrue(get(fh[1]).lower().startswith('insert into "table" values'))
        reset()
        cmd(".mode insert funkychicken\nselect " + all + ";\n")
        s.cmdloop()
        isempty(fh[2])
        self.assertTrue(get(fh[1]).lower().startswith("insert into funkychicken values"))
        testnasty()

        ###
        ### Output formats - json
        ###
        reset()
        all = "3,2.2,'string',null,x'0311'"
        cmd(".mode json\n.header ON\n select " + all + ";")
        s.cmdloop()
        isempty(fh[2])
        out = json.loads(get(fh[1]))
        self.assertEqual(out, [{"3": 3, "2.2": 2.2, "'string'": "string", "null": None, "x'0311'": "AxE="}])
        # a regular table
        reset()
        cmd(f"""create table jsontest([int], [float], [string], [null], [blob]);
                insert into jsontest values({ all });
                insert into jsontest values({ all });
                select * from jsontest;""")
        s.cmdloop()
        isempty(fh[2])
        out = json.loads(get(fh[1]))
        self.assertEqual(out, [{"int": 3, "float": 2.2, "string": "string", "null": None, "blob": "AxE="}] * 2)
        testnasty()

        ###
        ### Output formats - jsonl
        ###
        reset()
        cmd(".mode jsonl\n.header ON\n select " + all + ";")
        s.cmdloop()
        isempty(fh[2])
        v = get(fh[1]).strip()
        out = json.loads(v)
        self.assertEqual(out, {"3": 3, "2.2": 2.2, "'string'": "string", "null": None, "x'0311'": "AxE="})
        reset()
        cmd("select * from jsontest;")
        s.cmdloop()
        isempty(fh[2])
        out = [json.loads(line) for line in get(fh[1]).splitlines()]
        self.assertEqual(out, [{"int": 3, "float": 2.2, "string": "string", "null": None, "blob": "AxE="}] * 2)
        testnasty()

        ###
        ### Output formats - line
        ###
        reset()
        cmd(".header OFF\n.nullvalue *\n.mode line\nselect 3 as a, null as b, 0.0 as c, 'a' as d, x'aa' as e;\n")
        s.cmdloop()
        isempty(fh[2])
        out = get(fh[1]).replace(" ", "")
        self.assertTrue("a=3\n" in out)
        self.assertTrue("b=*\n" in out)
        self.assertTrue("c=0.0\n" in out)
        self.assertTrue("d=a\n" in out)
        self.assertTrue("e=<Binarydata>\n" in out)
        self.assertEqual(7, len(out.split("\n")))  # one for each col plus two trailing newlines
        # header should make no difference
        reset()
        cmd(".header ON\n.nullvalue *\n.mode line\nselect 3 as a, null as b, 0.0 as c, 'a' as d, x'aa' as e;\n")
        s.cmdloop()
        isempty(fh[2])
        self.assertEqual(out, get(fh[1]).replace(" ", ""))
        # wide column name
        reset()
        ln = "kjsfhgjksfdjkgfhkjsdlafgjkhsdkjahfkjdsajfhsdja" * 12
        cmd("select 3 as %s, 3 as %s1;" % (ln, ln))
        s.cmdloop()
        isempty(fh[2])
        self.assertEqual(get(fh[1]), " %s = 3\n%s1 = 3\n\n" % (ln, ln))
        testnasty()

        ###
        ### Output formats - list
        ###
        reset()
        cmd(".header off\n.mode list\n.nullvalue (\n.separator &\nselect 3 as a, null as b, 0.0 as c, 'a' as d, x'aa' as e;\n"
            )
        s.cmdloop()
        isempty(fh[2])
        self.assertEqual(get(fh[1]), '3&(&0.0&a&<Binary data>\n')
        reset()
        # header on
        cmd(".header on\n.mode list\n.nullvalue (\n.separator &\nselect 3 as a, null as b, 0.0 as c, 'a' as d, x'aa' as e;\n"
            )
        s.cmdloop()
        isempty(fh[2])
        self.assertTrue(get(fh[1]).startswith("a&b&c&d&e\n"))
        testnasty()

        ###
        ### Output formats - python
        ###
        reset()
        cmd(".header off\n.mode python\nselect 3 as a, null as b, 0.0 as c, 'a' as d, x'aa44bb' as e;\n")
        s.cmdloop()
        isempty(fh[2])
        v = eval(get(fh[1]))
        self.assertEqual(len(v), 1)  # 1 tuple
        self.assertEqual(v, ((3, None, 0.0, 'a', b"\xaa\x44\xbb"), ))
        reset()
        cmd(".header on\n.mode python\nselect 3 as a, null as b, 0.0 as c, 'a' as d, x'aa44bb' as e;\n")
        s.cmdloop()
        isempty(fh[2])
        v = eval("(" + get(fh[1]) + ")")  # need parentheses otherwise indent rules apply
        self.assertEqual(len(v), 2)  # headers and row
        self.assertEqual(v, (
            ("a", "b", "c", "d", "e"),
            (3, None, 0.0, 'a', b"\xaa\x44\xbb"),
        ))
        testnasty()

        ###
        ### Output formats - TCL
        ###
        reset()
        cmd(".header off\n.mode tcl\n.separator -\n.nullvalue ?\nselect 3 as a, null as b, 0.0 as c, 'a' as d, x'aa44bb' as e;\n"
            )
        s.cmdloop()
        isempty(fh[2])
        self.assertEqual(get(fh[1]), '"3"-"?"-"0.0"-"a"-"\\xAAD\\xBB"\n')
        reset()
        cmd(".header on\nselect 3 as a, null as b, 0.0 as c, 'a' as d, x'aa44bb' as e;\n")
        s.cmdloop()
        isempty(fh[2])
        self.assertTrue('"a"-"b"-"c"-"d"-"e"' in get(fh[1]))
        testnasty()

        ###
        ### Output formats - box/qbox/table
        ###
        if hasattr(s, "output_box"):
            # ensure others error
            reset()
            cmd(".mode tcl --foo bar")
            self.assertRaises(apsw.shell.Shell.Error, s.cmdloop)
            for mode in "box", "qbox", "table":
                reset()
                cmd(f".mode { mode } --no-unicode --word-wrap --width 65")
                s.cmdloop()
                isempty(fh[1])
                isempty(fh[2])
                testnasty()

        # What happens if db cannot be opened?
        s.process_args(args=["/"])
        reset()
        cmd("select * from sqlite_schema;\n.bail on\nselect 3;\n")
        self.assertRaises(apsw.CantOpenError, s.cmdloop)
        isempty(fh[1])
        self.assertTrue("unable to open database file" in get(fh[2]))

        # echo testing - multiple statements
        s.process_args([":memory:"])  # back to memory db
        reset()
        cmd(".bail off\n.echo on\nselect 3;\n")
        s.cmdloop()
        self.assertTrue("select 3;\n" in get(fh[2]))
        # multiline
        reset()
        cmd("select 3;select 4;\n")
        s.cmdloop()
        self.assertTrue("select 3;\n" in get(fh[2]))
        self.assertTrue("select 4;\n" in get(fh[2]))
        # multiline with error
        reset()
        cmd("select 3;select error;select 4;\n")
        s.cmdloop()
        # worked line should be present
        self.assertTrue("select 3;\n" in get(fh[2]))
        # as should the error
        self.assertTrue("no such column: error" in get(fh[2]))
        # is timing info output correctly?
        reset()
        timersupported = False
        try:
            cmd(".bail on\n.echo off\n.timer on\n.timer off\n")
            s.cmdloop()
            timersupported = True
        except s.Error:
            pass

        if timersupported:
            reset()
            # create something that should take some time to execute
            s.db.cursor().execute("create table xyz(x); begin;")
            s.db.cursor().executemany("insert into xyz values(?)", randomintegers(4000))
            s.db.cursor().execute("end")
            reset()
            # this takes .6 seconds on my machine so we should
            # definitely have non-zero timing information
            cmd(".timer ON\nselect max(x),min(x),max(x+x),min(x-x) from xyz union select x+max(x),x-min(x),3,4 from xyz union select x,x,x,x from xyz union select x,x,x,x from xyz;select 3;\n"
                )
            s.cmdloop()
            isnotempty(fh[1])
            isnotempty(fh[2])
        reset()
        cmd(".bail off\n.timer off")
        s.cmdloop()

        # command handling
        reset()
        cmd(".nonexist 'unclosed")
        s.cmdloop()
        isempty(fh[1])
        self.assertTrue("no closing quotation" in get(fh[2]).lower())
        reset()
        cmd(".notexist       ")
        s.cmdloop()
        isempty(fh[1])
        self.assertTrue('Unknown command "notexist"' in get(fh[2]))

        ###
        ### Commands - backup and restore
        ###

        reset()
        cmd(".backup with too many parameters")
        s.cmdloop()
        isempty(fh[1])
        isnotempty(fh[2])
        reset()
        cmd(".backup ")  # too few
        s.cmdloop()
        isempty(fh[1])
        isnotempty(fh[2])
        reset()
        cmd(".restore with too many parameters")
        s.cmdloop()
        isempty(fh[1])
        isnotempty(fh[2])
        reset()
        cmd(".restore ")  # too few
        s.cmdloop()
        isempty(fh[1])
        isnotempty(fh[2])
        # bogus filenames
        for i in ('/', '"main" /'):
            for c in (".backup ", ".restore "):
                reset()
                cmd(c + i)
                s.cmdloop()
                isempty(fh[1])
                isnotempty(fh[2])

        def randomtable(cur, dbname=None):
            name = list("abcdefghijklmnopqrstuvwxtz")
            random.shuffle(name)
            name = "".join(name)
            fullname = name
            if dbname:
                fullname = dbname + "." + fullname
            cur.execute("begin;create table %s(x)" % (fullname, ))
            cur.executemany("insert into %s values(?)" % (fullname, ), randomintegers(400))
            cur.execute("end")
            return name

        # Straight forward backup.  The gc.collect() is needed because
        # non-gc cursors hanging around will prevent the backup from
        # happening.
        n = randomtable(s.db.cursor())
        contents = s.db.cursor().execute("select * from " + n).fetchall()
        reset()
        cmd(".backup %stestdb2" % (TESTFILEPREFIX, ))
        gc.collect()
        s.cmdloop()
        isempty(fh[1])
        isempty(fh[2])
        reset()
        cmd("drop table " + n + ";")
        s.cmdloop()
        isempty(fh[1])
        isempty(fh[2])
        self.assertTrue(os.path.isfile("%stestdb2" % (TESTFILEPREFIX, )))
        reset()
        cmd(".restore %stestdb2" % (TESTFILEPREFIX, ))
        gc.collect()
        s.cmdloop()
        isempty(fh[1])
        isempty(fh[2])
        newcontents = s.db.cursor().execute("select * from " + n).fetchall()
        # no guarantee of result order
        contents.sort()
        newcontents.sort()
        self.assertEqual(contents, newcontents)

        # do they pay attention to the dbname
        s.db.cursor().execute("attach ':memory:' as memdb")
        n = randomtable(s.db.cursor(), "memdb")
        contents = s.db.cursor().execute("select * from memdb." + n).fetchall()
        reset()
        gc.collect()
        cmd(".backup memdb %stestdb2" % (TESTFILEPREFIX, ))
        s.cmdloop()
        isempty(fh[1])
        isempty(fh[2])
        s.db.cursor().execute("detach memdb; attach ':memory:' as memdb2")
        reset()
        gc.collect()
        cmd(".restore memdb2 %stestdb2" % (TESTFILEPREFIX, ))
        s.cmdloop()
        isempty(fh[1])
        isempty(fh[2])
        newcontents = s.db.cursor().execute("select * from memdb2." + n).fetchall()
        # no guarantee of result order
        contents.sort()
        newcontents.sort()
        self.assertEqual(contents, newcontents)

        ###
        ### Commands - bail
        ###
        reset()
        cmd(".bail")
        s.cmdloop()
        isempty(fh[1])
        isnotempty(fh[2])
        reset()
        cmd(".bail on\n.mode list\nselect 3;\nselect error;\nselect 4;\n")
        self.assertRaises(apsw.Error, s.cmdloop)
        self.assertTrue("3" in get(fh[1]))
        self.assertTrue("4" not in get(fh[1]))
        reset()
        cmd(".bail oFf\n.mode list\nselect 3;\nselect error;\nselect 4;\n")
        s.cmdloop()
        self.assertTrue("3" in get(fh[1]))
        self.assertTrue("4" in get(fh[1]))

        ###
        ### Commands - changes
        ###
        reset()
        cmd(".changes on\nselect 3;")
        s.cmdloop()
        isempty(fh[2])
        self.assertNotIn("changes:", get(fh[1]))
        reset()
        cmd("create table testchange(x); insert into testchange values(3);")
        s.cmdloop()
        isempty(fh[2])
        self.assertIn("changes:", get(fh[1]))
        reset()
        cmd(".changes off\ninsert into testchange values(4);")
        s.cmdloop()
        isempty(fh[2])
        self.assertNotIn("changes:", get(fh[1]))

        ###
        ### Commands - cd / connection / close
        ###
        @contextlib.contextmanager
        def chdir(path):
            before = os.getcwd()
            os.chdir(path)
            yield
            os.chdir(before)

        def in_open_dbs(filename):
            count = 0
            for c in apsw.connections():
                try:
                    if not c.filename:
                        continue
                except apsw.ConnectionClosedError:
                    continue
                if os.path.samefile(filename, c.filename):
                    count += 1
            return count

        with tempfile.TemporaryDirectory(prefix="apsw-shell-test-") as tmpd1:
            with chdir("."):
                reset()
                cmd(f".cd { shlex.quote( tmpd1 )}")
                s.cmdloop()
                isempty(fh[1])
                isempty(fh[2])
                self.assertTrue(os.path.samefile(tmpd1, os.getcwd()))

            with chdir(tmpd1):
                reset()
                V = "sentinel-jhdgfsfjdskh-1"
                cmd(f".open { shlex.quote(V) }\ncreate table foo(x);\n.open { shlex.quote(V) }")
                s.cmdloop()
                isempty(fh[1])
                isempty(fh[2])
                self.assertEqual(2, in_open_dbs(V))
                reset()
                cmd(f".open --wipe { shlex.quote(V) }")
                s.cmdloop()
                isempty(fh[1])
                isempty(fh[2])
                self.assertEqual(1, in_open_dbs(V))
                reset()
                cmd(f".close")
                s.cmdloop()
                isempty(fh[1])
                isempty(fh[2])
                self.assertEqual(0, in_open_dbs(V))

        ###
        ### Commands - databases
        ###
        reset()
        cmd(".databases foo")
        s.cmdloop()
        isempty(fh[1])
        isnotempty(fh[2])
        # clean things up
        s = shellclass(**kwargs)
        reset()
        cmd(".header oFF\n.databases")
        s.cmdloop()
        isempty(fh[2])
        for i in "main", "name", "file":
            self.assertTrue(i in get(fh[1]))
        reset()
        cmd("attach '%stestdb' as quack;\n.databases" % (TESTFILEPREFIX, ))
        s.cmdloop()
        isempty(fh[2])
        for i in "main", "name", "file", "testdb", "quack":
            self.assertTrue(i in get(fh[1]))
        reset()
        cmd("detach quack;")
        s.cmdloop()
        isempty(fh[2])
        for i in "testdb", "quack":
            self.assertTrue(i not in get(fh[1]))

        ###
        ### Command - dbconfig
        ###
        reset()
        cmd(".dbconfig")
        s.cmdloop()
        isempty(fh[2])
        self.assertIn("trigger_eqp:", get(fh[1]))
        reset()
        self.assertFalse(s.db.config(apsw.SQLITE_DBCONFIG_TRIGGER_EQP, -1))
        cmd(".dbconfig trigger_eqp 1")
        s.cmdloop()
        isempty(fh[2])
        self.assertTrue(s.db.config(apsw.SQLITE_DBCONFIG_TRIGGER_EQP, -1))

        ###
        ### Command - dbinfo
        ###
        if hasattr(s, "command_dbinfo"):
            with tempfile.TemporaryDirectory(prefix="apsw-test-shell-dbinfo-") as tmpd:
                reset()
                cmd(f".open { shlex.quote(tmpd) }/newdb")
                s.cmdloop()
                isempty(fh[2])
                reset()
                cmd(
                    textwrap.dedent("""
                    .dbinfo
                    pragma journal_mode=wal;
                    .dbinfo
                    pragma journal_mode=persist;
                    .dbinfo
                    pragma encoding="UTF-16";
                    .dbinfo
                    create table x(y);
                    .dbinfo
                """))
                s.cmdloop()
                isnotempty(fh[1])
                isempty(fh[2])
                reset()
                cmd(".close\n.connection 0")
                s.cmdloop()

        ###
        ### Commands - dump
        ###
        reset()
        cmd("create     table foo(x); create table bar(x);\n.dump foox")
        s.cmdloop()
        isempty(fh[1])
        isempty(fh[2])
        reset()
        cmd(".dump foo")
        s.cmdloop()
        isempty(fh[2])
        for i in "foo", "create table", "begin", "commit":
            self.assertTrue(i in get(fh[1]).lower())
        self.assertTrue("bar" not in get(fh[1]).lower())
        # can we do virtual tables?
        reset()
        if self.checkOptionalExtension("fts3", "create virtual table foo using fts3()"):
            reset()
            cmd("CREATE virtual TaBlE    fts3     using fts3(colA FRED  , colB JOHN DOE);\n"
                "insert into fts3 values('one', 'two');insert into fts3 values('onee', 'two');\n"
                "insert into fts3 values('one', 'two two two');")
            s.cmdloop()
            isempty(fh[1])
            isempty(fh[2])
            reset()
            cmd(".dump")
            s.cmdloop()
            isempty(fh[2])
            v = get(fh[1])
            for i in "pragma writable_schema", "create virtual table fts3", "cola fred", "colb john doe":
                self.assertTrue(i in v.lower())
        # analyze
        reset()
        cmd("drop table bar;create table bar(x unique,y);create index barf on bar(x,y);create index barff on bar(y);insert into bar values(3,4);\nanalyze;\n.dump bar"
            )
        s.cmdloop()
        isempty(fh[2])
        v = get(fh[1])
        for i in "analyze bar", "create index barf":
            self.assertTrue(i in v.lower())
        self.assertTrue("autoindex" not in v.lower())  # created by sqlite to do unique constraint
        self.assertTrue("sqlite_sequence" not in v.lower())  # not autoincrements
        # repeat but all tables
        reset()
        cmd(".dump")
        s.cmdloop()
        isempty(fh[2])
        v = get(fh[1])
        for i in "analyze bar", "create index barf":
            self.assertTrue(i in v.lower())
        self.assertTrue("autoindex" not in v.lower())  # created by sqlite to do unique constraint
        # foreign keys
        reset()
        cmd("create table xxx(z references bar(x));\n.dump")
        s.cmdloop()
        isempty(fh[2])
        v = get(fh[1])
        for i in "foreign_keys", "references":
            self.assertTrue(i in v.lower())
        # views
        reset()
        cmd("create view noddy as select * from foo;\n.dump noddy")
        s.cmdloop()
        isempty(fh[2])
        v = get(fh[1])
        for i in "drop view", "create view noddy":
            self.assertTrue(i in v.lower())
        # issue82 - view ordering
        reset()
        cmd("create table issue82(x);create view issue82_2 as select * from issue82; create view issue82_1 as select count(*) from issue82_2;\n.dump issue82%"
            )
        s.cmdloop()
        isempty(fh[2])
        v = get(fh[1])
        s.db.cursor().execute("drop table issue82 ; drop view issue82_1 ; drop view issue82_2")
        reset()
        cmd(v)
        s.cmdloop()
        isempty(fh[1])
        isempty(fh[2])
        # autoincrement
        reset()
        cmd("create table abc(x INTEGER PRIMARY KEY AUTOINCREMENT); insert into abc values(null);insert into abc values(null);\n.dump"
            )
        s.cmdloop()
        isempty(fh[2])
        v = get(fh[1])
        for i in "sqlite_sequence", "'abc', 2":
            self.assertTrue(i in v.lower())
        # user version
        self.assertTrue("user_version" not in v)
        reset()
        cmd("pragma user_version=27;\n.dump")
        s.cmdloop()
        isempty(fh[2])
        v = get(fh[1])
        self.assertTrue("pragma user_version=27;" in v)
        s.db.cursor().execute("pragma user_version=0")
        # some nasty stuff
        reset()
        cmd("create table nastydata(x,y); insert into nastydata values(null,'xxx\\u1234\\uabcd\\U00012345yyy\r\n\t\"this is nasty\u0001stuff!');"
            'create table "table"([except] int); create table [](""); create table [using]("&");')
        s.cmdloop()
        isempty(fh[1])
        isempty(fh[2])
        reset()
        cmd(".dump")
        s.cmdloop()
        isempty(fh[2])
        v = get(fh[1])
        self.assertTrue("nasty" in v)
        self.assertTrue("stuff" in v)
        # sanity check the dumps
        reset()
        cmd(v)  # should run just fine
        s.cmdloop()
        isempty(fh[1])
        isempty(fh[2])
        # drop all the tables we made to do another dump and compare with before
        for t in "abc", "bar", "foo", "fts3", "xxx", "noddy", "sqlite_sequence", "sqlite_stat1", \
                "issue82", "issue82_1", "issue82_2":
            reset()
            cmd("drop table %s;drop view %s;" % (t, t))
            s.cmdloop()  # there will be errors which we ignore
        reset()
        cmd(v)
        s.cmdloop()
        isempty(fh[1])
        isempty(fh[2])
        # another dump
        reset()
        cmd(".dump")
        s.cmdloop()
        isempty(fh[2])
        v2 = get(fh[1])
        v = re.sub("-- Date:.*", "", v)
        v2 = re.sub("-- Date:.*", "", v2)
        self.assertEqual(v, v2)
        # clean database
        reset()
        s = shellclass(args=[':memory:'], **kwargs)
        cmd(v)
        s.cmdloop()
        isempty(fh[1])
        isempty(fh[2])
        reset()
        cmd(v2 + "\n.dump")
        s.cmdloop()
        isempty(fh[2])
        v3 = get(fh[1])
        v3 = re.sub("-- Date:.*", "", v3)
        self.assertEqual(v, v3)
        # trailing comments
        reset()
        cmd("""create table xxblah(b -- ff
) -- xx
; create index xxfoo on xxblah(b -- ff
) -- xx
; create view xxbar as select * from xxblah -- ff
;
insert into xxblah values(3);
.dump
""")
        s.cmdloop()
        isempty(fh[2])
        dump = get(fh[1])
        reset()
        cmd("drop table xxblah; drop view xxbar;")
        s.cmdloop()
        isempty(fh[2])
        isempty(fh[1])
        reset()
        cmd(dump)
        s.cmdloop()
        isempty(fh[2])
        isempty(fh[1])
        self.assertEqual(s.db.cursor().execute("select * from xxbar").fetchall(), [(3, )])
        # check index
        reset()
        cmd("drop index xxfoo;")
        s.cmdloop()
        isempty(fh[1])
        isempty(fh[2])

        ###
        ### Command - echo
        ###
        reset()
        cmd(".echo")
        s.cmdloop()
        isempty(fh[1])
        isnotempty(fh[2])
        reset()
        cmd(".echo bananas")
        s.cmdloop()
        isempty(fh[1])
        isnotempty(fh[2])
        reset()
        cmd(".echo on on")
        s.cmdloop()
        isempty(fh[1])
        isnotempty(fh[2])
        reset()
        cmd(".echo off\nselect 3;")
        s.cmdloop()
        self.assertTrue("3" in get(fh[1]))
        self.assertTrue("select 3" not in get(fh[2]))
        reset()
        cmd(".echo on\nselect 3;")
        s.cmdloop()
        self.assertTrue("3" in get(fh[1]))
        self.assertTrue("select 3" in get(fh[2]))
        # more complex testing is done earlier including multiple statements and errors

        ###
        ### Command - encoding
        ###
        self.suppressWarning("ResourceWarning")
        for i in ".encoding one two", ".encoding", ".encoding utf8 another":
            reset()
            cmd(i)
            s.cmdloop()
            isempty(fh[1])
            isnotempty(fh[2])
        reset()
        cmd(".encoding this-does-not-exist")
        s.cmdloop()
        isempty(fh[1])
        self.assertTrue("no known encoding" in get(fh[2]).lower())
        # use iso8859-1 to make sure data is read correctly - it
        # differs from utf8
        us = "unitestdata \xaa\x89 34"
        write_whole_file(TESTFILEPREFIX + "test-shell-1",
                         "w",
                         f"insert into enctest values('{ us }');\n",
                         encoding="iso8859-1")
        gc.collect()
        reset()
        cmd(".encoding iso8859-1\ncreate table enctest(x);\n.echo on\n.read %stest-shell-1\n.echo off" %
            (TESTFILEPREFIX, ))
        s.cmdloop()
        self.assertEqual(s.db.cursor().execute("select * from enctest").fetchall()[0][0], us)
        self.assertTrue(us in get(fh[2]))
        reset()
        write_whole_file(TESTFILEPREFIX + "test-shell-1", "w", us + "\n", encoding="iso8859-1")
        cmd("drop table enctest;create table enctest(x);\n.import %stest-shell-1 enctest" % (TESTFILEPREFIX, ))
        s.cmdloop()
        isempty(fh[2])
        isempty(fh[1])
        self.assertEqual(s.db.cursor().execute("select * from enctest").fetchall()[0][0], us)
        reset()
        cmd(".output %stest-shell-1\n.mode list\nselect * from enctest;" % (TESTFILEPREFIX, ))
        s.cmdloop()
        self.assertEqual(
            read_whole_file(TESTFILEPREFIX + "test-shell-1", "rb").strip(),  # skip eol
            us.encode("iso8859-1"))
        reset()
        cmd(".output stdout\nselect '%s';\n" % (us, ))
        s.cmdloop()
        isempty(fh[2])
        self.assertTrue(us in get(fh[1]))

        ### encoding specifying error handling - see issue 108
        reset()
        cmd(".encoding utf8:replace")
        s.cmdloop()
        isempty(fh[1])
        isempty(fh[2])
        # non-existent error
        reset()
        cmd(".encoding cp437:blahblah")
        s.cmdloop()
        isempty(fh[1])
        isnotempty(fh[2])
        self.assertTrue("blahblah" in get(fh[2]))
        # check replace works
        reset()
        us = "\N{BLACK STAR}8\N{WHITE STAR}"
        write_whole_file(TESTFILEPREFIX + "test-shell-1",
                         "w",
                         f"insert into enctest values('{ us }');",
                         encoding="utf8")
        cmd(".encoding utf8\n.read %stest-shell-1\n.encoding cp437:replace\n.output %stest-shell-1\nselect * from enctest;\n.encoding utf8\n.output stdout"
            % (TESTFILEPREFIX, TESTFILEPREFIX))
        s.cmdloop()
        isempty(fh[2])
        isempty(fh[1])
        self.assertTrue("?8?" in read_whole_file(TESTFILEPREFIX + "test-shell-1", "rt", "cp437"))

        ###
        ### Command - exceptions
        ###
        reset()
        cmd("syntax error;")
        s.cmdloop()
        isempty(fh[1])
        isnotempty(fh[2])
        self.assertTrue(len(get(fh[2]).split("\n")) < 5)
        reset()
        s.db.create_scalar_function("make_error", lambda: 1 / 0)
        cmd(".exceptions on\nselect make_error();")
        s.cmdloop()
        isempty(fh[1])
        isnotempty(fh[2])
        self.assertTrue(len(get(fh[2]).split("\n")) > 10)
        self.assertTrue("sql = " in get(fh[2]))
        # deliberately leave exceptions on

        ###
        ### Command - exit
        ###
        for i in (".exit", ):
            reset()
            cmd(i)
            self.assertRaises(SystemExit, s.cmdloop)
            isempty(fh[1])
            isempty(fh[2])
            reset()
            cmd(i + " jjgflk")
            s.cmdloop()
            isempty(fh[1])
            isnotempty(fh[2])

        ###
        ### Explain auto format
        ###
        reset()
        cmd(".mode tcl\n.header off\nselect 3;")
        s.cmdloop()
        isempty(fh[2])
        self.assertIn('"3"', get(fh[1]))
        reset()
        cmd("explain select 3;")
        s.cmdloop()
        isempty(fh[2])
        self.assertIn('opcode', get(fh[1]))
        reset()
        cmd("explain query plan select 3;")
        s.cmdloop()
        isempty(fh[2])
        self.assertIn('detail', get(fh[1]))
        reset()
        cmd("select 3;")
        s.cmdloop()
        isempty(fh[2])
        self.assertIn('"3"', get(fh[1]))

        ###
        ### Command find
        ###
        reset()
        cmd(".find one two three")
        s.cmdloop()
        isempty(fh[1])
        isnotempty(fh[2])
        reset()
        cmd("create table findtest([x\" x],y); insert into findtest values(3, 'xx3'); insert into findtest values(34, 'abcd');"
            )
        s.cmdloop()
        isempty(fh[1])
        isempty(fh[2])
        reset()
        cmd(".find 3")
        s.cmdloop()
        isempty(fh[2])
        for text, present in (("findtest", True), ("xx3", True), ("34", False)):
            if present:
                self.assertTrue(text in get(fh[1]))
            else:
                self.assertTrue(text not in get(fh[1]))
        reset()
        cmd(".find does-not-exist")
        s.cmdloop()
        isempty(fh[1])
        isempty(fh[2])
        reset()
        cmd(".find ab_d")
        s.cmdloop()
        isempty(fh[2])
        for text, present in (("findtest", True), ("xx3", False), ("34", True)):
            if present:
                self.assertTrue(text in get(fh[1]))
            else:
                self.assertTrue(text not in get(fh[1]))
        reset()
        cmd(".find 3 table-not-exist")
        s.cmdloop()
        isempty(fh[1])
        isempty(fh[2])

        ###
        ### Command help
        ###
        reset()
        cmd(".help\n.help all\n.help import backup")
        s.cmdloop()
        isempty(fh[1])
        for i in ".import", "Reads data from the file":
            self.assertTrue(i in get(fh[2]))
        reset()
        cmd(".help backup notexist import")
        s.cmdloop()
        isempty(fh[1])
        for i in "Copies the contents", "No such command":
            self.assertTrue(i in get(fh[2]))
        # screw up terminal width
        origtw = s._terminal_width

        def tw(minimum):
            return minimum

        s._terminal_width = tw
        reset()
        cmd(".bail on\n.help all\n.bail off")
        s.cmdloop()
        isempty(fh[1])
        isnotempty(fh[2])

        ###
        ### Command - import
        ###
        # check it fundamentally works
        reset()
        cmd(".encoding utf16\ncreate table imptest(x real, y char);\n"
            "insert into imptest values(3.1, 'xabc');\n"
            "insert into imptest values(3.2, 'xabfff\"ffffc');\n"
            ".output %stest-shell-1\n.mode csv\nselect * from imptest;\n"
            ".output stdout" % (TESTFILEPREFIX, ))
        s.cmdloop()
        isempty(fh[1])
        isempty(fh[2])
        # make sure encoding took
        self.assertTrue(b"xab" not in read_whole_file(TESTFILEPREFIX + "test-shell-1", "rb"))
        data = s.db.cursor().execute("select * from imptest; delete from imptest").fetchall()
        self.assertEqual(2, len(data))
        reset()
        cmd(".import %stest-shell-1 imptest" % (TESTFILEPREFIX, ))
        s.cmdloop()
        isempty(fh[1])
        isempty(fh[2])
        newdata = s.db.cursor().execute("select * from imptest; drop table imptest").fetchall()
        data.sort()
        newdata.sort()
        self.assertEqual(data, newdata)
        # error handling
        for i in ".import", ".import one", ".import one two three", ".import nosuchfile nosuchtable", ".import nosuchfile sqlite_schema":
            reset()
            cmd(i)
            s.cmdloop()
            isempty(fh[1])
            isnotempty(fh[2])
        # wrong number of columns
        reset()
        cmd("create table imptest(x,y);\n.mode tabs\n.output %stest-shell-1\nselect 3,4;select 5,6;select 7,8,9;" %
            (TESTFILEPREFIX, ))
        s.cmdloop()
        isempty(fh[1])
        isempty(fh[2])
        reset()
        cmd(".output stdout\n.import %stest-shell-1 imptest" % (TESTFILEPREFIX, ))
        s.cmdloop()
        isempty(fh[1])
        isnotempty(fh[2])
        reset()
        # check it was done in a transaction and aborted
        self.assertEqual(0, s.db.cursor().execute("select count(*) from imptest").fetchall()[0][0])

        ###
        ### Command - autoimport
        ###

        # errors
        for i in ".autoimport", ".autoimport 1 2 3", ".autoimport nosuchfile", ".autoimport %stest-shell-1 sqlite_schema" % (
                TESTFILEPREFIX, ):
            reset()
            cmd(i)
            s.cmdloop()
            isempty(fh[1])
            isnotempty(fh[2])

        # check correct detection with each type of separator and that types are not mangled
        c = s.db.cursor()
        for row in (
            ('a,b', '21/1/20', '00'),
            ('  ', '1/1/20', 10),
            ('a"b', '1/1/01', '00'),
            ('+40', '01123', '2010 100 15'),
            ('2010//10//13', '2010/10/13  12', 2),
            ('2010/13/13 12:13', '13/13/2010 12:93', '13/2010/13'),
            ("+3", " 3", 3),
            ("03.03", "03.03.20", "03"),
            (
                (None, 2, 5.5),
                (None, 4, 99),
            ),
        ):

            c.execute("""drop table if exists aitest ; create table aitest("x y", ["], "3d")""")
            if isinstance(row[0], tuple):
                f = c.executemany
            else:
                f = c.execute
            f("insert into aitest values(?,?,?)", row)
            fname = TESTFILEPREFIX + "test-shell-1"
            for sep in "\t", "|", ",", "X":
                reset()
                cmd(".mode csv\n.headers on\n.output %stest-shell-1\n.separator \"%s\"\nselect * from aitest;\n.output stdout\n.separator X\ndrop table if exists \"test-shell-1\";\n.autoimport %stest-shell-1"
                    % (TESTFILEPREFIX, sep, TESTFILEPREFIX))
                s.cmdloop()
                isnotempty(fh[1])
                isempty(fh[2])
                self.assertTablesEqual(s.db, "aitest", s.db, "test-shell-1")

        # Change encoding back to sensible
        reset()
        cmd(".encoding utf8")
        s.cmdloop()

        # Check date detection
        for expect, fmt, sequences in (("1999-10-13", "%d-%d:%d", (
            (1999, 10, 13),
            (13, 10, 1999),
            (10, 13, 1999),
        )), ("1999-10-13T12:14:17", "%d/%d/%d/%d/%d/%d", (
            (1999, 10, 13, 12, 14, 17),
            (13, 10, 1999, 12, 14, 17),
            (10, 13, 1999, 12, 14, 17),
        )), ("1999-10-13T12:14:00", "%dX%dX%dX%dX%d", (
            (1999, 10, 13, 12, 14),
            (13, 10, 1999, 12, 14),
            (10, 13, 1999, 12, 14),
        ))):
            for seq in sequences:
                write_whole_file(TESTFILEPREFIX + "test-shell-1", "wt", ("a,b\nrow," + (fmt % seq) + "\n"))
                reset()
                cmd("drop table [test-shell-1];\n.autoimport %stest-shell-1" % (TESTFILEPREFIX, ))
                s.cmdloop()
                isempty(fh[2])
                imp = c.execute("select b from [test-shell-1] where a='row'").fetchall()[0][0]
                self.assertEqual(imp, expect)

        # Check diagnostics when unable to import
        for err, content in (
            ("current encoding", b"\x81\x82\x83\tfoo\n\x84\x97\xff\tbar"),
            ("known type", "abcdef\nhiojklmnop\n"),
            ("more than one", 'ab,c\tdef\nqr,dd\t\n'),
            ("ambiguous data format", "a,b\n1/1/2001,3\n2001/4/4,4\n"),
        ):
            if isinstance(content, bytes):
                continue
            write_whole_file(TESTFILEPREFIX + "test-shell-1", "wt", content)
            reset()
            cmd("drop table [test-shell-1];\n.autoimport %stest-shell-1" % (TESTFILEPREFIX, ))
            s.cmdloop()
            errmsg = get(fh[2])
            self.assertTrue(err in errmsg)

        ###
        ### Command - indices
        ###
        for i in ".indices", ".indices one two":
            reset()
            cmd(i)
            s.cmdloop()
            isempty(fh[1])
            isnotempty(fh[2])
        reset()
        cmd("create table indices(x unique, y unique); create index shouldseethis on indices(x,y);")
        s.cmdloop()
        isempty(fh[1])
        isempty(fh[2])
        reset()
        cmd(".indices indices")
        s.cmdloop()
        isempty(fh[2])
        for i in "shouldseethis", "autoindex":
            self.assertTrue(i in get(fh[1]))

        ###
        ### Command - load
        ###
        if hasattr(APSW, "testLoadExtension"):
            lf = LOADEXTENSIONFILENAME
            for i in ".load", ".load one two three":
                reset()
                cmd(i)
                s.cmdloop()
                isempty(fh[1])
                isnotempty(fh[2])
            reset()
            cmd(".load nosuchfile")
            s.cmdloop()
            isempty(fh[1])
            self.assertTrue("nosuchfile" in get(fh[2]) or "ExtensionLoadingError" in get(fh[2]))
            reset()
            cmd(".mode list\n.load " + lf + " alternate_sqlite3_extension_init\nselect doubleup(2);")
            s.cmdloop()
            isempty(fh[2])
            self.assertTrue("4" in get(fh[1]))
            reset()
            cmd(".mode list\n.load " + lf + "\nselect half(2);")
            s.cmdloop()
            isempty(fh[2])
            self.assertTrue("1" in get(fh[1]))

        ###
        ### Command - log
        ###
        reset()
        cmd(".log on\n+;")
        s.cmdloop()
        self.assertIn("SQLITE_ERROR", get(fh[2]))
        reset()
        cmd(".log off\n+;")
        s.cmdloop()
        self.assertNotIn("SQLITE_ERROR", get(fh[2]))

        ###
        ### Command - mode
        ###
        # already thoroughly tested in code above
        for i in ".mode", ".mode foo more", ".mode invalid":
            reset()
            cmd(i)
            s.cmdloop()
            isempty(fh[1])
            isnotempty(fh[2])

        ###
        ### command nullvalue & separator
        ###
        # already tested in code above
        for i in ".nullvalue", ".nullvalue jkhkl lkjkj", ".separator", ".separator one two":
            reset()
            cmd(i)
            b4 = s.nullvalue, s.separator
            s.cmdloop()
            isempty(fh[1])
            isnotempty(fh[2])
            self.assertEqual(b4, (s.nullvalue, s.separator))

        ###
        ### command output
        ###
        for i in ".output", ".output too many args", ".output " + os.sep:
            reset()
            cmd(i)
            b4 = s.stdout
            s.cmdloop()
            isempty(fh[1])
            isnotempty(fh[2])
            self.assertEqual(b4, s.stdout)

        ###
        ### Command - parameter
        ###
        reset()
        cmd("select :foo;")
        s.cmdloop()
        isempty(fh[1])
        self.assertIn("No binding present for 'foo' -", get(fh[2]))
        for val in ('orange', 'banana'):
            reset()
            cmd(f".parameter set foo '{ val }'\nselect $foo;")
            s.cmdloop()
            isempty(fh[2])
            self.assertIn(val, get(fh[1]))
        reset()
        cmd(".parameter list")
        s.cmdloop()
        isempty(fh[2])
        self.assertIn("banana", get(fh[1]))
        reset()
        cmd(".parameter unset foo\nselect $foo;")
        s.cmdloop()
        isempty(fh[1])
        self.assertIn("No binding present for 'foo' -", get(fh[2]))
        reset()
        cmd(".parameter set bar 3\n.parameter clear\nselect $bar;")
        s.cmdloop()
        isempty(fh[1])
        self.assertIn("No binding present for 'bar' -", get(fh[2]))

        ###
        ### Command prompt
        ###
        # not much to test until pty testing is working
        for i in ".prompt", ".prompt too many args":
            reset()
            cmd(i)
            b4 = s.prompt, s.moreprompt
            s.cmdloop()
            isempty(fh[1])
            isnotempty(fh[2])
            self.assertEqual(b4, (s.prompt, s.moreprompt))

        ###
        ### Command - py
        ###
        reset()
        cmd(".py None")
        s.cmdloop()
        isempty(fh[1])
        isempty(fh[2])
        reset()
        cmd(".py [")
        s.cmdloop()
        self.assertIn("Incomplete", get(fh[2]))
        # interactive use is tested interactively

        ###
        ### Command read
        ###
        # pretty much thoroughly tested above
        write_whole_file(TESTFILEPREFIX + "test-shell-1.py", "wt", """
assert apsw
assert shell
shell.write(shell.stdout, "hello world\\n")
""")
        for i in ".read", ".read one two", ".read " + os.sep:
            reset()
            cmd(i)
            s.cmdloop()
            isempty(fh[1])
            isnotempty(fh[2])

        reset()
        cmd(".read %stest-shell-1.py" % (TESTFILEPREFIX, ))
        s.cmdloop()
        isempty(fh[2])
        self.assertTrue("hello world" in get(fh[1]))

        # restore tested with backup

        ###
        ### Command - schema
        ###
        # make sure it works
        reset()
        cmd(".schema")
        s.cmdloop()
        isempty(fh[2])
        isnotempty(fh[1])
        reset()
        cmd("create table schematest(x);create index unrelatedname on schematest(x);\n.schema schematest foo notexist foo"
            )
        s.cmdloop()
        isempty(fh[2])
        for i in "schematest", "unrelatedname":
            self.assertTrue(i in get(fh[1]))

        # separator done earlier

        ###
        ### Command - shell
        ###
        reset()
        # this uses the process stdout/err which we can't capture without heroics
        cmd(".shell exit 1")
        s.cmdloop()
        self.assertIn("Exit code", get(fh[2]))
        reset()
        cmd(".shell %s > %s" % ("dir" if sys.platform == "win32" else "ls", os.devnull))
        s.cmdloop()
        # should always work on these platforms
        if sys.platform in {"win32", "linux", "darwin"}:
            self.assertNotIn("Exit code", get(fh[2]))

        ###
        ### Command - show
        ###
        # set all settings to known values
        resetcmd = ".echo off\n.changes off\n.headers off\n.mode list\n.nullvalue ''\n.output stdout\n.separator |\n.width 1 2 3\n.exceptions off"
        reset()
        cmd(resetcmd)
        s.cmdloop()
        isempty(fh[2])
        isempty(fh[1])
        reset()
        cmd(".show")
        s.cmdloop()
        isempty(fh[1])
        isnotempty(fh[2])
        baseline = get(fh[2])
        for i in ".echo on", ".changes on", ".headers on", ".mode column", ".nullvalue T", ".separator %", ".width 8 9 1", ".exceptions on":
            reset()
            cmd(resetcmd)
            s.cmdloop()
            isempty(fh[1])
            if not get(fh[2]).startswith(".echo off"):
                isempty(fh[2])
            reset()
            cmd(i + "\n.show")
            s.cmdloop()
            isempty(fh[1])
            # check size has not changed much
            self.assertTrue(abs(len(get(fh[2])) - len(baseline)) < 14)

        # output
        reset()
        cmd(".output %stest-shell-1\n.show" % (TESTFILEPREFIX, ))
        s.cmdloop()
        isempty(fh[1])
        self.assertTrue("output: " + TESTFILEPREFIX + "test-shell-1" in get(fh[2]))
        reset()
        cmd(".output stdout\n.show")
        s.cmdloop()
        isempty(fh[1])
        self.assertTrue("output: stdout" in get(fh[2]))
        self.assertTrue(not os.path.exists("stdout"))
        # errors
        reset()
        cmd(".show one two")
        s.cmdloop()
        isempty(fh[1])
        self.assertTrue("at most one parameter" in get(fh[2]))
        reset()
        cmd(".show notexist")
        s.cmdloop()
        isempty(fh[1])
        self.assertTrue("notexist: " not in get(fh[2]))

        ###
        ### Command tables
        ###
        reset()
        cmd(".tables")
        s.cmdloop()
        isempty(fh[2])
        isnotempty(fh[1])
        reset()
        cmd("create table tabletest(x);create index tabletest1 on tabletest(x);create index noway on tabletest(x);\n.tables tabletest\n.tables"
            )
        s.cmdloop()
        isempty(fh[2])
        self.assertTrue("tabletest" in get(fh[1]))
        self.assertTrue("tabletest1" not in get(fh[1]))
        self.assertTrue("noway" not in get(fh[1]))

        ###
        ### Command timeout
        ###
        for i in (".timeout", ".timeout ksdjfh", ".timeout 6576 78987"):
            reset()
            cmd(i)
            s.cmdloop()
            isempty(fh[1])
            isnotempty(fh[2])
        for i in (".timeout 1000", ".timeout 0", ".timeout -33"):
            reset()
            cmd(i)
            s.cmdloop()
            isempty(fh[1])
            isempty(fh[2])

        # timer is tested earlier

        ###
        ### Command - version
        ###
        reset()
        cmd(".version")
        s.cmdloop()
        isempty(fh[2])
        self.assertIn(apsw.apsw_version(), get(fh[1]))

        ###
        ### Command - vfsname / vfsinfo / vfslist
        ###
        name = s.db.open_vfs
        reset()
        cmd(".vfsname")
        s.cmdloop()
        self.assertEqual(s.db.vfsname('main') or "", get(fh[1]).strip())
        reset()
        cmd(".vfsname temp")
        s.cmdloop()
        self.assertEqual(s.db.vfsname('temp') or "", get(fh[1]).strip())
        reset()
        cmd(".vfsinfo")
        s.cmdloop()
        self.assertIn(name, get(fh[1]))
        reset()
        cmd(".vfslist")
        s.cmdloop()
        self.assertIn(name, get(fh[1]))

        ###
        ### Command width
        ###
        # does it work?
        reset()
        cmd(".width 10 10 10 0")
        s.cmdloop()
        isempty(fh[1])
        isempty(fh[2])

        def getw():
            reset()
            cmd(".show width")
            s.cmdloop()
            isempty(fh[1])
            return [int(x) for x in get(fh[2]).split()[1:]]

        self.assertEqual([10, 10, 10, 0], getw())
        # some errors
        for i in ".width", ".width foo", ".width 1 2 3 seven 3":
            reset()
            cmd(i)
            s.cmdloop()
            isempty(fh[1])
            isnotempty(fh[2])
            self.assertEqual([10, 10, 10, 0], getw())
        for i, r in ("9 0 9", [9, 0, 9]), ("10 -3 10 -3", [10, -3, 10, -3]), ("0", [0]):
            reset()
            cmd(".width " + i)
            s.cmdloop()
            isempty(fh[1])
            isempty(fh[2])
            self.assertEqual(r, getw())

        ###
        ### Unicode output with all output modes
        ###
        colname = "\N{BLACK STAR}8\N{WHITE STAR}"
        val = 'xxx\u1234\uabcdyyy this\" is nasty\u0001stuff!'
        noheadermodes = ('insert', )
        # possible ways val can be represented (eg csv doubles up double quotes)
        outputs = (val, val.replace('"', '""'), val.replace('"', '&quot;'), val.replace('"', '\\"'))
        for mode in [x[len("output_"):] for x in dir(shellclass) if x.startswith("output_")]:
            if mode in ("qbox", "table", "box"):
                # apsw.ext.format_query_table already tested elsewhere
                continue
            reset()
            cmd(".separator |\n.width 999\n.encoding utf8\n.header on\n.mode %s\nselect '%s' as '%s';" %
                (mode, val, colname))
            s.cmdloop()
            isempty(fh[2])
            # modes too complicated to construct the correct string
            if mode in ('python', 'tcl'):
                continue
            # all others
            if mode not in noheadermodes:
                self.assertIn(colname if "json" not in mode else json.dumps(colname), get(fh[1]))
            cnt = 0
            for o in outputs:
                cnt += (o if "json" not in mode else json.dumps(o)) in get(fh[1])
            self.assertTrue(cnt)

        # clean up files
        for f in fh:
            f.close()

    # Note that faults fire only once, so there is no need to reset
    # them.  The testing for objects bigger than 2GB is done in
    # testLargeObjects
    def testzzFaultInjection(self):
        "Deliberately inject faults to exercise all code paths"
        if not getattr(apsw, "test_fixtures_present", None):
            return

        apsw.faultdict = dict()

        def ShouldFault(name, pending_exception):
            r = apsw.faultdict.get(name, False)
            apsw.faultdict[name] = False
            return r

        sys.apsw_should_fault = ShouldFault

        # Verify we test all fault locations
        code = []
        for fn in glob.glob("*/*.c"):
            with open(fn, encoding="utf8") as f:
                code.append(f.read())
        code = "\n".join(code)

        with open(__file__, "rt", encoding="utf8") as f:
            test_code = f.read()

        seen = set()

        for macro, faultname in re.findall(r"(APSW_FAULT_INJECT)\s*[(]\s*(?P<fault_name>.*?)\s*,", code):
            if faultname == "faultName":
                continue
            if faultname not in test_code:
                raise Exception(f"Fault injected { faultname } not found in tests.py")
            if faultname in seen:
                raise Exception(f"Fault { faultname } seen multiple times")
            seen.add(faultname)

        def dummy(*args):
            1 / 0

        def dummy2(*args):
            return 7

        # The 1/0 in these tests is to cause a ZeroDivisionError so
        # that an exception is always thrown.  If we catch that then
        # it means earlier expected exceptions were not thrown.

        ## Virtual table code
        class Source:

            def Create(self, *args):
                return "create table foo(x,y)", Table()

            Connect = Create

        class Table:

            def __init__(self):
                self.data = [  #("rowid", "x", "y"),
                    [0, 1, 2], [3, 4, 5]
                ]

            def Open(self):
                return Cursor(self)

            def BestIndex(self, *args):
                return None

            def UpdateChangeRow(self, rowid, newrowid, fields):
                for i, row in enumerate(self.data):
                    if row[0] == rowid:
                        self.data[i] = [newrowid] + list(fields)

            def FindFunction(self, *args):
                return lambda *args: 1

        class Cursor:

            def __init__(self, table):
                self.table = table
                self.row = 0

            def Eof(self):
                return self.row >= len(self.table.data)

            def Rowid(self):
                return self.table.data[self.row][0]

            def Column(self, col):
                return self.table.data[self.row][1 + col]

            def Filter(self, *args):
                self.row = 0

            def Next(self):
                self.row += 1

            def Close(self):
                pass

        ## BlobDeallocException
        def f():
            db = apsw.Connection(":memory:")
            db.cursor().execute("create table foo(b);insert into foo(rowid,b) values(2,x'aabbccddee')")
            blob = db.blob_open("main", "foo", "b", 2, False)  # open read-only
            # deliberately cause problem
            try:
                blob.write(b'a')
            except apsw.ReadOnlyError:
                pass
            # garbage collect
            del blob
            gc.collect()

        self.assertRaisesUnraisable(apsw.ReadOnlyError, f)

        ## ConnectionReadError
        self.db.pragma("application_id", 0xdeadbeef)
        self.db.read("main", 0, 0, 1)
        apsw.faultdict["ConnectionReadError"] = True
        self.assertRaises(apsw.IOError, self.db.read, "main", 0, 0, 1)

        ### vfs routines

        class FaultVFS(apsw.VFS):

            def __init__(self, name="faultvfs", inherit="", makedefault=False):
                super().__init__(name, inherit, makedefault=makedefault)

            def xGetLastErrorLong(self):
                return "a" * 1024, None

            def xOpen(self, name, flags):
                return FaultVFSFile(name, flags)

        class FaultVFSFile(apsw.VFSFile):

            def __init__(self, name, flags):
                super().__init__("", name, flags)

        vfs = FaultVFS()

        ## APSWVFSBadVersion
        apsw.faultdict["APSWVFSBadVersion"] = True
        self.assertRaises(ValueError, apsw.VFS, "foo", "")
        self.assertTrue("foo" not in apsw.vfs_names())

        ## xReadReadBufferFail
        try:
            # This will fail if we are using auto-WAL so we don't run
            # the rest of the test in WAL mode.
            apsw.Connection(TESTFILEPREFIX + "testdb", vfs="faultvfs").cursor().execute("create table dummy1(x,y)")
            openok = True
        except apsw.CantOpenError:
            if len(apsw.connection_hooks) == 0:
                raise
            openok = False

        # The following tests cause failures when making the
        # connection because a connection hook turns on wal mode which
        # causes database reads which then cause failures
        if openok:

            ## xUnlockFails
            apsw.faultdict["xUnlockFails"] = True
            self.assertRaises(apsw.IOError,
                              apsw.Connection(TESTFILEPREFIX + "testdb", vfs="faultvfs").cursor().execute,
                              "select * from dummy1")

            ## xSyncFails
            apsw.faultdict["xSyncFails"] = True
            self.assertRaises(apsw.IOError,
                              apsw.Connection(TESTFILEPREFIX + "testdb", vfs="faultvfs").cursor().execute,
                              "insert into dummy1 values(3,4)")

            ## xFileSizeFails
            apsw.faultdict["xFileSizeFails"] = True
            self.assertRaises(apsw.IOError,
                              apsw.Connection(TESTFILEPREFIX + "testdb", vfs="faultvfs").cursor().execute,
                              "select * from dummy1")

        ## xCheckReservedLockFails
        apsw.faultdict["xCheckReservedLockFails"] = True
        self.assertRaises(apsw.IOError, vfstestdb, vfsname="faultvfs")

        ## xCheckReservedLockIsTrue
        apsw.faultdict["xCheckReservedLockIsTrue"] = True
        vfstestdb(vfsname="faultvfs")

        ## xCloseFails
        t = apsw.VFSFile("", os.path.abspath(TESTFILEPREFIX + "testfile"),
                         [apsw.SQLITE_OPEN_MAIN_DB | apsw.SQLITE_OPEN_CREATE | apsw.SQLITE_OPEN_READWRITE, 0])
        apsw.faultdict["xCloseFails"] = True
        self.assertRaises(apsw.IOError, t.xClose)
        del t

        # now catch it in the destructor
        def foo():
            t = apsw.VFSFile("", os.path.abspath(TESTFILEPREFIX + "testfile"),
                             [apsw.SQLITE_OPEN_MAIN_DB | apsw.SQLITE_OPEN_CREATE | apsw.SQLITE_OPEN_READWRITE, 0])
            apsw.faultdict["xCloseFails"] = True
            del t
            gc.collect()

        self.assertRaisesUnraisable(apsw.IOError, foo)

        ## BlobWriteTooBig
        apsw.faultdict["BlobWriteTooBig"] = True
        self.db.execute("CREATE TABLE blobby(x); insert into blobby values (zeroblob(1000))")
        blob = self.db.blob_open("main", "blobby", "x", self.db.last_insert_rowid(), True)
        self.assertRaises(ValueError, blob.write, b"1234")

        for k, v in apsw.faultdict.items():
            assert v is False, f"faultdict { k } never fired"

    def testFunctionFlags(self) -> None:
        "Flags to registered SQLite functions"
        self.db.create_scalar_function("donotcall", lambda x: x / 0, flags=apsw.SQLITE_DIRECTONLY)
        self.db.execute("""
            create table foo(y);
            insert into foo values(7);
            create view bar(z) as select donotcall(y) from foo;
        """)
        try:
            self.db.execute("select * from bar")
            1 / 0  # should not be reached
        except apsw.SQLError as e:
            self.assertIn("unsafe use of donotcall", str(e))

    def testBestPractice(self) -> None:
        "apsw.bestpractice module"
        if sys.version_info >= (3, 10):
            with self.assertNoLogs():
                apsw.log(apsw.SQLITE_NOMEM, "Zebras are striped")
        apsw.bestpractice.apply(apsw.bestpractice.recommended)
        with self.assertLogs() as l:
            apsw.log(apsw.SQLITE_NOMEM, "Zebras are striped")
        self.assertIn("Zebras", str(l.output))

        dqs = 'select "world"'

        with self.assertLogs() as l:
            # this connection was made before applying bestpractice
            self.db.execute(dqs)  # no error but does log

        self.assertIn("double-quoted string literal", str(l.output))

        con = apsw.Connection("")
        # now fail
        apsw.config(apsw.SQLITE_CONFIG_LOG, None)
        if sys.version_info >= (3, 10):
            with self.assertNoLogs():
                self.assertRaises(apsw.SQLError, con.execute, dqs)

    def testExtDataClassRowFactory(self) -> None:
        "apsw.ext.DataClassRowFactory"
        dcrf = apsw.ext.DataClassRowFactory()
        self.db.set_row_trace(dcrf)
        # sanity check
        for row in self.db.execute("select 3 as three, 'four' as four"):
            self.assertEqual(row.three, 3)
            self.assertEqual(row.four, 'four')
            row.four = "five"  # not frozen
        # rename check
        for row in self.db.execute("select 3 as three, 'four' as [4]"):
            self.assertEqual(row.three, 3)
            self.assertEqual(row._1, 'four')
        # no rename, kwargs
        dcrf2 = apsw.ext.DataClassRowFactory(rename=False, dataclass_kwargs={"frozen": True})
        self.db.set_row_trace(dcrf2)
        self.assertRaises(TypeError, self.db.execute("select 4 as [4]").fetchall)
        for row in self.db.execute("select 3 as three"):
            try:
                row.three = 4
            except dataclasses.FrozenInstanceError:
                pass
        db = apsw.Connection("")
        db.set_row_trace(dcrf)
        for row in db.execute(
                "create table foo([x y] some random typename here); insert into foo values(3); select * from foo"):
            self.assertEqual(row.__description__, (('x y', 'some random typename here'), ))
        # type annotations
        self.db.set_row_trace(dcrf)
        self.db.execute(
            "create table foo(one [], two [an integer], three VARCHAR(17), four cblob, five doUBl, six [none of those]); insert into foo values(1,2,3,4,5,6)"
        )
        self.assertEqual(dcrf.get_type("an integer"), int)
        for row in self.db.execute("select * from foo"):
            a = row.__annotations__
            self.assertEqual(a["one"], typing.Any)
            self.assertEqual(a["two"], int)
            self.assertEqual(a["three"], str)
            self.assertEqual(a["four"], bytes)
            self.assertEqual(a["five"], float)
            self.assertEqual(a["six"], typing.Union[float, int])

    def testExtTypesConverter(self) -> None:
        "apsw.ext.TypesConverterCursorFactory"
        tccf = apsw.ext.TypesConverterCursorFactory()

        class Point(apsw.ext.SQLiteTypeAdapter):

            def to_sqlite_value(self):
                return 3

        tccf.register_adapter(complex, lambda c: f"{ c.real };{ c.imag }")
        tccf.register_converter("COMPLEX", lambda v: complex(*(float(part) for part in v.split(";"))))
        self.db.cursor_factory = tccf
        self.db.execute("create table foo(a POINT, b COMPLEX)")
        self.db.execute("insert into foo values(?,?);", (Point(), 3 + 4j))
        self.db.execute(" insert into foo values(:one, :two)", {"one": Point(), "two": 3 + 4j})

        def datas():
            for _ in range(10):
                yield (Point(), 3 + 4j)

        self.db.executemany("insert into foo values(?,?)", datas())
        for row in self.db.execute("select * from foo"):
            self.assertEqual(row[0], 3)
            self.assertEqual(row[1], 3 + 4j)

        self.assertRaises(TypeError, tccf.adapt_value, {})
        self.assertEqual(tccf.convert_value("zebra", "zebra"), "zebra")

        def builtin_types():
            yield (None, )
            yield (3, )
            yield (b"aabbccddee", )
            yield ("hello world", )
            yield (3.1415, )

        self.assertEqual(self.db.executemany("select ?", builtin_types()).fetchall(), list(builtin_types()))

        class NotImplemented(apsw.ext.SQLiteTypeAdapter):
            pass

        self.assertRaises(TypeError, NotImplemented)

    def testExtStuff(self):
        "Various apsw.ext functions"
        apsw.ext.make_virtual_module(self.db, "g1", apsw.ext.generate_series)
        vals = {row[0] for row in self.db.execute("select value from g1 where start=1 and stop=10")}
        self.assertEqual(vals, {i for i in range(1, 10 + 1)})
        vals = {row[0] for row in self.db.execute("select value from g1 where start=10 and stop=1")}
        self.assertEqual(vals, {i for i in range(1, 10 + 1)})
        vals = {row[0] for row in self.db.execute("select value from g1(1, 10, 2)")}
        self.assertEqual(vals, {i for i in range(1, 10 + 1, 2)})
        self.assertRaises(ValueError, self.db.execute, "select *,start,stop,step from g1(1,10) where step=0")
        vals = [row[0] for row in self.db.execute("select value from g1(0.0, 1, 0.1)")]
        expected = [i / 10 for i in range(0, 10 + 1)]
        for l, r in zip(vals, expected):
            self.assertAlmostEqual(l, r)
        self.assertRaises(TypeError, self.db.execute, "select * from g1()")

        apsw.ext.make_virtual_module(self.db, "g2", apsw.ext.generate_series_sqlite)
        vals = {row[0] for row in self.db.execute("select value from g2 where start=1 and stop=10")}
        self.assertEqual(vals, {i for i in range(1, 10 + 1)})
        vals = {row[0] for row in self.db.execute("select value from g2 where start=1 and stop=10 and step=-1")}
        self.assertEqual(vals, {i for i in range(1, 10 + 1)})
        vals = {row[0] for row in self.db.execute("select value from g2(1, 10, 2)")}
        self.assertEqual(vals, {i for i in range(1, 10 + 1, 2)})
        self.assertEqual(
            self.db.execute("select *,start,stop from g2(1,10) where step=0").get,
            self.db.execute("select *,start,stop from g2(1,10) where step=1").get)
        self.assertRaises(ValueError, self.db.execute, "select * from g2 where stop=10 and step=1")
        self.assertRaises(TypeError, self.db.execute, "select * from g2(0.1, 1, 1)")

        # https://github.com/rogerbinns/apsw/issues/412
        apsw.ext.make_virtual_module(self.db, "genseries", apsw.ext.generate_series)
        apsw.ext.make_virtual_module(self.db, "genseries2", apsw.ext.generate_series)
        self.db.execute("""select *,genseries.start,genseries2.stop,genseries.step from genseries(1,10)
            inner join genseries2(10,1) on genseries.value=genseries2.value
            group by genseries2.value""")

        def stuff_dict():
            for i in range(10):
                yield {
                    "a a": i,
                    "bb": 3.1415,
                    " cc": b"aabbccddeeff",
                    "dd": None,
                    # the chr gives a codepoint that has no name in unicodedata
                    "eeeeeeeeeeeeeeeeeeee": f"\\\n\f\v\t\n\0{ i }" + chr(0x10ffff) + "\r \n\r\\ \nr\r \n\r \n\n",
                    "f": APSW.wikipedia_text,
                }

        def stuff_attr():

            class x:
                pass

            for row in stuff_dict():
                for k, v in row.items():
                    setattr(x, k, v)
                yield x

        def stuff_tuple():
            for row in stuff_dict():
                yield tuple(row.values())

        @dataclasses.dataclass
        class xx:
            one: str
            two: str
            three: str
            four: str
            five: str
            six: str

            def method(self):
                1 / 0

        def stuff_dataclass():
            for row in stuff_tuple():
                yield xx(*row)

        def stuff_stat_struct():
            yield os.stat(".")
            yield os.stat("..")

        nt = collections.namedtuple("nt", next(stuff_dict()).keys(), rename=True)

        def stuff_namedtuple():
            for row in stuff_tuple():
                yield nt(*row)

        for func, access, names in (
            (stuff_dict, apsw.ext.VTColumnAccess.By_Name, ('a a', 'bb', ' cc', 'dd', 'eeeeeeeeeeeeeeeeeeee', "f")),
            (stuff_tuple, apsw.ext.VTColumnAccess.By_Index, ('column0', 'column1', 'column2', 'column3', 'column4',
                                                             "column5")),
            (stuff_dataclass, apsw.ext.VTColumnAccess.By_Attr, ('one', 'two', 'three', 'four', 'five', 'six')),
            (stuff_namedtuple, apsw.ext.VTColumnAccess.By_Index, ('_0', 'bb', '_2', 'dd', 'eeeeeeeeeeeeeeeeeeee', 'f')),
            (stuff_stat_struct, apsw.ext.VTColumnAccess.By_Attr, None),
        ):
            if func is stuff_stat_struct:
                if sys.version_info < (3, 10):
                    a = apsw.ext.VTColumnAccess.By_Attr
                    n = names = tuple(member for member in dir(next(func())) if member.startswith("st_"))
                else:
                    n, a = apsw.ext.get_column_names(next(func()))
                    names = n
                    self.assertEqual(names, os.stat(".").__match_args__)
            else:
                n, a = apsw.ext.get_column_names(next(func()))
            self.assertEqual(access, a)
            self.assertEqual(names, n)
            func.columns = n
            func.column_access = a

        stuff_attr.columns = stuff_dict.columns
        stuff_attr.column_access = apsw.ext.VTColumnAccess.By_Attr

        self.assertRaises(TypeError, apsw.ext.get_column_names, object())

        funcs = (stuff_attr, stuff_dataclass, stuff_dict, stuff_tuple, stuff_stat_struct, stuff_namedtuple)

        def _nb():
            while True:
                yield random.choice((True, False))

        _nb = _nb()
        nb = lambda: next(_nb)

        def _ns():
            while True:
                yield 0
                yield 1
                yield 2
                yield lambda x: repr(x)

        _ns = _ns()
        ns = lambda: next(_ns)

        for f in funcs:
            apsw.ext.make_virtual_module(self.db, "workit", f, eponymous_only=nb(), repr_invalid=nb())
            apsw.ext.format_query_table(self.db,
                                        "select rowid, * from workit(); select * from workit()",
                                        bindings=tuple() if nb() else None,
                                        colour=nb(),
                                        quote=nb(),
                                        string_sanitize=ns(),
                                        truncate=3 if nb() else 35,
                                        text_width=30 if nb() else 130,
                                        use_unicode=nb(),
                                        word_wrap=nb())

        for width in range(200, 1, -17):
            try:
                apsw.ext.format_query_table(self.db,
                                            "select * from workit()",
                                            bindings=tuple() if nb() else None,
                                            colour=nb(),
                                            quote=nb(),
                                            string_sanitize=ns(),
                                            truncate=3 if nb() else 35,
                                            text_width=width,
                                            use_unicode=nb(),
                                            word_wrap=nb())
            except ValueError as e:
                self.assertIn("Results can't be fitted in text width", str(e))

        self.assertEqual(apsw.ext.format_query_table(self.db, "-- comment"), "")

        query = """
            -- comment
            select 3;
            -- another comment
            select 4;
        """
        self.db.row_trace = lambda x: 1 / 0
        apsw.ext.format_query_table(self.db, query)
        self.db.row_trace = None

        def messy(arg1, arg2, arg3=lambda x: 1 / 0):
            yield (1, )
            yield (object(), )

        messy.columns = ["x"]
        messy.column_access = "orange"

        try:
            apsw.ext.make_virtual_module(self.db, "messy", messy)
        except ValueError as e:
            self.assertIn("column_access", str(e))
            messy.column_access = apsw.ext.VTColumnAccess.By_Index

        messy.columns = ["arg1"]
        try:
            apsw.ext.make_virtual_module(self.db, "messy", messy)
        except ValueError as e:
            self.assertIn("Same name in ", str(e))
            self.assertIn("arg1", str(e))
            messy.columns = ["x"]

        messy.primary_key = 7
        try:
            apsw.ext.make_virtual_module(self.db, "messy", messy)
        except ValueError as e:
            self.assertIn("column number", str(e))
            messy.primary_key = 0

        for ri in (True, False):
            apsw.ext.make_virtual_module(self.db, "messy", messy, repr_invalid=True)
            self.db.execute("select *, arg3 from messy(1,2)").fetchall()

        try:
            self.db.execute("select * from messy(1,2,3,4)").fetchall()
        except apsw.SQLError as e:
            self.assertIn("too many arguments", str(e))

        self.assertRaises(TypeError, self.db.execute, "select * from messy(1)")
        self.assertRaises(ValueError, self.db.execute, "create virtual table fail using messy(1,2,3,4,5)")
        for query in (
                "select * from messy where arg1>3",
                "select * from messy(1,2) where arg2=7"
                "select * from messy(1,2) where arg2=7 and arg2=8",
                "select * from messy where arg1 in (1,2) and arg1=6",
        ):
            self.assertRaises(apsw.SQLError, self.db.execute, query)

    def testExtQueryInfo(self) -> None:
        "apsw.ext.query_info"
        qd = apsw.ext.query_info(self.db, "select 3; a syntax error")
        self.assertEqual(qd.query, "select 3; a syntax error")
        self.assertEqual(qd.bindings, None)
        self.assertEqual(qd.first_query, "select 3; ")
        self.assertEqual(qd.query_remaining, "a syntax error")
        self.assertEqual(qd.is_explain, 0)
        self.assertEqual(qd.is_readonly, True)
        self.assertEqual(qd.description, (('3', None), ))

        self.assertEqual(1, apsw.ext.query_info(self.db, "explain select 3").is_explain)
        self.assertEqual(2, apsw.ext.query_info(self.db, "explain query plan select 3").is_explain)

        self.db.execute(
            "create table one(x up); create table two(x down); insert into one values(3); insert into two values(3)")
        self.assertFalse(apsw.ext.query_info(self.db, "insert into two values(7)").is_readonly)

        # actions
        query = "select * from one join two"
        self.assertIsNone(apsw.ext.query_info(self.db, query).actions)
        qd = apsw.ext.query_info(self.db, query, actions=True)
        self.assertTrue(
            any(a.action_name == "SQLITE_READ" and a.table_name == "one" for a in qd.actions)
            and any(a.action_name == "SQLITE_READ" and a.table_name == "two" for a in qd.actions))

        # expanded_sql
        self.assertEqual("select 3, 'three'",
                         apsw.ext.query_info(self.db, "select ?, ?", (3, "three"), expanded_sql=True).expanded_sql)

        # bindings count/names
        qd = apsw.ext.query_info(self.db, "select ?2, :three, ?5, $six")
        self.assertEqual(qd.bindings_count, 6)
        self.assertEqual(qd.bindings_names, (None, "2", "three", None, "5", "six"))

        # explain / explain query_plan
        # from https://sqlite.org/lang_with.html
        query = """
WITH RECURSIVE
  xaxis(x) AS (VALUES(-2.0) UNION ALL SELECT x+0.05 FROM xaxis WHERE x<1.2),
  yaxis(y) AS (VALUES(-1.0) UNION ALL SELECT y+0.1 FROM yaxis WHERE y<1.0),
  m(iter, cx, cy, x, y) AS (
    SELECT 0, x, y, 0.0, 0.0 FROM xaxis, yaxis
    UNION ALL
    SELECT iter+1, cx, cy, x*x-y*y + cx, 2.0*x*y + cy FROM m
     WHERE (x*x + y*y) < 4.0 AND iter<28
  ),
  m2(iter, cx, cy) AS (
    SELECT max(iter), cx, cy FROM m GROUP BY cx, cy
  ),
  a(t) AS (
    SELECT group_concat( substr(' .+*#', 1+min(iter/7,4), 1), '')
    FROM m2 GROUP BY cy
  )
SELECT group_concat(rtrim(t),x'0a') FROM a;
        """
        self.assertIsNone(apsw.ext.query_info(self.db, query).explain)
        qd = apsw.ext.query_info(self.db, query, explain=True)
        self.assertTrue(all(isinstance(e, apsw.ext.VDBEInstruction) for e in qd.explain))
        # at time of writing it was 233 steps, so use ~10% of that
        self.assertGreater(len(qd.explain), 25)
        self.assertIsNone(apsw.ext.query_info(self.db, query).query_plan)
        qd = apsw.ext.query_info(self.db, query, explain_query_plan=True)

        def check_instance(node: apsw.ext.QueryPlan):
            return isinstance(node, apsw.ext.QueryPlan) and all(check_instance(s) for s in (node.sub or []))

        self.assertTrue(check_instance(qd.query_plan))

        def count(node: apsw.ext.QueryPlan):
            return 1 + sum(count(s) for s in (node.sub or []))

        # at time of writing it was 24 nodes
        self.assertGreater(count(qd.query_plan), 10)

    def testVFSFcntlPragma(self):
        "Test wrapping fcntl pragmas"

        class testvfs(apsw.VFS):
            name = "testingpragma"

            def __init__(self):
                super().__init__(self.name, "")

            def xOpen(self, name, flags):
                return testvfsfile(name, flags)

        testvfs = testvfs()
        seen_unraiseable = None

        class testvfsfile(apsw.VFSFile):

            def __init__(self, name, flags):
                super().__init__("", name, flags)

            def excepthook(self, etype, evalue, etraceback):
                nonlocal seen_unraiseable
                seen_unraiseable = (etype, evalue, etraceback)

            # note 'self' is for the test not this file instance
            def xFileControl(fileself, op, ptr):
                if op != apsw.SQLITE_FCNTL_PRAGMA:
                    return super().xFileControl(op, ptr)
                self.assertRaises(TypeError, apsw.VFSFcntlPragma, "a string")
                p = apsw.VFSFcntlPragma(ptr)
                self.assertRaisesRegex(RuntimeError, "__init__ has already been called, and cannot be called again",
                                       p.__init__, "issue 488")
                self.assertIn((p.name, p.value), test_pragmas)
                self.assertRaises(TypeError, setattr, p, "result", 3)
                v = test_pragmas[(p.name, p.value)]
                if isinstance(v, Exception):
                    p.result = v.args[0]
                    p.result = "message: " + p.result
                    raise v
                if v is not False:
                    p.result = v
                    return True
                return False

        con = apsw.Connection(self.db.db_filename("main"), vfs=testvfs.name)

        test_pragmas = {
            ("once", None): "some text for the result",
            ("once", "valueeee"): "completely different text",
            ("donot", "understand"): False,
            ("autherror", None): apsw.AuthError("yet more different text"),
        }

        for (k, v), r in test_pragmas.items():
            seen_unraiseable = res = exc = None
            sql = f"pragma { k }"
            if v:
                sql += f"({ v })"
            try:
                res = con.execute(sql).get
            except apsw.Error as e:
                exc = e
            if r is False:
                self.assertIsNone(exc)
                self.assertIsNone(res)
            elif isinstance(r, apsw.Error):
                self.assertIsInstance(exc, r.__class__)
                self.assertEqual(exc.args[0], r.args[0])
                self.assertIsNone(res)
            else:
                self.assertEqual(res, r)

    def testTpStr(self):
        "Helpful descriptions of str(apsw objects)"
        self.db.execute("create table x(y); insert into x values(x'abcdef1012')")
        blob =self.db.blob_open("main", "x", "y", self.db.last_insert_rowid(), 0)
        blob2 =self.db.blob_open("main", "x", "y", self.db.last_insert_rowid(), 0)
        blob2.close()
        db2=apsw.Connection("")
        cur2=db2.cursor()
        cur2.close()
        db3=apsw.Connection("")
        cur3=db3.cursor()
        db3.close()
        backup2=db2.backup("main", self.db, "main")
        backup2.close()
        name_catch=[]
        class TVFS(apsw.VFS):

            def __init__(self):
                apsw.VFS.__init__(self, "uritest", "")

            def xOpen(self, name, flags):
                assert isinstance(name, apsw.URIFilename)
                name_catch.append((name, str(name)))
                raise apsw.SQLError()

            # MacOS fails the name we provide returning
            # cantopen for this, so we avoid their code
            def xFullPathname(self, name: str) -> str:
                return name

        t = TVFS()

        with contextlib.suppress(apsw.SQLError):
            with tempfile.NamedTemporaryFile() as n:
                apsw.Connection(n.name, vfs="uritest")

        self.assertEqual(len(name_catch), 1)
        uriname, urinamestr = name_catch[0]

        objects = (self.db,
                   db2,
                   db3,
                   self.db.cursor(),
                   cur2,
                   cur3,
                   apsw.zeroblob(3),
                   blob,
                   blob2,
                   uriname,
                   apsw.VFS("aname", ""),
                   apsw.VFSFile("", self.db.db_filename("main"),
                       [apsw.SQLITE_OPEN_MAIN_DB | apsw.SQLITE_OPEN_CREATE | apsw.SQLITE_OPEN_READWRITE, 0]),
                   db2.backup("main", self.db, "main"),
                   backup2,
        )
        for o in objects:
            self.assertNotEqual(repr(o), str(o))
            # issue 501
            if isinstance(o, apsw.URIFilename):
                self.assertNotEqual(repr(o), urinamestr)
                self.assertNotEqual(str(o), urinamestr)

        # more issue 501
        with contextlib.suppress(ValueError):
            uriname.filename()
            1/0
        with contextlib.suppress(ValueError):
            uriname.parameters
            1/0
        with contextlib.suppress(ValueError):
            uriname.uri_boolean("name", False)
            1/0
        with contextlib.suppress(ValueError):
            uriname.uri_int("name", 0)
            1/0
        with contextlib.suppress(ValueError):
            uriname.uri_parameter("name")
            1/0

    # This test is run last by deliberate name choice.  If it did
    # uncover any bugs there isn't much that can be done to turn the
    # checker off.
    def testzzForkChecker(self):
        "Test detection of using objects across fork"
        # need to free up everything that already exists
        self.db.close()
        self.db = None
        gc.collect()
        # install it
        apsw.fork_checker()

        # return some objects
        def getstuff():
            db = apsw.Connection(":memory:")
            cur = db.cursor()
            for row in cur.execute(
                    "create table foo(x);insert into foo values(1);insert into foo values(x'aabbcc'); select last_insert_rowid()"
            ):
                blobid = row[0]
            blob = db.blob_open("main", "foo", "x", blobid, 0)
            db2 = apsw.Connection(":memory:")
            if hasattr(db2, "backup"):
                backup = db2.backup("main", db, "main")
            else:
                backup = None
            return (db, cur, blob, backup)

        # test the objects
        def teststuff(db, cur, blob, backup):
            if db:
                db.cursor().execute("select 3")
            if cur:
                cur.execute("select 3")
            if blob:
                blob.read(1)
            if backup:
                backup.step()

        # Sanity check
        teststuff(*getstuff())
        # get some to use in parent
        parent = getstuff()
        # to be used (and fail with error) in child
        child = getstuff()

        def childtest(*args):
            # we can't use unittest methods here since we are in a different process
            val = args[0]
            args = args[1:]
            # this should work
            teststuff(*getstuff())

            # ignore the unraisable stuff sent to sys.excepthook
            def eh(*args):
                pass

            sys.excepthook = eh
            # call with each separate item to check
            try:
                for i in range(len(args)):
                    a = [None] * len(args)
                    a[i] = args[i]
                    try:
                        teststuff(*a)
                    except apsw.ForkingViolationError:
                        pass
            except apsw.ForkingViolationError:
                # we get one final exception "between" line due to the
                # nature of how the exception is raised
                pass
            # this should work again
            teststuff(*getstuff())
            val.value = 1

        import multiprocessing
        val = multiprocessing.Value("i", 0)
        p = multiprocessing.Process(target=childtest, args=[val] + list(child))
        self.suppressWarning("DeprecationWarning")  # we are deliberately forking
        p.start()
        p.join()
        self.assertEqual(1, val.value)  # did child complete ok?
        teststuff(*parent)

        # we call shutdown to free mutexes used in fork checker,
        # so clear out all the things first
        del child
        del parent
        gc.collect()
        apsw.shutdown()


testtimeout = False  # timeout testing adds several seconds to each run


def vfstestdb(filename=TESTFILEPREFIX + "testdb2", vfsname="apswtest", closedb=True, mode=None, attachdb=None):
    "This method causes all parts of a vfs to be executed"
    gc.collect()  # free any existing db handles
    for suf in "", "-journal", "x", "x-journal":
        deletefile(filename + suf)

    db = apsw.Connection("file:" + filename + "?psow=0", vfs=vfsname, flags=openflags)
    if mode:
        db.cursor().execute("pragma journal_mode=" + mode)
    db.cursor().execute(
        "create table foo(x,y); insert into foo values(1,2); insert into foo values(date('now'), date('now'))")
    db.vfsname("main")
    if testtimeout:
        # busy
        db2 = apsw.Connection(filename, vfs=vfsname)
        if mode:
            db2.cursor().execute("pragma journal_mode=" + mode)
        db.set_busy_timeout(1100)
        db2.cursor().execute("begin exclusive")
        try:
            db.cursor().execute("begin immediate")
            1 / 0  # should not be reached
        except apsw.BusyError:
            pass
        db2.cursor().execute("end")

    # cause truncate to be called
    # see sqlite test/pager3.test where this (public domain) code is taken from
    # I had to add the pragma journal_mode to get it to work
    c = db.cursor()
    for row in c.execute("pragma journal_mode=truncate"):
        pass

    c.execute("""
                 create table t1(a unique, b);
                 insert into t1 values(1, 'abcdefghijklmnopqrstuvwxyz');
                 insert into t1 values(2, 'abcdefghijklmnopqrstuvwxyz');
                 update t1 set b=b||a||b;
                 update t1 set b=b||a||b;
                 update t1 set b=b||a||b;
                 update t1 set b=b||a||b;
                 update t1 set b=b||a||b;
                 update t1 set b=b||a||b;
                 create temp table t2 as select * from t1;
                 begin;
                 create table t3(x);""")
    try:
        c.execute("insert into t1 select 4-a, b from t2")
    except apsw.ConstraintError:
        pass
    c.execute("rollback")

    if attachdb:
        c.execute("attach '%s' as second" % (attachdb, ))

    if hasattr(APSW, "testLoadExtension"):
        # can we use loadextension?
        db.enableloadextension(True)
        try:
            db.loadextension("./" * 128 + LOADEXTENSIONFILENAME + "xxx")
        except apsw.ExtensionLoadingError:
            pass
        db.loadextension(LOADEXTENSIONFILENAME)
        assert (1 == curnext(db.cursor().execute("select half(2)"))[0])

    # Get the routine xCheckReservedLock to be called.  We need a hot journal
    # which this code adapted from SQLite's pager.test does
    if not iswindows:
        c.execute("create table abc(a,b,c)")
        for i in range(20):
            c.execute("insert into abc values(1,2,?)", (randomstring(200), ))
        c.execute("begin; update abc set c=?", (randomstring(200), ))

        write_whole_file(filename + "x", "wb", read_whole_file(filename, "rb"))
        write_whole_file(filename + "x-journal", "wb", read_whole_file(filename + "-journal", "rb"))

        f = open(filename + "x-journal", "ab")
        f.seek(-1032, 2)  # 1032 bytes before end of file
        f.write(b"\x00\x00\x00\x00")
        f.close()

        hotdb = apsw.Connection(filename + "x", vfs=vfsname)
        if mode:
            hotdb.cursor().execute("pragma journal_mode=" + mode)
        hotdb.cursor().execute("select sql from sqlite_schema")
        hotdb.close()

    if closedb:
        db.close()
    else:
        return db


if not iswindows:
    # note that a directory must be specified otherwise $LD_LIBRARY_PATH is used
    LOADEXTENSIONFILENAME = "./testextension.sqlext"
else:
    LOADEXTENSIONFILENAME = "testextension.sqlext"

MEMLEAKITERATIONS = 123
PROFILESTEPS = 250000


def setup():
    """Call this if importing this test suite as it will ensure tests
    we can't run are removed etc.  It will also print version
    information."""

    print_version_info()
    try:
        apsw.config(apsw.SQLITE_CONFIG_MEMSTATUS, True)  # ensure memory tracking is on
    except apsw.MisuseError:
        # if using amalgamation then something went wrong
        if apsw.using_amalgamation:
            raise
        # coverage uses sqlite and so the config call is too
        # late
        pass
    apsw.initialize()  # manual call for coverage
    memdb = apsw.Connection(":memory:")
    if not getattr(memdb, "enableloadextension", None):
        del APSW.testLoadExtension

    # earlier py versions make recursion error fatal
    if sys.version_info < (3, 10):
        del APSW.testIssue425

    forkcheck = False
    if hasattr(apsw, "fork_checker") and hasattr(os, "fork") and platform.python_implementation() != "PyPy":
        try:
            import multiprocessing
            if hasattr(multiprocessing, "get_start_method"):
                if multiprocessing.get_start_method() != "fork":
                    raise ImportError
            # sometimes the import works but doing anything fails
            val = multiprocessing.Value("i", 0)
            forkcheck = True
        except ImportError:
            pass

    # we also remove forkchecker if doing multiple iterations
    if not forkcheck or "APSW_TEST_ITERATIONS" in os.environ:
        del APSW.testzzForkChecker

    if not is64bit or "APSW_TEST_LARGE" not in os.environ:
        del APSW.testLargeObjects

    # We can do extension loading but no extension present ...
    if getattr(memdb, "enableloadextension", None) and not os.path.exists(LOADEXTENSIONFILENAME):
        print("Not doing the optional LoadExtension test.  You need to compile the extension first\n")
        print("  python3 setup.py build_test_extension")
        del APSW.testLoadExtension

    del memdb


test_types_vals = (
    "a simple string",  # "ascii" string
    "0123456789" * 200000,  # a longer string
    "a \u1234 unicode \ufe54 string \u0089",  # simple unicode string
    "\N{BLACK STAR} \N{WHITE STAR} \N{LIGHTNING} \N{COMET} ",  # funky unicode or an episode of b5
    "\N{MUSICAL SYMBOL G CLEF}",  # https://www.cmlenz.net/archives/2008/07/the-truth-about-unicode-in-python
    97,  # integer
    2147483647,  # numbers on 31 bit boundary (32nd bit used for integer sign), and then
    -2147483647,  # start using 32nd bit (must be represented by 64bit to avoid losing
    2147483648,  # detail)
    -2147483648,
    2147483999,
    -2147483999,
    992147483999,
    -992147483999,
    9223372036854775807,
    -9223372036854775808,
    b"a set of bytes",  # bag of bytes initialised from a string, but don't confuse it with a
    b"".join([b"\\x%02x" % (x, ) for x in range(256)]),  # string
    b"".join([b"\\x%02x" % (x, ) for x in range(256)]) * 20000,  # non-trivial size
    None,  # our good friend NULL/None
    1.1,  # floating point can't be compared exactly - assertAlmostEqual is used to check
    10.2,  # see Appendix B in the Python Tutorial
    1.3,
    1.45897589347E97,
    5.987987 / 8.7678678687676786,
    math.pi,
    True,  # derived from integer
    False)

if __name__ == '__main__':
    setup()

    def runtests():
        if os.getenv("PYTRACE"):
            import trace
            t = trace.Trace(count=0, trace=1, ignoredirs=[sys.prefix, sys.exec_prefix])
            t.runfunc(unittest.main)
        else:
            unittest.main()

    v = os.environ.get("APSW_TEST_ITERATIONS", None)
    if v is None:
        try:
            runtests()
        except SystemExit:
            exitcode = sys.exc_info()[1].code
    else:
        # we run all the tests multiple times which has better coverage
        # a larger value for MEMLEAKITERATIONS slows down everything else
        MEMLEAKITERATIONS = 5
        PROFILESTEPS = 1000
        v = int(v)
        for i in range(v):
            print(f"Iteration { i + 1 }  of { v }")
            try:
                runtests()
            except SystemExit:
                exitcode = sys.exc_info()[1].code

    # Free up everything possible
    del APSW
    del ThreadRunner
    del randomintegers

    # clean up sqlite and apsw
    del apsw.ext
    del apsw.bestpractice
    gc.collect()  # all cursors & connections must be gone
    apsw.shutdown()
    apsw.config(apsw.SQLITE_CONFIG_LOG, None)
    if hasattr(apsw, "_fini"):
        apsw._fini()
        gc.collect()
    del apsw

    gc.collect()

    sys.exit(exitcode)