#!/usr/bin/env python3

import gc
import unittest
import os
import sys
import platform
import warnings

import apsw

def suppressWarning(name):
    if hasattr(__builtins__, name):
        warnings.simplefilter("ignore", getattr(__builtins__, name))

# Name deliberate to make it run last
class zzZForkChecker(unittest.TestCase):

    def install(self):
        gc.collect(2)
        assert not apsw.connections()
        apsw.shutdown()
        apsw.fork_checker()

    def testAlreadyInit(self):
        apsw.initialize()
        con = apsw.Connection(":memory:")
        self.assertRaisesRegex(apsw.MisuseError, ".*All connections need to be closed.*", apsw.shutdown)
        con.close()
        self.assertRaisesRegex(apsw.MisuseError, ".*needs to be shutdown.*", apsw.fork_checker)

    def testManyConnections(self):
        # make sure dynamic stuff is done
        self.install()
        c = []
        for i in range(50):
            c.append(apsw.Connection(":memory:"))
        while x:=apsw.connections():
            x[0].close()

    def testForkChecker(self):
        "Test detection of using objects across fork"
        self.install()

        class Dummy(apsw.VFS):
            def __init__(self):
                super().__init__("one", "")

        # grabs vfs mutex
        d = Dummy()
        del d

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
            backup = db2.backup("main", db, "main")
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
            os._exit(0)

        suppressWarning("DeprecationWarning")  # we are deliberately forking
        pid = os.fork()

        if pid == 0:
            # child
            counter = 0

            def ueh(unraisable):
                if unraisable.exc_type != apsw.ForkingViolationError:
                    print("\n\nUnraisable exception in child process", unraisable)
                    return sys.__unraisablehook__(unraisable)
                nonlocal counter
                counter += 1
                if counter > 100:
                    os._exit(0)

            sys.unraisablehook = ueh
            try:
                childtest(*child)
            except:
                print("\n\nThis exception in THE CHILD PROCESS OF FORK CHECKER\n", file=sys.stderr)
                traceback.print_exc()
                print("\nEnd CHILD traceback\n\n")
                os._exit(1)
            os._exit(0)

        rc = os.waitpid(pid, 0)
        self.assertEqual(0, os.waitstatus_to_exitcode(rc[1]))

        teststuff(*parent)

        # we call shutdown to free mutexes used in fork checker,
        # so clear out all the things first
        del child
        del parent
        gc.collect()


# Fork checker is becoming less useful on newer Pythons because
# multiprocessing really doesn't want you to use fork and does
# alternate methods instead.  We also run sanitizers on most
# recent Python which makes things even more convoluted.
forkcheck = False

if hasattr(apsw, "fork_checker") and hasattr(os, "fork") and platform.python_implementation() != "PyPy" \
        and sys.version_info < (3, 13):
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

if forkcheck:
    __all__ = ("zzZForkChecker",)
else:
    __all__ = tuple()

if __name__ == '__main__':
    if forkcheck:
        unittest.main()