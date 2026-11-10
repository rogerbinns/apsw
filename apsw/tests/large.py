
from __future__ import annotations

import mmap
import sys
import unittest

import apsw


# yay
is64bit = sys.maxsize > 2**32

@unittest.skipIf(not is64bit, "A 64 bit build is required")
class Large(unittest.TestCase):

    def testLargeObjects(self):
        "Verify handling of large strings/blobs (>2GB) [requires 64 bit platform]"

        db = apsw.Connection("")

        # For binary/blobs I use an anonymous area slightly larger than 2GB chunk of memory, but don't touch any of it
        f = mmap.mmap(-1, 2 * 1024 * 1024 * 1024 + 25000)
        c = db.cursor()
        c.execute("create table foo(theblob)")
        self.assertRaises(apsw.TooBigError, c.execute, "insert into foo values(?)", (f,))
        c.execute("insert into foo values(?)", ("jkghjk" * 1024,))
        b = db.blob_open("main", "foo", "theblob", db.last_insert_rowid(), True)
        b.read(1)
        self.assertRaises(ValueError, b.write, f)

        def func():
            return f

        db.create_scalar_function("toobig", func)
        self.assertRaises(apsw.TooBigError, c.execute, "select toobig()")
        f.close()

if __name__ == "__main__":
    unittest.main()