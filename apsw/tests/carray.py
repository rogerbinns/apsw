#!/usr/bin/env python3

from __future__ import annotations

# This testing code deliberately does nasty stuff so mypy isn't helpful
# mypy: ignore-errors
# type: ignore

import array
import random
import unittest

import apsw


class CArray(unittest.TestCase):
    def setUp(self):
        self.db = apsw.Connection(":memory:")

    def tearDown(self):
        self.db.close()
        del self.db
        for c in apsw.connections():
            c.close()

    def testBasics(self):
        # does it work?
        values = [random.randint(-100, 100) for _ in range(1024)]

        arr = array.array("l", values)

        self.assertEqual(max(values), self.db.execute("select max(value) from carray(?)", (apsw.carray(arr),)).get)

        # arg parsing
        self.assertRaises(TypeError, apsw.carray, "hello")
        self.assertRaises(TypeError, apsw.carray, arr, "hello")
        self.assertRaises(TypeError, apsw.carray, arr, 1.1, "hello")
        self.assertRaises(TypeError, apsw.carray, arr, 1, 1, "hello")
        self.assertRaises(TypeError, apsw.carray, arr, 1, 1, 1, 1)
        self.assertRaises(TypeError, apsw.carray, arr, 1, 1, 1, flags=2)

        c = apsw.carray(arr)
        self.assertRaisesRegex(RuntimeError, ".*has already been called.*", c.__init__, arr)

    def testNumbers(self):
        arr = array.array("l", range(100, 0, -1))

        # non-contiguous buffer
        self.assertRaises(BufferError, apsw.carray, memoryview(arr)[::2])

        # auto-detection
        numbers = tuple(range(20))
        for format in "ildq":
            arr = array.array(format, numbers)
            self.assertEqual(
                list(arr), self.db.execute("select value from carray(?) order by value", (apsw.carray(arr),)).get
            )

        self.assertRaises(ValueError, apsw.carray, array.array("f", arr))

        # lie about format - treating int as float will not give the same values!
        self.assertNotEqual(
            list(arr),
            self.db.execute(
                "select value from carray(?) order by value", (apsw.carray(arr, flags=apsw.SQLITE_CARRAY_INT32),)
            ).get,
        )

        self.assertRaisesRegex(ValueError, ".*Unsupported flags.*", apsw.carray, arr, flags=247)

        for flag in "INT32", "INT64", "DOUBLE":
            self.assertRaisesRegex(
                ValueError,
                ".*not a multiple of.*",
                apsw.carray,
                b"123456",
                flags=getattr(apsw, f"SQLITE_CARRAY_{flag}"),
            )

        # Start and stop
        self.assertRaises(ValueError, apsw.carray, b"\0" * 8, start=-1, flags=apsw.SQLITE_CARRAY_INT32)
        self.assertRaises(ValueError, apsw.carray, b"\0" * 8, start=73, flags=apsw.SQLITE_CARRAY_INT32)
        self.assertRaises(ValueError, apsw.carray, b"\0" * 8, stop=73, flags=apsw.SQLITE_CARRAY_INT32)
        self.assertRaises(ValueError, apsw.carray, b"\0" * 16, stop=0, start=1, flags=apsw.SQLITE_CARRAY_INT32)

        # current limitation - needs to be at least one item
        self.assertRaises(ValueError, apsw.carray, b"\0" * 8, start=2, stop=2, flags=apsw.SQLITE_CARRAY_INT32)

    def testTuple(self):
        # str and blobs
        self.assertRaises(ValueError, apsw.carray, tuple(), flags=192)

        str_tuple = ("zero", "one", "two", "three", "")
        bin_tuple = (b"zero", b"one", b"two", b"three", b"")

        self.assertRaisesRegex(ValueError, ".*Start.*is beyond end.*", apsw.carray, str_tuple, start=294)
        self.assertRaisesRegex(ValueError, ".*Start.*is beyond end.*", apsw.carray, bin_tuple, start=1_000_000)

        self.assertRaisesRegex(ValueError, ".*Stop.*is beyond end.*", apsw.carray, str_tuple, stop=294)
        self.assertRaisesRegex(ValueError, ".*Stop.*is beyond end.*", apsw.carray, bin_tuple, stop=1_000_000)

        self.assertRaisesRegex(ValueError, ".*zero item array.*", apsw.carray, tuple())
        self.assertRaisesRegex(ValueError, ".*zero item array.*", apsw.carray, str_tuple, start=len(str_tuple))
        self.assertRaisesRegex(ValueError, ".*zero item array.*", apsw.carray, str_tuple, start=1, stop=1)
        self.assertRaisesRegex(ValueError, ".*zero item array.*", apsw.carray, bin_tuple, start=len(bin_tuple))
        self.assertRaisesRegex(ValueError, ".*zero item array.*", apsw.carray, bin_tuple, start=1, stop=1)

        self.assertRaisesRegex(ValueError, ".*Stop.* is before start.*", apsw.carray, str_tuple, start=2, stop=1)
        self.assertRaisesRegex(ValueError, ".*Stop.* is before start.*", apsw.carray, bin_tuple, start=3, stop=2)

        self.assertRaises(TypeError, apsw.carray, str_tuple, flags=apsw.SQLITE_CARRAY_BLOB)
        self.assertRaises(TypeError, apsw.carray, bin_tuple, flags=apsw.SQLITE_CARRAY_TEXT)

        # does it work?
        self.assertEqual(
            sorted(str_tuple),
            self.db.execute("select value from carray(?) order by value", (apsw.carray(str_tuple),)).get,
        )

        self.assertEqual(
            sorted(bin_tuple),
            self.db.execute("select value from carray(?) order by value", (apsw.carray(bin_tuple),)).get,
        )

        # inconsistencies in tuple members
        for bad in (
            (3 + 4j,),
            ("one", 3 + 4j),
            (b"hello", 3 + 4j),
            ("one", b"one"),
            (b"one", "one"),
        ):
            self.assertRaises(TypeError, apsw.carray, bad)

        # no nulls
        for bad in (
            "\0",
            "one\0",
            "\0two",
        ):
            self.assertRaisesRegex(ValueError, ".*embedded nulls.*", apsw.carray, ("one", bad, "three"))

    def testMultiArray(self):
        # multiple c array in same query to make sure they don't get
        # confused with each other

        a = apsw.carray(array.array("l", [96, 12, 423, -17, 6]))
        b = apsw.carray(array.array("d", [1.3, 0.3, 47.0, 64.2, -17, 0.3, 1.8]))

        self.assertEqual(
            [
                (-17, -17.0),
                (-17, 0.3),
                (-17, 0.3),
                (-17, 1.3),
                (-17, 1.8),
                (-17, 47.0),
                (-17, 64.2),
                (6, -17.0),
                (6, 0.3),
                (6, 0.3),
                (6, 1.3),
                (6, 1.8),
                (6, 47.0),
                (6, 64.2),
                (12, -17.0),
                (12, 0.3),
                (12, 0.3),
                (12, 1.3),
                (12, 1.8),
                (12, 47.0),
                (12, 64.2),
                (96, -17.0),
                (96, 0.3),
                (96, 0.3),
                (96, 1.3),
                (96, 1.8),
                (96, 47.0),
                (96, 64.2),
                (423, -17.0),
                (423, 0.3),
                (423, 0.3),
                (423, 1.3),
                (423, 1.8),
                (423, 47.0),
                (423, 64.2),
            ],
            self.db.execute(
                "SELECT a.value, b.value FROM carray(?) AS a, carray(?) AS b ORDER BY a.value, b.value", (a, b)
            ).get,
        )

    def testNumpy(self):
        try:
            import numpy as np
        except ImportError:
            return

        # multi-dimensional - only has to be contiguous - order doesn't matter
        for order in "CF":
            self.assertEqual(
                [1, 2, 3, 4, 5, 6],
                self.db.execute(
                    "select value from carray(?) order by value",
                    (apsw.carray(np.array([[3, 2, 1], [6, 5, 4]], order=order)),),
                ).get,
            )

    def testOffsets(self):
        arr = array.array("l", range(20))

        def get(start, stop):
            return self.db.execute(
                "select value from carray(?) order by value", (apsw.carray(arr, start=start, stop=stop),)
            ).get

        self.assertEqual([1, 2, 3], get(1, 4))
        self.assertEqual(19, get(19, -1))
        self.assertEqual(list(arr), get(0, -1))
        self.assertEqual(list(arr)[1:], get(1, -1))
        self.assertEqual(list(arr)[2:], get(2, 20))

        # 0 length limitation
        self.assertRaises(ValueError, apsw.carray, arr, start=20, stop=20)


has_carray = hasattr(apsw, "carray")

if has_carray:
    __all__ = ("CArray",)
else:
    del CArray
    __all__ = tuple()


if __name__ == "__main__":
    unittest.main()
