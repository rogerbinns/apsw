#!/usr/bin/env python3

from __future__ import annotations

# This testing code deliberately does nasty stuff so mypy isn't helpful
# mypy: ignore-errors
# type: ignore

import array
import collections.abc
import contextlib
import decimal
import gc
import json
import math
import os
import sys
import types
import unittest

import apsw
import apsw.ext
import apsw.fts5

encode_raw = apsw.jsonb_encode
decode_raw = apsw.jsonb_decode
detect = apsw.jsonb_detect

# \v came back as the wrong value before this release
backslash_v_fixed = tuple(int(v) for v in apsw.sqlite_lib_version().split(".")) >= (3, 51, 0)


class DetectDecodeMisMatch(Exception):
    pass


def decode(data, **kwargs):
    # this wrapper ensures that detect results are always the same as
    # decode - ie detect returns False for decodes that fail, and True for
    # success
    try:
        detection = detect(data)
        if detection is not True and detection is not False:
            raise Exception("not a bool")
    except Exception:
        raise DetectDecodeMisMatch("detection raised exception - it must never do that and only return bool")

    if kwargs:
        return decode_raw(data, **kwargs)

    try:
        res = decode_raw(data, **kwargs)
        if detection is not True:
            raise DetectDecodeMisMatch(f"detection gave {detection} while decode succeeded")
        return res
    except Exception:
        if detection is not False:
            raise DetectDecodeMisMatch(f"detection gave {detection} while decode raised an exception")
        raise


def encode(*args, **kwargs):
    encoded = encode_raw(*args, **kwargs)
    if detect(encoded) is not True:  # only allowed value
        raise DetectDecodeMisMatch("encoded data not detected")
    decode(encoded)
    return encoded


# this contains all the data types representable in json
example_data = {
    "foo": [None, True, 3.1, -3, ["nested", {"yes": ["this", "too", 3.1e-5]}]],
    "🤦🏼‍♂️": "𐌼𐌰𐌲 𐌲𐌻𐌴𐍃 𐌹̈𐍄𐌰𐌽",
    "": [],
    "𐌼𐌰𐌲 𐌲𐌻𐌴𐍃 𐌹̈𐍄𐌰𐌽": {},
    "null": None,
    "3": {"3": 3},
}

# sentinel
not_set = object()


class JSONB(unittest.TestCase):
    def setUp(self):
        self.db = apsw.Connection(":memory:")
        # set these up each time
        self.f_json = apsw.ext.Function(self.db, "json")
        self.f_jsonb = apsw.ext.Function(self.db, "jsonb")
        self.f_json_valid = apsw.ext.Function(self.db, "json_valid")

    def tearDown(self):
        self.db.close()
        del self.db
        for c in apsw.connections():
            c.close()

    def check_item(self, item, sqlite_value=not_set):
        # item should roundtrip through everything
        j0 = json.dumps(item)
        j1 = json.dumps(item, ensure_ascii=True)

        # check sqlite gets it right
        if sqlite_value is not not_set:
            assert json.loads(self.f_json(j0)) == sqlite_value
            assert json.loads(self.f_json(j1)) == sqlite_value
        else:
            assert json.loads(self.f_json(j0)) == item
            assert json.loads(self.f_json(j1)) == item

        # our jsonb encoder, sqlite json stringify
        assert json.loads(self.f_json(encode(item))) == (item if sqlite_value is not_set else sqlite_value)

        # our jsonb decoder, sqlite parse
        assert decode(self.f_jsonb(j0)) == (item if sqlite_value is not_set else sqlite_value)
        assert decode(self.f_jsonb(j1)) == (item if sqlite_value is not_set else sqlite_value)

        # our decoder and encoder
        assert decode(encode(item)) == (item if sqlite_value is not_set else sqlite_value)

    def check_invalid(self, encoded: bytes, include_sqlite: bool = True):
        # encoded item should be rejected as not valid jsonb by us
        self.assertFalse(detect(encoded))
        self.assertRaises(ValueError, decode, encoded)

        if include_sqlite:
            # and by sqlite which isn't as comprehensive
            self.assertFalse(self.f_json_valid(encoded, 8))

    def check_valid(self, encoded: bytes, value, *, include_sqlite_valid_check=True):
        # encoded should be accepted as valid jsonb
        self.assertTrue(detect(encoded))
        self.assertEqual(decode(encoded), value)

        # and by sqlite
        if include_sqlite_valid_check:
            self.assertTrue(self.f_json_valid(encoded, 8))
        self.assertEqual(json.loads(self.f_json(encoded)), value)

    def testBasics(self):
        for item in (
            None,
            False,
            True,
            3,
            3.3,
            "",
            "a",
            [],
            [None],
            ["", None],
            [[], {}],
            [[[]]],
            {},
            {"": None},
            {"": ""},
            {"": {}},
            {"": []},
        ):
            self.check_item(item)

    def testAllowedChars(self):
        # work out exactly what values are allowed in strings, and confirm we match
        for tag in {7, 8, 9, 10}:
            for value in range(0, 130):
                for repeat in (1, 2):
                    for backslash in (0, 1):
                        # this is to force >= 0x80 utf8 bytes
                        for suffix in ("", chr(1234)):
                            s = "\\" * backslash + chr(value) * repeat + suffix
                            encoded = make_item(tag, s)
                            sqlite = bool(self.f_json_valid(encoded, 8))
                            us = detect(encoded)

                            if sqlite and not us and value == 0:
                                # sqlite allows backslash zero byte which is nonsensical
                                if tag == 8:
                                    continue

                            j = self.f_json(encoded) if sqlite else None

                            if tag == 9 and j and sqlite and not us:
                                # sqlite incorrectly allows \x ANYTHING but generates garbage json
                                try:
                                    json.loads(j)
                                except json.decoder.JSONDecodeError:
                                    # correct sqlite's error
                                    sqlite = False

                            self.assertEqual(sqlite, us)

                            if sqlite:
                                left = json.loads(self.f_json(encoded))
                                right = decode(encoded)
                                # 3.50.4 gets \v wrong
                                if left.replace("\t", "\v") == right:
                                    continue
                                self.assertEqual(left, right)

    def testStrings(self):
        # our gnarly test strings
        test_strings = [s[0].decode("utf8") for s in apsw.fts5.tokenizer_test_strings()]
        self.check_item(test_strings)

        # mixed case
        self.check_valid(self.f_jsonb(r'"\uAbCd\u0fFf"'), "\uabcd\u0fff")

        # try to get each type of string - we only generate textraw
        seen = set()
        for test, expected in (
            ("hello", "hello"),
            ("table select []", "table select []"),
            (r"\u0020\n", " \n"),
            (r"\0", "\0"),
            (r"\0a", "\0a"),
            ("\\'", "'"),
            ("\\\n", ""),
            (
                r"\x5c\"\0\n\r\b\t\f\'" + ("\\v" if backslash_v_fixed else ""),
                "\\\"\0\n\r\b\t\f'" + ("\v" if backslash_v_fixed else ""),
            ),
        ):
            # this is to ensure we test non-ascii ranges too
            for suffix in ("", "".join(test_strings)):
                s = '"' + test + json.dumps(suffix)[1:-1] + '"'
                encoded = self.f_jsonb(s)
                seen.add(encoded[0] & 0x0F)
                self.assertEqual(decode(encoded), expected + suffix)
                self.assertEqual(json.loads(self.f_json(encoded)), expected + suffix)

        # I can't get sqlite to generate textraw, but we do ...
        self.assertEqual(seen, {7, 8, 9})

        # zero length
        for tag in {7, 8, 9, 10}:
            encoded = make_item(tag, "")
            self.check_valid(encoded, "")

        for encoded, expected in (
            (make_item(9, '"'), '"'),
            (make_item(9, "\x17"), "\x17"),
            (make_item(9, "\\0"), "\0"),
        ):
            self.check_valid(encoded, expected)

        # others
        for s in (
            "/",
            "\\",
            '"',
            "'",
            "\0",
            "\0\n\r\b\t\f\v\\\"'/",
            "0x" + chr(0x10FFFF) + chr(0x10FFFE) + chr(0x10FFFD) + chr(1) + chr(2),
        ):
            self.check_item(s)

        # escaping
        encoded = make_item(9, r"\'\\\b\f\n\r\t\0" + ("\\v" if backslash_v_fixed else ""))
        self.check_valid(encoded, "'\\\b\f\n\r\t\0" + ("\v" if backslash_v_fixed else ""))

        # JSON5 backslash LineTerminatorSequence should be swallowed
        for LineTerminatorSequence in (
            "\n",  # LF
            "\r",  # CR (not followed by LF)
            "\r\n",  # CR LF
            chr(0x2028),  # LS
            chr(0x2029),  # PS
        ):
            for prefix, suffix in (
                ("hello", "world"),
                ("hello", ""),
                ("", "world"),
                ("", ""),
            ):
                encoded = make_item(9, prefix + "\\" + LineTerminatorSequence + suffix)
                # test sqlite first
                self.assertEqual(prefix + suffix, json.loads(self.f_json(encoded)))
                # then us
                self.assertEqual(prefix + suffix, decode(encoded))
                # not allowed in JSON
                encoded = make_item(8, prefix + "\\" + LineTerminatorSequence + suffix)
                self.check_invalid(encoded)

        self.assertRaises(UnicodeEncodeError, encode, "a" + chr(0xD801) + "b")

    def testNumbers(self):
        "numbers"

        self.check_item(3.1415e-10)

        # sqlite turns nan into None
        self.check_item(math.nan, sqlite_value=None)

        for item in (
            0.0,
            float(123),
            float(-123),
            math.pi,
            -math.pi,
            math.e,
            -math.e,
            math.tau,
            -math.tau,
            math.inf,
            -math.inf,
        ):
            self.check_item(item)

        # create deliberate overlong representations and then add an E
        # to make sure they are all paid attention to
        for digits in 5, 15, 25, 35, 55, 100:
            # json allows plus sign and leading zeroes in exponent part
            for e in (
                None,
                0,
                10,
                -10,
                100,
                -100,
                "00001",
                "002",
                "0000",
                "-00001",
                "-002",
                "-0000",
                "+00001",
                "+002",
                "+0000",
            ):
                s = pi_digits(digits)
                if e is not None:
                    s += f"E{e}"
                expected = float(s)
                encoded = make_item(0x05, s)  # FLOAT
                encoded5 = make_item(0x06, s)  # FLOAT5

                self.assertTrue(self.f_json_valid(encoded, 8))
                self.assertTrue(self.f_json_valid(encoded5, 8))

                self.assertEqual(decode(encoded), expected)
                self.assertEqual(decode(encoded5), expected)

                self.assertEqual(json.loads(self.f_json(encoded)), expected)
                self.assertEqual(json.loads(self.f_json(encoded5)), expected)

        # json5 allows leading and trailing dots, and leading +.  sqlite doesn't allow leading + so neither do we
        for float5, expected in (
            (".123", None),
            ("123.", None),
            ("-.123", None),
            (".123E+11", None),
            (".123E-11", None),
            (".123E11", None),
            ("123.E+11", None),
            ("123.E11", None),
            ("123.E-11", None),
        ):
            if expected is None:
                expected = float(float5)
            # not valid json float
            self.check_invalid(make_item(5, float5))
            # but valid float5
            encoded5 = make_item(6, float5)
            self.check_valid(encoded5, expected)

        # json5 allows 0x hex and leading +
        for int5, expected in (
            ("0xdecaf", 0xDECAF),
            ("0Xdecaf", 0xDECAF),
            ("0XdEcAf", 0xDECAF),
            ("-0xdeCaf", -0xDECAF),
        ):
            if expected is None:
                expected = int(int5)
            # not valid int.  json int doesn't allow leading + or
            # leading zeroes
            if abs(expected) != 123 or int5[0] == "+":
                self.check_invalid(make_item(3, int5))
            # but valid int5 allows hex and leading + but not leading
            # zeroes
            encoded5 = make_item(4, int5)
            detect(encoded5)
            self.check_valid(encoded5, expected)

        # bad things
        class badint(int):
            def bad_str1(self):
                return 3 + 4j

            def bad_str2(self):
                1 / 0

            def bad_str3(self):
                return "a" + chr(0xD801) + "b"

        class badfloat(float):
            pass

        badint.__str__ = badint.bad_str1
        self.assertRaises(TypeError, encode, badint())
        badfloat.__str__ = badint.bad_str1
        self.assertRaises(TypeError, encode, badfloat())
        badint.__str__ = badint.bad_str2
        self.assertRaises(ZeroDivisionError, encode, badint())
        badfloat.__str__ = badint.bad_str2
        self.assertRaises(ZeroDivisionError, encode, badfloat())
        badint.__str__ = badint.bad_str3
        self.assertRaises(UnicodeEncodeError, encode, badint())
        badfloat.__str__ = badint.bad_str3
        self.assertRaises(UnicodeEncodeError, encode, badfloat())

        self.assertRaises(UnicodeEncodeError, encode, {badint(): 1})
        self.assertRaises(UnicodeEncodeError, encode, [1, badint(), 1])
        self.assertRaises(UnicodeEncodeError, encode, {badfloat(): 1})
        self.assertRaises(UnicodeEncodeError, encode, [1, badfloat(), 1])

        self.assertRaises(UnicodeEncodeError, encode, 3 + 4j, default=lambda x: badfloat())

    def testObjects(self):
        # python json allows these types to be keys and
        # does them as strings.  it does not call default
        for obj in (None, True, False, 99, math.pi):
            k = json.dumps(obj)
            self.check_item({obj: obj}, {k: obj})

        self.assertRaises(TypeError, encode, {3 + 3j: 3})

        # coverage - dict subclass path
        class sdict(dict):
            pass

        self.assertRaises(TypeError, encode, sdict({3 + 3j: 3}))
        self.assertRaises(TypeError, encode, sdict({3: 3 + 3j}))

        self.assertEqual(decode(encode({3 + 3j: 3}, skipkeys=True)), {})

        circular = {"a": 3, "b": 4, "c": 5}
        circular["b"] = circular
        self.assertRaisesRegex(ValueError, ".*circular reference.*", encode, circular)

        circular.pop("b")

        peers = {str(n): circular for n in range(5)}
        peers.update({str(n): list([k, v] for k, v in peers.items()) for n in range(5, 10)})
        self.assertEqual(json.loads(self.f_json(encode(peers))), peers)
        self.assertEqual(decode(encode(peers)), peers)

        self.assertEqual(decode(encode([peers] * 3)), [peers] * 3)

        # deliberately same ids
        zero_d, zero_l = {}, []
        self.assertEqual(decode(encode([zero_d, zero_d, zero_l, zero_l])), [zero_d, zero_d, zero_l, zero_l])

        self.assertRaises(ZeroDivisionError, encode, 3 + 4j, default=lambda x: 1 / 0)
        self.assertRaises(TypeError, encode, 3 + 4j, default=lambda x, y, z, a, b: 1 / 0)

        class funky:
            pass

        def meth(v):
            assert isinstance(v, funky)
            return make_item(4, "0x10")

        self.assertRaises(TypeError, encode, funky())
        self.assertEqual(decode(encode(funky(), default=meth)), 0x10)
        self.assertEqual(decode(encode(funky(), default=lambda v: 0x11)), 0x11)

        # invalid jsonb returned
        self.assertRaises(ValueError, encode, funky(), default=lambda v: b"0x01\x02")

        self.assertRaisesRegex(ValueError, ".*returned the object.*", encode, funky(), default=lambda v: v)

        self.assertRaises(ValueError, decode, b"", object_hook=lambda x: x, object_pairs_hook=lambda x: x)

        # collections.abc.Mapping

        # environ is one
        self.assertIsInstance(decode(encode(os.environ)), dict)

        # custom class
        class CustomMap(collections.abc.Mapping):
            def __init__(self):
                self._data = dict()
                # put in dummy data
                self._data[1] = "one"
                self._data["two"] = 2
                self._data[3] = {3: 3.3}
                self._data[4] = [None, True, False]
                self._data[5] = os.environ

            def __getitem__(self, key: Any) -> Any:
                return self._data[key]

            def __iter__(self):
                return iter(self._data)

            def __len__(self):
                return len(self._data)

            # various evil routines for checking error handling
            def bad_items1(self):
                return {1, 2, 3}

            def bad_items2(self):
                1 / 0

            def bad_len1(self):
                return 1 + 4j

            def bad_len2(self):
                return 1 / 0

        self.assertIsInstance(decode(encode(CustomMap())), dict)

        CustomMap.items = CustomMap.bad_items1
        self.assertRaisesRegex(ValueError, ".*mapping items not.*", encode, CustomMap())
        CustomMap.items = CustomMap.bad_items2
        self.assertRaises(ZeroDivisionError, encode, CustomMap())

        del CustomMap.items

        CustomMap.__len__ = CustomMap.bad_len1
        self.assertRaises(TypeError, encode, CustomMap())
        CustomMap.__len__ = CustomMap.bad_len2
        self.assertRaises(ZeroDivisionError, encode, CustomMap())

        # key sorting - this depends on dict keys being in sort order
        data = {"f": 1, "e": 2, "d": 3, "c": 4, "b": 5, "a": 6}
        self.assertEqual(decode(encode(data), object_pairs_hook=lambda x: x), list(data.items()))
        self.assertEqual(decode(encode(data, sort_keys=True), object_pairs_hook=lambda x: x), sorted(data.items()))

        # unsortable
        self.assertRaises(TypeError, encode, {1: 2, None: 3}, sort_keys=True)
        # this won't raise
        encode({1: 2, None: 3}, sort_keys=False)

    def testArrays(self):
        def meth():
            yield 1
            yield "two"
            yield [3]
            yield True

        # stuff should fail
        for v in (
            {1, 2, 3, 4},
            meth(),
            b"aabb",
            bytearray(),
            array.array("b", [1, 2, 3]),
            array.array("f", [1.1, 2.2, 3.3]),
        ):
            self.assertRaisesRegex(TypeError, ".*Unhandled object of type.*", encode, v)

    def testHooks(self):
        object_hook_got = []

        def object_hook(v):
            object_hook_got.append(v)
            return 73

        self.assertEqual(decode(encode({"hello": 3, "world": {1: 2}}), object_hook=object_hook), 73)
        self.assertEqual(object_hook_got, [{"1": 2}, {"hello": 3, "world": 73}])

        object_hook_got = []

        self.assertEqual(decode(encode({"hello": 3, "world": {1: 2}}), object_pairs_hook=object_hook), 73)
        self.assertEqual(object_hook_got, [[("1", 2)], [("hello", 3), ("world", 73)]])

        # read only dict
        frozendict = types.MappingProxyType
        d = {"a": {"1": "hello"}, "b": True}

        assert not isinstance(d, frozendict)
        self.assertEqual(d, decode(encode(d), object_hook=frozendict))
        self.assertIsInstance(decode(encode(d), object_hook=frozendict), frozendict)
        frozen = json.loads(json.dumps(d), object_hook=frozendict)
        self.assertEqual(d, decode(encode(frozen)))

        # lists don't compare equal to tuples
        self.assertNotEqual((1, 2, 3), decode(encode((1, 2, 3))))
        self.assertEqual((1, 2, 3), decode(encode((1, 2, 3)), array_hook=tuple))

        # float and int
        class floaty:
            def __init__(self, x):
                self.x = x

        class inty(floaty):
            pass

        x = decode(encode([1, 2, 1.1, 2.2]), parse_int=inty, parse_float=floaty)
        self.assertEqual(x[0].x, "1")
        self.assertEqual(x[1].x, "2")
        self.assertEqual(x[2].x, "1.1")
        self.assertEqual(x[3].x, "2.2")

        # error checking
        all_types = {"orange": [67567567, math.pi]}
        five_numbers = self.f_jsonb("[0x1234, 5., .1]")
        for kind in "parse_int", "parse_float", "object_hook", "array_hook", "object_pairs_hook":
            detect(encode(all_types))
            self.assertRaises(ZeroDivisionError, decode, encode(all_types), **{kind: lambda x: 1 / 0})
            if kind in {"parse_int", "parse_float"}:
                self.assertRaises(ZeroDivisionError, decode, five_numbers, **{kind: lambda *args: 1 / 0})
            self.assertRaises(TypeError, decode, encode(all_types), **{kind: kind})
            self.assertRaises(TypeError, decode, encode(all_types), **{kind: kind})

    def testRecursion(self):
        "recursion control"

        # I can't get this to fail with debug builds presumably
        # because something is disabled, no matter how big I make the
        # recursion.
        if "d" in getattr(sys, "abiflags", ""):
            # give up
            return

        # this test works but depends on magic numbers and isn't stable
        # across python versions.  if I add 10 to the current depth and
        # then do *10,000* nested calls to Py_EnterRecursiveCall it
        # may or may not generate RecursionError depending on the wind
        # direction.  fault injection also tests the code.

        # so disabling this test for now because it is too difficult to
        # trigger reliably
        return

        import inspect

        depth = len(inspect.stack(0))
        # Note: this testing depends on list being looked for before
        # dict in the C.  dict detection runs __instancecheck__ which
        # is python code, so that may hit the RecursionError instead
        # of our code.

        # encode nesting.  we use complex to get an exception if that
        # is reached

        item = 3 + 4j
        for _ in range(10000):
            item = [item]

        # deeply nested jsonb.  deepest is an object we can use
        # object_hook to see if we reached it
        encoded = make_item(12, None)
        for _ in range(10000):
            encoded = make_item(11, encoded)

        with recursion_limit(depth + 15):
            # encode
            self.assertRaisesRegex(RecursionError, ".*JSONB.*", encode, item)
            # decode vs detect
            self.assertRaisesRegex(RecursionError, ".*JSONB.*", decode, encoded, object_hook=lambda x: 1 / 0)
            self.assertFalse(detect(encoded))

    def testExtras(self):
        # various flags added later to match/address issues in stdlib json
        # when used with non-default values

        # allow_nan (which is also allow infinity)
        decode(encode({1: math.nan}))
        self.assertRaisesRegex(ValueError, ".*NaN value not allowed.", encode, {1: math.nan}, allow_nan=False)
        decode(encode({1: math.inf}))
        self.assertRaisesRegex(ValueError, ".*Infinity value not allowed.", encode, {1: math.inf}, allow_nan=False)

        # how non-str are stringified as object keys
        self.assertRaises(ValueError, encode, None, skipkeys=True, default_key=lambda: 1)
        self.assertRaises(TypeError, encode, {3 + 4j: 1}, default_key=lambda x: 1)
        self.assertRaises(ZeroDivisionError, encode, {3 + 4j: 1}, default_key=lambda x: 1 / 0)

        self.assertEqual({"": 1}, decode(encode({3 + 4j: 1}, default_key=lambda x: "")))

        # exact types
        class sint(int):
            pass

        class sstr(str):
            pass

        class sfloat(float):
            pass

        class slist(list):
            def __hash__(self):
                return 9

        class stuple(tuple):
            pass

        class sdict(dict):
            def __hash__(self):
                return 8

        subs = {sint, sstr, sfloat, slist, stuple, sdict}

        seen = set()
        seen_keys = set()

        for s in subs:
            # instance of subclass
            inst = s()
            parent = inst.__class__.__bases__[0](inst)

            encode(inst, exact_types=False)
            self.assertRaisesRegex(TypeError, ".*Unhandled object of type.*", encode, inst, exact_types=True)
            self.assertRaisesRegex(TypeError, ".*Unhandled object of type.*", encode, [1, inst, 3], exact_types=True)
            self.assertRaisesRegex(TypeError, ".*Unhandled object of type.*", encode, {1: 1, 2: inst}, exact_types=True)

            def conv(x):
                seen.add(x.__class__)
                return x.__class__.__bases__[0](x)

            self.assertEqual(encode(parent), encode(inst, default=conv, exact_types=True))

            # check object keys
            def conv(x):
                seen_keys.add(x.__class__)
                return x.__class__.__name__

            self.assertRaisesRegex(TypeError, ".*Keys must be str, .*not.*", encode, {inst: 3}, exact_types=True)
            encode({inst: 3}, default_key=conv, exact_types=True)

        self.assertEqual(seen, subs)
        self.assertEqual(seen_keys, subs)

        all_at_once = dict(
            sdict(
                [
                    (1, sint(3)),
                    (2, sstr("seven")),
                    (3, sfloat(3.14)),
                    (4, slist([sint()])),
                    (5, stuple(["hello", sstr("world")])),
                ]
            )
        )

        expected = {"1": 3, "2": "seven", "3": 3.14, "4": [0], "5": ["hello", "world"]}

        self.assertRaisesRegex(TypeError, ".*Unhandled object of type.*", encode, all_at_once, exact_types=True)
        self.assertEqual(expected, decode(encode(all_at_once)))

        def conv(x):
            return encode(x.__class__.__bases__[0](x))

        self.assertEqual(expected, decode(encode(all_at_once, default=conv, exact_types=True)))

        def conv(x):
            return 3

        self.assertRaisesRegex(
            TypeError,
            ".*default_key callback needs to return a str, not int.*",
            encode,
            {3 + 4j: 1},
            exact_types=True,
            default_key=conv,
        )

        # original use case using examples from enum doc
        if sys.version_info >= (3, 11):
            # python 3.10 doesn't have StrEnum
            import enum

            class Number(enum.IntEnum):
                ONE = 1
                TWO = 2
                THREE = 3

            class Color(enum.StrEnum):
                RED = "r"
                GREEN = "g"
                BLUE = "b"

            example = {"one": Number.ONE, "r": Color.RED, Number.TWO: "two", Color.GREEN: "g"}
            expected = {"one": 1, "r": "r", "2": "two", "g": "g"}

            self.assertEqual(expected, decode(encode(example)))

            self.assertRaises(TypeError, encode, example, exact_types=True)
            self.assertRaises(TypeError, encode, example, default=lambda x: str(x), exact_types=True)
            self.assertRaises(TypeError, encode, example, default_key=lambda x: str(x), exact_types=True)

            expected = {
                "one": "<Number.ONE: 1>",
                "r": "<Color.RED: 'r'>",
                "<Number.TWO: 2>": "two",
                "<Color.GREEN: 'g'>": "g",
            }
            self.assertEqual(
                expected,
                decode(encode(example, default=lambda x: repr(x), default_key=lambda x: repr(x), exact_types=True)),
            )

    def testBadContent(self):
        # not zero length
        self.check_invalid(b"")

        # buffer must be one object only
        encoded = make_item(0, None) + make_item(1, None)
        self.check_invalid(encoded)

        self.assertRaises(TypeError, detect, 3 + 4j)

        # reserved tags
        for tag in (13, 14, 15):
            encoded = make_item(tag, "foo")
            self.check_invalid(encoded)

        # none and bool with size
        for tag in 0, 1, 2:
            encoded = make_item(tag, ["null", "true", "false"][tag])
            self.check_invalid(encoded)

        # insufficient space
        self.check_invalid(make_item(5, "hello", length=3))
        self.check_invalid(make_item(5, "hello", length=3, len_encoding=8))
        self.check_invalid(make_item(5, "hello", length=3, len_encoding=8)[:4])
        self.check_invalid(make_item(5, "hello world one two")[:7])

        # not numbers
        for number in (
            "",
            "-",
            "--1",
            "1-2",
            "0.-1",
            ".-1",
            "-+1.2",
            "+1.-2",
            "1e-2-",
            "1e++2",
            "1e+-2",
            "1e-+2",
            "1e--2",
            "1.2.3",
            "1.2E3.4",
            "1.2E2E3",
            "1.2E+",
            "1.2E-",
            "1.2.3",
            "1\x99",
            "1\x03",
            "0x\x07",
            "0x\xa7",
            "0x",
            "x123",
            "+1",
            "+1.1",
            "1+1",
            "0xx89",
            "0x9exa",
            "0x999x999",
            "00x88",
            "0-1",
            "0x-1",
            "x1",
            "\r\n",
            "1.2\r\n",
            "E3",
            "3E",
            "00001",
            "+00001",
            "-00001",
            "+00002.2",
        ):
            #  int, int5, float, float5
            for kind in (3, 4, 5, 6):
                encoded = make_item(kind, number)
                # sqlite doesn't check enough
                include_sqlite = True
                if "0000" in number:
                    # it doesn't reject leading zeroes
                    include_sqlite = False
                elif kind in {5, 6} and number == "E3":
                    # it doesn't verify a leading floats have leading number or 0x
                    include_sqlite = False
                self.check_invalid(encoded, include_sqlite=include_sqlite)

        # not valid text
        invalid_texts = {
            7:  # TEXT
            (
                "one\x17two",
                'one"two',
                "one\\two",
            ),
            8:  # TEXTJ
            (
                "one\x17two",
                'one"two',
                r"one\u123two",
                r"one\u123",
                "one\\",
                r"\one",
                r"\'abc",
                r"\h",
                r"\v",
            ),
            9:  # TEXT5
            (
                r"hello\x1mark",
                "hello\\",
                r"\01",
                r"\h",
                r"\x3",
            ),
        }
        for tag in invalid_texts:
            for item in invalid_texts[tag]:
                # the prefix is to force code to go to the complex function
                for prefix in ("", "🤦🏼‍♂️"):
                    encoded = make_item(tag, prefix + item)
                    self.check_invalid(encoded, include_sqlite="mark" not in item)

        # not correctly formed surrogate pairs
        for s in (
            r"\ud8ab",
            r"\ud8abX",
            "\\ud8ab\\",
            r"\ud8ab\u",
            r"\ud8ab\uX",
            r"\ud8ab\udc0",
            r"\ud8ab\udbff",
        ):
            for tag in {8, 9}:
                self.check_invalid(make_item(tag, s), include_sqlite=False)

        # not object

        for kind in range(16):
            # tag not text
            if kind not in {7, 8, 9, 10}:
                self.check_invalid(make_item(12, make_item(kind, "hello") + make_item(0)))

        # missing value
        self.check_invalid(make_item(12, make_item(7, "hello")))

        # odd number
        self.check_invalid(make_item(12, make_item(7, "hello") + make_item(0) + make_item(6, "3.3")))

        # truncate
        self.check_invalid(make_item(12, (make_item(7, "hello") + make_item(7, "world"))[:-2]))

        # too long
        self.check_invalid(make_item(12, make_item(7, "hello") + make_item(7, "world"), length=33))

        # too short
        self.check_invalid(make_item(12, make_item(7, "hello") + make_item(7, "world"), length=6))

        # not array

        # truncate
        self.check_invalid(make_item(11, (make_item(7, "hello") + make_item(7, "world"))[:-2]))

        # too long
        self.check_invalid(make_item(11, make_item(7, "hello") + make_item(7, "world"), length=33))

        # too short
        self.check_invalid(make_item(11, make_item(7, "hello") + make_item(7, "world"), length=6))

    def testInvalidUTF8(self):
        # AI prompt: generate 20 byte strings in python syntax that are convincing but invalid utf8
        for s in (
            # CHATGPT
            # Overlong encodings
            b"\xc0\xaf",  # Overlong '/' (should be 0x2F)
            b"\xe0\x80\xaf",  # Overlong '/'
            b"\xf0\x80\x80\xaf",  # Overlong '/'
            b"\xf8\x80\x80\x80\xaf",  # Illegal 5-byte sequence
            # Lone continuation bytes
            b"\x80",
            b"\xbf",
            b"abc\x80xyz",
            b"\xa0\xa1",  # Two continuation bytes
            # Truncated multibyte sequences
            b"\xc2",  # Start of 2-byte char, no continuation
            b"\xe2\x82",  # Start of 3-byte char, missing continuation
            b"\xf0\x9f",  # Start of 4-byte char, incomplete
            b"\xf0\x9f\x92",  # Start of emoji sequence, truncated
            # Surrogates (forbidden in UTF-8)
            b"\xed\xa0\x80",  # U+D800
            b"\xed\xbf\xbf",  # U+DFFF
            b"\xed\xa0\x80abc",  # Surrogate plus ASCII
            # Invalid codepoint (> U+10FFFF)
            b"\xf4\x90\x80\x80",  # Too large (> max)
            b"\xf7\xbf\xbf\xbf",  # 4-byte sequence above range
            # Mixed valid + invalid
            b"hello\xc0world",  # Overlong + ASCII
            b"\xe2\x28\xa1",  # Invalid continuation (0x28 not in range)
            b"\xa0hello\xf5\x90",  # Continuation + illegal 5-byte start
            #
            # CLAUDE
            # 1. Orphaned continuation bytes (0x80-0xBF without starter)
            b"Hello \x80world",
            # 2. Invalid starter byte (0xFF is never valid in UTF-8)
            b"caf\xffe",
            # 3. Incomplete multibyte sequence (missing continuation bytes)
            b"Test\xc2 string",
            # 4. Overlong encoding of ASCII 'A' (should be 0x41, not 0xC1 0x81)
            b"String \xc1\x81 here",
            # 5. Invalid continuation byte pattern
            b"Data\xe2\x28\xa1",
            # 6. Lone high surrogate equivalent
            b"Text\xed\xa0\x80end",
            # 7. Invalid 4-byte sequence starter with wrong continuation
            b"File\xf0\x28\x8c\xbc",
            # 8. Truncated 3-byte sequence
            b"Name\xe2\x82",
            # 9. Invalid byte 0xFE (reserved)
            b"user\xfe\xfename",
            # 10. Wrong number of continuation bytes
            b"path/\xc2\xc2\x80",
            # 11. Overlong encoding of forward slash
            b"dir\xc0\xaf\xffile",
            # 12. Invalid second byte in 2-byte sequence
            b"log\xc2\x1f.txt",
            # 13. Continuation byte at start
            b"\x80config.json",
            # 14. Invalid 4-byte sequence (beyond Unicode range)
            b"img\xf7\xbf\xbf\xbf.jpg",
            # 15. Mixed invalid bytes
            b"temp\xff\x80\xbf",
            # 16. Incomplete at end of string
            b"output\xe2\x9c",
            # 17. Invalid starter 0xC0 (overlong encoding)
            b"backup\xc0\x80.zip",
            # 18. Wrong continuation in 3-byte sequence
            b"script\xe1\x28\x80.py",
            # 19. Multiple orphaned continuation bytes
            b"log\x80\x81\x82.log",
            # 20. Invalid high byte 0xF8 (5-byte sequences not allowed)
            b"data\xf8\x88\x80\x80\x80",
            #
            # GEMINI
            # Incomplete multi-byte sequences
            b"incomplete char \xe2\x98",  # Missing the 3rd byte of a 3-byte sequence.
            b"emoji fail \xf0\x9f\x98",  # Missing the 4th byte of a 4-byte sequence.
            b"ends dangling \xdf",  # Ends with an incomplete 2-byte character starter.
            b"not enough \xf4\x8f\xbf",  # Incomplete 4-byte sequence.
            b"one more needed \xe1\x80",  # Incomplete 3-byte sequence.
            # Invalid continuation bytes (0x80-0xBF)
            b"\x80startswithcontinuation",  # Starts with a continuation byte.
            b"misplaced\xbfcontinuation",  # Continuation byte follows an ASCII character.
            b"a \x99 b",  # Lone continuation byte.
            b"test \x61\x85\x62",  # Continuation byte between ASCII characters.
            b"another \x68\x65\x6c\x6c\x6f\x80",  # Continuation byte without a starter.
            # Invalid start bytes or byte values
            b"overlong slash \xc0\xaf",  # 0xc0 is an illegal start byte (overlong encoding).
            b"another overlong \xc1\x81",  # 0xc1 is an illegal start byte (overlong encoding).
            b"way too high \xf5\x80\x80\x80",  # 0xf5 is an invalid start byte (past U+10FFFF).
            b"forbidden byte \xfe",  # 0xfe is never used in UTF-8.
            b"also forbidden \xff",  # 0xff is never used in UTF-8.
            # Invalid sequences
            b"surrogate pair \xed\xa0\x80",  # Encodes U+D800, which is an invalid surrogate codepoint.
            b"wrong follower \xe2\x98\x41",  # ASCII 'A' (0x41) cannot be a continuation byte.
            b"starter after start \xe1\xc2\x80",  # Start byte (0xc2) cannot follow a start byte.
            b"starter after cont \xe1\x80\xc2",  # Start byte (0xc2) cannot follow a continuation byte.
            b"bad sequence \xf1\x80\x80\x42",  # ASCII 'B' (0x42) breaks the 4-byte sequence.
        ):
            for tag in {7, 8, 9, 10}:
                encoded = make_item(tag, s)
                # sqlite doesn't check utf8 validity
                self.check_invalid(encoded, include_sqlite=False)
                # this should not result in any memory errors in sqlite
                with contextlib.suppress(UnicodeDecodeError):
                    self.assertGreaterEqual(len(self.f_json(encoded)), 0)

    def testSizing(self):
        "length encoding"
        # the same item length can be encoded multiple ways with
        # leading zeroes.  this checks we handle them correctly.
        # while sqlite decodes 8 byte lengths, it rejects any value
        # longer than 4 bytes (4GB) because everything else in sqlite
        # is 2GB limited.

        for len_encoding in 0, 1, 2, 4, 8:
            vals = {
                0: (make_item(0, None, len_encoding=len_encoding), None),
                1: (make_item(1, None, len_encoding=len_encoding), True),
                2: (make_item(2, None, len_encoding=len_encoding), False),
                3: (make_item(3, "3", len_encoding=len_encoding), 3),
                4: (make_item(4, "0x3", len_encoding=len_encoding), 3),
                5: (make_item(5, "3.0", len_encoding=len_encoding), 3),
                6: (make_item(6, "3.", len_encoding=len_encoding), 3),
                7: (make_item(7, "3", len_encoding=len_encoding), "3"),
                8: (make_item(8, "3", len_encoding=len_encoding), "3"),
                9: (make_item(9, "4", len_encoding=len_encoding), "4"),
                10: (make_item(10, "3", len_encoding=len_encoding), "3"),
            }
            # array
            vals[11] = make_item(11, vals[3][0] + vals[4][0] + vals[7][0], len_encoding=len_encoding), [3, 3, "3"]
            # object
            vals[12] = (
                make_item(12, vals[7][0] + vals[3][0] + vals[9][0] + vals[9][0], len_encoding=len_encoding),
                {"3": 3, "4": "4"},
            )

            for k, (encoded, expected) in vals.items():
                # jsonb_valid says overlong encodings of null/true/false are invalid,  eg c000 is
                # rejected even though it is a compliant encoding of 0 length null.   sqlite also
                # decodes it wrong, so we give up and do things the sqlite way
                if k <= 2 and len_encoding > 0:
                    self.check_invalid(encoded)
                else:
                    self.check_valid(encoded, expected)

    def testRandomJSON(self):
        "sqlite randomjson extension if present"
        if not os.path.exists("randomjson.so"):
            return

        self.db.enable_load_extension(True)
        self.db.load_extension("./randomjson")

        for seed in range(2000, 3000):
            # we round trip the json through as many combinations of SQLite, Python and APSW
            # encoders / decoders as possible
            j, j5 = self.db.execute("SELECT random_json(:1), random_json5(:1)", (seed,)).get

            try:
                # some seeds cause illegal surrogate codepoints - we skip them as we treat that as invalid
                # example seeds are 47, 53, 70, 78
                check_strings_valid_utf8(json.loads(self.f_json(j)))
                check_strings_valid_utf8(json.loads(self.f_json(j5)))
            except UnicodeEncodeError:
                continue

            self.assertEqual(json.loads(j), decode(encode(json.loads(j))))
            self.assertEqual(json.loads(j), decode(self.f_jsonb(j)))
            self.assertEqual(json.loads(self.f_json(j5)), decode(self.f_jsonb(j5)))


class Conversion(unittest.TestCase):
    "the convert binding and jsonb apis"

    def setUp(self):
        self.db = apsw.Connection(":memory:")
        # set these up each time
        self.f_json = apsw.ext.Function(self.db, "json")
        self.f_jsonb = apsw.ext.Function(self.db, "jsonb")
        self.f_json_valid = apsw.ext.Function(self.db, "json_valid")

    def tearDown(self):
        self.db.close()
        del self.db
        for c in apsw.connections():
            c.close()

    def testPrefix(self):
        # check we don't treat a valid JSONB prefix of a BLOB as JSONB
        # which was a bug ...
        def cb(cur, n, val):
            return apsw.jsonb_decode(val)

        self.db.convert_jsonb = cb
        for v in (
            b"\x01\x73\x94\x65",
            make_item(5, "3.14") + b"5",
            make_item(5, "3.14")[:-1],
        ):
            x = self.db.execute("select ?", (v,)).get
            self.assertIsInstance(x, bytes)

    def testConvertBinding(self):
        "just convert binding"
        called = [0, 0]

        def con_conv(cur, argnum, value):
            self.assertIsInstance(cur, apsw.Cursor)
            self.assertIsInstance(argnum, int)
            self.assertGreaterEqual(argnum, 1)
            called[0] += 1
            return "con"

        def cur_conv(cur, argnum, value):
            self.assertIsInstance(cur, apsw.Cursor)
            self.assertIsInstance(argnum, int)
            self.assertGreaterEqual(argnum, 1)
            called[1] += 1
            return "cur"

        cur = self.db.cursor()

        self.assertIsNone(self.db.convert_binding)
        self.assertIsNone(cur.convert_binding)

        self.assertRaises(TypeError, self.db.execute, "select ?", (3 + 4j,))

        self.db.convert_binding = con_conv
        self.assertEqual("con", self.db.execute("select ?", (3 + 4j,)).get)
        self.assertEqual(called, [1, 0])

        called = [0, 0]
        cur.convert_binding = cur_conv
        self.assertEqual("cur", cur.execute("select ?", (3 + 4j,)).get)
        self.assertEqual(called, [0, 1])

        called = [0, 0]
        cur.convert_binding = None
        self.assertRaises(TypeError, cur.execute, "select ?", (3 + 4j,))
        self.assertEqual(called, [0, 0])

        # it should be called on the missing bindings since we don't check
        # each one is actually used
        self.assertEqual(
            "con",
            self.db.execute(
                "select ?3",
                (
                    1 + 4j,
                    2 + 4j,
                    3 + 4j,
                ),
            ).get,
        )
        self.assertEqual(called, [3, 0])
        self.db.convert_binding = None
        self.assertRaises(TypeError, self.db.execute, "select ?", (3 + 4j,))

        def func(*args):
            1 / 0

        self.db.convert_binding = func
        self.assertRaises(ZeroDivisionError, self.db.execute, "select ?", (3 + 4j,))

        def func(cur, n, val):
            if n == 3:
                return val
            return "foo"

        self.db.convert_binding = func
        self.assertRaisesRegex(
            ValueError,
            ".*convert_binding returned the same object it was passed.*",
            self.db.execute,
            "select ?3",
            (
                1 + 4j,
                2 + 4j,
                3 + 4j,
            ),
        )

        # basic json
        def conv(cur, n, val):
            return encode(val)

        self.db.convert_binding = conv

        self.assertEqual(example_data, decode(self.db.execute("select ?", (example_data,)).get))
        self.assertEqual("blob", self.db.execute("select typeof(?)", (example_data,)).get)
        self.assertEqual(example_data, json.loads(self.db.execute("select json(?)", (example_data,)).get))

        # decimal
        decimal.getcontext().prec = 128

        def conv(cur, n, val):
            self.assertIsInstance(val, decimal.Decimal)
            return make_item(5, str(val))

        self.db.convert_binding = conv

        d = decimal.Decimal("-0.786438726487326478632879468237648732687463287648723648762384732")
        self.assertNotEqual(float(d), d)
        self.assertEqual(d, decode(self.db.execute("select ?", (d,)).get, parse_float=decimal.Decimal))

        # traverse
        self.assertIn(conv, gc.get_referents(self.db))
        self.assertNotIn(conv, gc.get_referents(cur))
        cur.convert_binding = conv
        self.assertIn(conv, gc.get_referents(cur))

        # get set
        self.assertRaises(TypeError, setattr, self.db, "convert_binding", 3 + 4j)
        self.assertEqual(conv, self.db.convert_binding)
        self.assertRaises(TypeError, setattr, cur, "convert_binding", 3 + 4j)
        self.assertEqual(conv, cur.convert_binding)
        self.db.convert_binding = cur.convert_binding = None
        self.assertIsNone(self.db.convert_binding)
        self.assertIsNone(cur.convert_binding)

    def testConvertJSONB(self):
        "just convert jsonb"
        called = [0, 0]

        def con_conv(cur, argnum, value):
            self.assertRaises(apsw.ThreadingViolationError, cur.connection.close)
            self.assertRaises(apsw.ThreadingViolationError, cur.close)
            self.assertIsInstance(cur, apsw.Cursor)
            self.assertIsInstance(argnum, int)
            self.assertEqual(0, argnum)
            called[0] += 1
            return "con"

        def cur_conv(cur, argnum, value):
            self.assertRaises(apsw.ThreadingViolationError, cur.connection.close)
            self.assertRaises(apsw.ThreadingViolationError, cur.close)
            self.assertIsInstance(cur, apsw.Cursor)
            self.assertIsInstance(argnum, int)
            self.assertEqual(0, argnum)
            called[1] += 1
            return "cur"

        self.assertIsNone(self.db.convert_jsonb)

        t = make_item(3, "33")

        self.assertEqual(t, self.db.execute("select ?", (t,)).get)

        self.db.convert_jsonb = con_conv
        self.assertEqual("con", self.db.execute("select ?", (t,)).get)
        self.assertEqual(called, [1, 0])

        cur = self.db.cursor()
        self.assertIsNone(cur.convert_jsonb)
        cur.convert_jsonb = cur_conv
        called = [0, 0]
        self.assertEqual("cur", cur.execute("select ?", (t,)).get)
        self.assertEqual(called, [0, 1])

        called = [0, 0]
        cur.convert_jsonb = None
        self.assertEqual(t, cur.execute("select ?", (t,)).get)
        self.assertEqual(called, [0, 0])

        def func(*args):
            1 / 0

        self.db.convert_jsonb = func
        self.db.execute("select x'112233'").get
        self.assertRaises(ZeroDivisionError, getattr, self.db.execute("select ?", (t,)), "get")

        # basic json
        def conv(cur, n, val):
            return decode(val)

        self.db.convert_jsonb = conv

        self.assertEqual(example_data, self.db.execute("select jsonb(?)", (json.dumps(example_data),)).get)

        def conv(cur, n, val):
            return

        # decimal
        decimal.getcontext().prec = 128

        def conv(cur, n, val):
            return decode(val, parse_float=decimal.Decimal)

        self.db.convert_jsonb = conv

        d = decimal.Decimal("-0.786438726487326478632879468237648732687463287648723648762384732")
        self.assertNotEqual(float(d), d)
        self.assertEqual(d, self.db.execute("select ?", (make_item(5, str(d)),)).get)

        # traverse
        self.assertIn(conv, gc.get_referents(self.db))
        self.assertNotIn(conv, gc.get_referents(cur))
        cur.convert_jsonb = conv
        self.assertIn(conv, gc.get_referents(cur))

        # get set
        self.assertRaises(TypeError, setattr, self.db, "convert_jsonb", 3 + 4j)
        self.assertEqual(conv, self.db.convert_jsonb)
        self.assertRaises(TypeError, setattr, cur, "convert_jsonb", 3 + 4j)
        self.assertEqual(conv, cur.convert_jsonb)
        self.db.convert_jsonb = cur.convert_jsonb = None
        self.assertIsNone(self.db.convert_jsonb)
        self.assertIsNone(cur.convert_jsonb)


class Ext(unittest.TestCase):
    "related code in apsw.ext"

    def setUp(self):
        self.db = apsw.Connection(":memory:")

    def tearDown(self):
        self.db.close()
        del self.db
        for c in apsw.connections():
            c.close()

    def testFunction(self):
        called = 0

        def rowcb(*args):
            nonlocal called
            called += 1
            return ("rowcb", "was", "here")

        def execcb(*args):
            nonlocal called
            called += 1
            return True

        def convbind(*args):
            nonlocal called
            called += 1
            return "convbind"

        def convjsonb(*args):
            nonlocal called
            called += 1
            return "convjsonb"

        self.assertRaisesRegex(
            apsw.SQLError, ".*no such function.*", apsw.ext.Function(self.db, "this do\"esn't exist"), "hello"
        )

        self.db.row_trace = rowcb
        self.db.exec_trace = execcb
        self.db.convert_binding = convbind
        self.db.convert_jsonb = convjsonb

        self.assertEqual(8, self.db.execute("select length(?)", (3 + 4j,)).get)
        self.assertEqual(2, called)

        called = 0
        length = apsw.ext.Function(self.db, "length")
        self.assertEqual(5, length("hello"))
        self.assertEqual(0, called)

        length = apsw.ext.Function(
            self.db, "length", convert_binding=convbind, exec_trace=execcb, convert_jsonb=convjsonb
        )
        self.assertEqual(8, length(3 + 4j))
        self.assertEqual(2, called)


def check_strings_valid_utf8(obj):
    # checks all strings in a json decoded object came from valid utf8
    if isinstance(obj, str):
        obj.encode("utf-8")
    elif isinstance(obj, list):
        for i in obj:
            check_strings_valid_utf8(i)
    elif isinstance(obj, dict):
        for k, v in obj.items():
            check_strings_valid_utf8(k)
            check_strings_valid_utf8(v)


def make_item_test(tag: int, value=None, *, len_encoding: int = None, length: int = None):
    # value is str/bytes of the payload
    # len_encoding is how many bytes to use to encode the length, None means
    # auto
    # length is length bytes to use.  None means auto.  This is only useful to cause
    # deliberate errors.
    assert 0 <= tag <= 15

    if value is None and len_encoding is None and length is None:
        return bytes([tag])

    if value is None:
        value = b""
    elif isinstance(value, str):
        value = value.encode("utf8")
    else:
        assert isinstance(value, bytes)

    l = len(value) if length is None else length

    if len_encoding is None:
        if l <= 11:
            len_encoding = 0
        elif l <= 0xFF:
            len_encoding = 1
        elif l <= 0xFFFF:
            len_encoding = 2
        elif l <= 0xFFFF_FFFF:
            len_encoding = 4
        else:
            len_encoding = 8

    assert len_encoding in {0, 1, 2, 4, 8}

    if len_encoding == 0:
        assert l <= 11
        res = bytes([tag | (l << 4)])
    else:
        res = bytes([tag | ((12 + int(math.log2(len_encoding))) << 4)]) + l.to_bytes(len_encoding, "big")

    return res + value


def make_item(tag: int, value=None, *, len_encoding: int = None, length: int = None):
    if tag > 12 or len_encoding is not None or length is not None:
        return make_item_test(tag, value, len_encoding=len_encoding, length=length)

    check = make_item_test(tag, value)
    against = apsw.ext.make_jsonb(tag, value)

    assert check == against

    return check


def pi_digits(count: int):
    # adapted from a chatgpt answer
    res = []
    q, r, t, k, n, l = 1, 0, 1, 1, 3, 3
    while len(res) < count:
        if 4 * q + r - t < n * t:
            res.append(str(n))
            q, r, t, k, n, l = 10 * q, 10 * (r - n * t), t, k, (10 * (3 * q + r) // t) - 10 * n, l
        else:
            q, r, t, k, n, l = q * k, (2 * q + r) * l, t * l, k + 1, (q * (7 * k + 2) + r * l) // (t * l), l + 2

    return res[0] + "." + "".join(res[1:])


@contextlib.contextmanager
def recursion_limit(value: int):
    existing = sys.getrecursionlimit()
    sys.setrecursionlimit(value)
    try:
        yield
    finally:
        sys.setrecursionlimit(existing)


__all_ = ("JSONB", "Conversion", "Ext")


if __name__ == "__main__":
    unittest.main()
