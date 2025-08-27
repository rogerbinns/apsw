#!/usr/bin/env python3

# This testing code deliberately does nasty stuff so mypy isn't helpful
# mypy: ignore-errors
# type: ignore

import unittest
import functools
import sys
import math
import json

import apsw
import apsw.ext
import apsw.fts5

# these methods will move to being in C and part of apsw module
import apsw.jsonb

encode = apsw.jsonb.encode
decode_raw = apsw.jsonb.decode
detect = apsw.jsonb.detect


class DetectDecodeMisMatch(Exception):
    pass


def decode(data, *, object_hook=None, object_pairs_hook=None):
    # this wrapper ensures that detect results are always the same as
    # decode - ie detect returns False for decodes that fail, and True for
    # success
    try:
        detection = detect(data)
    except Exception:
        raise DetectDecodeMisMatch("detection raised exception - it must never do that and only return bool")

    try:
        res = decode_raw(data, object_hook=object_hook, object_pairs_hook=object_pairs_hook)
        if detection is not True:
            raise DetectDecodeMisMatch(f"detection gave {detection} while decode succeeded")
        return res
    except Exception:
        if detection is not False:
            raise DetectDecodeMisMatch(f"detection gave {detection} while decode raised an exception")
        raise


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

    def testUnicodeStrings(self):
        "hairy unicode strings"

        test_strings = [s[0].decode("utf8") for s in apsw.fts5.tokenizer_test_strings()]
        self.check_item(test_strings)

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

    def check_invalid(self, encoded: bytes):
        # encoded item should be rejected as not valid jsonb by us
        self.assertFalse(detect(encoded))
        self.assertRaises(ValueError, decode, encoded)

        # and by sqlite
        self.assertFalse(self.f_json_valid(encoded, 8))

    def check_valid(self, encoded: bytes, value):
        # encoded should be accepted as valid jsonb
        self.assertTrue(detect(encoded))
        self.assertEqual(decode(encoded), value)

        # and by sqlite
        self.assertTrue(self.f_json_valid(encoded, 8))
        self.assertEqual(json.loads(self.f_json(encoded)), value)

    def testEmpty(self):
        "empty / none / etc"

        for item in (None, "", [], [None], ["", None], [[], {}], [[[]]], {}, {"": None}, {"": ""}, {"": {}}, {"": []}):
            self.check_item(item)

    def testNumbers(self):
        "numbers"

        self.check_item(3.1415E-10)

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

        # json5 allows leading and trailing dots, and leading +
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
            # ::TODO:: sqlite bug this is not accepted - json doesn't allow leading + but json5 does
            # ("+123.", None),
            # ("+.123", None),
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
            # ::TODO:: sqlite doesn't allow leading +
            # ("+0XdeCaf", 0xDECAF),
        ):
            if expected is None:
                expected = int(int5)
            print(f"{int5=}")
            # not valid int.  json int doesn't allow leading + or
            # leading zeroes
            if abs(expected) != 123 or int5[0] == "+":
                self.check_invalid(make_item(3, int5))
            # but valid int5 allows hex and leading + but not leading
            # zeroes
            encoded5 = make_item(4, int5)
            print(f"{int5=} {expected=} {encoded5.hex()=}")
            #breakpoint()
            self.check_valid(encoded5, expected)

    def testBadContent(self):
        for number in (
            "--1",
            "-+1.2",
            "+1.-2",
            "1e-2-",
            "1e++2",
            "1.2.3",
            "1.2E3.4",
            "1.2E2E3",
            "1.2E+",
            "1.2E-",
            "1.2.3",
            "0x",
            "0xx89",
            "0x9exa",
            "0x999x999",
            "00x88",
            # ::TODO:: sqlite doesn't reject these jsonb and should
            # "0001",
            # "+0001",
            # "-001",
            #"+002.2",

        ):
            #  int, int5, float, float5
            for kind in (3, 4, 5, 6):
                print(f"{kind=} {number=!r}")
                encoded = make_item(kind, number)
                self.check_invalid(encoded)


def make_item(tag: int, value=None, len_encoding: int = None):
    # value is str/bytes of the payload
    # len_encoding is how many bytes to use to encode the length, None means
    # auto
    assert 0 <= tag <= 15

    if value is None:
        return bytes([tag])

    if isinstance(value, str):
        value = value.encode("utf8")
    else:
        assert isinstance(value, bytes)

    l = len(value)

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
        res = bytes([tag | ((12 + int(math.log2(len_encoding))) << 4)]) + l.to_bytes(len_encoding)

    return res + value


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


__all_ = ("JSONB",)


if __name__ == "__main__":
    unittest.main()
