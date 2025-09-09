#!/usr/bin/env python3

# This testing code deliberately does nasty stuff so mypy isn't helpful
# mypy: ignore-errors
# type: ignore

import collections.abc
import contextlib
import json
import math
import os
import types
import unittest

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
            # Various None/Empty
            None,
            False,
            True,
            "",
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
            # simple
            "hello world",
            0,
        ):
            self.check_item(item)

    def testStrings(self):
        # our gnarly test strings
        test_strings = [s[0].decode("utf8") for s in apsw.fts5.tokenizer_test_strings()]
        self.check_item(test_strings)

        # mixed case
        self.check_valid(self.f_jsonb(r'"\uAbCd\u0fFf"'), "\uabcd\u0fff")

        # try to get each type of string - we only generate textraw
        for s, expected in (
            ('"hello"', "hello"),
            (r'"\u0020\n"', " \n"),
            (r'"\0"', "\0"),
            (r'"\0a"', "\0a"),
            # ::TODO:: \v needs to be added back once SQLite fixes bug
            (r'"\x5c\"\0\n\r\b\t\f\'"', "\\\"\0\n\r\b\t\f'"),
        ):
            encoded = self.f_jsonb(s)
            self.assertEqual(decode(encoded), expected)
            self.assertEqual(json.loads(self.f_json(encoded)), expected)

        # zero length
        for tag in {7, 8, 9, 10}:
            encoded = make_item(tag, "")
            self.check_valid(encoded, "")

        # others
        for s in (
            "\0",
            "\0\n\r\b\t\f\v\\\"'/",
            "0x" + chr(0x10FFFF) + chr(0x10FFFE) + chr(0x10FFFD) + chr(1) + chr(2),
        ):
            self.check_item(s)

        # escaping
        # ::TODO:: \v needs to be added back once SQLite fixes bug
        encoded = make_item(9, r"\'\\\b\f\n\r\t\0")
        self.check_valid(encoded, "'\\\b\f\n\r\t\0")

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
            # not valid int.  json int doesn't allow leading + or
            # leading zeroes
            if abs(expected) != 123 or int5[0] == "+":
                self.check_invalid(make_item(3, int5))
            # but valid int5 allows hex and leading + but not leading
            # zeroes
            encoded5 = make_item(4, int5)
            detect(encoded5)
            self.check_valid(encoded5, expected)

    def testObjects(self):
        # python json allows these types to be keys and
        # does them as strings.  it does not call default
        for obj in (None, True, False, 99, math.pi):
            k = json.dumps(obj)
            self.check_item({obj: obj}, {k: obj})

        self.assertRaises(TypeError, encode, {3 + 3j: 3})

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

        class funky:
            pass

        def meth(v):
            assert isinstance(v, funky)
            return make_item(4, "0x10")

        self.assertRaises(TypeError, encode, funky())
        self.assertEqual(decode(encode(funky(), default=meth)), 0x10)
        self.assertEqual(decode(encode(funky(), default=lambda v: 0x11)), 0x11)

        self.assertRaises(ValueError, encode, funky(), default=lambda v: b"0x01\x02")

        self.assertRaisesRegex(ValueError, ".*returned the object.*", encode, funky(), default=lambda v: v)

        self.assertRaises(ValueError, decode, b"", object_hook=lambda x: x, object_pairs_hook=lambda x: x)


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
        self.assertNotEqual((1,2,3), decode(encode((1,2,3))))
        self.assertEqual((1,2,3), decode(encode((1,2,3)), array_hook=tuple))

        # float and int
        class floaty:
            def __init__(self, x):
                self.x = x

        class inty(floaty):
            pass

        x = decode(encode([1, 2, 1.1, 2.2]), int_hook=inty, float_hook=floaty)
        self.assertEqual(x[0].x, "1")
        self.assertEqual(x[1].x, "2")
        self.assertEqual(x[2].x, "1.1")
        self.assertEqual(x[3].x, "2.2")

        # error checking
        all_types = {"orange": [67567567, math.pi]}
        for kind in "int_hook", "float_hook", "object_hook", "array_hook", "object_pairs_hook":
            detect(encode(all_types))
            self.assertRaises(ZeroDivisionError, decode, encode(all_types), **{kind: lambda x: 1/0})
            self.assertRaises(TypeError, decode, encode(all_types), **{kind: kind})

    def testBadContent(self):
        # not zero length
        self.check_invalid(b"")

        # buffer must be one object only
        encoded = make_item(0, None) + make_item(1, None)
        self.check_invalid(encoded)

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
            "--1",
            "1-2",
            "0.-1",
            ".-1",
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
            "0-1",
            "0x-1",
            "x1",
            "\r\n",
            "1.2\r\n",
            "E3",
            # ::TODO:: sqlite doesn't reject these jsonb and should
            "0001",
            "+0001",
            "-001",
            "+002.2",
        ):
            #  int, int5, float, float5
            for kind in (3, 4, 5, 6):
                encoded = make_item(kind, number)
                self.check_invalid(encoded, include_sqlite=False)

        # not valid text
        for encoded in (
            # TEXTJ
            make_item(8, r"one\u123two"),
            make_item(8, r"one\u123"),
            make_item(8, "one\\"),
            make_item(8, r"\one"),
            make_item(8, r"\'abc"),
            make_item(8, r"\h"),
            # TEXT5
            make_item(9, r"hello\x1mark"),
            make_item(9, "hello\\"),
            make_item(9, r"\01"),
            make_item(9, r"\h"),
        ):
            self.check_invalid(encoded, include_sqlite=b"mark" not in encoded)

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
        # the same item length can be encoded multiple ways with leading
        # zeroes.  this checks we handle them correctly.  while sqlite
        # decodes 8 byte lengths, it rejects any longer than 4 bytes (4GB)
        # because everything else in sqlite is 2GB limited.

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
                # rejected even though it is a compliant encoding of 0 length null.
                include_sqlite_valid_check = True if 3 <= k <= 10 or len_encoding == 0 else False
                self.check_valid(encoded, expected, include_sqlite_valid_check=include_sqlite_valid_check)

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


def make_item(tag: int, value=None, *, len_encoding: int = None, length: int = None):
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
