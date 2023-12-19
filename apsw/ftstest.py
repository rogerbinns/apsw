#!/usr/bin/env python3

# This testing code deliberately does nasty stuff so mypy isn't helpful
# mypy: ignore-errors
# type: ignore

# FTS test code is here while under development.  It will be merged
# into the main test suite once complete


import unittest
import tempfile

import apsw
import apsw.ext
import apsw.fts


class FTS(unittest.TestCase):
    def setUp(self):
        self.db = apsw.Connection("")

    def tearDown(self):
        self.db.close()
        del self.db

    def testFTSTokenizerAPI(self):
        "Test C interface for tokenizers"
        try:
            self.db.fts5_tokenizer("ascii")
        except apsw.NoFTS5Error:
            return

        self.assertRaisesRegex(apsw.SQLError, "Finding tokenizer named .*", self.db.fts5_tokenizer, "doesn't exist")

        # Sanity check
        test_args = ["one", "two", "three"]
        test_text = "The quick brown fox Aragonés jumps over the lazy dog"
        test_data = [
            (0, 3, "The"),
            ("quick",),
            (10, 15, "brown", "brawn", "bruin"),
            (16, 19, "fox"),
            (20, 29, "Aragonés"),
            "jumps",
            ("over", "under"),
            (41, 44, "the"),
            (45, 49, "lazy"),
            (50, 53, "dog"),
        ]
        test_reason = apsw.FTS5_TOKENIZE_AUX

        # tokenizer as a function
        def func_tok(con, args):
            self.assertIs(con, self.db)
            self.assertEqual(args, test_args)

            def tokenize(utf8, reason):
                self.assertEqual(utf8.decode("utf8"), test_text)
                self.assertEqual(reason, test_reason)
                return test_data

            return tokenize

        # and a class
        class class_tok:
            def __init__(innerself, con, args):
                self.assertIs(con, self.db)
                self.assertEqual(args, test_args)

            def __call__(innserself, utf8, reason):
                self.assertEqual(utf8.decode("utf8"), test_text)
                self.assertEqual(reason, test_reason)
                return test_data

        self.db.register_fts5_tokenizer("func_tok", func_tok)
        self.db.register_fts5_tokenizer("class_tok", class_tok)

        for name in ("func_tok", "class_tok"):
            for include_offsets in (True, False):
                for include_colocated in (True, False):
                    res = self.db.fts5_tokenizer(name, test_args)(
                        test_text.encode("utf8"),
                        test_reason,
                        include_offsets=include_offsets,
                        include_colocated=include_colocated,
                    )
                    self.verify_token_stream(test_data, res, include_offsets, include_colocated)

    def verify_token_stream(self, expected, actual, include_offsets, include_colocated):
        self.assertEqual(len(expected), len(actual))
        for l, r in zip(expected, actual):
            # we turn l back into a list with offsets
            if isinstance(l, str):
                l = [l]
            l = list(l)
            if not isinstance(l[0], int):
                l = [0, 0] + l
            # then tear back down based on include
            if not include_colocated:
                l = l[:3]
            if not include_offsets:
                l = l[2:]
            if include_colocated or include_offsets:
                l = tuple(l)
            else:
                assert len(l) == 1
                l = l[0]
                assert isinstance(l, str)

            self.assertEqual(l, r)

    def testFTSHelpers(self):
        "Test various FTS helper functions"
        ## convert_tokenize_reason
        for pat, expected in (
            ("QUERY", {apsw.FTS5_TOKENIZE_QUERY}),
            (
                "DOCUMENT AUX QUERY_PREFIX AUX",
                {
                    apsw.FTS5_TOKENIZE_DOCUMENT,
                    apsw.FTS5_TOKENIZE_AUX,
                    apsw.FTS5_TOKENIZE_QUERY | apsw.FTS5_TOKENIZE_PREFIX,
                },
            ),
        ):
            self.assertEqual(apsw.fts.convert_tokenize_reason(pat), expected)
        self.assertRaises(ValueError, apsw.fts.convert_tokenize_reason, "AUX BANANA")

        ## tokenizer_test_strings
        def verify_test_string_item(item):
            value, comment = item
            self.assertTrue(comment)
            self.assertIsInstance(comment, str)
            self.assertIsInstance(value, bytes)
            self.assertEqual(value, value.decode("utf8", "replace").encode("utf8"))

        tests = apsw.fts.tokenizer_test_strings()
        for count, item in enumerate(tests):
            verify_test_string_item(item)
        self.assertGreater(count, 16)

        with tempfile.NamedTemporaryFile("wb") as tf:
            some_text = "hello Aragonés 你好世界"
            items = apsw.fts.tokenizer_test_strings(tf.name)
            self.assertEqual(len(items), 1)
            verify_test_string_item(items[0])
            tf.write(some_text.encode("utf8"))
            tf.flush()
            items = apsw.fts.tokenizer_test_strings(tf.name)
            self.assertEqual(len(items), 1)
            verify_test_string_item(items[0])
            self.assertEqual(items[0][0], some_text.encode("utf8"))
            tf.seek(0)
            for i in range(10):
                tf.write(f"# { i }\t\r\n## ignored\n".encode("utf8"))
                tf.write((some_text + f"{ i }  \n").encode("utf8"))
            tf.flush()
            items = apsw.fts.tokenizer_test_strings(tf.name)
            self.assertEqual(10, len(items))
            for i, (value, comment) in enumerate(items):
                self.assertEqual(comment, f"{ i }")
                self.assertNotIn(b"##", value)
                self.assertEqual((some_text + f"{ i }").encode("utf8"), value)

        ## convert_categories
        self.assertRaises(ValueError, apsw.fts.convert_categories, "L* !BANANA")
        self.assertEqual(apsw.fts.convert_categories("L* Pc !N* N* !N*"), {"Pc", "Lm", "Lo", "Lu", "Lt", "Ll"})

        ## extract_html_text
        some_html = (
            """<!decl><!--comment-->&copy;&#62;<?pi><hello/><script>script</script><svg>ddd<svg>ffff"""
            """</svg>ggg&lt;<?pi2></svg><hello>bye</hello>"""
        )
        h = apsw.fts.extract_html_text(some_html)
        self.assertEqual(h.html, some_html)
        self.assertEqual(h.text.strip(), "©> bye")
        self.assertEqual(h.offsets, [(0, 0), (1, 21), (2, 27), (3, 32), (4, 117), (7, 120), (9, 129)])
        self.assertRaises(ValueError, h.text_offset_to_html_offset, -1)
        self.assertRaises(ValueError, h.text_offset_to_html_offset, len(h.text) + 1)
        offsets = [h.text_offset_to_html_offset(i) for i in range(len(h.text) + 1)]
        self.assertEqual(offsets, [0, 21, 27, 32, 117, 118, 119, 120, 121])

        ## shingle
        self.assertRaises(ValueError, apsw.fts.shingle, "", 3)
        self.assertRaises(ValueError, apsw.fts.shingle, "hello", 0)
        self.assertEqual(apsw.fts.shingle("hello", 1), ("h", "e", "l", "l", "o"))
        self.assertEqual(apsw.fts.shingle("hello", 3), ("hel", "ell", "llo"))
        self.assertEqual(apsw.fts.shingle("hello", 80), ("hello",))

        ## convert_string_to_python
        self.assertIs(apsw.fts.convert_string_to_python("apsw.fts.shingle"), apsw.fts.shingle)

        ## parse_tokenizer_args
        ta = apsw.fts.TokenizerArgument
        self.db.register_fts5_tokenizer("dummy", lambda *args: None)

        def t(args):
            return self.db.fts5_tokenizer("dummy", args)

        for spec, args, expected in (
            ({}, [], {}),
            ({"foo": 3}, [], {"foo": 3}),
            ({"foo": 3, "a1": 1}, ["a1", "1"], {"foo": 3, "a1": "1"}),
            ({"foo": 3}, ["foo"], (ValueError, "Expected a value for parameter foo")),
            ({}, ["foo"], (ValueError, "Unexpected parameter name foo")),
            ({"foo": 3, "+": None}, ["foo", "3", "dummy"], {"foo": "3", "+": t([])}),
            (
                {"foo": 3, "+": None},
                ["foo", "3", "dummy", "more", "args", "here"],
                {"foo": "3", "+": t(["more", "args", "here"])},
            ),
            ({"+": None}, ["dummy"], {"+": t([])}),
            ({"+": None}, [], (ValueError, "Expected additional tokenizer and arguments")),
            ({"+": t(["fred"])}, [], {"+": t(["fred"])}),
            ({"foo": ta(default=4)}, [], {"foo": 4}),
            ({"foo": ta(convertor=int)}, ["foo", "4"], {"foo": 4}),
            ({"foo": ta(convertor=int)}, ["foo", "four"], (ValueError, "invalid literal for int.*")),
            ({"foo": ta(choices=("one", "two"))}, ["foo", "four"], (ValueError, ".*was not allowed choice.*")),
        ):
            if isinstance(expected, tuple):
                self.assertRaisesRegex(expected[0], expected[1], apsw.fts.parse_tokenizer_args, self.db, spec, args)
            else:
                options = apsw.fts.parse_tokenizer_args(self.db, spec, args)
                if "+" in spec:
                    tok = options.pop("+")
                    e = expected.pop("+")
                    self.assertIs(tok.connection, e.connection)
                    self.assertEqual(tok.args, e.args)
                    self.assertEqual(tok.name, e.name)
                self.assertEqual(expected, options)


if __name__ == "__main__":
    unittest.main()
