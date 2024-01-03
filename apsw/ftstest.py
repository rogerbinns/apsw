#!/usr/bin/env python3

# This testing code deliberately does nasty stuff so mypy isn't helpful
# mypy: ignore-errors
# type: ignore

# FTS test code is here while under development.  It will be merged
# into the main test suite once complete


import unittest
import tempfile
import sys

import apsw
import apsw.ext
import apsw.fts


class APSW(unittest.TestCase):
    def setUp(self):
        self.db = apsw.Connection("")

    def tearDown(self):
        self.db.close()
        del self.db

    def has_fts5(self):
        try:
            self.db.fts5_tokenizer("ascii")
            return True
        except apsw.NoFTS5Error:
            return False

    def testFTSTokenizerAPI(self):
        "Test C interface for tokenizers"
        if not self.has_fts5():
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

        self.assertRaisesRegex(ValueError, "Too many args.*", self.db.fts5_tokenizer, "func_tok", [""] * 1000)

        self.assertIn("class_tok", str(self.db.fts5_tokenizer("class_tok", ["one", "two", "three"])))

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

        bad_results = [
            (-73, 0, "one"),
            (0, 10000, "two"),
            3.7,
            (0, "hello"),
            (0, 1, 3.8),
            (0, 1, "hello", 3.8),
            tuple(),
            (0, 3.8),
            (0, 3.8, "hello"),
        ]

        bad_results_orig = bad_results[:]

        def bad_tok(con, args):
            def tokenize(utf8, reason):
                nonlocal bad_results
                yield bad_results.pop()

            return tokenize

        self.db.register_fts5_tokenizer("bad_tok", bad_tok)

        self.assertRaisesRegex(
            ValueError, ".*reason is not an allowed value.*", self.db.fts5_tokenizer("unicode61", []).__call__, b"", 0
        )

        while bad_results:
            self.assertRaises(
                ValueError, self.db.fts5_tokenizer("bad_tok", []).__call__, b"abc", apsw.FTS5_TOKENIZE_DOCUMENT
            )

        def bad_tok2(con, args):
            options = apsw.fts.parse_tokenizer_args(con, {"+": None}, args)

            def tokenize(utf8, reason):
                for start, end, *tokens in options["+"](utf8, reason):
                    yield start, end, *tokens

            return tokenize

        self.db.register_fts5_tokenizer("bad_tok2", bad_tok2)
        bad_result = bad_results_orig[:]
        while bad_results:
            self.assertRaises(
                ValueError,
                self.db.fts5_tokenizer("bad_tok2", ["bad_tok"]).__call__,
                b"abc",
                apsw.FTS5_TOKENIZE_DOCUMENT,
            )

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
        if not self.has_fts5():
            return
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

        ## convert_unicode_categories
        self.assertRaises(ValueError, apsw.fts.convert_unicode_categories, "L* !BANANA")
        self.assertEqual(apsw.fts.convert_unicode_categories("L* Pc !N* N* !N*"), {"Pc", "Lm", "Lo", "Lu", "Lt", "Ll"})
        self.assertEqual(
            apsw.fts.convert_unicode_categories("* !P* !Z*"), apsw.fts.convert_unicode_categories("[CLMNS]*")
        )
        ## convert_number_ranges
        for t in "3-", "a", "", "3-5-7", "3,3-", "3,a", "3,4-a":
            self.assertRaises(ValueError, apsw.fts.convert_number_ranges, t)
        for t, expected in (
            ("3", {3}),
            ("3,4,5", {3, 4, 5}),
            ("3-7", {3, 4, 5, 6, 7}),
            ("2-3,3-9", {2, 3, 4, 5, 6, 7, 8, 9}),
            ("6-2", set()),
        ):
            self.assertEqual(apsw.fts.convert_number_ranges(t), expected)

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

    def testAPSWTokenizerWrappers(self):
        "Test tokenizer wrappers supplied by apsw.fts"
        if not self.has_fts5():
            return
        test_reason = apsw.FTS5_TOKENIZE_AUX
        test_data = b"a 1 2 3 b"
        test_res = ((0, 1, "a"), (2, 3, "1"), (4, 5, "2", "deux", "two"), (6, 7, "3"), (8, 9, "b"))

        def source(con, args):
            apsw.fts.parse_tokenizer_args(con, {}, args)

            def tokenize(utf8, flags):
                self.assertEqual(flags, test_reason)
                self.assertEqual(utf8, test_data)
                return test_res

            return tokenize

        self.db.register_fts5_tokenizer("source", source)

        @apsw.fts.TransformTokenizer
        def transform_wrapped_func(s):
            return self.transform_test_function(s)

        @apsw.fts.StopWordsTokenizer
        def stopwords_wrapped_func(s):
            return self.stopwords_test_function(s)

        @apsw.fts.SynonymTokenizer
        def synonym_wrapped_func(s):
            return self.synonym_test_function(s)

        self.db.register_fts5_tokenizer("transform_wrapped", transform_wrapped_func)
        self.db.register_fts5_tokenizer("transform_param", apsw.fts.TransformTokenizer(self.transform_test_function))
        self.db.register_fts5_tokenizer("transform_arg", apsw.fts.TransformTokenizer())

        self.db.register_fts5_tokenizer("stopwords_wrapped", stopwords_wrapped_func)
        self.db.register_fts5_tokenizer("stopwords_param", apsw.fts.StopWordsTokenizer(self.stopwords_test_function))
        self.db.register_fts5_tokenizer("stopwords_arg", apsw.fts.StopWordsTokenizer())

        self.db.register_fts5_tokenizer("synonym_wrapped", synonym_wrapped_func)
        self.db.register_fts5_tokenizer("synonym_param", apsw.fts.SynonymTokenizer(self.synonym_test_function))
        self.db.register_fts5_tokenizer("synonym_arg", apsw.fts.SynonymTokenizer())

        for name in ("transform", "stopwords", "synonym"):
            returns = []
            for suffix in "wrapped", "param", "arg":
                param_name = {"transform": "transform", "stopwords": "test", "synonym": "get"}[name]
                args_with = [param_name, f"apsw.ftstest.APSW.{ name }_test_function", "source"]
                args_without = ["source"]
                tokname = f"{ name }_{ suffix }"

                if suffix == "arg":
                    self.assertRaisesRegex(
                        ValueError,
                        "A callable must be provided by decorator, or parameter",
                        self.db.fts5_tokenizer,
                        tokname,
                        args_without,
                    )
                    tok = self.db.fts5_tokenizer(tokname, args_with)
                else:
                    self.assertRaisesRegex(
                        apsw.SQLError, "Finding tokenizer named .*", self.db.fts5_tokenizer, tokname, args_with
                    )
                    tok = self.db.fts5_tokenizer(tokname, args_without)

                returns.append(tok(test_data, test_reason))

            self.assertNotEqual(returns[0], test_res)
            self.assertEqual(returns[0], returns[1])
            self.assertEqual(returns[1], returns[2])

            apsw.fts.convert_string_to_python(f"apsw.ftstest.APSW.{ name }_test_function_check")(self, returns[0])

    @staticmethod
    def transform_test_function(s):
        if s == "1":
            return "one"
        if s == "2":
            return ("two", "ii", "2")
        if s == "3":
            return tuple()
        return s

    def transform_test_function_check(self, s):
        # check the above happened
        self.assertEqual(s, [(0, 1, "a"), (2, 3, "one"), (4, 5, "two", "ii", "2", "deux"), (8, 9, "b")])

    @staticmethod
    def stopwords_test_function(s):
        return s in {"a", "deux", "b"}

    def stopwords_test_function_check(self, s):
        self.assertEqual(s, [(2, 3, "1"), (4, 5, "2", "two"), (6, 7, "3")])

    @staticmethod
    def synonym_test_function(s):
        syn = APSW.transform_test_function(s)
        return syn if syn != s else None

    def synonym_test_function_check(self, s):
        self.assertEqual(
            s, [(0, 1, "a"), (2, 3, "1", "one"), (4, 5, "2", "two", "ii", "deux"), (6, 7, "3"), (8, 9, "b")]
        )

    def testFTSFunction(self):
        if not self.has_fts5():
            return
        self.db.execute(
            """
            create virtual table testfts using fts5(a,b,c, tokenize="unicode61 remove_diacritics 2");
            insert into testfts values('a b c', 'b c d', 'c d e');
            insert into testfts values('1 2 3', '2 3 4', '3 4 5');
        """
        )

        contexts = []

        def identity(api, param):
            contexts.append(api)
            return param

        self.db.register_fts5_function("identity", identity)

        x = self.db.execute("select identity(testfts,a) from testfts('e OR 5')").get
        self.assertEqual(x, ["a b c", "1 2 3"])

        aux_sentinel = object()

        def check_api(api: apsw.FTS5ExtensionApi, *params):
            contexts.append(api)
            self.assertEqual(api.column_count, 3)
            self.assertEqual(api.row_count, 2)
            self.assertIn(api.rowid, {1, 2})
            self.assertTrue(api.aux_data is None or api.aux_data is aux_sentinel)
            if api.aux_data is None:
                api.aux_data = aux_sentinel
            # ::TODO:: remove once release happens
            if apsw.SQLITE_VERSION_NUMBER >= 3045000:
                self.assertEqual(api.phrases, (("c",), ("d",), ("5",)))
                self.assertRaises(apsw.RangeError, api.inst_tokens, 999)
                inst = tuple(api.inst_tokens(i) for i in range(api.inst_count))
                correct = [(("c",), ("c",), ("d",), ("c",), ("d",)), (("5",),)]
                self.assertIn(inst, correct)
            self.assertIn(api.inst_count, {1, 5})

            correct = {((0, 1, 2), (1, 2), ()), ((), (), (2,))}
            self.assertRaises(apsw.RangeError, api.phrase_columns, 9999)
            pc = tuple(api.phrase_columns(i) for i in range(len(api.phrases)))
            self.assertIn(pc, correct)

            correct = [([[2], [1], [0]], [[], [2], [1]], [[], [], []]), ([[], [], []], [[], [], []], [[], [], [2]])]
            self.assertRaises(apsw.RangeError, api.phrase_locations, 9999)
            pl = tuple(api.phrase_locations(i) for i in range(len(api.phrases)))
            self.assertIn(pl, correct)

            correct = {-1: 18, 0: 6, 1: 6, 2: 6}
            self.assertRaises(apsw.RangeError, api.column_total_size, 999)
            for k, v in correct.items():
                self.assertEqual(api.column_total_size(k), v)

            correct = {-1: {9}, 0: {3}, 1: {3}, 2: {3}}
            self.assertRaises(apsw.RangeError, api.column_size, 999)
            for k, v in correct.items():
                self.assertIn(api.column_size(k), v)

            correct = {
                (1, 0, b"a b c"),
                (1, 1, b"b c d"),
                (1, 2, b"c d e"),
                (2, 0, b"1 2 3"),
                (2, 1, b"2 3 4"),
                (2, 2, b"3 4 5"),
            }
            self.assertRaises(apsw.RangeError, api.column_text, 99)
            for col in range(api.column_count):
                self.assertIn((api.rowid, col, api.column_text(col)), correct)

            self.assertRaises(apsw.RangeError, api.query_phrase, 9999, lambda: None, None)

            def cb(api2, l):
                self.assertTrue(api2 is not api)
                l.append((api2.rowid, tuple(api.phrase_locations(i) for i in range(len(api.phrases)))))

            def cberror(api2, _):
                1 / 0

            correct = (
                [(1, ([[2], [1], [0]], [[], [2], [1]], [[], [], []]))],
                [(1, ([[2], [1], [0]], [[], [2], [1]], [[], [], []]))],
                [(2, ([[2], [1], [0]], [[], [2], [1]], [[], [], []]))],
                [(1, ([[], [], []], [[], [], []], [[], [], [2]]))],
                [(1, ([[], [], []], [[], [], []], [[], [], [2]]))],
                [(2, ([[], [], []], [[], [], []], [[], [], [2]]))],
            )
            for i in range(len(api.phrases)):
                l = []
                api.query_phrase(i, cb, l)
                self.assertIn(l, correct)
                self.assertRaises(ZeroDivisionError, api.query_phrase, i, cberror, None)

            correct = (
                (True, True, [(0, 5, "hello"), (7, 12, "world"), (13, 22, "aragones")]),
                (True, False, [(0, 5, "hello"), (7, 12, "world"), (13, 22, "aragones")]),
                (False, True, [("hello",), ("world",), ("aragones",)]),
                (False, False, ["hello", "world", "aragones"]),
                (True, True, [(0, 5, "hello"), (7, 12, "world"), (13, 22, "aragones")]),
                (True, False, [(0, 5, "hello"), (7, 12, "world"), (13, 22, "aragones")]),
                (False, True, [("hello",), ("world",), ("aragones",)]),
                (False, False, ["hello", "world", "aragones"]),
            )

            test_text = "hello, world Aragonés"
            for include_offsets in (True, False):
                for include_colocated in (True, False):
                    res = api.tokenize(
                        test_text.encode("utf8"), include_offsets=include_offsets, include_colocated=include_colocated
                    )
                    self.assertIn((include_offsets, include_colocated, res), correct)

        self.db.register_fts5_function("check_api", check_api)
        for _ in self.db.execute("select check_api(testfts) from testfts('c d OR 5')"):
            pass

        # the same structure is in tools/fi.py - update that if you update this
        extapi = {
            "attr": {"aux_data", "column_count", "inst_count", "phrases", "row_count", "rowid"},
            (0,): {
                "column_size",
                "column_text",
                "column_total_size",
                "inst_tokens",
                "phrase_columns",
                "phrase_locations",
            },
            (0, lambda *args: None, None): {"query_phrase"},
            (b"abcd e f g h",): {"tokenize"},
        }
        for ctx in contexts:
            items = set(n for n in dir(ctx) if not n.startswith("_"))
            for args, names in extapi.items():
                for name in names:
                    if args == "attr":
                        self.assertRaises(apsw.InvalidContextError, getattr, ctx, name)
                        if name == "aux_data":
                            self.assertRaises(apsw.InvalidContextError, setattr, ctx, name, dict())
                    else:
                        self.assertRaises(apsw.InvalidContextError, getattr(ctx, name), *args)
                    items.remove(name)
            self.assertEqual(len(items), 0)

    def testzzFaultInjection(self):
        "Deliberately inject faults to exercise all code paths"
        ### Copied from main tests
        if not getattr(apsw, "test_fixtures_present", None):
            return

        apsw.faultdict = dict()

        def ShouldFault(name, pending_exception):
            r = apsw.faultdict.get(name, False)
            apsw.faultdict[name] = False
            return r

        sys.apsw_should_fault = ShouldFault
        ### end copied from main tests

        if self.has_fts5():
            apsw.faultdict["FTS5TokenizerRegister"] = True
            self.assertRaises(apsw.NoMemError, self.db.register_fts5_tokenizer, "foo", lambda *args: None)
            apsw.faultdict["FTS5FunctionRegister"] = True
            self.assertRaises(apsw.BusyError, self.db.register_fts5_function, "foo", lambda *args: None)
            apsw.faultdict["xTokenCBFlagsBad"] = True
            self.assertRaisesRegex(
                ValueError,
                "Invalid tokenize flags.*",
                self.db.fts5_tokenizer("unicode61", []),
                b"abc def",
                apsw.FTS5_TOKENIZE_DOCUMENT,
            )
            apsw.faultdict["xTokenCBOffsetsBad"] = True
            self.assertRaisesRegex(
                ValueError,
                "Invalid start .* or end .*",
                self.db.fts5_tokenizer("unicode61", []),
                b"abc def",
                apsw.FTS5_TOKENIZE_DOCUMENT,
            )
            apsw.faultdict["xTokenCBColocatedBad"] = True
            self.assertRaisesRegex(
                ValueError,
                "FTS5_TOKEN_COLOCATED set.*",
                self.db.fts5_tokenizer("unicode61", []),
                b"abc def",
                apsw.FTS5_TOKENIZE_DOCUMENT,
            )
            apsw.faultdict["TokenizeRC"] = True

            def tokenizer(con, args):
                def tokenize(utf8, reason):
                    yield "hello"
                    yield ("hello", "world")

                return tokenize

            self.db.register_fts5_tokenizer("simple", tokenizer)
            self.assertRaises(
                apsw.NoMemError, self.db.fts5_tokenizer("simple", []), b"abc def", apsw.FTS5_TOKENIZE_DOCUMENT
            )
            apsw.faultdict["TokenizeRC2"] = True
            self.assertRaises(
                apsw.NoMemError, self.db.fts5_tokenizer("simple", []), b"abc def", apsw.FTS5_TOKENIZE_DOCUMENT
            )

            self.db.execute("""create virtual table ftstest using fts5(x); insert into ftstest values('hello world')""")
            def cb(api: apsw.FTS5ExtensionApi):
                api.row_count
                api.aux_data = "hello"
                api.phrases
                api.inst_count
                api.tokenize(b"hello world")

            self.db.register_fts5_function("errmaker", cb)
            for fault in ("xRowCountErr", "xSetAuxDataErr", "xQueryTokenErr", "xInstCountErr", "xTokenizeErr"):
                apsw.faultdict[fault] = True
                self.assertRaises(apsw.NoMemError, self.db.execute, "select errmaker(ftstest) from ftstest('hello')")


if __name__ == "__main__":
    unittest.main()
