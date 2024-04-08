#!/usr/bin/env python3

# This testing code deliberately does nasty stuff so mypy isn't helpful
# mypy: ignore-errors
# type: ignore

# Unicode and FTS support is a lot of code and API surface unrelated to
# database access, so it is kept im this separate file.   The main test
# suite does however load this and run it.

import unittest
import tempfile
import sys
import unicodedata
import itertools
import collections

import apsw
import apsw.ext
import apsw.fts


class FTS(unittest.TestCase):
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
            options = apsw.fts.parse_tokenizer_args({"+": None}, con, args)

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

    def testAPSWFTSTokenizers(self):
        "Test apsw.fts tokenizers"
        if not self.has_fts5():
            return

        test_text = """ 😂❤️ 𐌼𐌰𐌲 العالم!
            Olá, mundo! 8975984
            नमस्ते, दुनिया!"""

        test_utf8 = test_text.encode("utf8")

        ## PyUnicodeTokenizer
        self.db.register_fts5_tokenizer("pyunicode", apsw.fts.PyUnicodeTokenizer)

        self.assertRaises(ValueError, self.db.fts5_tokenizer, "pyunicode", ["tokenchars", "%$#*", "separators", "$"])

        self.assertEqual(self.db.fts5_tokenizer("pyunicode", [])(b"", apsw.FTS5_TOKENIZE_DOCUMENT), [])
        self.assertEqual(self.db.fts5_tokenizer("pyunicode", [])(b"a", apsw.FTS5_TOKENIZE_DOCUMENT), [(0, 1, "a")])

        correct = (
            ("N*:::", "8975984"),
            ("L* !Lu:::", "𐌼𐌰𐌲:العالم:lá:mundo:नमस:त:द:न:य"),
            ("N*:::C* L* So", "😂:❤:𐌼:𐌰:𐌲:ا:ل:ع:ا:ل:م:\n:O:l:á:m:u:n:d:o:8975984:\n:न:म:स:त:द:न:य"),
            ("N*::ud:", "8975984"),
            ("N*::ud:C* L* So", "😂:❤:𐌼:𐌰:𐌲:ا:ل:ع:ا:ل:م:\n:O:l:á:m:n:o:8975984:\n:न:म:स:त:द:न:य"),
            ("L* !Lu:::C* L* So", "😂:❤:𐌼:𐌰:𐌲:ا:ل:ع:ا:ل:م:\n:O:l:á:m:u:n:d:o:\n:न:म:स:त:द:न:य"),
            ("N*:﹘❴⸡ᡃ｣\u2005ᾞᴧٙ꭛::", "8975984"),
            ("N*:﹘❴⸡ᡃ｣\u2005ᾞᴧٙ꭛::C* L* So", "😂:❤:𐌼:𐌰:𐌲:ا:ل:ع:ا:ل:م:\n:O:l:á:m:u:n:d:o:8975984:\n:न:म:स:त:द:न:य"),
            ("N*:﹘❴⸡ᡃ｣\u2005ᾞᴧٙ꭛:ud:", "8975984"),
            ("L* !Lu::ud:", "𐌼𐌰𐌲:العالم:lá:m:n:o:नमस:त:द:न:य"),
            ("N*:﹘❴⸡ᡃ｣\u2005ᾞᴧٙ꭛:ud:C* L* So", "😂:❤:𐌼:𐌰:𐌲:ا:ل:ع:ا:ل:م:\n:O:l:á:m:n:o:8975984:\n:न:म:स:त:द:न:य"),
            ("L* !Lu::ud:C* L* So", "😂:❤:𐌼:𐌰:𐌲:ا:ل:ع:ا:ل:م:\n:O:l:á:m:n:o:\n:न:म:स:त:द:न:य"),
            ("L* !Lu:﹘❴⸡ᡃ｣\u2005ᾞᴧٙ꭛::", "𐌼𐌰𐌲:العالم:lá:mundo:नमस:त:द:न:य"),
            ("L* !Lu:﹘❴⸡ᡃ｣\u2005ᾞᴧٙ꭛::C* L* So", "😂:❤:𐌼:𐌰:𐌲:ا:ل:ع:ا:ل:م:\n:O:l:á:m:u:n:d:o:\n:न:म:स:त:द:न:य"),
            ("L* !Lu:﹘❴⸡ᡃ｣\u2005ᾞᴧٙ꭛:ud:", "𐌼𐌰𐌲:العالم:lá:m:n:o:नमस:त:द:न:य"),
            ("L* !Lu:﹘❴⸡ᡃ｣\u2005ᾞᴧٙ꭛:ud:C* L* So", "😂:❤:𐌼:𐌰:𐌲:ا:ل:ع:ا:ل:م:\n:O:l:á:m:n:o:\n:न:म:स:त:द:न:य"),
        )
        for categories in {"N*", "L* !Lu"}:
            for tokenchars in {"", "﹘❴⸡ᡃ｣ ᾞᴧٙ꭛"}:
                for separators in {"", "ud"}:
                    for single_token_categories in {"", "C* L* So"}:
                        key = ":".join((categories, tokenchars, separators, single_token_categories))
                        args = [
                            "categories",
                            categories,
                            "tokenchars",
                            tokenchars,
                            "separators",
                            separators,
                            "single_token_categories",
                            single_token_categories,
                        ]
                        result = []
                        for start, end, token in self.db.fts5_tokenizer("pyunicode", args)(
                            test_utf8, apsw.FTS5_TOKENIZE_DOCUMENT
                        ):
                            self.assertEqual(test_utf8[start:end].decode("utf8"), token)
                            result.append(token)
                        result = ":".join(result)

                        self.assertIn((key, result), correct)

        ## NGramTokenizer
        test_utf8 = ("中文(繁體) Fr1AnçAiS češt2ina  🤦🏼‍♂️ straße" * 4).encode("utf8")
        self.db.register_fts5_tokenizer("ngram", apsw.fts.NGramTokenizer)

        for include_categories in ("Ll N*", None):
            for reason in (apsw.FTS5_TOKENIZE_DOCUMENT, apsw.FTS5_TOKENIZE_QUERY):
                sizes = collections.Counter()
                # verify all bytes are covered
                got = [None] * len(test_utf8)
                # verify QUERY mode only has one length per offset
                by_start_len = [None] * len(test_utf8)
                args = ["ngrams", "3,7,9-12"]
                if include_categories:
                    args += ["include_categories", include_categories]
                for start, end, *tokens in self.db.fts5_tokenizer("ngram", args)(test_utf8, reason):
                    self.assertEqual(1, len(tokens))
                    if reason == apsw.FTS5_TOKENIZE_QUERY:
                        self.assertIsNone(by_start_len[start])
                        by_start_len[start] = len(tokens[0])
                    self.assertIn(len(tokens[0]), {3, 7, 9, 10, 11, 12})
                    sizes[len(tokens[0])] += 1
                    token_bytes = tokens[0].encode("utf8")
                    if include_categories is None:
                        self.assertEqual(len(token_bytes), end - start)
                    else:
                        # token must be equal or subset of utf8
                        self.assertLessEqual(len(token_bytes), end - start)
                    if include_categories is None:
                        for offset, byte in zip(range(start, start + end), token_bytes):
                            self.assertTrue(got[offset] is None or got[offset] == byte)
                            got[offset] = byte
                    if include_categories:
                        cats = apsw.fts.convert_unicode_categories(include_categories)
                        self.assertTrue(all(unicodedata.category(t) in cats for t in tokens[0]))
                self.assertTrue(all(got[i] is not None) for i in range(len(got)))

                # size seen should be increasing, decreasing count for DOCUMENT,
                if reason == apsw.FTS5_TOKENIZE_DOCUMENT:
                    for l, r in itertools.pairwise(sorted(sizes.items())):
                        self.assertLess(l[0], r[0])
                        self.assertGreaterEqual(l[1], r[1])
                else:
                    # there should be more of the longest than all the others
                    vals = [x[1] for x in sorted(sizes.items())]
                    self.assertGreater(vals[-1], sum(vals[:-1]))

        # longer than ngrams
        token = self.db.fts5_tokenizer("ngram", ["ngrams", "20000"])(
            test_utf8, apsw.FTS5_TOKENIZE_DOCUMENT, include_colocated=False, include_offsets=False
        )[0]
        self.assertEqual(test_utf8, token.encode("utf8"))
        # zero len
        self.assertEqual([], self.db.fts5_tokenizer("ngram")(b"", apsw.FTS5_TOKENIZE_DOCUMENT))

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
                self.assertRaisesRegex(expected[0], expected[1], apsw.fts.parse_tokenizer_args, spec, self.db, args)
            else:
                options = apsw.fts.parse_tokenizer_args(spec, self.db, args)
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
            apsw.fts.parse_tokenizer_args({}, con, args)

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

        # synonym reason
        test_text = "one two three"

        @apsw.fts.SynonymTokenizer
        def func(n):
            1 / 0

        self.db.register_fts5_tokenizer("pyunicode", apsw.fts.PyUnicodeTokenizer)
        self.db.register_fts5_tokenizer("synonym-reason", func)

        self.assertEqual(
            self.db.fts5_tokenizer("pyunicode")(test_text.encode("utf8"), apsw.FTS5_TOKENIZE_QUERY),
            self.db.fts5_tokenizer("synonym-reason", ["pyunicode"])(test_text.encode("utf8"), apsw.FTS5_TOKENIZE_QUERY),
        )

        # stopwords reason
        @apsw.fts.StopWordsTokenizer
        def func(n):
            1 / 0

        self.db.register_fts5_tokenizer("stopwords-reason", func)

        self.assertEqual(
            self.db.fts5_tokenizer("pyunicode")(test_text.encode("utf8"), apsw.FTS5_TOKENIZE_QUERY),
            self.db.fts5_tokenizer("stopwords-reason", ["pyunicode"])(
                test_text.encode("utf8"), apsw.FTS5_TOKENIZE_QUERY | apsw.FTS5_TOKENIZE_PREFIX
            ),
        )
        self.db.register_fts5_tokenizer("stopwords-reason", func)

        ## SimplifyTokenizer
        test_text = "中文(繁體) Fr1AnçAiS češt2ina  🤦🏼‍♂️ straße"
        test_utf8 = test_text.encode("utf8")

        self.db.register_fts5_tokenizer("simplify", apsw.fts.SimplifyTokenizer)

        # no args should have no effect
        baseline = self.db.fts5_tokenizer("pyunicode")(test_utf8, test_reason)
        nowt = self.db.fts5_tokenizer("simplify", ["pyunicode"])(test_utf8, test_reason)
        self.assertEqual(baseline, nowt)

        # require tokenizer
        self.assertRaises(ValueError, self.db.fts5_tokenizer, "simplify")

        # get all codepoints except spacing
        tok_args = ["pyunicode", "categories", "* !Z*"]

        def toks(args, text):
            return self.db.fts5_tokenizer("simplify", args + tok_args)(
                text.encode("utf8"), test_reason, include_offsets=False, include_colocated=False
            )

        def codepoints(tokens, caseless=False):
            res = []
            for token in tokens:
                for t in token:
                    if caseless:
                        if t == t.upper() and t == t.lower():
                            continue
                    res.append(t)
            return res

        self.assertTrue(any(unicodedata.category(c) == "Sk" for c in codepoints(toks([], test_text))))
        self.assertFalse(
            any(unicodedata.category(c) == "Sk" for c in codepoints(toks(["remove_categories", "S*"], test_text)))
        )
        self.assertTrue(any(c.upper() == c for c in codepoints(toks([], test_text))))
        self.assertFalse(any(c.upper() == c for c in codepoints(toks(["case", "casefold"], test_text), caseless=True)))

        norms = "NFD", "NFC", "NFKD", "NFKC"

        for nin, nout in itertools.product(norms, norms):
            if unicodedata.is_normalized(nin, test_text):
                # make sure normalization is not changed
                self.assertTrue(all(unicodedata.is_normalized(nin, token) for token in toks([], test_text)))
            else:
                # make sure it is
                self.assertTrue(
                    all(unicodedata.is_normalized(nin, token) for token in toks(["normalize_pre", nin], test_text))
                )
                self.assertTrue(
                    all(unicodedata.is_normalized(nin, token) for token in toks(["normalize_post", nin], test_text))
                )
            if nin != nout:
                self.assertTrue(
                    all(
                        unicodedata.is_normalized(nout, token)
                        for token in toks(["normalize_pre", nin, "normalize_post", nout], test_text)
                    )
                )

        ## NGramTokenTokenizer
        self.db.register_fts5_tokenizer("ngramtoken", apsw.fts.NGramTokenTokenizer)
        test_text = "a deep example make sure normalization is not changed "
        for reason in (apsw.FTS5_TOKENIZE_QUERY, apsw.FTS5_TOKENIZE_DOCUMENT):
            res = []
            for start, end, *tokens in self.db.fts5_tokenizer("ngramtoken", ["ngrams", "3,5,7", "pyunicode"])(
                test_text.encode("utf8"), reason
            ):
                res.append(f"{start}:{end}:{':'.join(tokens)}")
            # the correct values were derived by inspection
            if reason == apsw.FTS5_TOKENIZE_QUERY:
                self.assertEqual(
                    res,
                    [
                        "0:1:a",
                        "2:6:dee:eep",
                        "7:14:examp:xampl:ample",
                        "15:19:mak:ake",
                        "20:24:sur:ure",
                        "25:38:normali:ormaliz:rmaliza:malizat:alizati:lizatio:ization",
                        "39:41:is",
                        "42:45:not",
                        "46:53:chang:hange:anged",
                    ],
                )
            else:
                self.assertEqual(
                    res,
                    [
                        "0:1:a",
                        "2:6:dee:eep",
                        "7:14:exa:xam:amp:mpl:ple:examp:xampl:ample",
                        "15:19:mak:ake",
                        "20:24:sur:ure",
                        "25:38:nor:orm:rma:mal:ali:liz:iza:zat:ati:tio:ion:norma:ormal:rmali:maliz:aliza:lizat:izati:zatio:ation:normali:ormaliz:rmaliza:malizat:alizati:lizatio:ization",
                        "39:41:is",
                        "42:45:not",
                        "46:53:cha:han:ang:nge:ged:chang:hange:anged",
                    ],
                )

        ## HTMLTokenizer
        test_html = "<t>text</b><fooo/>mor<b>e</b> stuff&amp;things<yes yes>yes<>/no>a&#1234;b"
        self.db.register_fts5_tokenizer("html", apsw.fts.HTMLTokenizer)
        # htmltext is separately tested
        self.assertEqual(
            self.db.fts5_tokenizer("html", ["pyunicode", "tokenchars", "&"])(
                test_html.encode("utf8"), apsw.FTS5_TOKENIZE_DOCUMENT, include_colocated=False, include_offsets=False
            ),
            ["text", "mor", "e", "stuff&things", "yes", "no", "aӒb"],
        )
        # queries should be pass through
        self.assertEqual(
            self.db.fts5_tokenizer("html", ["pyunicode", "tokenchars", "&<"])(
                "<b>a</b>".encode("utf8"), apsw.FTS5_TOKENIZE_QUERY, include_colocated=False, include_offsets=False
            ),
            ["<b", "a<", "b"],
        )

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


class Unicode(unittest.TestCase):
    # generated by python -m apsw.unicode breaktestgen
    break_tests = {
        "grapheme": (
            " \u034f",
            "a÷🇦🇧÷🇨🇩÷b",
            "\u1100\uac01",
            "\u0903\u094d",
            "\u093c\u0300",
            "\u000d÷🇦",
            "\u0001÷\u0d4e",
            "\u0600\u0308\u0a03",
            "\u1160÷\u1100",
            "\uac01÷ ",
            "\u0903÷\uac01",
            "\u0915÷\uac01",
            "\u0300÷\u0378",
            "\u200d÷\u11a8",
            "\u000d÷\u0308\u0a03",
            "\u0600\u0308÷🇦",
            "\u1160\u0308÷\u0378",
            "\u0900\u0308÷\uac01",
            "\u0915\u0308÷\u0600",
            "\u094d\u0308÷ ",
            "\u0915\u094d\u094d\u0924",
            "👶\U0001f3ff\u0308\u200d👶\U0001f3ff",
        ),
        "word": (
            "\u0001\u00ad",
            "\u0671\u0644\u0631\u064e\u0651\u062d\u0650\u064a\u0645\u0650÷ ÷\u06dd١",
            "\u0001÷🇦",
            "A÷\u000b",
            "0÷\u0022",
            "\u0027÷\u000d",
            "\u0300÷\u0022",
            "\u3031\u0308÷\u0027",
            "0\u0308a\u2060",
            "\u0027\u0308÷🇦",
            "\u200d\u0308÷\u05d0",
            "\u000a÷\u0308÷\u0001",
            "\u005f\u0308a÷\u0027",
            "a÷\u003a÷\u0001",
            "1÷\u0027÷\u000d",
            "\u002c÷1÷\u002e\u2060",
            "a÷\u003a\u0308÷\u3031",
            "1÷\u003a÷a\u2060",
            "\u003a\u0308÷a÷\u0027\u2060",
            "1÷\u002e\u2060÷a\u2060",
            "a÷🇦🇧÷🇨÷b",
            "a\u005fa÷\u002c÷\u002c÷a",
        ),
        "sentence": (
            "\u0001\u0001",
            "\u2060t\u2060h\u2060e\u2060 \u2060r\u2060e\u2060s\u2060p\u2060\u002e\u2060 \u2060l\u2060e\u2060a\u2060d\u2060e\u2060r\u2060s\u2060 \u2060a\u2060r\u2060e\u2060\u2060",
            "\u0009\u0021",
            "Aa",
            "0\u0001",
            "\u002e\u00ad",
            "\u0022\u00ad",
            "\u00ad0",
            "\u0001\u0308\u000d",
            "\u000d÷\u002c",
            "\u0085÷\u01bb",
            "a\u0308\u000a",
            "A\u0308\u002c",
            "0\u0308\u01bb",
            "\u0021\u0308\u000a",
            "\u0022\u0308\u002c",
            "\u00ad\u0308\u01bb",
            "3\u002e4",
            "\u000a÷\u0308\u0009",
            "\u0085÷\u0308\u0300",
            "\u2060\u0021\u2060 \u2060 \u2060\u2060",
            "\u2060e\u2060t\u2060c\u2060\u002e\u2060\u0029\u2060\u2019\u2060 \u2060t\u2060h\u2060e\u2060\u2060",
        ),
        "line_break": (
            "\u1b05\u0009",
            "En ÷gång ÷undföll ÷det ÷honom ÷dock\u002c ÷medan ÷han ÷släpade ÷på ÷det ÷våta ÷höet\u003a ÷\u00bbVarför ÷är ÷höet ÷redan ÷torrt ÷och ÷inkört ÷där ÷borta ÷på ÷Solbacken\u002c ÷och ÷här ÷hos ÷oss ÷är ÷det ÷vått\u003f\u00bb ÷— ÷\u00bbDärför ÷att ÷de ÷ha ÷oftare ÷sol ÷än ÷vi\u002e\u00bb",
            "\u1100\u0025",
            "\u00ab᭐",
            "\u0023\u0308\u0001",
            "\u000d÷\uac00",
            "\u2024÷$",
            "\u2329 —",
            "\u1b44÷\u0028",
            "\u0029 \u002f",
            "\u1b05\u0308÷\u1bc0",
            "\u000b÷\u0308\u0025",
            "\u05d0 ÷\u00b4",
            "\u1160 ÷\u1bc0",
            "\u0022 ÷0",
            "🇦\u0308 \u0029",
            "\u200d ÷\u0028",
            "\u0009\u0308 ÷\u0025",
            "᭐\u0308 ÷\u0028",
            " ÷\u0308÷\u1b05",
            "\U00050005\u0308 ÷\uac00",
            "\u007bczerwono\u007d\u00ad÷‑niebieska",
        ),
    }

    def testBreaks(self):
        "Verifies breaktest locations"
        from . import unicode as the_module

        marker = "÷"
        for kind in "grapheme", "word", "sentence", "line_break":
            meth = getattr(the_module, f"{kind}_next_break")
            meth_next = getattr(the_module, f"{kind}_next")
            meth_iter_with_offsets = getattr(the_module, f"{kind}_iter_with_offsets")
            meth_iter = getattr(the_module, f"{kind}_iter")

            # type and range checking
            self.assertRaises(TypeError, meth)
            self.assertRaises(TypeError, meth, 3)
            self.assertRaises(TypeError, meth, b"abc")
            self.assertRaises(TypeError, meth, "some text", "hello")
            self.assertRaises(ValueError, meth, "some text", -1)
            self.assertRaises(ValueError, meth, "some text", 1000)
            self.assertRaises(ValueError, meth, "some text", sys.maxsize)
            # we can reference index at len
            self.assertEqual((4, 4), meth_next("some", 4))
            self.assertEqual(tuple(), tuple(meth_iter("some", 4)))
            self.assertEqual(tuple(), tuple(meth_iter_with_offsets("some", 4)))

            for text in self.break_tests[kind]:
                test = ""
                breaks = []
                for c in text:
                    if c == marker:
                        breaks.append(len(test))
                    else:
                        test += c
                breaks.append(len(test))

                offset = 0
                seen = []
                while offset < len(test):
                    offset = meth(test, offset)
                    seen.append(offset)

                self.assertEqual(seen, breaks)

                offset = 0
                count = 0
                break_pairs = list(itertools.pairwise([0] + breaks))
                seen = []
                if kind == "word":
                    # for when no word is present
                    break_pairs.append((breaks[-1], breaks[-1]))
                while offset < len(test):
                    start, end = meth_next(test, offset)
                    self.assertGreaterEqual(start, offset)
                    self.assertIn((start, end), break_pairs)
                    if start < len(test):
                        seen.append((start, end, test[start:end]))
                    count += 1
                    offset = end

                if kind != "word":
                    self.assertEqual(count, len(breaks))
                else:
                    self.assertLessEqual(count, len(breaks))

                self.assertEqual(list(meth_iter(test)), list(s[2] for s in seen))
                self.assertEqual(list(meth_iter_with_offsets(test)), seen)

                # extra stuff for word making sure all the flags work
                if kind == "word":
                    # note this block depends on dict ordering
                    w = {"letter": "abc", "number": "1", "emoji": "🤦🏼‍♂️", "regional_indicator": "🇬🇧"}
                    test = "".join(f"({w[k]})" for k in w)

                    for combo in set(itertools.permutations([False, True] * len(w), len(w))):
                        kwargs = {k: combo[i] for i, k in enumerate(w)}
                        res = tuple(meth_iter(test, **kwargs))
                        expected = tuple(w[k] for i, k in enumerate(w) if combo[i])
                        self.assertEqual(res, expected)

# ::TODO:: make main test suite run this one
# eg https://docs.python.org/3/library/unittest.html#load-tests-protocol
# in main could add the TestCases from this module first so they
# get run before forkchecker

if __name__ == "__main__":
    unittest.main()
