#!/usr/bin/env python3

# This testing code deliberately does nasty stuff so mypy isn't helpful
# mypy: ignore-errors
# type: ignore

# Unicode and FTS support is a lot of code and API surface unrelated to
# database access, so it is kept im this separate file.   The main test
# suite does however load this and run it.

import collections
import functools
import itertools
import json
import os
import pathlib
import random
import re
import subprocess
import sys
import tempfile
import typing
import unittest
import zlib
import zipfile

import apsw
import apsw.ext
import apsw.fts5
import apsw.fts5aux
import apsw.fts5query
import apsw.unicode

try:
    itertools.pairwise
except AttributeError:
    # Py <= 3.9 doesn't have it.  We monkeypatch because only
    # referenced in this test code

    # from the docs
    def pairwise(iterable):
        # pairwise('ABCDEFG') → AB BC CD DE EF FG

        iterator = iter(iterable)
        a = next(iterator, None)

        for b in iterator:
            yield a, b
            a = b

    itertools.pairwise = pairwise


class BecauseWindowsTempfile:
    "Work around Windows preventing concurrent access to a file opened for writing"

    def __init__(self, mode, encoding=None):
        self.kwargs = {"mode": mode, "encoding": encoding}
        f = tempfile.NamedTemporaryFile(delete=False)
        self.name = f.name
        f.close()

    def write_whole_file(self, contents):
        with open(self.name, **self.kwargs) as f:
            f.write(contents)

    def __del__(self):
        try:
            os.remove(self.name)
        except OSError:
            pass

    def __enter__(self):
        return self

    def __exit__(self, *args):
        try:
            os.remove(self.name)
        except OSError:
            pass


# we do more stuff under coverage
coverage_run = bool(os.environ.get("COVERAGE_RUN", ""))


class FTS(unittest.TestCase):
    def setUp(self):
        self.db = apsw.Connection("")

    def tearDown(self):
        self.db.close()
        del self.db

    def testLocale(self):
        "Ensure locale parameter is passed on and correct"

        # This exercises all the APIs and wrappers.  Our tokenizers
        # were added before locale was added, so we also verify they
        # all have the parameter now

        TEST_LOCALE = "🇫🇮你好世界"

        def byte_tok(con, params):
            def tokenizer(some_bytes, reason, locale):
                self.assertEqual(locale, TEST_LOCALE)
                for b in some_bytes:
                    yield (b"!" + bytes([b])).decode(errors="replace")

            return tokenizer

        @apsw.fts5.StringTokenizer
        def string_tok(con, params):
            def tokenizer(some_text, reason, locale):
                self.assertEqual(locale, TEST_LOCALE)
                for t in some_text:
                    # StringTokenizer wrapped have to give offsets -
                    # if they don't want to then then there is no
                    # point in wrapping
                    yield 0, 0, "!" + t

            return tokenizer

        def wrap_tok(con, params):
            options = apsw.fts5.parse_tokenizer_args({"+": None}, con, params)

            def tokenizer(utf8, reason, locale):
                self.assertEqual(locale, TEST_LOCALE)
                for start, end, *tokens in options["+"](utf8, reason, locale):
                    yield start, end, *("!" + token for token in tokens)

            return tokenizer

        all_tokenizers = dict(
            apsw.fts5.map_tokenizers, **{"byte_tok": byte_tok, "string_tok": string_tok, "wrap_tok": wrap_tok}
        )

        all_tokenizers["regex"] = functools.partial(apsw.fts5.RegexTokenizer, pattern=".")
        all_tokenizers["regex pre"] = functools.partial(apsw.fts5.RegexPreTokenizer, pattern="c")
        all_tokenizers["stopwords"] = apsw.fts5.StopWordsTokenizer(lambda *x: False)
        all_tokenizers["synonyms"] = apsw.fts5.SynonymTokenizer(lambda *x: None)
        all_tokenizers["transform"] = apsw.fts5.TransformTokenizer(lambda x: x.upper())

        apsw.fts5.register_tokenizers(self.db, all_tokenizers)

        def extapi(api: apsw.FTS5ExtensionApi, one, two, three):
            for column in range(api.column_count):
                self.assertEqual(api.column_locale(column), TEST_LOCALE)
            return 7

        self.db.register_fts5_function("extapi", extapi)

        test_content = {
            "*": "abcdef ghicjk",
            "html": "<tag>text<more>another one",
            "json": '{"key": "value"  "morekey": "morevalue"',
        }

        for tok in all_tokenizers:
            if tok in {"querytokens", "wrap_tok"}:
                continue
            tokenize = [tok]
            if tok in {"html", "json", "simplify", "regex pre", "stopwords", "synonyms", "transform"}:
                tokenize.append("byte_tok")
            if tok in {
                "ngram",
                "unicodewords",
                "regex",
                "regex pre",
            }:
                tokenize.insert(0, "wrap_tok")
            table = apsw.fts5.Table.create(
                self.db, tok, ["one", "two", "three"], support_query_tokens=True, locale=True, tokenize=tokenize
            )
            content = test_content.get(tok, test_content["*"])
            self.db.execute(
                f"insert into {table.quoted_table_name} values(fts5_locale(?, ?), fts5_locale(?, ?), fts5_locale(?, ?))",
                (
                    TEST_LOCALE,
                    content,
                    TEST_LOCALE,
                    content,
                    TEST_LOCALE,
                    content,
                ),
            )
            tokens = table.tokens
            self.assertTrue(all(token.startswith("!") for token in tokens))

            self.db.execute(
                f"select * from {table.quoted_table_name}(fts5_locale(?, ?))", (TEST_LOCALE, "hello world")
            ).get

            self.assertIsNotNone(
                self.db.execute(f"select extapi({table._qname}, one, two, three) from {table.quoted_table_name}").get
            )

        # check non-locale handling gives error
        table = apsw.fts5.Table.create(
            self.db, "no locale", ["one", "two", "three"], support_query_tokens=True, locale=False
        )
        self.assertRaises(
            apsw.MismatchError,
            self.db.execute,
            f"insert into { table.quoted_table_name } values(?,fts5_locale(?,?),?)",
            ("a", "b", "c", "d"),
        )

    def testFTSTokenizerAPI(self):
        "Test C interface for tokenizers"

        self.assertRaisesRegex(apsw.SQLError, "No tokenizer named .*", self.db.fts5_tokenizer, "doesn't exist")
        self.assertFalse(self.db.fts5_tokenizer_available("doesn't exist"))
        self.assertTrue(self.db.fts5_tokenizer_available("unicode61"))

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

            def tokenize(utf8, reason, locale):
                self.assertEqual(utf8.decode("utf8"), test_text)
                self.assertEqual(reason, test_reason)
                return test_data

            return tokenize

        # and a class
        class class_tok:
            def __init__(innerself, con, args):
                self.assertIs(con, self.db)
                self.assertEqual(args, test_args)

            def __call__(innserself, utf8, reason, locale):
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
                        None,
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
            def tokenize(utf8, reason, locale):
                nonlocal bad_results
                yield bad_results.pop()

            return tokenize

        self.db.register_fts5_tokenizer("bad_tok", bad_tok)

        self.assertRaisesRegex(
            ValueError,
            ".*flags is not an allowed value.*",
            self.db.fts5_tokenizer("unicode61", []).__call__,
            b"",
            0,
            None,
        )

        while bad_results:
            self.assertRaises(
                ValueError, self.db.fts5_tokenizer("bad_tok", []).__call__, b"abc", apsw.FTS5_TOKENIZE_DOCUMENT, None
            )

        def bad_tok2(con, args):
            options = apsw.fts5.parse_tokenizer_args({"+": None}, con, args)

            def tokenize(utf8, reason, locale):
                for start, end, *tokens in options["+"](utf8, reason, locale):
                    1 / 0

            return tokenize

        self.db.register_fts5_tokenizer("bad_tok2", bad_tok2)
        bad_results = bad_results_orig[:]
        while bad_results:
            self.assertRaises(
                ValueError,
                self.db.fts5_tokenizer("bad_tok2", ["bad_tok"]).__call__,
                b"abc",
                apsw.FTS5_TOKENIZE_DOCUMENT,
                None,
            )

        def not_a_tokenizer(con, args):
            return 3

        self.db.register_fts5_tokenizer("not_a_tokenizer", not_a_tokenizer)
        self.assertRaises(TypeError, self.db.fts5_tokenizer, "not_a_tokenizer", [])

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

    def testTokenizerShortInputs(self):
        "Small inputs to tokenizers"

        apsw.fts5.register_tokenizers(self.db, apsw.fts5.map_tokenizers)

        self.db.register_fts5_tokenizer("regex", functools.partial(apsw.fts5.RegexTokenizer, pattern="."))

        self.db.register_fts5_tokenizer("regexpre", functools.partial(apsw.fts5.RegexPreTokenizer, pattern="."))

        content = '🇬🇧 <;>"a❤️&\t\\﻿世🤦🏼‍♂️'

        for i in range(0, 3):
            for seq in itertools.permutations(content, i):
                text = "".join(seq)
                for reason in "AUX", "DOCUMENT", "QUERY":
                    reason_code = getattr(apsw, f"FTS5_TOKENIZE_{reason}")
                    for tokenizer, args in (
                        ("unicodewords", ["categories", "*"]),
                        ("simplify", ["unicodewords", "categories", "*"]),
                        ("html", ["unicodewords", "categories", "*"]),
                        ("json", ["unicodewords", "categories", "*"]),
                        ("ngram", ["ngrams", "1"]),
                        ("ngram", ["ngrams", "3"]),
                        ("regex", []),
                        ("regexpre", ["ascii"]),
                    ):
                        self.db.fts5_tokenizer(tokenizer, args)(text.encode(), reason_code, None)

    def testAPSWFTSTokenizers(self):
        "Test apsw.fts5 tokenizers"

        test_text = """ 🤦🏼‍♂️❤️ 🇧🇧𐌼𐌰𐌲 العالم!
            Olá, mundo! 8975984
            नमस्ते, दुनिया!"""

        test_utf8 = test_text.encode("utf8")

        ## UnicodeWordsTokenizer
        self.db.register_fts5_tokenizer("unicodewords", apsw.fts5.UnicodeWordsTokenizer)

        self.assertRaises(ValueError, self.db.fts5_tokenizer, "unicodewords", ["zebra"])

        self.assertEqual(self.db.fts5_tokenizer("unicodewords", [])(b"", apsw.FTS5_TOKENIZE_DOCUMENT, None), [])
        self.assertEqual(
            self.db.fts5_tokenizer("unicodewords", [])(b"a", apsw.FTS5_TOKENIZE_DOCUMENT, "hello"), [(0, 1, "a")]
        )

        correct = (
            ("L* !Lu:0:0", "𐌼𐌰𐌲:العالم:Olá:mundo:नमस्ते:दुनिया"),
            ("L* !Lu:0:1", "🇧🇧:𐌼𐌰𐌲:العالم:Olá:mundo:नमस्ते:दुनिया"),
            ("L* !Lu:1:0", "🤦🏼‍♂️:❤️:𐌼𐌰𐌲:العالم:Olá:mundo:नमस्ते:दुनिया"),
            ("L* !Lu:1:1", "🤦🏼‍♂️:❤️:🇧🇧:𐌼𐌰𐌲:العالم:Olá:mundo:नमस्ते:दुनिया"),
            ("N*:0:0", "8975984"),
            ("N*:0:1", "🇧🇧:8975984"),
            ("N*:1:0", "🤦🏼‍♂️:❤️:8975984"),
            ("N*:1:1", "🤦🏼‍♂️:❤️:🇧🇧:8975984"),
        )
        for categories in {"N*", "L* !Lu"}:
            for emoji in (0, 1):
                for ri in (0, 1):
                    result = []
                    key = f"{categories}:{emoji}:{ri}"
                    args = ["categories", categories, "emoji", str(emoji), "regional_indicator", str(ri)]
                    for start, end, token in self.db.fts5_tokenizer("unicodewords", args)(
                        test_utf8, apsw.FTS5_TOKENIZE_DOCUMENT, None
                    ):
                        self.assertEqual(test_utf8[start:end].decode("utf8"), token)
                        result.append(token)
                    result = ":".join(result)

                    self.assertIn((key, result), correct)

        ## NGramTokenizer
        test_utf8 = ("中文(繁體) Fr1AnçAiS češt2ina 🤦🏼‍♂️straße" * 4).encode("utf8")
        self.db.register_fts5_tokenizer("ngram", apsw.fts5.NGramTokenizer)

        self.assertRaises(ValueError, self.db.fts5_tokenizer, "ngram", ["ngrams", "-3"])

        self.assertEqual(self.db.fts5_tokenizer("ngram")(b"", apsw.FTS5_TOKENIZE_QUERY, "fred"), [])

        for include_categories in (
            None,
            "Ll N*",
            "P* N*",
        ):
            for reason in (
                apsw.FTS5_TOKENIZE_QUERY,
                apsw.FTS5_TOKENIZE_DOCUMENT,
            ):
                sizes = collections.Counter()
                # verify all bytes are covered
                got = [None] * len(test_utf8)
                # verify QUERY mode only has one length per offset
                by_start_len = [None] * len(test_utf8)
                args = ["ngrams", "3,7,9-12" + (",193" if reason == apsw.FTS5_TOKENIZE_QUERY else "")]
                if include_categories:
                    args += ["categories", include_categories]
                for start, end, *tokens in self.db.fts5_tokenizer("ngram", args)(test_utf8, reason, None):
                    self.assertEqual(1, len(tokens))
                    if reason == apsw.FTS5_TOKENIZE_QUERY:
                        self.assertIsNone(by_start_len[start])
                        by_start_len[start] = apsw.unicode.grapheme_length(tokens[0])
                    self.assertIn(apsw.unicode.grapheme_length(tokens[0]), {3, 7, 9, 10, 11, 12})
                    sizes[apsw.unicode.grapheme_length(tokens[0])] += 1
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
                    else:
                        cats = apsw.fts5.convert_unicode_categories(include_categories)
                        self.assertTrue(
                            any(apsw.unicode.category(t) in cats for t in tokens[0]), f"{tokens[0]!r} {cats=}"
                        )
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
            test_utf8, apsw.FTS5_TOKENIZE_DOCUMENT, include_colocated=False, include_offsets=False, locale="1"
        )[0]
        self.assertEqual(test_utf8, token.encode("utf8"))
        # zero len
        self.assertEqual([], self.db.fts5_tokenizer("ngram")(b"", apsw.FTS5_TOKENIZE_DOCUMENT, None))

        ## Regex
        pattern = r"\d+"  # digits
        flags = re.ASCII  # only ascii recognised
        tokenizer = functools.partial(apsw.fts5.RegexTokenizer, pattern=pattern, flags=flags)
        self.db.register_fts5_tokenizer("my_regex", tokenizer)

        # ASCII/Arabic and non-ascii digits
        text = "text2abc 3.14 tamil ௦௧௨௩௪ bengali ০১২৩৪ arabic01234"
        self.assertEqual(
            self.db.fts5_tokenizer("my_regex")(text.encode("utf8"), apsw.FTS5_TOKENIZE_DOCUMENT, None),
            [(4, 5, "2"), (9, 10, "3"), (11, 13, "14"), (66, 71, "01234")],
        )

        ## RegexPre
        tokenizer = functools.partial(apsw.fts5.RegexPreTokenizer, pattern=r"[a7v]+")
        self.db.register_fts5_tokenizer("my_regexpre", tokenizer)
        # we want to check that all text except the regex is available
        # to subsequent tokenizer and at correct offset
        text = "jkhda😂❤️🤣kjdCześćŁłćąŚąćęłńśźżƍɕʑ̨Ꟁꟁaaa7vstraße"
        tokens = self.db.fts5_tokenizer("my_regexpre", ["ngram", "ngrams", "1000"])(
            text.encode("utf8"), apsw.FTS5_TOKENIZE_DOCUMENT, None
        )
        self.assertEqual(
            ["jkhd", "a", "😂❤️🤣kjdCześćŁłćąŚąćęłńśźżƍɕʑ̨Ꟁꟁ", "aaa7v", "str", "a", "ße"], [t[2] for t in tokens]
        )
        for start, end, token in tokens:
            self.assertEqual(text.encode()[start:end].decode(), token)

    def testCLI(self):
        "Test command line interface"
        if coverage_run:
            cov_params = ["-m", "coverage", "run", "--source", "apsw", "-p"]
            env = os.environ.copy()
            env["ASAN_OPTIONS"] = "detect_leaks=false"
        else:
            cov_params = []
            env = None

        proc = subprocess.run(
            [sys.executable] + cov_params + ["-m", "apsw.fts5", "unicodewords"], capture_output=True, env=env
        )
        self.assertEqual(0, proc.returncode)
        self.assertIn(b"Tips", proc.stdout)

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
            self.assertEqual(apsw.fts5.convert_tokenize_reason(pat), expected)
        self.assertRaises(ValueError, apsw.fts5.convert_tokenize_reason, "AUX BANANA")

        ## tokenizer_test_strings
        def verify_test_string_item(item):
            value, comment = item
            self.assertTrue(comment)
            self.assertIsInstance(comment, str)
            self.assertIsInstance(value, bytes)
            self.assertEqual(value, value.decode("utf8", "replace").encode("utf8"))

        tests = apsw.fts5.tokenizer_test_strings()
        for count, item in enumerate(tests):
            verify_test_string_item(item)
        self.assertGreater(count, 16)

        with BecauseWindowsTempfile("wb") as tf:
            some_text = "hello Aragonés 你好世界"
            items = apsw.fts5.tokenizer_test_strings(tf.name)
            self.assertEqual(len(items), 1)
            verify_test_string_item(items[0])
            tf.write_whole_file(some_text.encode("utf8"))
            items = apsw.fts5.tokenizer_test_strings(tf.name)
            self.assertEqual(len(items), 1)
            verify_test_string_item(items[0])
            self.assertEqual(items[0][0], some_text.encode("utf8"))
            data = b""
            for i in range(10):
                data += f"# { i }\t\r\n## ignored\n".encode("utf8")
                data += (some_text + f"{ i }  \n").encode("utf8")
            tf.write_whole_file(data)
            items = apsw.fts5.tokenizer_test_strings(tf.name)
            self.assertEqual(10, len(items))
            for i, (value, comment) in enumerate(items):
                self.assertEqual(comment, f"{ i }")
                self.assertNotIn(b"##", value)
                self.assertEqual((some_text + f"{ i }").encode("utf8"), value)

        ## convert_unicode_categories
        self.assertRaises(ValueError, apsw.fts5.convert_unicode_categories, "L* !BANANA")
        self.assertEqual(apsw.fts5.convert_unicode_categories("L* Pc !N* N* !N*"), {"Pc", "Lm", "Lo", "Lu", "Lt", "Ll"})
        self.assertEqual(
            apsw.fts5.convert_unicode_categories("* !P* !Z*"), apsw.fts5.convert_unicode_categories("[CLMNS]*")
        )
        ## convert_number_ranges
        for t in "3-", "a", "", "3-5-7", "3,3-", "3,a", "3,4-a":
            self.assertRaises(ValueError, apsw.fts5.convert_number_ranges, t)
        for t, expected in (
            ("3", {3}),
            ("3,4,5", {3, 4, 5}),
            ("3-7", {3, 4, 5, 6, 7}),
            ("2-3,3-9", {2, 3, 4, 5, 6, 7, 8, 9}),
            ("6-2", set()),
        ):
            self.assertEqual(apsw.fts5.convert_number_ranges(t), expected)

        ## convert_boolean
        self.assertRaises(ValueError, apsw.fts5.convert_boolean, "yes")
        self.assertRaises(ValueError, apsw.fts5.convert_boolean, "-1")
        self.assertEqual(False, apsw.fts5.convert_boolean("0"))
        self.assertEqual(False, apsw.fts5.convert_boolean("FALSE"))
        self.assertEqual(True, apsw.fts5.convert_boolean("1"))
        self.assertEqual(True, apsw.fts5.convert_boolean("true"))

        ## extract_html_text
        some_html = (
            """<!decl><!--comment-->&copy;&#62;<?pi><hello/><script>script</script><svg>ddd<svg>ffff"""
            """</svg>ggg&lt;<?pi2></svg><hello>bye</hello>"""
        )
        text, om = apsw.fts5.extract_html_text(some_html)
        self.assertEqual(text.strip(), "©>\nbye")
        offsets = [om(i) for i in range(len(text) + 1)]
        self.assertEqual(offsets, [0, 21, 27, 32, 117, 118, 119, 120, 128])
        self.assertRaises(IndexError, om, -1)
        self.assertRaises(IndexError, om, len(text) + 1)

        ## extract_json_text
        some_json = r"""["one", "", {"two": "three"}, "four:", "a\"b", "'\ud83c\udf82\\ሴ\u1234ስ"]"""
        text, om = apsw.fts5.extract_json(some_json, include_keys=False)
        self.assertEqual(text, "\none\nthree\nfour:\na\"b\n'🎂\\ሴሴስ")
        offsets = [om(i) for i in range(0, len(text) + 1, 3)]
        self.assertEqual(offsets, [0, 4, 22, 25, 32, 35, 41, 48, 63, 71])
        self.assertEqual(om(len(text)), 71)
        self.assertRaises(IndexError, om, -1)
        self.assertRaises(IndexError, om, len(text) + 1)

        text, om = apsw.fts5.extract_json(some_json, include_keys=True)
        self.assertEqual(text, "\none\ntwo\nthree\nfour:\na\"b\n'🎂\\ሴሴስ")
        offsets = [om(i) for i in range(0, len(text) + 1, 3)]
        self.assertEqual(offsets, [0, 4, 15, 21, 24, 31, 34, 40, 44, 61, 70])
        self.assertEqual(om(len(text)), 71)
        self.assertRaises(IndexError, om, -1)
        self.assertRaises(IndexError, om, len(text) + 1)

        ## convert_string_to_python
        self.assertIs(apsw.fts5.convert_string_to_python("apsw.fts5.quote_name"), apsw.fts5.quote_name)

        ## parse_tokenizer_args
        ta = apsw.fts5.TokenizerArgument
        # the factory must return a callable hence nested lambdas
        self.db.register_fts5_tokenizer("dummy", lambda *args: lambda *args: ("nothing",))

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
                self.assertRaisesRegex(expected[0], expected[1], apsw.fts5.parse_tokenizer_args, spec, self.db, args)
            else:
                options = apsw.fts5.parse_tokenizer_args(spec, self.db, args)
                if "+" in spec:
                    tok = options.pop("+")
                    e = expected.pop("+")
                    self.assertIs(tok.connection, e.connection)
                    self.assertEqual(tok.args, e.args)
                    self.assertEqual(tok.name, e.name)
                self.assertEqual(expected, options)

    def testAPSWTokenizerWrappers(self):
        "Test tokenizer wrappers supplied by apsw.fts5"
        test_reason = apsw.FTS5_TOKENIZE_AUX
        test_data = b"a 1 2 3 b"
        test_res = ((0, 1, "a"), (2, 3, "1"), (4, 5, "2", "deux", "two"), (6, 7, "3"), (8, 9, "b"))

        def source(con, args):
            apsw.fts5.parse_tokenizer_args({}, con, args)

            def tokenize(utf8, flags, locale):
                self.assertEqual(flags, test_reason)
                self.assertEqual(utf8, test_data)
                return test_res

            return tokenize

        self.db.register_fts5_tokenizer("source", source)

        @apsw.fts5.TransformTokenizer
        def transform_wrapped_func(s):
            return self.transform_test_function(s)

        @apsw.fts5.StopWordsTokenizer
        def stopwords_wrapped_func(s):
            return self.stopwords_test_function(s)

        @apsw.fts5.SynonymTokenizer
        def synonym_wrapped_func(s):
            return self.synonym_test_function(s)

        self.db.register_fts5_tokenizer("transform_wrapped", transform_wrapped_func)
        self.db.register_fts5_tokenizer("transform_param", apsw.fts5.TransformTokenizer(self.transform_test_function))
        self.db.register_fts5_tokenizer("transform_arg", apsw.fts5.TransformTokenizer())

        self.db.register_fts5_tokenizer("stopwords_wrapped", stopwords_wrapped_func)
        self.db.register_fts5_tokenizer("stopwords_param", apsw.fts5.StopWordsTokenizer(self.stopwords_test_function))
        self.db.register_fts5_tokenizer("stopwords_arg", apsw.fts5.StopWordsTokenizer())

        self.db.register_fts5_tokenizer("synonym_wrapped", synonym_wrapped_func)
        self.db.register_fts5_tokenizer("synonym_param", apsw.fts5.SynonymTokenizer(self.synonym_test_function))
        self.db.register_fts5_tokenizer("synonym_arg", apsw.fts5.SynonymTokenizer())

        for name in ("transform", "stopwords", "synonym"):
            returns = []
            for suffix in "wrapped", "param", "arg":
                param_name = {"transform": "transform", "stopwords": "test", "synonym": "get"}[name]
                args_with = [param_name, f"apsw.ftstests.FTS.{ name }_test_function", "source"]
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
                        apsw.SQLError, "No tokenizer named .*", self.db.fts5_tokenizer, tokname, args_with
                    )
                    tok = self.db.fts5_tokenizer(tokname, args_without)

                if name == "synonym":
                    test_reason = apsw.FTS5_TOKENIZE_QUERY
                returns.append(tok(test_data, test_reason, None))
                test_reason = apsw.FTS5_TOKENIZE_AUX

            self.assertNotEqual(returns[0], test_res)
            self.assertEqual(returns[0], returns[1])
            self.assertEqual(returns[1], returns[2])

            apsw.fts5.convert_string_to_python(f"apsw.ftstests.FTS.{ name }_test_function_check")(self, returns[0])

        # synonym reason
        test_text = "one two three"

        @apsw.fts5.SynonymTokenizer
        def func(n):
            1 / 0

        self.db.register_fts5_tokenizer("unicodewords", apsw.fts5.UnicodeWordsTokenizer)
        self.db.register_fts5_tokenizer("synonym-reason", func)

        self.assertEqual(
            self.db.fts5_tokenizer("unicodewords")(test_text.encode("utf8"), apsw.FTS5_TOKENIZE_QUERY, None),
            self.db.fts5_tokenizer("synonym-reason", ["unicodewords"])(
                test_text.encode("utf8"), apsw.FTS5_TOKENIZE_AUX, None
            ),
        )

        # stopwords reason
        @apsw.fts5.StopWordsTokenizer
        def func(n):
            1 / 0

        self.db.register_fts5_tokenizer("stopwords-reason", func)

        self.assertEqual(
            self.db.fts5_tokenizer("unicodewords")(test_text.encode("utf8"), apsw.FTS5_TOKENIZE_QUERY, None),
            self.db.fts5_tokenizer("stopwords-reason", ["unicodewords"])(
                test_text.encode("utf8"), apsw.FTS5_TOKENIZE_QUERY | apsw.FTS5_TOKENIZE_PREFIX, None
            ),
        )
        self.db.register_fts5_tokenizer("stopwords-reason", func)

        ## SimplifyTokenizer
        test_text = "中文(繁體) Fr1AnçAiSⅦčešt2ina  🤦🏼‍♂️ straße"
        test_utf8 = test_text.encode("utf8")

        self.db.register_fts5_tokenizer("simplify", apsw.fts5.SimplifyTokenizer)

        # no args should have no effect
        baseline = self.db.fts5_tokenizer("unicodewords")(test_utf8, test_reason, None)
        nowt = self.db.fts5_tokenizer("simplify", ["unicodewords"])(test_utf8, test_reason, None)
        self.assertEqual(baseline, nowt)

        # require tokenizer arg
        self.assertRaises(ValueError, self.db.fts5_tokenizer, "simplify")

        for strip, casefold, expected in (
            (
                False,
                False,
                [("中",), ("文",), ("繁",), ("體",), ("Fr1AnçAiSⅦčešt2ina",), ("🤦🏼\u200d♂️",), ("straße",)],
            ),
            (
                False,
                True,
                [("中",), ("文",), ("繁",), ("體",), ("fr1ançaisⅶčešt2ina",), ("🤦🏼\u200d♂️",), ("strasse",)],
            ),
            (True, False, [("中",), ("文",), ("繁",), ("體",), ("Fr1AncAiSVIIcest2ina",), ("🤦♂",), ("straße",)]),
            (True, True, [("中",), ("文",), ("繁",), ("體",), ("fr1ancaisviicest2ina",), ("🤦♂",), ("strasse",)]),
        ):
            res = self.db.fts5_tokenizer("simplify", ["casefold", str(casefold), "strip", str(strip), "unicodewords"])(
                test_utf8, test_reason, None, include_offsets=False
            )
            self.assertEqual(res, expected)

        ## HTMLTokenizer
        test_html = "<t>text</b><fooo/>mor<b>e</b> stuff&amp;things<yes yes>yes<>/no>a&#1234;b"
        self.db.register_fts5_tokenizer("html", apsw.fts5.HTMLTokenizer)
        # html text is separately tested
        self.assertEqual(
            # Po is for the ampersand
            self.db.fts5_tokenizer("html", ["unicodewords", "categories", "L* N* Po"])(
                test_html.encode("utf8"),
                apsw.FTS5_TOKENIZE_DOCUMENT,
                None,
                include_colocated=False,
                include_offsets=False,
            ),
            ["text", "mor", "e", "stuff", "&", "things", "yes", "/", "no", "aӒb"],
        )
        # non html should be pass through
        self.assertEqual(
            self.db.fts5_tokenizer("html", ["unicodewords", "categories", "*"])(
                "hello<world>".encode("utf8"),
                apsw.FTS5_TOKENIZE_QUERY,
                None,
                include_colocated=False,
                include_offsets=False,
            ),
            ["hello", "<", "world", ">"],
        )

        ## JSONTokenizer
        test_json = json.dumps({"key": "value", "k\u1234": ["'", '"']})
        self.db.register_fts5_tokenizer("json", apsw.fts5.JSONTokenizer)
        # json text is separately tested
        self.assertEqual(
            # Po is for the quotes
            self.db.fts5_tokenizer("json", ["unicodewords", "categories", "L* N* Po"])(
                test_json.encode("utf8"),
                apsw.FTS5_TOKENIZE_DOCUMENT,
                None,
                include_colocated=False,
                include_offsets=False,
            ),
            ["value", "'", '"'],
        )
        self.assertEqual(
            self.db.fts5_tokenizer("json", ["include_keys", "1", "unicodewords", "categories", "L* N* Po"])(
                test_json.encode("utf8"),
                apsw.FTS5_TOKENIZE_DOCUMENT,
                None,
                include_colocated=False,
                include_offsets=False,
            ),
            ["key", "value", "kሴ", "'", '"'],
        )
        # non json should be pass through
        self.assertEqual(
            self.db.fts5_tokenizer("json", ["unicodewords", "categories", "*"])(
                "hello<world>".encode("utf8"),
                apsw.FTS5_TOKENIZE_QUERY,
                None,
                include_colocated=False,
                include_offsets=False,
            ),
            ["hello", "<", "world", ">"],
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
        syn = FTS.transform_test_function(s)
        return syn if syn != s else None

    def synonym_test_function_check(self, s):
        self.assertEqual(
            s, [(0, 1, "a"), (2, 3, "1", "one"), (4, 5, "2", "two", "ii", "deux"), (6, 7, "3"), (8, 9, "b")]
        )

    def testFTSFunction(self):
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

            self.assertRaises(apsw.RangeError, api.phrase_column_offsets, -1, 0)
            self.assertRaises(apsw.RangeError, api.phrase_column_offsets, 0, 99)
            for p in range(api.phrase_count):
                for c in range(api.column_count):
                    self.assertEqual(api.phrase_column_offsets(p, c), api.phrase_locations(p)[c])

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
                        test_text.encode("utf8"),
                        None,
                        include_offsets=include_offsets,
                        include_colocated=include_colocated,
                    )
                    self.assertIn((include_offsets, include_colocated, res), correct)

        self.db.register_fts5_function("check_api", check_api)
        for _ in self.db.execute("select check_api(testfts) from testfts('c d OR 5')"):
            pass

        # the same structure is in tools/fi.py - update that if you update this
        extapi = {
            "attr": {"aux_data", "column_count", "inst_count", "phrase_count", "phrases", "row_count", "rowid"},
            (0,): {
                "column_locale",
                "column_size",
                "column_text",
                "column_total_size",
                "inst_tokens",
                "phrase_columns",
                "phrase_locations",
            },
            (0, 0): ("phrase_column_offsets",),
            (0, lambda *args: None, None): {"query_phrase"},
            (b"abcd e f g h", "hello"): {"tokenize"},
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
            # check all members were tested
            self.assertEqual(len(items), 0, f"untested {items=}")

    def testzzFaultInjection(self):
        "Deliberately inject faults to exercise all code paths"
        ### Copied from main tests
        if not getattr(apsw, "apsw_fault_inject", None):
            return

        apsw.faultdict = dict()

        def ShouldFault(name, pending_exception):
            r = apsw.faultdict.get(name, False)
            apsw.faultdict[name] = False
            return r

        sys.apsw_should_fault = ShouldFault
        ### end copied from main tests

        if not has_fts5:
            return

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
            None,
        )
        apsw.faultdict["xTokenCBOffsetsBad"] = True
        self.assertRaisesRegex(
            ValueError,
            "Invalid start .* or end .*",
            self.db.fts5_tokenizer("unicode61", []),
            b"abc def",
            apsw.FTS5_TOKENIZE_DOCUMENT,
            None,
        )
        apsw.faultdict["xTokenCBColocatedBad"] = True
        self.assertRaisesRegex(
            ValueError,
            "FTS5_TOKEN_COLOCATED set.*",
            self.db.fts5_tokenizer("unicode61", []),
            b"abc def",
            apsw.FTS5_TOKENIZE_DOCUMENT,
            None,
        )
        apsw.faultdict["TokenizeRC"] = True

        def tokenizer(con, args):
            def tokenize(utf8, reason, locale):
                yield "hello"
                yield ("hello", "world")

            return tokenize

        self.db.register_fts5_tokenizer("simple", tokenizer)
        self.assertRaises(
            apsw.NoMemError, self.db.fts5_tokenizer("simple", []), b"abc def", apsw.FTS5_TOKENIZE_DOCUMENT, None
        )
        apsw.faultdict["TokenizeRC2"] = True
        self.assertRaises(
            apsw.NoMemError, self.db.fts5_tokenizer("simple", []), b"abc def", apsw.FTS5_TOKENIZE_DOCUMENT, None
        )

        self.db.execute("""create virtual table ftstests using fts5(x); insert into ftstests values('hello world')""")

        def cb(api: apsw.FTS5ExtensionApi):
            api.row_count
            api.aux_data = "hello"
            api.phrases
            api.inst_count
            api.tokenize(b"hello world", api.column_locale(0))

        self.db.register_fts5_function("errmaker", cb)
        for fault in ("xRowCountErr", "xSetAuxDataErr", "xQueryTokenErr", "xInstCountErr", "xTokenizeErr"):
            apsw.faultdict[fault] = True
            self.assertRaises(apsw.NoMemError, self.db.execute, "select errmaker(ftstests) from ftstests('hello')")


lines = [
    line.strip()
    for line in
    # The English is random lines from the APSW documentation.  The
    # non-English is random lines from wikipedia.
    """
    If the html doesn’t start with whitespace then < or &, it is not considered HTML
    This is useful for queries where less than an entire word has been provided such as doing
    Numeric ranges to generate. Smaller values allow showing results with less input but a larger
    Recognises a special tokens marker and returns those tokens for a query. This is useful for
    Tokenizer wrapper that simplifies tokens by neutralizing case conversion, canonicalization, and
    To use you need a callable that takes a str, and returns a str, a sequence of str, or None. For
    A call cannot be made to an inherited Virtual File System (VFS) method as the VFS does not
    To interrupt the shell press Control-C. (On Windows if you press Control-Break then the program
    In addition to executing SQL, these are the commands available with their description. Commands
    ASPW lets you use SQLite in multi-threaded programs and will let other threads execute while
    You can install tracers on cursors or connections as an easy way of seeing exactly what gets
    APSW includes a tracer that lets you easily trace SQL execution as well as providing a summary
    A report is also generated by default. This is example output from running the test suite. When
    This shows the longest running queries with time in seconds.
    APSW includes a speed tester to compare SQLite performance across different versions of SQLite,
    Some access layers try to interpret your SQL and manage transactions behind your back, which may
    making up a user perceived character, word and sentence splitting, and where line breaks can be
    Exception handling has been updated, with multiple exceptions in the same SQLite control flow
    Type stubs and typing information in the documentation use newer Python conventions such as |
    Fixed regression in statement cache update (version 3.38.1-r1) where trailing whitespace in queries
    The corresponding SQLite version is embedded privately inside and not affected by or visible to
    APSW needs to know the options chosen so it can adapt. For example if extension loading is
    APSW includes tests which use the standard Python testing modules to verify correct operation.
    This uses difflib.get_close_matches() algorithm to find close matches. Note that it is a
    If the value is not None, then it is changed. It is not recommended to change SQLite’s own
    The prefix is to ensure your own config names don’t clash with those used by SQLite. For
    A sequence of column names. If you are using an external
    This is purely statistical and has no understanding of the tokens. Tokens that occur only in
    This method finds the text that produced a token, by re-tokenizing the documents
    The following tokenizer parameters are accepted. A segment is considered a word if a codepoint
    This is useful to process command line arguments and arguments to tokenizers. It automatically
    APSW provided auxiliary functions for use with register_functions()
    tokens.  Tokens that occur only in this row, or only once in
    Registers auxiliary functions named in map with the connection, if not already registered
    If it starts with a # then it is considered to be multiple text sections where a # line contains a
    This is a hierarchical representation using Python dictionaries which is easy for logging
    If True then the phrase must match the beginning of a column (‘^’ was used)
    SQL is based around the entire contents of a value. You can test for equality, you can do greater
    onto the same token. For example you could stem run, ran, runs, running, and runners to
    Provided by the apsw.fts5 module. This includes Table for creating and working with FTS5 tables
    When multiple exceptions occur in the same SQLite control flow then they will be chained
    sqlite3_log is also called so that you will have the context of when the exception happened
    シビュラとは恍惚状態で神託を伝えた古代の巫女で、彼女たちの神託をまとめた書
    年免費教育、興建地下鐵路和地方行政改革等重要的政策和建設，使香港的社會面貌出
    توسعه می‌یافت. سیستم‌عامل فری‌بی‌اس‌دی به گونهٔ یک نرم‌افزار آزاد توسعه می‌یابد، این گفته به
    lösen und eine Befreiungs­armee zu rekru­tieren, womit die Macht der Pflanzer in den Sklaven­staaten gebrochen werden
    """.splitlines()
    if line.strip()
]


test_content: dict[int, tuple[str, str]] = {
    zlib.crc32((lines[i] + lines[i + 1]).encode()): (
        # each column has common text
        "l’étape humphrey humphrey " + lines[i],
        "L'encyclopédie appleby appleby " + lines[i + 1],
    )
    for i in range(0, len(lines), 2)
}

del lines


class FTS5Table(unittest.TestCase):
    def setUp(self):
        self.db = apsw.Connection("")

    def insert_content(self, table: apsw.fts5.Table):
        for rowid, cols in test_content.items():
            val = table.upsert(*cols, rowid=rowid)
            assert val == rowid, f"{val=} {rowid=}"
        self.assertGreaterEqual(table.command_merge(-7), 3)

    def testTableContent(self):
        "Reading, writing, changing table content"

        # the table and column names used deliberately have characters that need to be quoted in
        # SQL so we can verify they are

        self.db.execute(r"""
            create table normal(oid, "with space");
            create table withrowid(special INTEGER PRIMARY KEY AUTOINCREMENT, [oi\ ' " d], "co""12");
            create view view_normal(rowid, "1-2", "select") as select rowid,oid,"with space" from normal;
            """)

        fts = apsw.fts5.Table.create(self.db, "f\"t's5", ["_rowid_", '"'])
        self.insert_content(fts)
        self.assertEqual(fts.row_count, len(test_content))

        fts_rowid = apsw.fts5.Table.create(
            self.db, "fts5-2", None, content="withrowid", content_rowid="special", generate_triggers=True
        )
        self.insert_content(fts_rowid)
        self.assertEqual(fts_rowid.row_count, len(test_content))

        # populate normnal so rebuild picks up the content because we can't
        self.db.execute('insert into normal(rowid,oid,"with space") select rowid, _rowid_, """" from "f""t\'s5"')
        fts_view = apsw.fts5.Table.create(self.db, "fts_view", None, content="view_normal")
        self.assertRaisesRegex(apsw.SQLError, ".*modify.*is a view", self.insert_content, fts_view)
        self.assertEqual(fts_view.row_count, len(test_content))

        tables = fts, fts_rowid, fts_view
        for key, cols in test_content.items():
            for table in tables:
                self.assertEqual(table.row_by_id(key, table.structure.columns), cols)
                for column in range(len(cols)):
                    self.assertEqual(table.row_by_id(key, table.structure.columns[column]), cols[column])

        for table in tables:
            for key in test_content:
                assert key + 1 not in test_content
                for key in test_content:
                    column_names = table.structure.columns
                    c = table.change_cookie
                    self.assertRaises(KeyError, table.row_by_id, key + 1, column_names[1])
                    if table is not fts_view:
                        self.assertEqual(key, table.upsert(str(key), rowid=key))
                        self.assertEqual(table.row_by_id(key, column_names[0]), str(key))
                        self.assertNotEqual(c, table.change_cookie)
                        c = table.change_cookie
                        self.assertEqual(key + 10, table.upsert("hello", rowid=key + 10))
                        self.assertEqual("hello", table.row_by_id(key + 10, column_names[0]))
                        self.assertNotEqual(c, table.change_cookie)
                        c = table.change_cookie
                        self.assertTrue(table.delete(key))
                        self.assertTrue(table.delete(key + 10))
                        self.assertRaises(KeyError, table.row_by_id, key, column_names[1])
                        self.assertFalse(table.delete(key))
                        self.assertNotEqual(c, table.change_cookie)

            keys = list(test_content.keys())
            if table is fts_view:
                self.assertRaises(ValueError, table.command_delete, keys[0])
                self.assertRaises(ValueError, table.command_delete, keys[0], "one", "two", "three")
            else:
                self.insert_content(table)
                self.assertNotEqual(0, table.row_count)
                if table.structure.content:
                    table.command_delete(99, "one", "two")
                    table.command_delete_all()
                    if not table.structure.content:
                        self.assertEqual(0, table.row_count)
                        table.command_integrity_check()
                    else:
                        # this gives corrupt error on external content
                        # table because the FTS table no longer
                        # matches the external content so we skip
                        self.assertRaises(apsw.CorruptError, table.command_integrity_check)
                else:
                    self.assertRaisesRegex(apsw.SQLError, ".*may only be used with.*", table.command_delete_all)

        # coverage and errors
        self.db.execute("attach '' as 'second'; create table main.under_test(blocker)")
        table = apsw.fts5.Table.create(self.db, "Under_Test", ["one", "Two"], schema="second")

        key = 12345
        self.assertEqual(key, table.upsert(TWO="hello", rOwID=key))
        self.assertEqual("hello", table.row_by_id(key, "tWO"))
        self.assertEqual("hello", self.db.execute("select two from second.under_test").get)

        self.assertRaises(ValueError, table.upsert, "one", "two", "three")
        self.assertRaises(ValueError, table.upsert)
        self.assertRaises(ValueError, table.upsert, "hello", OnE=key)
        self.assertRaises(ValueError, table.upsert, "hello", OnskjdsafE=key)

        apsw.fts5.Table(self.db, "UNDER_TEST", schema="secOND")
        self.assertRaises(ValueError, apsw.fts5.Table, self.db, "UNDER_TEST", schema="zebra")
        self.assertRaises(ValueError, apsw.fts5.Table, self.db, "UNDER_TEST")
        self.assertRaises(ValueError, apsw.fts5.Table, self.db, "xyz", schema="zebra")

    def testConfig(self):
        "config related items"
        name = "test \"']\\ specimen"
        schema = "-1"
        self.db.execute(
            "attach '' as "
            + apsw.fts5.quote_name(schema)
            + "; create table main."
            + apsw.fts5.quote_name(name)
            + "(blocker)"
        )
        table1 = apsw.fts5.Table.create(self.db, name, ["one", "two"], schema=schema)
        table2 = apsw.fts5.Table(self.db, name, schema=schema)

        for name, default, newval in (
            ("automerge", 4, 16),
            ("crisismerge", 16, 8),
            ("deletemerge", 10, 4),
            ("insttoken", False, True),
            ("pgsz", 4050, 4072),
            ("rank", "bm25()", "bm25(10.0, 5.0)"),
            ("secure_delete", False, True),
            ("usermerge", 4, 16),
        ):
            existing1 = getattr(table1, f"config_{name}")()
            existing2 = getattr(table2, f"config_{name}")()
            self.assertEqual(existing1, existing2)
            self.assertEqual(existing1, default)
            self.assertNotEqual(existing1, newval)
            self.assertIs(type(existing2), type(newval))
            getattr(table1, f"config_{name}")(newval)
            new = getattr(table2, f"config_{name}")()
            self.assertEqual(newval, new)
            self.assertIs(type(new), type(newval))

        table1.config("hello", "world")
        self.assertEqual(table2.config("hello"), "world")
        self.assertIsNone(table2.config("hello", prefix="yes"))

    def testTableCreate(self):
        "Table creation"
        schema = "-1"
        self.db.execute("attach '' as " + apsw.fts5.quote_name(schema))

        # Set everything to non-default
        kwargs = {
            "columns": ("one", "four", "two", "five", "three"),
            "name": "&\"'",
            "unindexed": {"four", "five"},
            "tokenize": ("porter", "ascii"),
            "prefix": {1, 5, 8},
            "content": "",
            "content_rowid": "3'\"4",
            "contentless_delete": True,
            "contentless_unindexed": True,
            "detail": "column",
            "tokendata": True,
            "locale": True,
        }

        apsw.fts5.Table.create(self.db, schema=schema, support_query_tokens=True, rank="bm25 (1,2)", **kwargs)

        table = apsw.fts5.Table(self.db, kwargs["name"], schema=schema)

        for k, v in kwargs.items():
            got = getattr(table.structure, k)
            if k == "tokenize":
                v = ("querytokens",) + v
            elif k == "content_rowid":
                # conrent needs to be a value which it isn't
                v = None
            self.assertEqual(got, v)

        # check items not in structure
        self.assertTrue(table.supports_query_tokens)
        self.assertEqual(table.config_rank(), "bm25 (1,2)")

        # prefix as single int
        t = apsw.fts5.Table.create(self.db, "prefix", ["one", "two"], schema=schema, prefix=3)
        self.assertEqual(t.structure.prefix, {3})
        # drop if exists
        apsw.fts5.Table.create(self.db, "prefix", ["one", "two"], schema=schema, prefix={3, 4}, drop_if_exists=True)
        t2 = apsw.fts5.Table(self.db, "prefix", schema=schema)
        self.assertEqual(t2.structure.prefix, {3, 4})
        # querytokens
        t = apsw.fts5.Table.create(self.db, "sqt", ["one", "two"], schema=schema, support_query_tokens=True)
        t2 = apsw.fts5.Table(self.db, "sqt", schema=schema)
        self.assertTrue(t2.supports_query_tokens)
        self.assertEqual(t2.structure.tokenize, ("querytokens", "unicode61"))

        # errors
        c = apsw.fts5.Table.create
        self.assertRaisesRegex(ValueError, ".*specify an external content.*", c, self.db, "fail", None)
        self.assertRaisesRegex(
            ValueError, ".*is in unindexed, but not in columns.*.*", c, self.db, "fail", ["one"], unindexed=["two"]
        )
        self.assertRaisesRegex(
            apsw.SQLError, ".*already exists.*", c, self.db, kwargs["name"], schema=schema, columns=["one"]
        )
        self.assertRaisesRegex(
            ValueError, ".*external table.*does not exist", c, self.db, "fail", columns=None, content="doesn't exist"
        )
        self.assertRaisesRegex(ValueError, ".*rank.*", c, self.db, "fail", ["one", "two"], rank="this does not exist")

        # coverage for parsing SQL of tables made outside of our create method
        q = apsw.fts5.quote_name
        sql = f"""create     virtual    table /* */  {q(schema)}.parse /* - */ using /* - */fts5   (
                    \x09hello\x0d /* comment */ , -- comment
                    123, -- yes fts5 allows numbers as column names
                    🤦🏼‍♂️,
                    detail                    =                    full)
            """
        self.db.execute(sql)
        t = apsw.fts5.Table(self.db, "parse", schema=schema)
        # verifies column names are correctly quoted
        rowid = t.upsert("one", "two", "three")
        self.assertEqual(t.row_by_id(rowid, t.structure.columns), ("one", "two", "three"))

        # not a fts5 table
        self.db.execute(f"create table {q(schema)}.foo(x)")
        self.assertRaisesRegex(ValueError, ".*Not a virtual table.*", apsw.fts5.Table, self.db, "foo", schema=schema)

        # check nothing happened in main or temp
        self.assertIsNone(self.db.execute("select * from sqlite_schema").get)
        self.assertIsNone(self.db.execute("select * from temp.sqlite_schema").get)

    def testMisc(self):
        t = apsw.fts5.Table.create(self.db, "hello", ["one", "two", "three"], unindexed=["two"])
        self.assertIn("<FTS5Table", str(t))
        self.assertIn("<FTS5Table", repr(t))
        self.assertIn("hello", str(t))
        self.assertIn("main", str(t))

        self.assertEqual(t.columns_indexed, ("one", "three"))
        self.assertEqual(t.column_named("one"), "one")
        self.assertEqual(t.column_named("onE"), "one")
        self.assertIsNone(t.column_named("onE "))

    def testContent(self):
        "Content based tests"
        schema = "-1"
        self.db.execute("attach '' as " + apsw.fts5.quote_name(schema))

        t = apsw.fts5.Table.create(
            self.db,
            "😂❤️🤣🤣 😭🙏😘",
            ["🇿🇦 🍿", "👍🏻 सं"],
            schema=schema,
            support_query_tokens=True,
            tokenize=["simplify", "casefold", "true", "strip", "true", "unicodewords"],
            locale=True,
        )
        self.insert_content(t)

        def mq(s):
            return apsw.fts5query.to_query_string(apsw.fts5query.from_dict(s))

        matches = list(t.search(mq("example"), "yes"))
        self.assertEqual(len(matches), 3)
        self.assertEqual(
            str(matches[0]),
            "MatchInfo(query_info=QueryInfo(phrases=(('example',),)), rowid=3912225165, column_size=[20, 13], phrase_columns=[[0]])",
        )

        matches = list(t.search("example OR statistical"))
        self.assertEqual(
            str(matches[1]),
            "MatchInfo(query_info=QueryInfo(phrases=(('example',), ('statistical',))), rowid=3912225165, column_size=[20, 13], phrase_columns=[[0], []])",
        )
        self.assertEqual(len(matches), 4)

        matches = list(t.search("lösen"))
        self.assertIn("losen", str(matches))
        self.assertIn("rowid=2064310668", str(matches))

        matches = list(t.search("willnotmatch"))
        self.assertEqual(0, len(matches))

        ## Key tokens and more like were developed on large data sets.
        # These tests don't have enough meaningful content, but do
        # still work.  The tests verify stability

        # key tokens
        def token_check(rowid, expected, **kwargs):
            kt = t.key_tokens(rowid, **kwargs)
            zip_kwargs = {"strict": True} if sys.version_info >= (3, 10) else {}
            for got, expected in zip(kt, expected, **zip_kwargs):
                # float values
                self.assertAlmostEqual(got[0], expected[0])
                self.assertEqual(got[1], expected[1])

        token_check(
            3283224240,
            [(0.03225806451612903, "only"), (0.016129032258064516, "tokens"), (0.012096774193548387, "in")],
            limit=3,
        )
        token_check(
            3283224240,
            [(0.06666666666666667, "only"), (0.03333333333333333, "tokens"), (0.016666666666666666, "in")],
            columns=t.columns[0],
            limit=3,
        )

        token_check(3283224240, [(0.005434782608695652, "appleby")], columns=t.columns[1], limit=3)

        # more like
        def mlr(rowids, lim, cols):
            return {mi.rowid for mi in t.more_like(rowids, token_limit=lim, columns=cols)}

        expected = {
            742136966: {
                3752288227,
                110396968,
                3167611882,
                2478625915,
                2651692906,
                3912225165,
                3283224240,
                1535409456,
                942672272,
                2691318995,
                1967567003,
                2183555614,
                418639071,
            },
            1751198350: {
                1314334723,
                1925714052,
                742136966,
                2163134344,
                2064310668,
                3912225165,
                942672272,
                2931333528,
                1967567003,
                2183555614,
                110396968,
                3283224240,
                2910429744,
                1535409456,
                2691318995,
                418639071,
                3394832226,
                3752288227,
                2651692906,
                3167611882,
                2478625915,
                3630190717,
            },
            3912225165: {
                1314334723,
                3752288227,
                2163134344,
                3167611882,
                2478625915,
                3283224240,
                1967567003,
                3630190717,
                2183555614,
            },
        }
        for rowid in 742136966, 1751198350, 3912225165:
            found = mlr([rowid], 2, None)
            self.assertEqual(found, expected[rowid])
            found.add(rowid)
            # check it gets exhausted
            while True:
                more = mlr(found, 3, None)
                for r in found:
                    self.assertNotIn(r, more)
                if more:
                    found.update(more)
                else:
                    break

        expected = {
            (2478625915, 0): (110396968, 418639071),
            (2478625915, 1): (110396968, 418639071),
            (1535409456, 0): (110396968, 418639071),
            (1535409456, 1): (110396968, 418639071),
            (2651692906, 0): (110396968, 418639071),
            (2651692906, 1): (110396968, 418639071),
            (418639071, 0): (110396968, 742136966),
            (418639071, 1): (110396968, 742136966),
        }

        for rowid in 2478625915, 1535409456, 2651692906, 418639071:
            for cols in (0, 1):
                found = sorted(mlr(rowid, 3, t.columns[cols]))[:2]
                self.assertEqual(tuple(found), expected[(rowid, cols)])
                t.supports_query_tokens = False

        # token cache and related
        before = {
            "token_count": t.token_count,
            "row_count": t.row_count,
            "tokens_per_column": t.tokens_per_column,
            "tokens": t.tokens,
        }
        # words that are the same as their tokens and deliberately do not already exist
        test_tokens = ("jkfhdskjfhdsjk", "kldsjfkldsjflkjds")
        self.assertTrue(all((token not in t.tokens and not t.is_token(token)) for token in test_tokens))
        rowid = t.upsert(*test_tokens)
        self.assertTrue(all((token in t.tokens and t.is_token(token)) for token in test_tokens))
        for k, v in before.items():
            self.assertNotEqual(v, getattr(t, k))
        t.delete(rowid)
        for k, v in before.items():
            self.assertEqual(v, getattr(t, k))

        # misc
        self.assertEqual(t.token_frequency(3), [("appleby", 46), ("humphrey", 46), ("the", 27)])
        self.assertEqual(t.token_doc_frequency(3), [("appleby", 23), ("humphrey", 23), ("lencyclopedie", 23)])

        # check nothing happened in main (temp has fts5vocab tables)
        self.assertIsNone(self.db.execute("select * from sqlite_schema").get)

    def testSimilarity(self):
        "closest tokens, query suggest"
        t = apsw.fts5.Table.create(
            self.db,
            "table",
            ["with a space", "Special", ",pIqaD", "t͡ɬɪ.ŋɑn xol]", "noindex"],
            unindexed=["noindex"],
            tokenize=["simplify", "casefold", "true", "strip", "true", "unicodewords"],
        )
        for row in (
            ("one", "Two", " thRee", "four"),
            ("trouble", "troubleD", "tribble", "tribbbles"),
            ("if something or no one",),
        ):
            t.upsert(*row)
        self.insert_content(t)

        # check text for token first
        self.assertEqual(t.text_for_token("troubled", 10), "troubleD")
        self.assertRaises(ValueError, t.text_for_token, "sdfsed", 10)
        self.assertEqual([tok[1] for tok in t.closest_tokens("zebra", cutoff=0, n=3)], ["break", "breaks", "ran"])
        # include known token
        self.assertEqual([tok[1] for tok in t.closest_tokens("break", cutoff=0, n=3)], ["breaks", "back", "re"])

        # some of these show scope for future improvements
        for query, expected in (
            ("noindex: hello", '",pIqaD": shell'),
            ("cial:troubled3", "Special: troubleD"),
            (
                "{xol orange}: (recomended OR rygestered)",
                '{"t͡ɬɪ.ŋɑn xol]" "with a space"}: (recommended OR registered)',
            ),
            ("specIal:one", None),
            ("one+two", None),
            ("thre* OR forer", "thre* OR for"),
            ("some thing noone", "some This no one"),  # Splitting such as (comment defeats spellcheck.sh)
            ("tribb bles", "tribbbles"),
            ("let ape", "let l’étape"),
            ('com mand hump hrey "sql ite"', 'column and humphrey "sql it"'),
            ('hello world "-" world hello', 'shell word "-" word shell'),
            # from debug history but a nice pathological case
            (
                '"pp any(isinstance(child, AND) for child in node.queries)"',
                '"appleby an(sentence(chained, AND) for chained in queries"',
            ),
        ):
            res: str = t.query_suggest(query) or t.query_suggest(query, 0)
            self.assertEqual(res, expected)
            # check query is valid
            list(t.search(res or query))


class FTS5Aux(unittest.TestCase):
    def setUp(self):
        self.db = apsw.Connection("")
        self.t = apsw.fts5.Table.create(self.db, "yes", ["one", "two"], tokenize=["unicode61"])
        for rowid, vals in test_content.items():
            self.t.upsert(*vals, rowid=rowid)

    def testBM25(self):
        # we just need to check that sqlite's bm25 matches ours

        apsw.fts5.register_functions(self.db, {"pybm25": apsw.fts5aux.bm25})

        for query in (
            "if trace input",
            '"as well" that',
            '"humphrey when" OR "humphrey this"',
            "kjdsfhjfhdsjkhfsd in",
            "jkhkjfsdhkjf sdkjhfjksdhfksd",
        ):
            for us, them in self.db.execute("select pybm25(yes), bm25(yes) from yes(?)", (query,)):
                self.assertAlmostEqual(us, them)

        self.assertTrue(
            all(0 == score for score in self.db.execute("select bm25(yes,0,0) from yes(?)", ("humphrey appleby",)).get)
        )
        self.assertTrue(
            all(0 == score for score in self.db.execute("select bm25(yes,0,1) from yes(?)", ("humphrey",)).get)
        )

    def testIdf(self):
        def myfunc(api):
            return json.dumps(apsw.fts5aux.inverse_document_frequency(api))

        apsw.fts5.register_functions(self.db, {"myfunc": myfunc, "MyFunc": myfunc})

        for query, expected in (
            ("the", [1e-06]),
            ("apsw", [0.990398704027877]),
            ('if the OR "if the"', [0.4228568508200336, 1e-06, 2.151762203259462]),
            ("jdsjflkdjsflkj OR type", [3.8501476017100584, 2.70805020110221]),
            ('html humphrey "humphrey if"', [2.70805020110221, 1e-06, 1.466337068793427]),
            (
                "(word AND can OR you NOT type OR two:example) OR the",
                [1.7676619176489945, 1.466337068793427, 0.6007738604289301, 2.70805020110221, 2.70805020110221, 1e-06],
            ),
        ):
            vals = json.loads(self.db.execute("select myfunc(yes) from yes(?) limit 1", (query,)).get)
            for x, y in zip(vals, expected):
                self.assertAlmostEqual(x, y)

    def testSubsequence(self):
        apsw.fts5.register_functions(self.db, {"subsequence": apsw.fts5aux.subsequence})

        for query, expected in (
            ("this", [(0.0, 1314334723), (0.0, 1967567003), (0.0, 2163134344)]),
            ('this OR "is this"', [(0.0, 1314334723), (0.0, 1967567003), (0.0, 2163134344)]),
            ("is this", [(0.39999999999999997, 3912225165), (0.0, 1967567003), (0.0, 2163134344)]),
            ("this is", [(2.0, 1967567003), (2.0, 2163134344), (2.0, 3630190717)]),
            ("humphrey this", [(2.0, 2163134344), (2.0, 2478625915), (0.2857142857142857, 3283224240)]),
            ("and and", [(0.6666666666666666, 110396968), (0.0, 742136966), (0.0, 1314334723)]),
        ):
            rows = self.db.execute(
                "select bm25(yes) - subsequence(yes) AS boost, rowid from yes(?) order by boost desc, rowid limit 3",
                (query,),
            ).get
            for (boost, rowid), (expected_boost, expected_rowid) in zip(rows, expected):
                self.assertAlmostEqual(boost, expected_boost)
                self.assertEqual(rowid, expected_rowid)

        self.assertEqual(
            (0, 1314334723),
            self.db.execute(
                "select bm25(yes,0,1) - subsequence(yes,0,1) as boost, rowid from yes(?) order by boost desc, rowid limit 1",
                # sequence only in column 0
                ("humphrey This",),
            ).get,
        )

    def testPositionRank(self):
        apsw.fts5.register_functions(self.db, {"pos": apsw.fts5aux.position_rank})

        for query, expected in (
            (
                "this",
                [
                    (0.2833333333333334, 3912225165),
                    (0.20000000000000004, 3630190717),
                    (0.20000000000000004, 3752288227),
                ],
            ),
            (
                "fkdsjflkds OR documents OR line",
                [
                    (0.09090909090909083, 2163134344),
                    (0.05882352941176472, 110396968),
                    (0.05555555555555536, 2478625915),
                ],
            ),
            (
                "this OR humphrey",
                [(0.8666666666666665, 3912225165), (0.7833333333333332, 1967567003), (0.7833333333333332, 2163134344)],
            ),
            (
                "tokens",
                [(0.3666666666666667, 3283224240), (0.2019230769230771, 3167611882), (0.12916666666666643, 1967567003)],
            ),
        ):
            rows = self.db.execute(
                "select bm25(yes) - pos(yes) AS boost, rowid from yes(?) order by boost desc, rowid limit 3",
                (query,),
            ).get
            for (boost, rowid), (expected_boost, expected_rowid) in zip(rows, expected):
                self.assertAlmostEqual(boost, expected_boost)
                self.assertEqual(rowid, expected_rowid)

        self.assertEqual(
            (0, 2691318995),
            self.db.execute(
                "select bm25(yes,0,1) - pos(yes,0,1) as boost, rowid from yes(?) order by boost desc, rowid limit 1",
                # phrases only in column 0
                ("humphrey beginning",),
            ).get,
        )


class Unicode(unittest.TestCase):
    # generated by python -m apsw.unicode breaktestgen
    break_tests = {
        "grapheme": (
            " \u200c",
            "a÷🇦🇧÷🇨🇩÷b",
            "\u1100\uac01",
            "\u0d4e ",
            "\u094d\u200d",
            "\u000d÷\u0904",
            "\u0001÷\u0378",
            "\u0600\u0308\u094d",
            "\u1160÷\uac01",
            "\uac01÷ ",
            "\u0904÷\u1160",
            "⌚\u0308\u0a03",
            "\u0900÷\u0d4e",
            "\u0378÷ ",
            "\u000a÷\u0308\u0900",
            "\u0600\u0308÷\u0d4e",
            "\u11a8\u0308÷\u000a",
            "\u0903\u0308÷\u11a8",
            "\u0915\u0308÷⌚",
            "\u094d\u0308÷🇦",
            "a\u200d÷✁",
            "\u0001÷\u0308÷\u0378",
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
            "⏩⏩",
            "En ÷gång ÷undföll ÷det ÷honom ÷dock\u002c ÷medan ÷han ÷släpade ÷på ÷det ÷våta ÷höet\u003a ÷\u00bbVarför ÷är ÷höet ÷redan ÷torrt ÷och ÷inkört ÷där ÷borta ÷på ÷Solbacken\u002c ÷och ÷här ÷hos ÷oss ÷är ÷det ÷vått\u003f\u00bb ÷— ÷\u00bbDärför ÷att ÷de ÷ha ÷oftare ÷sol ÷än ÷vi\u002e\u00bb",
            "\u002c0",
            "☝\u3005",
            "　÷\uac00",
            "\u2329\u0308\u0025",
            "$\u0308\u000d",
            "\u00b4\u0308$",
            "—÷\uac01",
            "\u0028\u0308\u0025",
            "\u3041÷￼",
            "\ufe19 ÷￼",
            "  ÷✊",
            "\u007d ÷￼",
            "᭐\u0308 \u000d",
            "\U00011003 ÷\u3005",
            "\u0023 ÷\u2024",
            "\uac00\u0308 ÷$",
            "\u0022\u0308 ÷\u05d0",
            "\u1b44\u0308 ÷\u0001",
            "\u0023\u0308 ÷\u3005",
            "\u54ea÷\u4e2a÷\u5546÷\u6807÷\u4ee5÷\u4eba÷\u540d÷\u4e3a÷\u540d\uff0c÷\u56e0÷\u7279÷\u8272÷\u5c0f÷\u5403÷\u201c\u4e94÷\u53f0÷\u6742÷\u70e9÷\u6c64\u201d÷\u800c÷\u5165÷\u9009÷\u201c\u65b0÷\u7586÷\u8001÷\u5b57÷\u53f7\u201d\uff1f",
        ),
    }

    def testBreaks(self):
        "Verifies breaktest locations"

        marker = "÷"
        for kind in "grapheme", "word", "sentence", "line_break":
            meth = getattr(apsw.unicode, f"{kind}_next_break")
            meth_next = getattr(apsw.unicode, f"{kind}_next")
            meth_iter_with_offsets = getattr(apsw.unicode, f"{kind}_iter_with_offsets")
            meth_iter = getattr(apsw.unicode, f"{kind}_iter")

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

    def testBreaksFull(self):
        "Tests full official break tests (if available)"

        testzip = extended_testing_file("UCD.zip")
        if not testzip:
            return

        with zipfile.ZipFile(testzip) as zip:
            for kind, base in (
                ("grapheme", "Grapheme"),
                ("word", "Word"),
                ("sentence", "Sentence"),
                ("line_break", "Line"),
            ):
                with tempfile.NamedTemporaryFile("wb", prefix=f"ftstestbreaks-{ kind }", suffix=".txt") as tmpf:
                    with zip.open(f"auxiliary/{base}BreakTest.txt") as src:
                        tmpf.write(src.read())
                    tmpf.flush()

                    proc = self.exec("breaktest", kind, tmpf.name)
                    self.assertEqual(proc.returncode, 0, f"Failed {proc=}")

    def exec(self, *args):
        if coverage_run:
            cov_params = ["-m", "coverage", "run", "--source", "apsw", "-p"]
            env = os.environ.copy()
            env["ASAN_OPTIONS"] = "detect_leaks=false"
        else:
            cov_params = []
            env = None

        return subprocess.run(
            [sys.executable] + cov_params + ["-m", "apsw.unicode"] + list(args), capture_output=True, env=env
        )

    def testCodepointNames(self):
        self.assertRaises(TypeError, apsw.unicode.codepoint_name)
        self.assertRaises(TypeError, apsw.unicode.codepoint_name, "hello")
        self.assertRaises(ValueError, apsw.unicode.codepoint_name, -2)
        self.assertRaises(ValueError, apsw.unicode.codepoint_name, sys.maxunicode + 1)

        for codepoint, name in (
            (sys.maxunicode, None),
            (0, None),
        ):
            self.assertEqual(apsw.unicode.codepoint_name(codepoint), name)
            self.assertEqual(apsw.unicode.codepoint_name(chr(codepoint)), name)

        testzip = extended_testing_file("UCD.zip")
        if not testzip:
            return

        tested = set()
        with zipfile.ZipFile(testzip) as zip:
            with zip.open("extracted/DerivedName.txt") as src:
                for line in src.read().decode().splitlines():
                    if not line or line.startswith("#"):
                        continue
                    line = line.split(";")
                    assert len(line) == 2
                    if ".." in line[0]:
                        start, end = (int(l.strip(), 16) for l in line[0].split(".."))
                    else:
                        start = end = int(line[0].strip(), 16)
                    name = line[1].strip()
                    for codepoint in range(start, end + 1):
                        if name.endswith("-*"):
                            expected = name[:-1] + f"{codepoint:04X}"
                        else:
                            expected = name
                        self.assertEqual(apsw.unicode.codepoint_name(codepoint), expected, f"{codepoint=:04X}")
                        self.assertNotIn(codepoint, tested)
                        tested.add(codepoint)

        for codepoint in range(0, sys.maxunicode + 1):
            if codepoint not in tested:
                self.assertIsNone(apsw.unicode.codepoint_name(codepoint), f"{codepoint=:04X}")

    def testCoverage(self):
        "Exhaustive codepoints for coverage"
        # this takes a while to run, so only do so if env variable set or debug
        # interpreter
        if "d" not in getattr(sys, "abiflags", "") and not coverage_run:
            return

        for codepoint in range(0, sys.maxunicode + 1):
            c = chr(codepoint)
            c10 = c * 10
            self.assertIsNotNone(apsw.unicode.strip(c))
            for n in "grapheme", "word", "sentence", "line_break":
                tuple(getattr(apsw.unicode, f"{n}_iter")(c10))
            # this catches the maxchar calulation being wrong and will give a C level assertion failure like
            # Objects/unicodeobject.c:621: _PyUnicode_CheckConsistency: Assertion failed: maxchar >= 128
            # it also reads the codepoints so will catch uninitialized memory
            sum(ord(c) for c in apsw.unicode.strip(c10) * 2)
            sum(ord(c) for c in apsw.unicode.casefold(c10) * 2)
            age = apsw.unicode.version_added(codepoint)
            if age is not None:
                self.assertIn(age, apsw.unicode.version_dates)

    def testCLI(self):
        "Exercise command line interface"
        text = ""
        for codepoints in self.cat_examples.values():
            # surrogates not allowed
            if 0xD800 in codepoints:
                continue
            # we skip null because it can't be used as a cli parameter
            text += "".join(chr(c) for c in codepoints if c)

        with BecauseWindowsTempfile("wt", encoding="utf8") as tmpf:
            tmpf.write_whole_file(text)

            for kind in "grapheme", "word", "sentence", "line_break":
                proc = self.exec("show", "--text-file", tmpf.name, kind)
                if proc.returncode != 0:
                    print(proc.stdout.decode(), file=sys.stderr)
                    print(proc.stderr.decode(), file=sys.stderr)
                self.assertEqual(proc.returncode, 0, f"Failed {proc=}")

            proc = self.exec("textwrap", "--guess-paragraphs", tmpf.name)
            self.assertEqual(proc.returncode, 0, f"Failed {proc=}")

            proc = self.exec("textwrap", "--use-stdlib", tmpf.name)
            self.assertEqual(proc.returncode, 0, f"Failed {proc=}")

            proc = self.exec("codepoint", text)
            self.assertEqual(proc.returncode, 0, f"Failed {proc=}")

            if coverage_run:
                proc = self.exec("benchmark", "--size", "0.1", "--others", "all", tmpf.name)
                self.assertEqual(proc.returncode, 0, f"Failed {proc=}")

    cat_examples = {
        # this features the lowest and highest codepoint for each category
        "Cc": (0x0000, 0x000D, 0x0019, 0x0084, 0x0090, 0x009C, 0x009F),
        "Cf": (0x00AD, 0x206B, 0x1D175, 0xE003A, 0xE005B, 0xE007C, 0xE007F),
        "Cn": (0x0378, 0x4ED75, 0x771C4, 0x9F613, 0xC7A62, 0x10FFFE, 0x10FFFF),
        "Co": (0xE000, 0xF5266, 0xFBDCB, 0x102932, 0x109497, 0x10FFFC, 0x10FFFD),
        "Cs": (0xD800, 0xD99A, 0xDB33, 0xDCCC, 0xDE65, 0xDFFE, 0xDFFF),
        "Ll": (0x0061, 0x048B, 0x1F52, 0xAB7C, 0x1D4F8, 0x1E942, 0x1E943),
        "Lm": (0x02B0, 0x1D3D, 0x1DBB, 0x10782, 0x1AFF5, 0x1E94B),
        "Lo": (0x00AA, 0x8C70, 0x17011, 0x24543, 0x2ACB1, 0x323AD, 0x323AF),
        "Lt": (0x01C5, 0x1F8A, 0x1F8F, 0x1F9C, 0x1FA9, 0x1FAE, 0x1FFC),
        "Lu": (0x0041, 0x04B0, 0x1EBE, 0xA7C9, 0x1D4DC, 0x1E91D, 0x1E921),
        "Mc": (0x0903, 0x0DD8, 0x1B40, 0x11182, 0x119E4, 0x1D172),
        "Me": (0x0488, 0x20DD, 0x20DF, 0x20E2, 0x20E4, 0xA671, 0xA672),
        "Mn": (0x0300, 0x0A51, 0x1CE6, 0x112EA, 0x1DA24, 0xE01EC, 0xE01EF),
        "Nd": (0x0030, 0x0DEC, 0x1C41, 0x110F6, 0x11F51, 0x1FBF6, 0x1FBF9),
        "Nl": (0x16EE, 0x3025, 0x1015C, 0x1240E, 0x1243C, 0x1246A, 0x1246E),
        "No": (0x00B2, 0x24EE, 0x102EF, 0x10E72, 0x1D2E7, 0x1F109, 0x1F10C),
        "Pc": (0x005F, 0x2040, 0x2054, 0xFE33, 0xFE34, 0xFE4D, 0xFE4E, 0xFE4F, 0xFF3F),
        "Pd": (0x002D, 0x2010, 0x2014, 0x2E3A, 0x301C, 0xFE32, 0x10EAD),
        "Pe": (0x0029, 0x2771, 0x2990, 0x2E5C, 0xFE38, 0xFF63),
        "Pf": (0x00BB, 0x201D, 0x203A, 0x2E03, 0x2E05, 0x2E0A, 0x2E0D, 0x2E1D, 0x2E21),
        "Pi": (0x00AB, 0x201C, 0x2039, 0x2E04, 0x2E0C, 0x2E20),
        "Po": (0x0021, 0x1363, 0x2CFC, 0xFE46, 0x114C6, 0x1E95E, 0x1E95F),
        "Ps": (0x0028, 0x276C, 0x298B, 0x2E55, 0xFE17, 0xFF5B, 0xFF62),
        "Sc": (0x0024, 0x0BF9, 0x20A9, 0x20B5, 0xA838, 0x1E2FF, 0x1ECB0),
        "Sk": (0x005E, 0x02E6, 0x0375, 0xA703, 0xAB5B, 0x1F3FC, 0x1F3FF),
        "Sm": (0x002B, 0x2286, 0x27D8, 0x29B5, 0x2A78, 0x1EEF0, 0x1EEF1),
        "So": (0x00A6, 0x285C, 0xA49A, 0x1D99C, 0x1F5D4, 0x1FBC8, 0x1FBCA),
        "Zl": (0x2028,),
        "Zp": (0x2029,),
        "Zs": (0x0020, 0x2001, 0x2004, 0x2007, 0x200A, 0x3000),
    }

    def all_text(self, seed=None):
        text = ""
        for values in self.break_tests.values():
            text += "".join(values)
        for codepoints in self.cat_examples.values():
            text += "".join(chr(cp) for cp in codepoints)
        if seed is not None:
            return "".join(random.Random(seed).sample(text, len(text)))
        return text

    def testCategory(self):
        "Category lookup"
        meth = apsw.unicode.category
        self.assertRaises(TypeError, meth, b"aaa")
        self.assertRaises(TypeError, meth)
        self.assertRaises(TypeError, meth, "one", 2)
        self.assertRaises(ValueError, meth, -1)
        self.assertRaises((ValueError, OverflowError), meth, sys.maxsize)
        self.assertRaises(ValueError, meth, sys.maxunicode + 1)
        self.assertRaises(TypeError, meth, "avbc")

        # check we cover 0 and sys.maxunicode
        self.assertEqual("Cc", meth(0))
        self.assertEqual("Cn", meth(sys.maxunicode))

        catcheck = apsw._unicode.has_category
        makemask = apsw.unicode._cats_to_mask

        for cat, codepoints in self.cat_examples.items():
            for codepoint in codepoints:
                self.assertEqual(cat, meth(codepoint))
                self.assertEqual(cat, meth(chr(codepoint)))
                for testcat in self.cat_examples:
                    text = chr(codepoint)
                    mask = makemask([testcat], False, False)
                    self.assertEqual(testcat == cat, catcheck(text, 0, 1, mask))

        self.assertRaises(TypeError, apsw.unicode.is_extended_pictographic, b"ddd")
        self.assertRaises(TypeError, apsw.unicode.is_extended_pictographic, (3,))
        self.assertFalse(apsw.unicode.is_extended_pictographic(""))
        self.assertFalse(apsw.unicode.is_extended_pictographic("abc"))
        self.assertTrue(apsw.unicode.is_extended_pictographic("a🤦🏼‍♂️bc"))
        self.assertFalse(apsw.unicode.is_extended_pictographic("a🇬🇧bc"))

        self.assertRaises(TypeError, apsw.unicode.is_regional_indicator, b"ddd")
        self.assertRaises(TypeError, apsw.unicode.is_regional_indicator, (3,))
        self.assertFalse(apsw.unicode.is_regional_indicator(""))
        self.assertFalse(apsw.unicode.is_regional_indicator("abc"))
        self.assertFalse(apsw.unicode.is_regional_indicator("a🤦🏼‍♂️bc"))
        self.assertTrue(apsw.unicode.is_regional_indicator("a🇬🇧bc"))

    def testStrip(self):
        "Stripping accents, marks, punctuation etc"
        meth = apsw.unicode.strip

        self.assertRaises(TypeError, meth, 3)
        self.assertRaises(TypeError, meth, None)
        self.assertRaises(TypeError, meth, b"abc")

        for text in "jkdfshfkjdsh 234324jh32 jkhjkhAkdsk".split():
            self.assertEqual(text, meth(text))
            self.assertEqual(id(text), id(meth(text)))

        # which categories are ok
        ok = {"Lu", "Ll", "Lo", "Nd", "Nl", "No", "Sc", "Sm", "So"}

        text = self.all_text()

        self.assertIn("a", meth(text))
        for c in meth(text):
            cat = apsw.unicode.category(c)
            self.assertIn(cat, ok)

        for source, expect in (
            # from the doc
            ("áççéñțś", "accents"),
            ("e.g.", "eg"),
            ("don't", "dont"),
            ("देवनागरी", "दवनगर"),
            ("Ⅲ", "III"),
            ("🄷🄴🄻🄻🄾", "HELLO"),
            ("", ""),
        ):
            res = meth(source)
            self.assertEqual(res, expect)
            for c in res:
                cat = apsw.unicode.category(c)
                self.assertIn(cat, ok)

            # should not change when given output as input
            self.assertEqual(expect, meth(expect))

    def testCaseFold(self):
        "Case folding"
        self.assertRaises(TypeError, apsw.unicode.casefold)
        self.assertRaises(TypeError, apsw.unicode.casefold, 3)
        self.assertRaises(TypeError, apsw.unicode.casefold, b"abd")

        self.assertEqual("", apsw.unicode.casefold(""))
        # check ascii
        text = "HelLLiol JKH093'';\n\098123Ulkdaf"
        # for <127 (ascii) casefold is same as lower
        self.assertEqual(apsw.unicode.casefold(text), text.lower())

        # unchanged text should have same id
        for text in "abcd ds fsd 3424 23sdvxc".split():
            self.assertEqual(id(text), id(apsw.unicode.casefold(text)))

        # examples from fts_test_strings
        for text, expected in (
            ("İstanbul", "i̇stanbul"),
            ("i̇stanbul", "i̇stanbul"),
            ("straße", "strasse"),
            ("STRASSE", "strasse"),
            ("ΣΧΟΛΗ", "σχολη"),
            ("σχολη", "σχολη"),
            ("İnci", "i̇nci"),
            ("ıncı", "ıncı"),
        ):
            self.assertEqual(expected, apsw.unicode.casefold(text))

        # some interesting codepoints that all change and potentially expand
        for text in (
            "212A 006B",
            "017F 0073",
            "00DB 00FB",
            "FF36 FF56",
            "00DF 0073 0073",
            "1E9E 0073 0073",
            "FB06 0073 0074",
            "012C 012D",
            "014E 014F",
            "FB16 057E 0576",
            "FB04 0066 0066 006C",
            "104D2 104FA",
            "1E921 1E943",
        ):
            conv = "".join(chr(int(c, 16)) for c in text.split())
            self.assertEqual(apsw.unicode.casefold(conv[0]), conv[1:])

    def testFinding(self):
        "grapheme aware startswith/endswith/find"
        zwj = "\u200d"
        bird = chr(0x1F426)
        fire = chr(0x1F525)
        ctilde = chr(0x303)

        sw = apsw.unicode.grapheme_startswith
        ew = apsw.unicode.grapheme_endswith
        fi = apsw.unicode.grapheme_find

        self.assertRaises(TypeError, sw, None, 3)
        self.assertRaises(TypeError, sw, "", 3)
        self.assertRaises(TypeError, sw, "", b"a")
        self.assertRaises(TypeError, ew, None, 3)
        self.assertRaises(TypeError, ew, "", 3)
        self.assertRaises(TypeError, ew, "", b"a")
        self.assertRaises(TypeError, fi, None, None, 3.2, 4.5)
        self.assertRaises(TypeError, fi, "", None, 3.2, 4.5)
        self.assertRaises(TypeError, fi, "", "", 3.2, 4.5)
        self.assertRaises(TypeError, fi, b"", None, 3.2, 4.5)
        self.assertRaises(TypeError, fi, "", b"", 3, 4)
        self.assertRaises(TypeError, fi, "", "", 1, 2, 3)

        # for simple strings check we get same answers as regular Python
        for haystack, needle in (
            # other combos
            ("abca", "a"),
            ("abca", "ab"),
            ("abca", "ca"),
            ("123456", "123456"),
            ("123456", "12345"),
            ("123456", "1234567"),
            ("aaaaa", "a"),
            ("aaaaa", "aa"),
            ("aaaaa", "aaa"),
            ("aaaaa", "aaaa"),
            ("aaaaa", "aaaaa"),
            ("aaaaa", "aab"),
            # all strings start and end with empty string
            ("", ""),
            ("abc", ""),
        ):
            self.assertEqual(haystack.startswith(needle), sw(haystack, needle))
            self.assertEqual(haystack.endswith(needle), ew(haystack, needle))
            for start in range(-10, 10):
                for end in range(-10, 10):
                    self.assertEqual(
                        haystack.find(needle, start, end),
                        fi(haystack, needle, start, end),
                        f"{haystack=} {needle=} {start=} {end=}",
                    )

        # now make multi-codepoint grapheme clusters
        self.assertFalse(sw(f"abc{zwj}", "abc"))
        self.assertFalse(sw(f"{bird}{zwj}{fire}{bird}", f"{bird}"))
        self.assertFalse(ew(f"abc{bird}{zwj}{fire}", f"{fire}"))
        self.assertFalse(sw(f"a{ctilde}b", "a"))
        self.assertFalse(sw(f"a{ctilde}b", "a"))
        self.assertFalse(sw(f"aa{ctilde}b", "aa"))
        self.assertTrue(ew(f"a{ctilde}b", "b"))
        self.assertTrue(ew(f"a{ctilde}bc", "bc"))
        self.assertEqual(3, fi(f"{bird}{zwj}{fire}{fire}", f"{fire}"))

    def testSubstr(self):
        "grapheme aware substr"
        su = apsw.unicode.grapheme_substr

        self.assertRaises(TypeError, su, b"")
        self.assertRaises(TypeError, su, 3)
        self.assertRaises(TypeError, su, "", 3.2)

        # normal text should give the same answers
        for text in ("", "0", "012345"):
            for start in range(-10, 10):
                for end in range(-10, 10):
                    self.assertEqual(text[start:end], su(text, start, end))

        zwj = "\u200d"
        bird = chr(0x1F426)
        fire = chr(0x1F525)
        ctilde = chr(0x303)

        for text, start, end, expected in (
            (f"{bird}{zwj}{fire}c{ctilde}", 1, 2, f"c{ctilde}"),
            (f"{bird}{zwj}{fire}c{ctilde}", -10, 20, f"{bird}{zwj}{fire}c{ctilde}"),
            (f"{bird}{zwj}{fire}c{ctilde}", -1, None, f"c{ctilde}"),
            ("\r\n", 1, None, ""),
            ("\r\n", 2, None, ""),
        ):
            self.assertEqual(expected, su(text, start, end))

    def testLength(self):
        "grapheme aware length"
        l = apsw.unicode.grapheme_length
        self.assertRaises(TypeError, l)
        self.assertRaises(TypeError, l, b"a")
        self.assertRaises(TypeError, l, "", 3.1)
        self.assertRaises(ValueError, l, "", -2)
        self.assertRaises(ValueError, l, " ", 7)

        zwj = "\u200d"
        bird = chr(0x1F426)
        fire = chr(0x1F525)
        ctilde = chr(0x303)

        for text, offset, expected in (
            ("", 0, 0),
            ("abc", 0, 3),
            ("abc", 1, 2),
            ("abc", 2, 1),
            ("abc", 3, 0),
            (f"a{ctilde}{bird}", 0, 2),
            (f"a{ctilde}{bird}{zwj}{fire}", 0, 2),
        ):
            self.assertEqual(l(text, offset), expected)

    def testWidths(self):
        "terminal column widths"
        # ::TODO:: shell output with U+00AD SOFT HYPHEN is coming out
        # wrong. select '­armee '; but cursory check of these methods
        # is fine.  Figure it out
        zwj = "\u200d"
        bird = chr(0x1F426)
        fire = chr(0x1F525)
        ctilde = chr(0x303)
        fitz = chr(0x1F3FB)
        tw = apsw.unicode.text_width
        self.assertRaises(TypeError, tw)
        self.assertRaises(TypeError, tw, b"aa")
        for text, expected in (
            ("a", 1),
            ("", 0),
            (f"a{ctilde}", 1),
            (f"aa{ctilde}", 2),
            (f"{bird}a{ctilde}", 3),
            (f"{bird}{zwj}{fire}a{ctilde}", 3),
            (f"{bird}{zwj}{bird}{zwj}{bird}", 2),
            (f"{bird}{fitz}", 2),
            ("abc\taa", -1),
            ("abd\rsss", -1),
            ("\u1100", 2),
            (f"\u1100{ctilde}", 2),
        ):
            self.assertEqual(expected, tw(text), repr(text))

        tws = apsw.unicode.text_width_substr
        self.assertRaises(ValueError, tws, "", 0, 0)
        self.assertRaises(ValueError, tws, "ab\rc", 4)
        text = f"{bird}{zwj}{fire}a{ctilde}"
        self.assertEqual((0, ""), tws(text, 1))
        self.assertEqual((2, f"{bird}{zwj}{fire}"), tws(text, 2))
        self.assertEqual((3, text), tws(text, 3))
        self.assertEqual((3, text), tws(text, 300))

    # !p! marks that as a paragraph - there shuld be exactly
    # one per guessed paragraph
    paragraph_test = """
one two         three !p! fhddsf hsd jksdh fsdhj fsd
lkjsdhjf jsdhjkf    hsdf jksdhf kjhsdkjfh sdk
sjkldhfjk sdhkfjhs     sdfjlksdj
  dsfhsdjk !p! dhsfhsd sdhjfh
  ldsfjlksdj       sdklfjsdf lksdjf
fkldsjf     jsdfjsdkljf !p! fkldjfklsdjflds
sadas

abc!p!d\u2029 !p!abc\u0085!p!def

        tabs !p!
\tcontinue of last
3. abdc !p!
   more
-  not !p!
.3- another !p!
*** more !p!
:4: again !p! yup
"""

    def testWrapping(self):
        "wrapping and related functionality"
        ctilde = chr(0x303)

        # line hard breaks
        breaks = ("\r\n", "\r", "\n", "\x0c", "\x0d", "\x85", chr(0x02028), chr(0x2029))

        text = "".join("%s%s" % z for z in zip((str(i) for i in range(20)), breaks))

        lines = list(apsw.unicode.split_lines(text))
        self.assertEqual(len(lines), len(breaks))

        # check the line break chars are not included
        for line in lines:
            self.assertEqual(len(line), 1)
            for break_ in breaks:
                self.assertNotIn(break_, line)

        # paragraph guessing
        for para in apsw.unicode.guess_paragraphs(self.paragraph_test).split("\n"):
            if not para.strip():
                continue
            self.assertIn("!p!", para)
            # should only be one
            self.assertEqual(2, len(para.split("!p!")), f"{para=}")

        # tab expanding
        for text in (
            "",
            "\t",
            "aaa\tbbbb",
            "\ta",
            "b\t",
            "\t\t\t\t\t",
            "a\t\tv\t\tb\t",
        ):
            for n in range(0, 5):
                self.assertEqual(apsw.unicode.expand_tabs(text, n), text.expandtabs(n))

        for text, expected in (
            (f"a{ctilde}\tb", f"a{ctilde}       b"),
            (f"a{ctilde}a{ctilde}\tb", f"a{ctilde}a{ctilde}      b"),
            ("÷🇦\x01÷ൎ\u0600̈ਃᅠ÷ᄀ각÷ ः÷각क÷각̀÷\u0378\u200d÷", "÷🇦.÷ൎ؀̈ਃᅠ÷ᄀ각÷ ः÷각क÷각̀÷.÷"),
        ):
            self.assertEqual(apsw.unicode.expand_tabs(text), expected)

        # Japanese text from https://sqlite.org/forum/forumpost/6e234df298bde5b6da613866e4ba4d79a453bd9a32a608828f5a2e07ba5215f4
        text = (
            self.paragraph_test
            + self.all_text()
            + self.all_text(0)
            + self.all_text(1)
            + """夜明~朝ごはんの歌朝の通学路馬鹿騒ぎ追憶\n\t"""
        )
        text = apsw.unicode.guess_paragraphs(text)

        for width in (1, 3, 5, 9, 17, 37, 49, 87, 247, 1024):
            for line in apsw.unicode.text_wrap(text, width):
                self.assertEqual(apsw.unicode.text_width(line), width)
            for line in apsw.unicode.text_wrap(text, width, combine_space=False):
                self.assertEqual(apsw.unicode.text_width(line), width)
            if width > 3:
                for line in apsw.unicode.text_wrap(text, width, combine_space=False, hyphen="--"):
                    self.assertEqual(apsw.unicode.text_width(line), width)

    def testUPM(self):
        "UTF8 Position Mapper"
        # it is not publicly documented or exposed
        import apsw._unicode

        cls = apsw._unicode.to_utf8_position_mapper

        # basic arg parsing
        self.assertRaises(TypeError, cls)
        self.assertRaises(TypeError, cls, "abc")
        self.assertRaises(TypeError, cls, b"abc", 3)
        test = cls(b"abc")
        self.assertRaises(TypeError, test)
        self.assertRaises(TypeError, test, 3.2)
        self.assertRaises(TypeError, test, 3, 4)
        self.assertRaises(ValueError, test, -1)

        # empty bytes
        test = cls(b"")
        self.assertEqual(test(0), 0)
        self.assertEqual(test.str, "")
        self.assertRaises(IndexError, test, 1)

        # one byte
        test = cls(b"a")
        self.assertEqual(test(0), 0)
        self.assertEqual(test(1), 1)
        self.assertEqual(test.str, "a")
        self.assertRaises(IndexError, test, 2)

        cls = apsw._unicode.from_utf8_position_mapper

        # basic arg parsing
        self.assertRaises(TypeError, cls)
        self.assertRaises(TypeError, cls, b"abc")
        self.assertRaises(TypeError, cls, "abc", 3)
        test = cls("abc")
        self.assertRaises(TypeError, test)
        self.assertRaises(TypeError, test, 3.2)
        self.assertRaises(TypeError, test, 3, 4)
        self.assertRaises(ValueError, test, -1)

        # empty string
        test = cls("")
        self.assertEqual(test(0), 0)
        self.assertEqual(test.bytes, b"")
        self.assertRaises(IndexError, test, 1)

        # one char
        test = cls("a")
        self.assertEqual(test(0), 0)
        self.assertEqual(test(1), 1)
        self.assertEqual(test.bytes, b"a")
        self.assertRaises(IndexError, test, 2)

        # invalid offsets - only 4 plus at end are ok
        test = cls(chr(0x7F) + chr(0x7FF) + chr(0xFFFF) + chr(0x10FFFF))
        ok = 0
        for offset in range(0, len(test.bytes) + 1):
            try:
                test(offset)
                ok += 1
            except ValueError:
                pass
        self.assertEqual(ok, 5)

        # some random testing
        def xchr(howmany):
            while howmany:
                c = random.randint(0, sys.maxunicode)
                if 0xD800 <= c <= 0xDFFF:
                    # surrogates not allowed
                    continue
                yield chr(c)
                howmany -= 1

        for seed in range(10):
            random.seed(seed)
            # str offset to utf8 offset
            offsets: list[tuple[int, int]] = []
            utf8 = b""
            string = ""

            for i, c in enumerate(xchr(random.randint(0, 10_000))):
                offsets.append((i, len(utf8)))
                utf8 += c.encode("utf8")
                string += c

            # check end indexing
            offsets.append((len(offsets), len(utf8)))
            random.shuffle(offsets)

            from_utf8 = apsw._unicode.from_utf8_position_mapper(string)
            to_utf8 = apsw._unicode.to_utf8_position_mapper(utf8)

            self.assertEqual(from_utf8.bytes.decode("utf8"), to_utf8.str)
            self.assertEqual(from_utf8.bytes, to_utf8.str.encode("utf8"))

            for str_offset, utf8_offset in offsets:
                self.assertEqual(from_utf8(utf8_offset), str_offset)
                self.assertEqual(to_utf8(str_offset), utf8_offset)


class FTS5Query(unittest.TestCase):
    def setUp(self):
        self.db = apsw.Connection("")
        self.db.execute('create virtual table search using fts5(one, [two space], "three""quote", ":", "{")')
        self.table = apsw.fts5.Table(self.db, "search")

    def tearDown(self):
        self.db.close()
        del self.db

    def testQueryTokens(self):
        "Query tokens"
        qt = apsw.fts5query.QueryTokens
        for tokens in (
            ["one"],
            ["one", "two"],
            [""],
            ["\0"],
            ["one", ("1st", "first"), "two"],
            ["日本語", "Tiếng Việt"],
        ):
            encoded_tokens = qt(tokens).encode()
            decoded = qt.decode(encoded_tokens)
            self.assertEqual(tokens, decoded.tokens)
            # as bytes
            self.assertEqual(tokens, qt.decode(encoded_tokens.encode()).tokens)
            self.assertIsNone(qt.decode("@" + encoded_tokens))

        apsw.fts5.register_tokenizers(self.db, apsw.fts5.map_tokenizers)

        table_with = apsw.fts5.Table.create(self.db, "with", columns=["1"], support_query_tokens=True)
        table_without = apsw.fts5.Table.create(self.db, "without", columns=["1"], support_query_tokens=False)

        self.assertTrue(table_with.supports_query_tokens)
        self.assertFalse(table_without.supports_query_tokens)

        test_tokens = ["!%$#", ("1'st", "first"), "don't"]
        encoded_tokens = qt(test_tokens).encode()
        doc = table_with.tokenize(
            encoded_tokens.encode("utf8"), apsw.FTS5_TOKENIZE_DOCUMENT, include_offsets=False, include_colocated=True
        )
        self.assertEqual(doc, [("tokens",), ("1",), ("st",), ("first",), ("don",), ("t",)])
        query = table_with.tokenize(
            encoded_tokens.encode("utf8"), apsw.FTS5_TOKENIZE_QUERY, include_offsets=False, include_colocated=True
        )
        self.assertEqual(query, [("!%$#",), ("1'st", "first"), ("don't",)])
        without_query = table_without.tokenize(
            encoded_tokens.encode("utf8"), apsw.FTS5_TOKENIZE_QUERY, include_offsets=False, include_colocated=True
        )
        self.assertEqual(without_query, doc)

    def testParsing(self):
        "Conversion between query strings, dataclasses, and dicts"
        q = apsw.fts5query.quote
        qt = apsw.fts5query.QueryTokens(["one", ("first", "1st"), "two"])
        c = self.table.structure.columns
        for query in f"""
            # from the doc
            colname : NEAR("one two" "three four", 10)
            "colname" : one + two + three
            {{col1 col2}} : NEAR("one two" "three four", 10)
            {{col2 col1 col3}} : one + two + three
            - colname : NEAR("one two" "three four", 10)
            - {{col2 col1 col3}} : one + two + three
            {{a b}} : ( {{b c}} : "hello" AND "world" )
            (b : "hello") AND ({{a b}} : "world")
            b : (uvw AND xyz)
            a : xyz
            # ones I used during dev
            NEAR(one two three)
            (one three)
            (one two) AND three NOT four OR five
            (one two) AND (three NOT four) OR five
            # be nasty
            {q(c[2])}: hello {q(c[3])}
            # NEAR is only if followed by (
            NEAR AND hello
            # query tokens
            {q(qt)}
            - {q(qt)} : {q(qt)}
            # coverage
            ^one+two*+three
            ^one two* three
            (((^one*)))
            NEAR(one, 77) OR two
            one OR (two three NOT four)
            (one OR two) AND three
            (one OR two) NOT (three AND four)
            "" NOT "" OR "" AND "" "" ^""
            one:(two) AND three
            """.splitlines():
            query = query.strip()
            if not query or query.startswith("#"):
                continue
            # transform from query-string to parsed to dict to parsed
            # to query-string and ensure all the conversions match
            parsed = apsw.fts5query.parse_query_string(query)
            as_dict = apsw.fts5query.to_dict(parsed)
            from_dict = apsw.fts5query.from_dict(as_dict)
            self.assertEqual(parsed, from_dict)
            as_query = apsw.fts5query.to_query_string(from_dict)
            # we can't compare query strings because white space, NEAR
            # defaults, parentheses, optional AND etc will change
            self.assertEqual(parsed, apsw.fts5query.parse_query_string(as_query))

        # these are queries where the left and right must parse the same as each other
        # despite different syntax sugar
        for row in """
                one:two AND three  ==   one:(two) AND three
                ((((one))))        ==   one
                (one) OR (two)     ==   one OR two
                one two AND (three) OR four  == one two AND three OR four
                one:((((two))))    ==   one:two
            """.splitlines():
            if not row.strip():
                continue
            left, right = (s.strip() for s in row.split("=="))
            left_parsed = apsw.fts5query.parse_query_string(left)
            right_parsed = apsw.fts5query.parse_query_string(right)
            self.assertEqual(left_parsed, right_parsed, f"{left=} {right=}")

        for dict_query in (
            "hello",
            ("hello", "world"),
            {"one", "two"},
            {"one"},
            qt,
            {"@": "PHRASE", "initial": True, "prefix": False, "phrase": "hello world!"},
            {"@": "AND", "queries": {"hello", "world"}},
            {"@": "AND", "queries": {"hello"}},
            {"@": "NOT", "match": "hello", "no_match": "world"},
            {"@": "COLUMNFILTER", "columns": ['"', ":"], "filter": "include", "query": "hello"},
            {"@": "COLUMNFILTER", "columns": ['"', ":"], "filter": "include", "query": ("hello", "world")},
        ):
            from_dict = apsw.fts5query.from_dict(dict_query)
            as_query = apsw.fts5query.to_query_string(from_dict)
            parsed = apsw.fts5query.parse_query_string(as_query)
            self.assertEqual(from_dict, parsed)

    def testParseErrors(self):
        "Verify error queries are detected as such"
        for query in """
            + one
            one + + two
            one OR + two
            one two +
            NEAR (one, "10")
            NEAR(,10)
            NEAR()
            NEAR(one,
            NEAR(one
            NEAR(one, 10()
            (((^one))) two
            -one
            -one:
            one:(two) three
            "
            " ""
            "NEAR"(one)
            :
            one:two:three
            one . two
            one + .
            {
            {}
            - NEAR (
            {one :
            {one}:-
            ((one) two)
            (((one OR two))
        """.splitlines():
            # we don't skip blank lines because they are also a parse error
            self.assertRaises(apsw.fts5query.ParseError, apsw.fts5query.parse_query_string, query)

    def testErrors(self):
        "General invalid values and types"
        self.assertRaises(TypeError, apsw.fts5query.to_dict, "hello")
        self.assertRaises(TypeError, apsw.fts5query.to_dict, None)
        for q in (
            3 + 4j,
            3.0,
            {"@": "PHRASE", "initial": 7, "phrase": ""},
            {"@": "PHRASE", "initial": True, "phrase": 3},
            {"@": "NEAR", "phrases": ["hello"], "distance": "near"},
            {"hello", 3},
        ):
            self.assertRaises(TypeError, apsw.fts5query.from_dict, q)

        for q in (
            dict(),
            set(),
            list(),
            tuple(),
            {"@": "hello"},
            {"@": "AND", "queries": []},
            {"@": "OR", "queries": []},
            {"@": "AND", "queries": set()},
            {"@": "NEAR", "phrases": set()},
            {"@": "NEAR", "phrases": ("hello",), "distance": -2},
            {"@": "NOT", "match": "hello"},
            {"@": "NOT", "no_match": "hello"},
            {"@": "COLUMNFILTER", "columns": ['"', ":"], "filter": "ZZinclude", "query": ("hello", "world")},
            {"@": "COLUMNFILTER", "columns": ['"', ":"], "filter": "include", "ZZquery": ("hello", "world")},
            {"@": "COLUMNFILTER", "columns": [], "filter": "include", "query": ("hello", "world")},
            {"@": "COLUMNFILTER", "columns": ["one", 2], "filter": "include", "query": ("hello", "world")},
        ):
            self.assertRaises(ValueError, apsw.fts5query.from_dict, q)

        self.assertRaises(TypeError, apsw.fts5query.to_query_string, "not a dataclass")

    def testWalk(self):
        "walk and related functions"
        if sys.version_info >= (3, 10):
            # py 3.9 is unable to type check and give a nicer error
            self.assertRaises(TypeError, list, apsw.fts5query.walk({1: 2}))
            self.assertRaises(TypeError, apsw.fts5query.extract_with_column_filters, {1: 2}, 1)
            self.assertRaises(TypeError, apsw.fts5query.applicable_columns, {1: 2}, 2, 3)

        def n(v):
            # turns instance into class basename
            if isinstance(v, typing.Sequence):
                return tuple(n(vv) for vv in v)
            return type(v).__name__

        for qs, expected in {
            "one": (((), "PHRASE"),),
            "one AND (two OR three NOT ^four+five*+six* OR -{col1 col2}: ^yes) OR NEAR(nine+ten, 11)": (
                ((), "OR"),
                (("OR",), "AND"),
                (("OR", "AND"), "PHRASE"),
                (("OR", "AND"), "OR"),
                (("OR", "AND", "OR"), "OR"),
                (("OR", "AND", "OR", "OR"), "PHRASE"),
                (("OR", "AND", "OR", "OR"), "NOT"),
                (("OR", "AND", "OR", "OR", "NOT"), "PHRASE"),
                (("OR", "AND", "OR", "OR", "NOT"), "PHRASE"),
                (("OR", "AND", "OR"), "COLUMNFILTER"),
                (("OR", "AND", "OR", "COLUMNFILTER"), "PHRASE"),
                (("OR",), "NEAR"),
                (("OR", "NEAR"), "PHRASE"),
            ),
        }.items():
            parsed = apsw.fts5query.parse_query_string(qs)
            walked = []
            for parent, node in apsw.fts5query.walk(parsed):
                walked.append((n(parent), n(node)))
            self.assertEqual(tuple(walked), expected)

        # we use the NEAR as the node
        query = "one AND {cola 🤦🏼‍♂️ አማርኛ}:({cold}: string AND -🤦🏼‍♂️:NEAR(seven))"
        parsed = apsw.fts5query.parse_query_string(query)
        self.assertIsInstance(parsed, apsw.fts5query.AND)

        target = None
        for parent, node in apsw.fts5query.walk(parsed):
            if isinstance(node, apsw.fts5query.NEAR):
                assert target is None
                target = node
        self.assertIsInstance(target, apsw.fts5query.NEAR)

        self.assertRaises(ValueError, apsw.fts5query.extract_with_column_filters, apsw.fts5query.AND([]), parsed)
        self.assertRaises(ValueError, apsw.fts5query.applicable_columns, apsw.fts5query.AND([]), parsed, ["one"])

        extracted = apsw.fts5query.extract_with_column_filters(target, parsed)
        self.assertEqual(
            apsw.fts5query.to_dict(extracted),
            {
                "@": "COLUMNFILTER",
                "query": {
                    "@": "COLUMNFILTER",
                    "query": {"@": "NEAR", "phrases": [{"@": "PHRASE", "phrase": "seven"}]},
                    "columns": ["🤦🏼‍♂️"],
                    "filter": "exclude",
                },
                "columns": ["cola", "🤦🏼‍♂️", "አማርኛ"],
                "filter": "include",
            },
        )

        self.assertRaises(KeyError, apsw.fts5query.applicable_columns, target, parsed, ["አማርኛ"])

        applicable_columns = apsw.fts5query.applicable_columns(
            target, parsed, ("col0", "COLa", "አማርኛ", "🤦🏼‍♂️", "CoLc", "cold", "cole")
        )
        self.assertEqual(applicable_columns, {"COLa", "አማርኛ"})


def extended_testing_file(name: str) -> typing.Union[pathlib.Path, None]:
    "Returns path if found"

    # bigger data files used for testing are not shipped with apsw or part
    # of the repository but will be used for testing if present.  They
    # must be in a directory named apsw-extended-testing alongside the
    # apsw source directory.

    # this is for documentation purposes
    sources = {
        "UCD.zip": {
            "description": "Unicode codes databases",
            "url": "https://www.unicode.org/Public/UCD/latest/ucd/UCD.zip",
        },
        # https://opendata.stackexchange.com/a/17386
        # https://github.com/fictivekin/openrecipes
        "20170107-061401-recipeitems.json.gz": {
            "description": "Recipes",
            "url": "https://s3.amazonaws.com/openrecipes/20170107-061401-recipeitems.json.gz",
        },
    }

    if name not in sources:
        # make it a fatal error to give an unknown name
        sys.exit(f"unknown source { name= }")

    location = pathlib.Path(__file__).parent.parent.parent / "apsw-extended-testing" / name

    return location if location.exists() else None


has_fts5 = "ENABLE_FTS5" in apsw.compile_options

if not has_fts5:
    del FTS
    del FTS5Query
    del FTS5Table

__all__ = ("Unicode",) + (
    (
        "FTS",
        "FTS5Query",
        "FTS5Table",
    )
    if has_fts5
    else tuple()
)

if __name__ == "__main__":
    unittest.main()
