#!/usr/bin/env python3

"Various classes and functions to work with full text search"

from __future__ import annotations

import unicodedata
import pathlib

import apsw

unicode_categories = {
    "Lu": "Letter Uppercase",
    "Ll": "Letter Lowercase",
    "Lt": "Letter titlecase",
    "Lm": "Letter modifier",
    "Lo": "Letter other",
    "Mn": "Mark nonspacing",
    "Mc": "Mark spacing combining",
    "Me": "Mark enclosing",
    "Nd": "Number decimal digit",
    "Nl": "Number letter",
    "No": "Number other",
    "Pc": "Punctuation connector",
    "Pd": "Punctuation dash",
    "Ps": "Punctuation open",
    "Pe": "Punctuation close",
    "Pi": "Punctuation initial quote",
    "Pf": "Punctuation final quote",
    "Po": "Punctuation other",
    "Sm": "Symbol math",
    "Sc": "Symbol currency",
    "Sk": "Symbol modifier",
    "So": "Symbol other",
    "Zs": "Separator space",
    "Zl": "Separator line",
    "Zp": "Separator paragraph",
    "Cc": "Other control",
    "Cf": "Other format",
    "Cs": "Other surrogate",
    "Co": "Other private use",
    "Cn": "Other not assigned",
}
"Unicode categories and descriptions for reference"


def tokenizer_test_strings(
    filename: str = None, forms: tuple[str, ...] | None = ("NFC", "NFKC", "NFD", "NFKD")
) -> tuple[tuple[bytes, str], ...]:
    """Provides utf-8 bytes sequences for interesting test strings

    :param filename: File to load.  If None then the builtin on is used
    :param forms: What :func:`normalized <unicodedata.normalize>` forms to also include

    :returns: A tuple where each item is a tuple of utf8 bytes and comment str
    """
    # importlib.resources should be used, but has deprecation galore, and
    # bad python version compatibility
    filename = filename or pathlib.Path(__file__).with_name("fts_test_strings")
    test_strings: list[str] = []
    with open(filename, "rt", encoding="utf8") as f:
        lines = [line for line in f.readlines() if not line.startswith("##")]
        while lines:
            if not lines[0].startswith("#"):
                raise ValueError(f"Expected line to start with # - got { lines[0] }")
            comment = lines.pop(0)[1:].strip()
            text: list[str] = []
            while lines and not lines[0].startswith("#"):
                text.append(lines.pop(0))
            test_strings.append(("".join(text).rstrip("\n"), comment))

    seen: list[tuple[str, str]] = []
    for s in test_strings:
        seen.append(s)
        for form in forms or []:
            formed = (unicodedata.normalize(form, s[0]), s[1])
            if formed not in seen:
                seen.append(formed)
    return tuple((x[0].encode("utf8"), x[1]) for x in seen)


def UnicodeCategorySegmenter(
    utf8: bytes, categories: str, tokens: str = "", separators: str = ""
) -> Iterator[tuple[int, int, str]]:
    """Yields tokens from the UTF8 as a tuple of (start, end, token) using :func:`Unicode categories <unicodedata.category>`

    start is the start byte offset of the token in the utf8.
    end is the byte offset of the first byte not in token.
    token is the token text.

    :param categories: Which Unicode categories to consider part of tokens.  Can include wildcards.
        For example :code:`L* N* Pc` is all letters, numbers and punctuation connectors
    :param tokens: Any characters in tokens are considered part of the token, no matter what category
       they are in
    :param separators: Any characters in separators are considered not part of tokens, no matter
       what category they are in

    This method is useful for building your own tokenizer.
    """
    # Figure out categories expanding wild cards
    cats = set()
    for cat in categories.split():
        if cat in unicode_categories:
            cats.add(cat)
            continue
        found = set(n for n in unicode_categories if fnmatch.fnmatchcase(n, cat))
        if not found:
            raise ValueError(f"'{ cat }' doesn't match any Unicode categories")
        cats.update(found)

    # check overrides
    tokens: set[str] = set(tokens)
    separators: set[str] = set(separators)
    if tokens & separators:
        raise ValueError(f"Codepoints are in both tokens and separators { tokens & separators }")

    # extracting loop, codepoint at a time
    i = 0
    start = None
    token = ""
    while i < len(utf8):
        b = utf8[i]
        if b & 0b1111_0000 == 0b1111_0000:
            l = 4
        elif b & 0b1110_0000 == 0b1110_0000:
            l = 3
        elif b & 0b1100_0000 == 0b1100_0000:
            l = 2
        else:
            l = 1
        codepoint = utf8[i : i + l].decode("utf8")
        if (codepoint in tokens or unicodedata.category(codepoint) in cats) and codepoint not in separators:
            if not token:
                start = i
            token += codepoint
            i += l
            continue
        if token:
            yield start, i, token
            token = ""
            start = None
        i += l
    if token:
        yield start, i, token


def RegularExpressionSegmenter(utf8: bytes, pattern: str, flags: int = 0) -> Iterator[tuple[int, int, str]]:
    """Yields tokens from the UTF8 as a tuple of (start, end, token) using :mod:`regular expressions <re>`

    start is the start byte offset of the token in the utf8.
    end is the byte offset of the first byte not in token.
    token is the token text.

    :param pattern: The `regular expression <https://docs.python.org/3/library/re.html#regular-expression-syntax>`__.
        For example :code:`\\\\w+` is all alphanumeric and underscore characters.
    :param flags: `Regular expression flags <https://docs.python.org/3/library/re.html#flags>`__

    This method is useful for building your own tokenizer.
    """

    text = utf8.decode("utf8")
    for match in re.finditer(pattern, text, flags):
        # positions in str
        s = match.start()
        # now utf8 bytes
        s = len(text[:s].encode("utf8"))
        token = match.group()
        yield s, s + len(token.encode("utf8")), token
class FTS5Table:
    """A helpful wrapper around a FTS5 table  !!! Current experiment & thinking

    The table must already exist.  You can use the class method
    :meth:`create` to create a new FTS5 table.
    """

    def __init__(self, db: apsw.Connection, name: str, schema: str = "main"):
        self.db = db
        self.name = name
        self.schema = schema
        # ::TODO:: figure out if name and schema need quoting and generate
        # self.qname self.qschema
        pass

    @functools.cached_property
    def columns(self) -> tuple[str]:
        "Columns of this table"
        return self.db.execute("select name from { self.schema }.table_info({ self.name })").get

    def query(self, query: str) -> apsw.Cursor:
        "Returns a cursor making the query"
        return self.db.execute("select rowid, rank, * from { self.schema }.{ self.name }(?)", (query,))

    def insert(self, *args: str, **kwargs: str) -> None:
        """Does insert with columns by positional or named via kwargs

        * Uses normalize option on all values
        * auto-stringize each value too?
        """
        ...

    # some method helpers pattern, not including all of them yet

    def delete(self, *args):
        "https://www.sqlite.org/fts5.html#the_delete_command"
        pass

    def delete_all(self, *args):
        "https://www.sqlite.org/fts5.html#the_delete_all_command"
        pass

    def optimize(self):
        "https://www.sqlite.org/fts5.html#the_optimize_command"
        pass

    def pgsz(self, val:int):
        "https://www.sqlite.org/fts5.html#the_pgsz_configuration_option"
        pass

    def rebuild(self):
        "https://www.sqlite.org/fts5.html#the_rebuild_command"
        pass


    def config(self, name, value=None):
        "https://www.sqlite.org/fts5.html#configuration_options_config_table_"
        # check on forum
        # can we store our own stuff here, eg unicodedata version
        # so if it differs then can run rebuild
        # normalize val so insert normalizes all strings
        # perhaps x-apsw prefix?
        pass

    @functools.cache
    def fts5vocab_name(self, type: Literal["row"] | Literal["col"] | Literal["instance"]) -> str:
        """
        Creates fts5vocab table in temp and returns name
        """
        name = f"temp.fts5vocab_{ self.schema }_{ self.name }_{ type }"

        # need a "IF NOT EXISTS" somewhere in there
        self.db.execute(f"create virtual table { name }(?, ?, ?)", (self.schema, self.name, type))
        return name


    @classmethod
    def create(
        cls,
        db: apsw.Connection,
        name: str,
        columns: list[str],
        *,
        schema: str = "main",
        tokenizer: list[str] | None = None,
        prefix: str | None = None,
        content: str | None = None,
        contentless_delete: int | None = None,
        content_rowid: str | None = None,
        generate_triggers: bool = False,
    ) -> FTS5Table:
        """Creates the table

        Various kwargs same as `FTS5 options <https://www.sqlite.org/fts5.html#fts5_table_creation_and_initialization>`__

        :param generate_triggers: As in https://www.sqlite.org/fts5.html#external_content_tables

        If making an external content table then run :meth:`rebuild` after this create.
        """
        # ... make table, having fun quoting tokenizer etc
        return cls(db, name, schema)


if __name__ == "__main__":
    import html
    import argparse
    import importlib
    from dataclasses import dataclass

    def show_tokenization(tok: apsw.FTS5Tokenizer, utf8: bytes, reason: int, args: list[str] = []) -> str:
        """Runs the tokenizer and produces a html fragment showing the results for manual inspection"""

        offset: int = 0

        @dataclass
        class Row:
            start: int
            end: int
            utf8: bytes
            token_num: int | None = None
            token: str | None = None

        seq: list[Row | str] = []
        for toknum, row in enumerate(tok(utf8, reason, args)):
            start, end, *tokens = row
            if end < start:
                seq.append(show_tokenization_remark(f"start { start } is after end { end }", "error"))
            if start < offset:
                seq.append(
                    show_tokenization_remark(f"start { start } is before end of previous item { offset }", "error")
                )
            if start > offset:
                # white space
                seq.append(Row(start=offset, end=start, utf8=utf8[offset:start]))
            for t in tokens:
                seq.append(Row(start=start, end=end, utf8=utf8[start:end], token_num=toknum, token=t))
            offset = end

        if offset < len(utf8):
            # trailing white space
            seq.append(Row(start=offset, end=len(utf8), utf8=utf8[offset:]))

        # Generate html

        def ud(c: str) -> str:
            r = ""
            gc = unicodedata.category(c)
            explain = unicode_categories
            r += f"{ gc } { explain[gc] }"
            for meth in (
                unicodedata.bidirectional,
                unicodedata.combining,
                unicodedata.east_asian_width,
                unicodedata.mirrored,
            ):
                v = meth(c)
                if v and v != "Na":  # Na is east asian width non-applicable
                    r += f" { meth.__name__ }={ v }"
            return r

        def hex_utf8_bytes(utf8: bytes) -> str:
            codepoints = []
            i = 0
            while i < len(utf8):
                b = utf8[i]
                if b & 0b1111_0000 == 0b1111_0000:
                    codepoints.append(utf8[i : i + 4])
                elif b & 0b1110_0000 == 0b1110_0000:
                    codepoints.append(utf8[i : i + 3])
                elif b & 0b1100_0000 == 0b1100_0000:
                    codepoints.append(utf8[i : i + 2])
                else:
                    codepoints.append(utf8[i : i + 1])
                i += len(codepoints[-1])

            res = []
            for seq in codepoints:
                res.append("<span class=codepbytes>" + "&thinsp;".join("%02x" % x for x in seq) + "</span>")

            return " ".join(res)

        def byte_codepoints(b: bytes | str, open="{", close="}") -> str:
            if isinstance(b, bytes):
                b = b.decode("utf8", "replace")
            return "<wbr>".join(
                f"<span class=codepoint title='{ html.escape(ud(c), True) }'>"
                f"{ open}{ html.escape(unicodedata.name(c, 'UNKNOWN')) }{ close }"
                "</span>"
                for c in b
            )

        out = ""
        for row in seq:
            if row.token is None:  # space
                out += "<tr class='not-token'>"
                # token num
                out += "<td></td>"
                # start
                out += f"<td>{ row.start }</td>"
                # end
                out += f"<td>{ row.end }</td>"
                # bytes
                out += f"<td>{ hex_utf8_bytes(row.utf8) }</td>"
                # bytes val
                out += f"<td>{ html.escape(row.utf8.decode('utf8', 'replace')) }</td>"
                # token
                out += "<td></td>"
                # bytes codepoints - already escaped
                out += f"<td>{ byte_codepoints(row.utf8) }</td>"
                # token codepoints
                out += "<td></td>"
                out += "</tr>\n"
                continue

            out += "<tr class='token'>"
            # token num
            out += f"<td>{ row.token_num }</td>"
            # start
            out += f"<td>{ row.start }</td>"
            # end
            out += f"<td>{ row.end }</td>"
            # bytes
            out += f"<td>{ hex_utf8_bytes(row.utf8) }</td>"
            # bytes val
            out += f"<td>{ html.escape(row.utf8.decode('utf8', 'replace')) }</td>"
            # token
            out += f"<td>{ html.escape(row.token) }</td>"
            # bytes codepoints - already escaped
            out += f"<td>{ byte_codepoints(row.utf8) }</td>"
            # token codepoints - already escaped
            out += f"<td>{ byte_codepoints(row.token) }</td>"
            out += "</tr>\n"

        return out

    # column tips
    ct = [
        "Token number or blank for non-token area",
        "Start byte offset into the utf8 buffer",
        "End byte offset into the utf8 buffer.  This points to the next byte after the token.  End - Start should equal the token length",
        "Hex of the bytes with those making up\neach codepoint alternately underlined",
        "Decoded text from the bytes",
        "Token that was returned",
        "Each codepoint from the bytes",
        "Each codepoint from the token",
    ]

    show_tokenization_header = f"""<table class='tokenization-results'><thead><tr>
            <th title='{ ct[0] }'>#</th>
            <th title='{ ct[1] }'>Start</th>
            <th title='{ ct[2] }'>End</th>
            <th title='{ ct[3] }'>Hex</th>
            <th title='{ ct[4] }'>Bytes</th>
            <th title='{ ct[5] }'>Token</th>
            <th title='{ ct[6] }'>Bytes codepoints</th>
            <th title='{ ct[7] }'>Token codepoints</th
            ></tr></thead><tbody>"""
    show_tokenization_footer = """</tbody></table><details class=infobox><summary>Tips</summary>
    <ul><li>Hover over column headers to get descriptions<li>Hover over codepoints to get category and other information
    <li>You can resize columns from the bottom right of each header cell
    </ul><h3><a href="https://www.unicode.org/reports/tr15/">Normal forms</a></h3><dl>
    <p>Text that <i>looks</i> the same can be represented by different sequences of codepoints, for historical and
    compatibility reasons.  Those different sequences then encode to different sequences of bytes
    and will be considered different tokens, not matching in queries.</p>
    <dt>NFD</dt><dd>Canonical Decomposition breaking codepoints into multiples, so \u212B {ANGSTROM SIGN}
    becomes A {LATIN CAPITAL LETTER A} and {COMBINING RING ABOVE}</dd>
    <dt>NFC</dt><dd>Canonical Composition combining multiple codepoints into one, so \u0043 {LATIN CAPITAL
    LETTER C} and \u0327 {COMBINING CEDILLA} become \u00C7 {LATIN CAPITAL LETTER C WITH CEDILLA}.</dd>
    <dt>NFKD</dt><dd>Compatibility decomposition like NFD but codepoints become compatibility equivalents,
    so 2\u2075 become 2 5, and \u2160 {ROMAN NUMERAL ONE} becomes I (LATIN CAPITAL LETTER I).</dd>
    <dt>NFKC</dt><dd>Compatibility composition like NFC but codepoints become compatibility equivalents.</dd>
    </dl></details>"""
    show_tokenization_css = """
    <style>

    html {
        scroll-padding-top: 100px;
    }

    table.tokenization-results thead {
        position: sticky;
        top: 0;
        background: darkgoldenrod;
    }

    table.tokenization-results td,
    table.tokenization-results th {
        border: 1px solid black;
        padding: 3px;
        min-width: 5px;
    }

    table.tokenization-results td {
        vertical-align: top;
    }

    table.tokenization-results th {
        resize: horizontal;
        overflow: auto;
    }

    table.tokenization-results {
        border-collapse: collapse
    }

    table.tokenization-results tr.result {
        background-color: lightblue;
        font-weight: bold;
    }

    table.tokenization-results tr.toc {
        background-color: powderblue;
        font-weight: bold;
    }

    table.tokenization-results tr.result .message {
        display: block;
        background-color: white;
        font-weight: normal;
    }


    /* token number */
    .tokenization-results .token td:nth-child(1) {
        text-align: right;
        font-weight: bold;
    }

    /* byte offsets */
    .tokenization-results td:nth-child(2),
    .tokenization-results td:nth-child(3) {
        text-align: right;
    }

    /* bytes */
    .tokenization-results td:nth-child(4) {
        font-family: monospace;
        font-size: 95%;
    }

    /* non token space */
    .tokenization-results .not-token {
        background-color: lightgray;
    }

    /* token */
    .tokenization-results .token {
        background-color: lightyellow;
    }

    .tokenization-results td .codepbytes:nth-child(odd) {
        text-decoration: underline;
    }

    .infobox {
        position: fixed;
        bottom: 0;
        right: 0;
        float: right;
        background-color: khaki;
        border: 1px solid black;
        padding: 3px;
        max-width: 50%;
    }

    .infobox summary {
        font-weight: bold;
        background-color: aquamarine;
        font-size: 110%;
    }
    </style>
    """

    def show_tokenization_remark(remark: str, kind: str = "notice", id: str = None, link: str = None, message: str = None) -> str:
        id = f"id='{ id }'" if id is not None else ""
        ls = f"<a href='#{ link }'>" if link else ""
        le = "</a>" if link else ""
        if message:
            newline = "\n" # fstring can't have backslash
            message = f"<br><span class=message>{ html.escape(message).replace(newline, '<br>') }</span>"
        else:
            message = ""
        return f"<tr class='remark { kind }' { id }><td colspan=8>{ ls }{ html.escape(remark) }{ le }{ message }</td></tr>\n"

    parser = argparse.ArgumentParser(
        prog="python3 -m apsw.fts",
        description="Runs FTS5 tokenizer against test text producing a HTML report for manual inspection.",
    )
    parser.add_argument(
        "--text-file",
        metavar="TEXT-FILE-NAME",
        help="Filename containing test strings.  Default is builtin. "
        """If you provide your own file, it must be a line starting with #, and then the following
           lines up till the next one starting with a # are gathered and treated as one
           string.  The file must be UTF-8 encoded.""",
    )
    parser.add_argument(
        "--output",
        "-o",
        type=argparse.FileType("wb"),
        default="-",
        help="Where to send the binary UTF8 encoded output html [stdout]",
    )
    parser.add_argument(
        "--normalize",
        default="NFC,NFKC,NFD,NFKD",
        help="Also normalize into these forms in addition to the original bytes.  Supply an empty string to omit any normalization [%(default)s]",
    )
    parser.add_argument(
        "--reason",
        help="Tokenize reasons, comma separated.  Choices are DOCUMENT, QUERY, QUERY_PREFiX, AUX. [%(default)s]",
        default="DOCUMENT",
    )
    parser.add_argument(
        "--register",
        action="append",
        default=[],
        help="Registers tokenizers.  This option can be specified multiple times.  Format is name=mod.submod.callable "
        "where name is what is registered with FTS5 and callable is the factory function.  The module containing "
        "callable will be imported.",
        metavar="name=mod.part.callable",
    )

    parser.add_argument(
        "args",
        nargs="+",
        help="Tokenizer and arguments to run.  FTS5 builtin tokenizers are ascii, trigram, unicode61, and porter.  "
        "For example to run the trigram tokenizer on unicode keeping diacritics use: trigram unicode61 remove_diacritics 0",
    )
    options = parser.parse_args()

    # fixup values
    options.normalize = set(s.strip() for s in options.normalize.upper().split(",") if s.strip())
    unknown = options.normalize - {"NFC", "NFKC", "NFD", "NFKD"}
    if unknown:
        parser.error(f"Unexpected normmalize { unknown }")

    reason_map = {
        "DOCUMENT": apsw.FTS5_TOKENIZE_DOCUMENT,
        "QUERY": apsw.FTS5_TOKENIZE_QUERY,
        "QUERY_PREFIX": apsw.FTS5_TOKENIZE_QUERY | apsw.FTS5_TOKENIZE_PREFIX,
        "AUX": apsw.FTS5_TOKENIZE_AUX,
    }

    options.reason = set(s.strip() for s in options.reason.upper().split(","))
    unknown = options.reason - set(reason_map.keys())
    if unknown:
        parser.error(f"Unexpected reason { unknown }")

    con = apsw.Connection("")

    # registrations
    for reg in options.register:
        try:
            name, mod = reg.split("=", 1)
            mod, call = mod.rsplit(".", 1)
            mod = importlib.import_module(mod)
            call = getattr(mod, call)
            con.register_fts5_tokenizer("name", call)
        except Exception as e:
            if hasattr(e, "add_note"):
                e.add_note(f"Processing --register { reg }")
            raise

    # go
    tok = con.fts5_tokenizer(options.args[0])

    # we build it all up in memory
    results = []
    for utf8, comment in tokenizer_test_strings(filename=options.text_file):
        forms = [utf8]
        for norm in options.normalize:
            s = unicodedata.normalize(norm, utf8.decode("utf8")).encode("utf8")
            if s not in forms:
                forms.append(s)
        seen = {}
        for form in forms:
            for reason in options.reason:
                h = show_tokenization(tok, form, reason_map[reason], options.args[1:])
                if h not in seen:
                    seen[h] = []
                seen[h].append(reason)
        results.append((comment, utf8, seen))

    w = lambda s: options.output.write(s.encode("utf8") + b"\n")

    w('<html><head><meta charset="utf-8"></head><body>')
    w(show_tokenization_css)
    w(show_tokenization_header)
    sections = []
    counter = 1
    for comment, utf8, seen in results:
        for h, reasons in seen.items():
            normalized = [
                f for f in ("NFC", "NFKC", "NFD", "NFKD") if unicodedata.is_normalized(f, utf8.decode("utf8"))
            ]
            if normalized:
                forms = ": forms " + " ".join(normalized)
            else:
                forms = ": not normalized"
            w(
                show_tokenization_remark(
                    f"{ comment } : { ' '.join(reasons) } { forms }",
                    kind="toc",
                    link=counter,
                )
            )
            sections.append(
                show_tokenization_remark(
                    f"{ comment } : { ' '.join(reasons) } { forms }",
                    kind="result",
                    id=counter,
                    message=utf8.decode("utf8")
                )
            )
            if not h:
                h = "<tr><td colspan=8></i>No bytes</i></td></tr>"
            sections.append(h)
            counter += 1
    for s in sections:
        w(s)
    w(show_tokenization_footer)
    w("</body></html")
    options.output.close()
