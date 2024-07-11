"""
Implementation of FTS5 auxiliary functions in Python
"""

from __future__ import annotations

import dataclasses
import math
from typing import Sequence

import apsw


# This section is a translation of the bm25 C code from the `SQLite
# source https://sqlite.org/src/file?name=ext/fts5/fts5_aux.c serving
# as an example of how to write a ranking function.  It uses the same
# naming conventions and code structure.


@dataclasses.dataclass
class _Bm25Data:
    """Statistical information about each query overall"""

    nPhrase: int
    "Number of phrases in query"
    avgdl: float
    "Average number of tokens in each row"
    aIDF: list[float]
    "Inverse Document Frequency for each phrase"
    weights: list[float]
    "Per column weight - how much each occurrence counts for, defaulting to 1"


def _Bm25GetData(api: apsw.FTS5ExtensionApi, args: apsw.SQLiteValues) -> _Bm25Data:
    """Returns current :class:`_Bm25Data`, calculating it if necessary"""
    # Data is stored as aux_data which starts out as None, so return
    # the value if we previously calculated it for this query.
    data = api.aux_data
    if data is not None:
        return data

    # weights must be at least column_count long defaulting to 1.0.
    # Extra values are ignored.  This is done once here and remembered
    # while the C code does it on every row.
    weights: list[float] = list(args)
    if len(weights) < api.column_count:
        weights.extend([1.0] * (api.column_count - len(weights)))

    # number of phrases and rows in table
    nPhrase = api.phrase_count
    nRow = api.row_count

    # average document length (in tokens) for the table is total
    # number of tokens in all columns of all rows divided by the
    # number of rows
    avgdl = api.column_total_size(-1) / nRow

    # Calculate the inverse document frequency for each phrase
    aIDF: list[float] = []
    for i in range(nPhrase):
        # We need to know how many times the phrase occurs.
        nHit = 0

        def CountCb(_api: apsw.FTS5ExtensionApi, _closure: None):
            # Callback for each row matched.  The parameters are
            # unused.
            nonlocal nHit
            nHit += 1

        api.query_phrase(i, CountCb, None)

        # See the comment in the C code for details on IDF calculation
        idf = math.log((nRow - nHit + 0.5) / (nHit + 0.5))
        # ensure it is at least a positive small number
        idf = max(1e-6, idf)

        aIDF.append(idf)

    # Save for next time
    data = _Bm25Data(nPhrase, avgdl, aIDF, weights)
    api.aux_data = data
    return data


def bm25(api: apsw.FTS5ExtensionApi, *args: apsw.SQLiteValue) -> float:
    """Perform the BM25 calculation for a matching row

    It accepts weights for each column (default 1) which means how much
    a hit in that column counts for.

    The builtin function is `described here
    <https://www.sqlite.org/fts5.html#the_bm25_function>`__.
    This is a translation of the SQLite C version into Python
    for illustrative purposes.
    """

    data = _Bm25GetData(api, args)

    k1 = 1.2
    b = 0.75

    # This counts how often each phrase occurs in thr row.  For each
    # hit we multiply by the weight for the column, which defaults to
    # 1.0
    aFreq: list[float] = []

    for i in range(data.nPhrase):
        freq: float = 0
        for column, offsets in enumerate(api.phrase_locations(i)):
            freq += data.weights[column] * len(offsets)
        aFreq.append(freq)

    # total number of tokens in this row
    nTok = api.column_size(-1)

    # calculate the score, starting with some constants
    k1 = 1.2
    b = 0.75

    D = nTok
    score: float = 0.0

    for i in range(data.nPhrase):
        score += data.aIDF[i] * ((aFreq[i] * (k1 + 1.0)) / (aFreq[i] + k1 * (1 - b + b * D / data.avgdl)))

    # The score has a bigger (positive) number meaning a better match.
    # FTS5 wants you to do 'ORDER BY rank' giving the better matches
    # first.  Negating the score achieves that.

    return -score


def inverse_document_frequency(api: apsw.FTS5ExtensionApi) -> list[float]:
    """Measures how rare each search phrase is in the content

    This helper method is intended for use in your own ranking
    functions.  The result is the idf for each phrase.

    A phrase occurring in almost every row will have a value close to
    zero, while less frequent phrases have increasingly large positive
    numbers.

    The values will always be at least 0.000001 so you don't have to
    worry about negative numbers or division by zero.
    """

    # This is ported from the bm25 code above, but using Pythonic
    # naming
    idfs: list[float] = []
    row_count = api.row_count

    for i in range(api.phrase_count):
        # We need to know how many times the phrase occurs.
        hits = 0

        def count_callback(_api: apsw.FTS5ExtensionApi, _closure: None):
            # Callback for each row matched.  The parameters are
            # unused.
            nonlocal hits
            hits += 1

        api.query_phrase(i, count_callback, None)

        # See the comment in the C code for details on IDF calculation
        idf = math.log((row_count - hits + 0.5) / (hits + 0.5))
        # ensure it is at least a positive small number
        idf = max(1e-6, idf)

        idfs.append(idf)

    return idfs


def subsequence(api: apsw.FTS5ExtensionApi, *args: apsw.SQLiteValue):
    """Ranking function boosting tokens in order with any separation

    You can search for A B C and rows where those tokens occur in that
    order rank better.  They don't have to be next to each other - ie
    other tokens can separate them.  The tokens must appear in the
    same column to get a score boost.

    You can change the ranking function on a `per query basis
    <https://www.sqlite.org/fts5.html#sorting_by_auxiliary_function_results>`__
    or via :meth:`~apsw.fts.FTS5Table.config_rank` for all queries.
    """
    # start with the bm25 base score
    score = bm25(api, *args)

    # degrade to bm25 if not enough phrases
    if api.phrase_count < 2:
        return score

    # negate the score so bigger number means better match again
    score = -score

    # work out which columns apply
    columns: set[int] = set.intersection(*(set(api.phrase_columns(i)) for i in range(api.phrase_count)))

    if not columns:
        # none of them, so degrade score
        score = score / api.phrase_count
        # negate again
        return -score

    boost = 0

    # shortest span possible - number of tokens in each phrase except 1 for last
    shortest_possible = sum(len(phrase for phrase in api.phrases[:-1])) + 1

    for column in columns:
        boost += sum(shortest_possible / span for span in _column_spans(api, column)) * api.aux_data.weights[column]

    if boost:
        score += max(math.log(boost), 1e-5)

    return -score


def _column_spans(api: apsw.FTS5ExtensionApi, column: int):
    # Helper for subsequence to get the spans (distance between first
    # token of first phrase and first token of last phrase
    offsets = [api.phrase_column_offsets(phrase, column) for phrase in range(api.phrase_count)]

    pos = [-1] * api.phrase_count

    try:
        while True:
            pos[0] += 1
            offset = offsets[0][pos[0]]
            for i in range(1, api.phrase_count):
                while True:
                    pos[i] += 1
                    if offsets[i][pos[i]] > offset:
                        offset = offsets[i][pos[i]]
                        break
            # advance phrase[0] because it could have occurred
            # multiple times before phrase[1] - eg A A A B C D where
            # pos[0] could be indexing the first A, but it needs to be
            # the last A before B.  This doesn't matter for any of the
            # other phrases because we only care about the distance
            # from A to D.
            offset = offsets[1][pos[1]]
            while (
                # Can we advance?
                pos[0] + 1 < len(offsets[0])
                # should we advance?
                and offsets[0][pos[0] + 1] < offset
            ):
                pos[0] += 1
            yield offsets[-1][pos[-1]] - offsets[0][pos[0]]

    except IndexError:
        # we don't bother constantly checking for overrun above as any
        # overrun means there are no more matches
        pass
