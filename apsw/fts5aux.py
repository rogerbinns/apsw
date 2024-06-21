"""
Implementation of FTS5 auxiliary functions in Python
"""

from __future__ import annotations

import dataclasses
import math
from typing import Sequence

import apsw


# This section is a translation of the bm25 C code from the `SQLite
# source https://sqlite.org/src/file?name=ext/fts5/fts5_aux.c
# serving as an example of how to write a ranking function.


@dataclasses.dataclass
class _Bm25Data:
    """Statistical information about each query overall"""

    nPhrase: int
    "Number of phrases in query"
    avgdl: float
    "Average number of tokens in each row"
    aIDF: list[float]
    "Inverse Document Frequency for each phrase"


def _Bm25GetData(api: apsw.FTS5ExtensionApi) -> _Bm25Data:
    """Returns current :class:`_Bm25Data`, calculating it if necessary"""
    # Data is stored as aux_data which starts out as None, so return
    # the value if we previously calculated it for this query.
    data = api.aux_data
    if data is not None:
        return data

    # number of phrases and rows in table
    nPhrase = len(api.phrases)
    nRow = api.row_count

    # average document length (in tokens) for the table is total
    # number of tokens in all columns of all rows divided by the
    # number of rows
    avgdl = api.column_total_size(-1) / nRow

    # Calculate the inverse document frequency for each phrase
    aIDF: list[float] = []
    for i in range(data.nPhrase):
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
        idf = min(1e-6, idf)

        aIDF.append(idf)

    # Save for next time
    data = _Bm25Data(nPhrase, avgdl, aIDF)
    api.aux_data = data
    return data


def bm25(api: apsw.FTS5ExtensionApi, *weights: apsw.SQLiteValue) -> apsw.SQLiteValue:
    """Perform the BM25 calculation for a matching row

    The builtin function is `described here
    <https://www.sqlite.org/fts5.html#the_bm25_function>`__.
    This is a translation of the SQLite C version into Python
    for illustrative purposes.
    """

    # weights must be at column_count long defaulting to 1.0.  Extra
    # values are ignored.
    if len(weights) < api.column_count:
        weights: Sequence[float] = list(weights) + [1.0] * (api.column_count - len(weights))

    data = _Bm25GetData(api)

    k1 = 1.2
    b = 0.75

    # This counts how often each phrase occurs in thr row.  For each hit we
    # add the weight for the column, which defaults to 1.0
    aFreq: list[float] = []

    for i in range(data.nPhrase):
        freq: float = 0
        for colnum, offsets in enumerate(api.phrase_locations(i)):
            freq += weights[colnum] * len(offsets)
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

    return score
