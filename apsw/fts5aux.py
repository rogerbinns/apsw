"""
:mod:`apsw.fts5aux` Implementation of FTS5 auxiliary functions in Python.

Auxiliary functions are used for ranking results, and for processing search
results.

"""

from __future__ import annotations

import dataclasses
import math

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
    """Calculates :class:`_Bm25Data`"""

    # weights must be at least column_count long defaulting to 1.0.
    # Extra values are ignored.  This is done once here and remembered
    # while the C code does it on every row.
    weights: list[float] = list(args)
    if len(weights) < api.column_count:
        weights.extend([1] * (api.column_count - len(weights)))

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

    # aux_data is used to store the overall statistical data,
    # calculated once by _Bm25getData
    data = api.aux_data or _Bm25GetData(api, args)

    k1 = 1.2
    b = 0.75

    # This counts how often each phrase occurs in the row.  For each
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
    functions.  The result is the idf for each phrase in the query.

    A phrase occurring in almost every row will have a value close to
    zero, while less frequent phrases have increasingly large positive
    numbers.

    The values will always be at least 0.000001 so you don't have to
    worry about negative numbers or division by zero, even for phrases
    that are not found.
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
    """Ranking function boosting rows where tokens are in order

    :func:`bm25` doesn't take into account ordering.  Phrase matches
    like ``"big truck"`` must occur exactly together in that order.
    Matches for ``big truck`` scores the same providing both words
    exist anywhere.  This function boosts matches where the order does
    match so ``big red truck`` gets a boost while ``truck, big`` does
    not for the same query.

    If the query has phrases and operators (AND, OR, NOT) then those
    operators are not visible to this function, and it looks for
    ordering of each phrase.  For example ``big OR truck NOT red``
    will result in this function boosting ``big ... truck ... red`` in
    that order.  See :attr:`apsw.fts5.QueryInfo.phrases`.

    It accepts parameters giving the weights for each column (default 1).
    """
    # start with the bm25 base score
    score = bm25(api, *args)

    # degrade to bm25 if not enough phrases
    if api.phrase_count < 2:
        return score

    boost = 0

    # work out which columns apply
    columns: set[int] = set.intersection(*(set(api.phrase_columns(i)) for i in range(api.phrase_count)))

    if columns:
        # shortest span possible - number of tokens in each phrase except 1 for last
        shortest_possible = sum(len(phrase) for phrase in api.phrases[:-1]) + 1

        for column in columns:
            if api.aux_data.weights[column]:
                boost += (
                    sum(shortest_possible / span for span in _column_spans(api, column)) * api.aux_data.weights[column]
                )

    # make it more negative to come earlier
    return score - boost


def _column_spans(api: apsw.FTS5ExtensionApi, column: int):
    # Helper for subsequence to get the spans (distance between first
    # token of first phrase and first token of last phrase
    offsets = [api.phrase_column_offsets(phrase, column) for phrase in range(api.phrase_count)]

    # these start at -1 because the loop below always advances by one first
    pos: list[int] = [-1] * api.phrase_count

    try:
        while True:
            # This finds a span starting with phrase[0] and all the
            # intermediate phrases, finishing on finding the last
            # phrase so we have a complete subsequence
            offset = -1
            for column in range(api.phrase_count):
                while True:
                    pos[column] += 1
                    if offsets[column][pos[column]] > offset:
                        offset = offsets[column][pos[column]]
                        break

            # If looking for A B C D the above could have stopped on
            # finding A B C A B C D.  We now start at the penultimate
            # phrase C and advance it to just before D, going
            # backwards through the phrases so we end up with the
            # shortest possible A B C D.

            offset = offsets[-1][pos[-1]]
            for column in range(api.phrase_count - 2, -1, -1):
                for test_pos in range(len(offsets[column]) - 1, pos[column], -1):
                    if offsets[column][test_pos] < offset:
                        pos[column] = test_pos
                        break
                offset = offsets[column][pos[column]]

            yield offsets[-1][pos[-1]] - offsets[0][pos[0]]

    except IndexError:
        # we don't bother constantly checking for overrun above as any
        # overrun means there are no more matches
        pass


def position_rank(api: apsw.FTS5ExtensionApi, *args: apsw.SQLiteValue):
    """Ranking function boosting the earlier in a column phrases are located

    :func:`bm25` doesn't take into where phrases occur.  It makes no
    difference if a phrase occurs at the beginning, middle, or end.
    This boost takes into account how early the phrase match is,
    suitable for content with more significant text towards the
    beginning.

    If the query has phrases and operators (AND, OR, NOT) then those
    operators are not visible to this function, and only the location
    of each phrase is taken into consideration.  See
    :attr:`apsw.fts5.QueryInfo.phrases`.

    It accepts parameters giving the weights for each column (default 1).
    """
    # start with the bm25 base score
    score = bm25(api, *args)
    weights = api.aux_data.weights
    boost = 0

    for phrase in range(api.phrase_count):
        for column in range(api.column_count):
            weight = weights[column]
            if weight:
                boost += sum(weight / (1 + offset) for offset in api.phrase_column_offsets(phrase, column))

    # make it more negative to come earlier
    return score - boost
