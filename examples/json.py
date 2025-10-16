#!/usr/bin/env python3

# This code uses Python's optional typing annotations.  You can
# ignore them and do not need to use them.  If you do use them
# then you must include this future annotations line first.
from __future__ import annotations

import datetime
import decimal
from typing import Any
from pprint import pprint

import apsw

# A database to work on
connection = apsw.Connection("")

# And the table
connection.execute(
    """CREATE TABLE items(
        name,
        shape,
        extra
    );
"""
)


# This method is used to show queries and results
def query(sql, bindings=None):
    print(f"\n\n{sql}")
    try:
        result = connection.execute(sql, bindings).fetchall()
        if len(result):
            pprint(result[0] if len(result) == 1 else result)
    except Exception as exc:
        print("Exception:", exc)


### json_quick: Quick start
# :ref:`Described here <jsontype>` in the :doc:`JSON background and
# detail <jsonb>`.
#
# If you do not use blobs and just want it to look like SQLite has
# JSON as a native data type, then do the following.


def convert_binding(cursor, num, value):
    # called to convert unknown types - we convert to JSONB
    return apsw.jsonb_encode(value)


def convert_jsonb(cursor, column, value):
    # called when a blob is valid JSONB
    return apsw.jsonb_decode(value)


# install the callbacks
connection.convert_binding = convert_binding
connection.convert_jsonb = convert_jsonb

# data we use
example_data = {
    "origin": "Spain",
    "diameter": 7.5,
    "sugar": 23,
    "tags": ["citrus", "sweet", "juice"],
}

# Note how we can INSERT the data
connection.execute(
    "INSERT INTO items VALUES('orange', 'round', ?)", (example_data,)
)

# And get it back
query("SELECT shape, extra FROM items WHERE name='orange'")

### json_functions: SQLite JSON functions
# SQLite has `over 30 functions <https://sqlite.org/json1.html>`__ for dealing with JSON.
# Here are some of the most useful.

# -> extracts a subcomponent in JSON text format.  Note how origin
# includes the double quotes in the response.  $ is used to indicate
# top level.
query("SELECT extra -> '$.origin' FROM items")

# ->> extracts as a SQLite value so origin in a plain string.
query("SELECT extra ->> '$.origin' FROM items")

# Lets get the first tag
query("SELECT extra ->> '$.tags[0]' FROM items")

# Iterate over each tag
query(
    "SELECT name, shape, value FROM items, json_each(items.extra, '$.tags')"
)

### convert binding: Converting bindings
# The :meth:`~Connection.convert_binding` function is called with more
# context about the binding.  This can be helpful if you want to
# customise how a value is encoded.


# A new version of the callback
def convert_binding(cursor: apsw.Cursor, num: int, value: Any):
    print(f"convert_binding callback {num=} {value=}")
    print(f"{cursor.bindings_count=}")
    print(f"{cursor.bindings_names=}")

    # you can also use cursor.connection to get the connection and
    # from there any other pertinent information
    return apsw.jsonb_encode(value)


connection.convert_binding = convert_binding

query(
    "SELECT $name, $shape, $extra",
    {
        "name": ["dog", "puppy"],
        "shape": [True],
        "extra": {3: "three"},
    },
)

### jsonb_encode_custom: Customising encoding values
# Examples of encoding various types.


def convert_binding(cursor, num, value):
    if isinstance(value, bytes):
        # Creates an alphanumeric string
        value = base64.b64encode(value)

    elif isinstance(value, datetime.datetime):
        # Creates ISO8601
        value = value.isoformat()

    elif isinstance(value, decimal.Decimal):
        # Create JSONb directly.  Tag 5 is FLOAT.  str of a
        # Decimal gives a full precision string of the value
        return apsw.ext.make_jsonb(5, str(value))

    return apsw.jsonb_encode(value)


connection.convert_binding = convert_binding

# some values to test
binary = b"\x01\x73\x94\x65"

datestamp = datetime.datetime.now()

# we will loose precision on reading this back - see later when that
# is addressed
decimal.getcontext().prec = 50
d = decimal.Decimal("0.7843262344923523492342352344523423423423")

query("SELECT ?, ?, ?", (d, binary, datestamp))

### jsonb_encode_type: Encoding objects
# That turns one value into another.  It doesn't help with objects
# that have multiple fields. We want to turn them into an
# object with multiple fields.

# Using a complex number that has real and imaginary parts
example = 3 + 4j


def convert_binding(cursor, num, value):
    if isinstance(value, complex):
        return apsw.jsonb_encode(
            {
                # I made up this key as unlikely to clash with any others
                "$py$type": "complex",
                # fields from complex
                "real": value.real,
                "imag": value.imag,
            }
        )
    return apsw.jsonb_encode(value)


connection.convert_binding = convert_binding

### jsonb_decode: Decoding JSONB
# Your decoder can be automatically called if a blob would be returned
# and is valid JSONB.  Like encoding you have context available.


def convert_jsonb(
    cursor: apsw.Cursor, column_number: int, jsonb: bytes
) -> Any:
    print(f"convert_jsonb callback {column_number=} {len(jsonb)=}")
    print(f"{cursor.description=}")
    return apsw.jsonb_decode(jsonb)


connection.convert_jsonb = convert_jsonb

query(
    "SELECT extra AS the_extra, ? AS complex_num FROM items",
    (3 + 4j,),
)

### jsonb_decode_customise: Customising decoding
# :func:`apsw.jsonb_decode` has a parameter for each value
# type when decoding.  The most useful is ``object_hook``

