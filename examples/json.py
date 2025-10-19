#!/usr/bin/env python3

# This code uses Python's optional typing annotations.  You can
# ignore them and do not need to use them.  If you do use them
# then you must include this future annotations line first.
from __future__ import annotations
from typing import Any

import base64
import contextvars
import datetime
import decimal
from types import MappingProxyType

from pprint import pprint

import apsw
import apsw.ext

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


def convert_binding(
    cursor: apsw.Cursor, num: int, value: Any
) -> bytes:
    # called to convert unknown types - we convert to JSONB
    return apsw.jsonb_encode(value)


def convert_jsonb(
    cursor: apsw.Cursor, column: int, value: bytes
) -> Any:
    # Called when a blob is valid JSONB.You can decode
    # it, or return it as is.
    return apsw.jsonb_decode(value)


# install the callbacks
connection.convert_binding = convert_binding
connection.convert_jsonb = convert_jsonb

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

# If you want the data returned to be read only then use this.
# Note how the list becomes a tuple and dict becomes MappingProxyType
# which doesn't allow writes.


def convert_jsonb_readonly(
    cursor: apsw.Cursor, column: int, value: bytes
) -> Any:
    return apsw.jsonb_decode(
        value, array_hook=tuple, object_hook=MappingProxyType
    )


connection.convert_jsonb = convert_jsonb_readonly

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

# Iterate over each tag - you would typically use this with a JOIN
query(
    "SELECT name, shape, value FROM items, json_each(items.extra, '$.tags')"
)

### custom_conversion: Customising conversion
# The :meth:`~Cursor.convert_binding` and
# :meth:`~Cursor.convert_jsonb` functions are provided with the
# :class:`Cursor` as the first parameter, and a second parameter with
# the binding number or column being returned.  You can use
# :attr:`Cursor.bindings_names` and :attr:`Cursor.description` for
# more details about the value being converted.  You can also use
# :attr:`Cursor.connection` to get back to the connection and your own
# data structures.  :mod:`contextvars` can be used to provide more.
#
# :func:`~apsw.jsonb_encode` and :func:`~apsw.jsonb_decode` have parameters
# like the :mod:`json` module functions for controlling how non-JSON
# Python types can be converted to JSON compatible ones, and how JSON values
# are converted back to Python objects.
#
# This section shows all of these in action at once!


volume: contextvars.ContextVar[str] = contextvars.ContextVar(
    "volume", default="quiet"
)


def py_to_json(value):
    # Used by jsonb_encode to convert types that aren't JSON compatible
    if isinstance(value, bytes):
        # base64 encode binary data.  Note it returns bytes so we have to
        # convert to text.
        return base64.b64encode(value).decode("ascii")

    if isinstance(value, datetime.datetime):
        # ISO8601
        return value.isoformat()

    if isinstance(value, decimal.Decimal):
        # Create JSONB bytes directly.  Tag 5 is FLOAT.  str of a Decimal
        # gives a full precision string of the value
        return apsw.ext.make_jsonb(5, str(value))

    if isinstance(value, complex):
        # The above are all single values.  For objects with multiple
        # fields we return a dict with their members and a key to
        # detect this
        return {
            # I made up this key as unlikely to be used in other dicts
            "$py$type": "complex",
            # fields from complex
            "real": value.real,
            "imag": value.imag,
        }

    raise TypeError(f"Can't convert {value!r}")


def convert_binding(cursor: apsw.Cursor, num: int, value: Any):
    # note that binding numbers start at 1
    print(f"\nconvert_binding callback {num=} {value=!r:.20}...")
    print(f"{cursor.bindings_count=}")
    print(f"{cursor.bindings_names=}")
    print(f"contextvar {volume.get()=}")

    return apsw.jsonb_encode(value, default=py_to_json)


connection.convert_binding = convert_binding

# The above deals with conversion to JSONB, now deal with
# conversion from JSONB


def object_hook(value: dict):
    # We will use this to convert back to a Python type
    match value.get("$py$type"):
        case None:
            # Doesn't have this key, so return as is
            return value
        case "complex":
            return complex(value["real"], value["imag"])
        case _:
            raise ValueError("Unknown $py$type")


def convert_jsonb(cursor: apsw.Cursor, num: int, value: bytes):
    print(f"\nconvert_jsonb callback {num=} {value=!r:.20}...")
    # the description will contain the column names, declared types and
    # more if using description_full
    print(f"columns are {[col[0] for col in cursor.description]}")
    print(f"contextvar {volume.get()=}\n")

    # We want decimal to handle float conversion because it has more
    # precision
    return apsw.jsonb_decode(
        value, object_hook=object_hook, parse_float=decimal.Decimal
    )


connection.convert_jsonb = convert_jsonb

example_data = {
    "binary": b"\x01\x73\x94\x65",
    "date stamp": datetime.datetime.now(),
    "decimal": decimal.Decimal(
        "0.7843262344923523492342352344523423423423"
    ),
    "complex": 3 + 4j,
}

# Use the contextvar
with volume.set("loud"):
    query(
        "SELECT $name AS scope, $data AS hello",
        {
            "name": "test",
            "data": example_data,
        },
    )

### jsonb_cleanup: Cleanup
# No cleanup is needed.  Converters are automatically cleared when
# connections and cursors are no longer used.
