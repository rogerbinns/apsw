#!/usr/bin/env python3

# This is prototyping jsonb support
# https://github.com/rogerbinns/apsw/issues/563
#
# Once everything is figured out, it will most likely be converted to C
# APIs are encode, detect, and decode
# The python JSON module dump and load APIs are used for shaping this API

from typing import Any, TypeAlias
from collections.abc import Mapping, Sequence, Callable, Buffer


JSONBTypes: TypeAlias = str | None | int | float | Sequence["JSONBTypes"] | Mapping[str, "JSONBTypes"]


def encode(
    obj: Any, *, skipkeys: bool = False, check_circular: bool = True, default: Callable[[Any], JSONBTypes] | None = None
) -> bytes:
    """Encodes object as JSONB

    :param obj: Object to encode
    :param check_circular: Detects if containers contain themselves
       (even indirectly) and raises :exc:`ValueError`.  If ``False``
       and there is a circular reference, you get
       :exc:`RecursionError` (or worse).
    :param default: Called if an object can't be encoded, and should
       return an object that can be encoded.  If not provided a
       :exc:`TypeError` is raised.
    """
    pass


def decode(
    data: Buffer,
    *,
    object_hook: Callable[[dict[str, JSONBTypes | Any]], Any] | None = None,
    object_pairs_hook: Callable[[list[tuple[str, JSONBTypes | Any]]], Any] | None = None,
) -> Any:
    """Decodes binary data into a Python object

    :param data: Binary data to decode
    :param object_hook: Called after a JSON object has been decoded into a Python :class:`dict`
        and should return a replacement value to use instead.
    :param object_pairs_hook: Called after a JSON object has been
        decoded with a list of tuples, each consisting of a
        :class:`str` and corresponding value, and should return a
        return a replacement value to use instead.

    Only one of ``object_hook`` or ``object_pairs_hook`` can be provided.  ``object_pairs_hook`` is
    useful when you care about the order of keys, and want to handle duplicate keys.

    ::TODO:: we only need both if SQLite allows duplicate keys
    """
    pass


def detect(data: Buffer) -> bool:
    """Returns ``True`` if data is valid JSONB, otherwise ``False``.

    No exceptions are raised if data isn't bytes, or contains corrupt JSONB data.
    """
    # The way this will work is that decode takes an alloc flag
    # (default True) causing it to actually allocate the data
    # structures.  This method will pass in False for the flag, and
    # swallow any exceptions.
    try:
        decode(data, alloc=False)
        return True
    except Exception:
        pass
    return False
