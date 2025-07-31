#!/usr/bin/env python3

# This is prototyping jsonb support
# https://github.com/rogerbinns/apsw/issues/563
#
# Once everything is figured out, it will most likely be converted to C
# APIs are encode, detect, and decode
# The python JSON module dump and load APIs are used for shaping this API

from typing import Any, TypeAlias
from collections.abc import Mapping, Sequence, Callable, Buffer

from enum import IntEnum


# json module uses isinstance of dict, list, tuple and not abstract types
JSONBTypes: TypeAlias = str | None | int | float | Sequence["JSONBTypes"] | Mapping[str, "JSONBTypes"]


# C data structure, passed as a point to encoding routines
class JSONBuffer:
    data: bytearray  # void *
    size: int  # size_t, also current offset
    allocated: int  # size_t, so we don't keep doing small realloc
    default: Callable[[Any], JSONBTypes] | None  # unknown type converter or NULL
    skip_keys: bool  # skipping non-string dict keys
    seen: set[int] | None  # Non-NULL if check_circular containing ids of seen containers in the call stack


class JSONBTag(IntEnum):
    NULL = 0
    TRUE = 1
    FALSE = 2
    INT = 3
    INT5 = 4
    FLOAT = 5
    FLOAT5 = 6
    TEXT = 7
    TEXTJ = 8
    TEXT5 = 9
    TEXTRAW = 10
    ARRAY = 11
    OBJECT = 12
    # 13 - 15 are reserved


def jsonb_grow_buffer(buf: JSONBuffer, count: int):
    # this would be a realloc
    buf.data += bytearray(count)
    buf.size += count
    buf.allocated += count


def jsonb_add_tag(buf: JSONBuffer, tag: JSONBTag, length: int):
    # length is either correct length, or the maximum length that
    # will be adjusted later.  size_t
    assert 0 <= tag <= JSONBTag.OBJECT

    offset = buf.size

    if length <= 11:
        jsonb_grow_buffer(buf, 1)
        buf.data[offset] = (length << 4) | tag
    elif length <= 0xFF:
        jsonb_grow_buffer(buf, 2)
        buf.data[offset] = (12 << 4) | tag
        buf.data[offset + 1] = length
    elif length <= 0xFFFF:
        jsonb_grow_buffer(buf, 3)
        buf.data[offset] = (13 << 4) | tag
        buf.data[offset + 1] = (length & 0xFF00) >> 8
        buf.data[offset + 2] = (length & 0x00FF) >> 0
    elif length <= 0xFFFF_FFFF:
        jsonb_grow_buffer(buf, 5)
        buf.data[offset] = (14 << 4) | tag
        buf.data[offset + 1] = (length & 0xFF00_0000) >> 24
        buf.data[offset + 2] = (length & 0x00FF_0000) >> 16
        buf.data[offset + 3] = (length & 0x0000_FF00) >> 8
        buf.data[offset + 4] = (length & 0x0000_00FF) >> 0
    else:
        # SQLite can't support blobs this large ... but the
        # pattern follows above with tag 15 and 8 bytes
        # of length
        raise apsw.TooBigError()


def jsonb_update_tag(buf: JSONBuffer, tag_offset: int, tag: JSONBTag, new_length: int):
    # the tag is only used as a sanity check
    assert tag_offset < buf.size
    assert buf.data[tag_offset] & 0x0F == tag
    assert new_length <= 0xFFFF_FFFF

    # we only allow 14 - 4 byte sizes
    assert (buf.data[tag_offset] & 0xF0) >> 4 == 14
    buf.data[tag_offset + 1] = (new_length & 0xFF00_0000) >> 24
    buf.data[tag_offset + 2] = (new_length & 0x00FF_0000) >> 16
    buf.data[tag_offset + 3] = (new_length & 0x0000_FF00) >> 8
    buf.data[tag_offset + 4] = (new_length & 0x0000_00FF) >> 0


def jsonb_append_data(buf: JSONBuffer, data: bytes):
    assert isinstance(data, bytes)  # void*
    offset = buf.size
    jsonb_grow_buffer(buf, len(data))
    buf.data[offset : offset + len(data)] = data


def encode_internal(buf: JSONBuffer, obj: Any):
    if obj is None:
        jsonb_add_tag(buf, JSONBTag.NULL, 0)
        return
    elif obj is True:
        jsonb_add_tag(buf, JSONBTag.TRUE, 0)
        return
    elif obj is False:
        jsonb_add_tag(buf, JSONBTag.FALSE, 0)
        return
    elif isinstance(obj, int):
        s = str(obj).encode("utf8")
        jsonb_add_tag(buf, JSONBTag.INT, len(s))
        jsonb_append_data(buf, s)
        return
    elif isinstance(obj, float):
        # json module does this
        #        if o != o:
        #            text = 'NaN'
        #        elif o == _inf:
        #            text = 'Infinity'
        #        elif o == _neginf:
        #            text = '-Infinity'
        # but they are technically JSON5
        s = str(obj).encode("utf8")
        jsonb_add_tag(buf, JSONBTag.FLOAT, len(s))
        jsonb_append_data(buf, s)
        return
    elif isinstance(obj, str):
        s = obj.encode("utf8")
        jsonb_add_tag(buf, JSONBTag.TEXTRAW, len(s))
        jsonb_append_data(buf, s)
        return

    if buf.seen is not None and id(obj) in buf.seen:
        raise ValueError("circular reference detected")

    if isinstance(obj, Mapping):
        tag_offset = buf.size
        jsonb_add_tag(buf, JSONBTag.OBJECT, 0xFFFF_FFFF if len(obj) else 0)

        if len(obj):
            if buf.seen is not None:
                buf.seen.add(id(obj))

            data_offset = buf.size

            # PyMapping_Items
            for k, v in obj.items():
                # the json module converts base types to str
                if isinstance(k, str):
                    k = k
                elif k is None:
                    k = "null"
                elif k is True:
                    k = "true"
                elif k is False:
                    k = "false"
                elif isinstance(k, int):
                    k = str(k)
                elif isinstance(k, float):
                    # need to handle nan/+-infinity
                    k = str(k)
                elif buf.skip_keys:
                    continue
                else:
                    raise TypeError(f"Keys must be  str, int, float, bool or None. not {type(k)}")
                encode_internal(buf, k)
                encode_internal(buf, v)

            size = buf.size - data_offset
            jsonb_update_tag(buf, tag_offset, JSONBTag.OBJECT, size)

            if buf.seen is not None:
                buf.seen.remove(id(obj))
        return

    if isinstance(obj, Sequence):
        # we probably want PySequence_Check but not PyBuffer_Check.
        # bytes and bytearray are sequence - check what json module does
        tag_offset = buf.size
        jsonb_add_tag(buf, JSONBTag.ARRAY, 0xFFFF_FFFF if len(obj) else 0)

        if len(obj):
            if buf.seen is not None:
                buf.seen.add(id(obj))

            data_offset = buf.size

            # PyuSequence_FAST?
            for v in obj:
                encode_internal(buf, v)

            size = buf.size - data_offset
            jsonb_update_tag(buf, tag_offset, JSONBTag.ARRAY, size)

            if buf.seen is not None:
                buf.seen.remove(id(obj))

        return

    if buf.default is not None:
        replacement = buf.default(obj)
        assert id(replacement) != id(obj)

        saved = buf.default
        buf.default = None
        encode_internal(buf, replacement)
        buf.default = saved
        return

    raise TypeError(f"Unhandled object {type(obj)} {repr(obj)[:60]}")


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
    buf = JSONBuffer()
    buf.data = bytearray(0)
    buf.size = 0
    buf.allocated = 0
    buf.seen = set() if check_circular else None
    buf.default = default
    buf.skip_keys = skipkeys
    encode_internal(buf, obj)
    return bytes(buf.data[: buf.size])


# used for decoding
class JSONBBuffer:
    buf: Buffer # what we are decoding
    offset: int # current decode position
    size: int # offset of last position we can access + 1
    object_hook: Callable[[dict[str, JSONBTypes | Any]], Any] | None = None
    object_pairs_hook: Callable[[list[tuple[str, JSONBTypes | Any]]], Any] | None = None
    alloc: bool # True to decode, False to just check validity
    is_valid: bool = True # used to mark if buf was not valid.  in alloc == false mode we do not raise exceptions

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

    Only one of ``object_hook`` or ``object_pairs_hook`` can be
    provided.  ``object_pairs_hook`` is useful when you want something
    other than a dict, care about the order of keys, want to convert
    them (eg case, numbers), or want to handle duplicate keys.
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


if __name__ == "__main__":
    import apsw, apsw.ext, json

    con = apsw.Connection("")

    # get json
    fjson = apsw.ext.Function(con, "json")
    # validity check
    check = apsw.ext.Function(con, "json_valid")

    foo = encode({1: {True: 4.1}, "π\n": [1,2,3, "😂❤️🤣𐌲𐌻𐌴𐍃"]})

    print(f"{check(foo,8)=}")
    print(f"{fjson(foo)=}")


