#!/usr/bin/env python3

# This is prototyping jsonb support
# https://github.com/rogerbinns/apsw/issues/563
#
# Once everything is figured out, it will most likely be converted to C
# APIs are encode, detect, and decode
# The python JSON module dump and load APIs are used for shaping this API

import sys
import math
import inspect
import traceback
from typing import Any, TypeAlias
from collections.abc import Mapping, Sequence, Callable, Buffer

from enum import IntEnum, Enum, IntFlag


# json module uses isinstance of dict, list, tuple and not abstract types
JSONBTypes: TypeAlias = str | None | int | float | Sequence["JSONBTypes"] | Mapping[str, "JSONBTypes"]


# C data structure, passed as a point to encoding routines
class JSONBuffer:
    data: bytearray  # void *
    size: int  # size_t, also current offset
    allocated: int  # size_t, so we don't keep doing small realloc
    default: Callable[[Any], JSONBTypes | Buffer] | None  # unknown type converter or NULL
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
    RESERVED_13 = 13
    RESERVED_14 = 14
    RESERVED_15 = 15

    __str__ = Enum.__str__  # get pretty names


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
        # SQLite can't support blobs this large ... but the pattern
        # follows above with tag 15 and 8 bytes of length.  ::TODO::
        # document the restriction not because of our code but because
        # of SQLite implementation limits
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
    elif isinstance(obj, float):  # ::TODO:: figure out numpy
        # ::TODO:: fix nan/infinity once SQLite allows it
        if math.isnan(obj):
            # this would be correct
            s = "NaN"
            tag = JSONBTag.FLOAT5
            # sqlite does this
            jsonb_add_tag(buf, JSONBTag.NULL, 0)
            return
        elif obj == math.inf:
            # this would be correct
            s = "Infinity"
            tag = JSONBTag.FLOAT5
            # sqlite does this
            s = "9e999"
            tag = JSONBTag.FLOAT
        elif obj == -math.inf:
            # this would be correct
            s = "-Infinity"
            tag = JSONBTag.FLOAT5
            # sqlite does this
            s = "-9e999"
            tag = JSONBTag.FLOAT
        else:
            s = str(obj)
            tag = JSONBTag.FLOAT
        assert len(s) == len(s.encode("utf8"))
        s = s.encode("utf8")
        jsonb_add_tag(buf, tag, len(s))
        jsonb_append_data(buf, s)
        return
    elif isinstance(obj, str):
        # ::TODO:: scan for quotes etc and use TEXT instead of TEXTRAW?
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

        if id(replacement) == id(obj):
            raise ValueError("default callback returned the object is was passed and did not encode it")

        if isinstance(replacement, Buffer):
            if not detect(replacement):
                raise ValueError("item returned by default callback is not valid JSONB")
            offset = buf.size
            jsonb_grow_buffer(buf, len(replacement))
            buf.data[offset : offset + len(replacement)] = replacement
        else:
            if buf.seen is not None:
                buf.seen.add(id(obj))
            encode_internal(buf, replacement)
            if buf.seen is not None:
                buf.seen.remove(id(obj))

        return

    raise TypeError(f"Unhandled object {type(obj)} {repr(obj)[:60]}")


def encode(
    obj: Any,
    *,
    skipkeys: bool = False,
    check_circular: bool = True,
    default: Callable[[Any], JSONBTypes | Buffer] | None = None,
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

       It can also return binary data in JSONB format.  For example
       numpy.float128 could encode itself as a full precision JSONB
       float.
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
class JSONBDecodeBuffer:
    buf: Buffer  # what we are decoding
    offset: int  # current decode position
    end_offset: int  # offset of last position we can access + 1
    object_hook: Callable[[dict[str, JSONBTypes | Any]], Any] | None = None
    object_pairs_hook: Callable[[list[tuple[str, JSONBTypes | Any]]], Any] | None = None
    alloc: bool  # True to decode data, False to not bother


# for when we'd return NULL in C.  Can't distinguish None at Python level
C_NULL = object()


def malformed(buf: JSONBDecodeBuffer, note: str):
    # print(f"  malformed at line {inspect.stack()[1].lineno}")

    if buf.alloc:
        raise ValueError(f"{note} at offset {buf.offset}")
    return C_NULL


def decode_one(buf: JSONBDecodeBuffer):
    if buf.offset >= buf.end_offset:
        return malformed(buf, "offset")

    # raises if reserved value since we don't define it in enum
    tag: JSONBTag = JSONBTag(buf.buf[buf.offset] & 0x0F)
    tag_len: int = (buf.buf[buf.offset] & 0xF0) >> 4
    buf.offset += 1

    value_offset = buf.offset

    if tag_len >= 12:
        var_len = {12: 1, 13: 2, 14: 4, 15: 8}[tag_len]
        if buf.offset + var_len > buf.end_offset:
            return malformed(buf, "insufficient space for length")

        value_offset += var_len

        tag_len = 0
        while var_len:
            tag_len <<= 8
            tag_len += buf.buf[buf.offset]
            buf.offset += 1
            var_len -= 1

    # value_offset is now start of value, after tag + length bytes

    if value_offset + tag_len > buf.end_offset:
        return malformed(buf, "insufficient space for value")

    # buf.offset is now the start of the next value
    buf.offset = value_offset + tag_len

    if tag == JSONBTag.NULL:
        if tag_len != 0:
            return malformed(buf, "NULL has length")
        return None

    if tag == JSONBTag.TRUE:
        if tag_len != 0:
            return malformed(buf, "True has length")
        return True

    if tag == JSONBTag.FALSE:
        if tag_len != 0:
            return malformed(buf, "False has length")
        return False

    if tag == JSONBTag.INT:
        if not check_int(buf.buf, value_offset, buf.offset):
            return malformed(buf, "Not an int")
        text = buf.buf[value_offset : buf.offset].decode("utf8")
        return int(text) if buf.alloc else None

    if tag == JSONBTag.INT5:
        # SQLite only allows hex and doesn't allow leading +
        if not check_int5hex(buf.buf, value_offset, buf.offset):
            return malformed(buf, "Not an int5")
        sign = +1
        match chr(buf.buf[value_offset]):
            case "-":
                sign = -1
                value_offset += 1
            case "+":  # ::TODO:: delete this when spec settled
                sign = +1
                value_offset += 1
        # +2 for 0x
        text = buf.buf[value_offset + 2 : buf.offset].decode("utf8")
        return sign * int(text, 16) if buf.alloc else None

    if tag == JSONBTag.FLOAT:
        if not check_float(buf.buf, value_offset, buf.offset):
            return malformed(buf, "Not a float")
        text = buf.buf[value_offset : buf.offset].decode("utf8")
        return float(text) if buf.alloc else None

    if tag == JSONBTag.FLOAT5:
        if not check_float5(buf.buf, value_offset, buf.offset):
            return malformed(buf, "Not a float5")
        text = buf.buf[value_offset : buf.offset].decode("utf8")
        return float(text) if buf.alloc else None

    if tag in (JSONBTag.TEXT, JSONBTag.TEXTJ, JSONBTag.TEXT5, JSONBTag.TEXTRAW):
        # this is for coverage checking and won't be translated to C
        if tag == JSONBTag.TEXT:
            True
        elif tag == JSONBTag.TEXTJ:
            True
        elif tag == JSONBTag.TEXT5:
            True
        elif tag == JSONBTag.TEXTRAW:
            True

        binary = buf.buf[value_offset : buf.offset]
        if len(binary) == 0:
            return ""
        if tag in {JSONBTag.TEXT, JSONBTag.TEXTRAW}:
            if buf.alloc is False:
                length, maxchar = decode_utf8_string(binary, None, 0)
                if not length or not maxchar:
                    return C_NULL
                return True
            return binary.decode("utf8")
        length, maxchar = decode_utf8_string(binary, None, 1 if tag == JSONBTag.TEXTJ else 2)
        if not maxchar:
            if not buf.alloc:
                return C_NULL
            raise ValueError(f"incorrect encoded string at offset {value_offset}")

        if length == 0:
            return ""

        uni = PyUnicode(length, maxchar)
        length2, maxchar2 = decode_utf8_string(binary, uni, 1 if tag == JSONBTag.TEXTJ else 2)
        assert length == length2 and maxchar == maxchar2
        return uni.as_string()

    if tag == JSONBTag.ARRAY:
        res = list() if buf.alloc else None
        saved_end = buf.end_offset
        buf.end_offset = buf.offset
        buf.offset = value_offset
        while buf.offset < buf.end_offset:
            item = decode_one(buf)
            if item is C_NULL:
                assert buf.alloc is False
                return item
            if res is not None:
                res.append(item)
        if buf.offset != buf.end_offset:
            return malformed(buf, "incorrectly sized array")
        buf.end_offset = saved_end
        return res

    if tag == JSONBTag.OBJECT:
        if buf.alloc:
            builder = list() if buf.object_pairs_hook else dict()
        else:
            builder = None

        saved_end = buf.end_offset
        buf.end_offset = buf.offset
        buf.offset = value_offset
        while buf.offset < buf.end_offset:
            # check we have a string key
            if buf.buf[buf.offset] & 0x0F not in {JSONBTag.TEXT, JSONBTag.TEXTJ, JSONBTag.TEXT5, JSONBTag.TEXTRAW}:
                return malformed(buf, "object keys must be a string")
            key = decode_one(buf)
            if key is C_NULL:
                assert buf.alloc is False
                return key
            if buf.offset >= buf.end_offset:
                return malformed(buf, "no value for key")
            value = decode_one(buf)
            if value is C_NULL:
                assert buf.alloc is False
                return value

            if buf.object_pairs_hook:
                builder.append((key, value))
            elif builder is not None:
                builder[key] = value

        if buf.offset != buf.end_offset:
            return malformed(buf, "incorrectly sized object")

        if buf.object_hook:
            res = buf.object_hook(builder)
        elif buf.object_pairs_hook:
            res = buf.object_pairs_hook(builder)
        else:
            res = builder

        buf.end_offset = saved_end
        return res

    return malformed(buf, f"unknown tag {tag}")


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
    if object_hook and object_pairs_hook:
        raise ValueError("You can't provide both object_hook and object_pairs_hook")

    if not len(data):
        raise ValueError("JSONB must be at least one byte long")

    buf = JSONBDecodeBuffer()
    buf.buf = data
    buf.offset = 0
    buf.end_offset = len(data)
    buf.object_hook = object_hook
    buf.object_pairs_hook = object_pairs_hook
    buf.alloc = True

    res = decode_one(buf)
    # decode modifies buf.end_offset
    if buf.offset != len(data):
        raise ValueError("not a valid JSONB value")
    return res


def detect(data: Buffer) -> bool:
    """Returns ``True`` if data is valid JSONB, otherwise ``False``.

    No exceptions are raised if data isn't bytes, or contains corrupt JSONB data.
    If this returns ``True`` then no exceptions will arise from decode
    """
    # The way this will work is that decode takes an alloc flag
    # (default True) causing it to actually allocate the data
    # structures.  This method will pass in False for the flag, and
    # swallow any exceptions.
    try:
        if not len(data):
            return False

        buf = JSONBDecodeBuffer()
        buf.buf = data
        buf.offset = 0
        buf.end_offset = len(data)
        buf.object_hook = None
        buf.object_pairs_hook = None
        buf.alloc = False
        v = decode_one(buf)
        # decode modifies buf.end_offset
        if v is C_NULL or buf.offset != len(data):
            return False
        return True
    except Exception as exc:
        print(f"{exc} raised in call from detect {data=}")
        traceback.print_exc()
        pass
    return False


class PyUnicode:
    def __init__(self, length: int, maxchar: int):
        assert length > 0 and maxchar > 0
        self.codepoints = [None] * length
        self.maxchar = maxchar
        assert maxchar <= sys.maxunicode

    def WRITE(self, index: int, codepoint: int):
        assert isinstance(index, int) and isinstance(codepoint, int)
        assert 0 <= index < len(self.codepoints)
        assert 0 <= codepoint <= self.maxchar, f"{codepoint=}"
        self.codepoints[index] = codepoint

    def READ(self, index) -> int:
        assert 0 <= index < len(self.codepoints)
        assert self.codepoints[index] is not None
        return self.codepoints[index]

    def as_string(self):
        assert all(cp is not None for cp in self.codepoints)
        return "".join(chr(cp) for cp in self.codepoints)


def check_float(buf: bytes, start: int, end: int) -> bool:
    # optional sign
    # at least one digit
    # dot
    # at least one digit
    # optional E
    #   optional sign
    #     at least one digit

    seen_sign = seen_dot = seen_digit = seen_e = seen_first_is_zero = False

    for t in buf[start:end]:
        # should only be in ascii range of utf8
        if t < 32 or t > 127:
            return False
        t = chr(t)

        match t:
            case "-" | "+":
                # + only allowed after E
                if t == "+" and not seen_e:
                    return False
                # can't have more than one
                if seen_sign:
                    return False
                # can't be after digits
                if seen_digit:
                    return False
                # can't be after dot
                if seen_dot:
                    return False
                seen_sign = True
            case ".":
                # can't be after E
                if seen_e:
                    return False
                # can't have more than one
                if seen_dot:
                    return False
                # must be after at least one digit
                if not seen_digit:
                    return False
                # a digit will be required after this
                seen_dot = True
                seen_digit = False
            case "0" | "1" | "2" | "3" | "4" | "5" | "6" | "7" | "8" | "9":
                if seen_e or seen_dot:
                    # all digits allowed after E or dot
                    seen_digit = True
                    continue
                # leading zero not allowed
                if seen_digit and seen_first_is_zero:
                    return False
                # leading zero but could 0.123
                if not seen_digit and t == "0":
                    seen_first_is_zero = True
                seen_digit = True
            case "e" | "E":
                # must be at least one digit
                if not seen_digit:
                    return False
                # can't have more than one E
                if seen_e:
                    return False
                # reset state to post E
                seen_e = True
                seen_digit = False
                seen_sign = False
                seen_dot = False
            case _:
                return False

    return seen_digit


def check_float5(buf: bytes, start: int, end: int) -> bool:
    # optional sign
    # at least one digit with at most one dot anywhere including
    #   before or after any digits.  This is the big JSON5 difference
    # optional E
    #   optional sign
    #     at least one digit

    # handle Nan/infinity - ::TODO:: not valid in SQLite (yet)
    match buf[start:end]:
        case b"NaN" | b"+Infinity" | b"Infinity" | b"-Infinity":
            return True

    seen_sign = seen_dot = seen_digit = seen_e = seen_first_is_zero = False

    for t in buf[start:end]:
        # should only be in ascii range of utf8
        if t < 32 or t > 127:
            return False
        t = chr(t)

        match t:
            case "+" | "-":
                # can't have more than one
                if seen_sign:
                    return False
                # can't be after digits
                if seen_digit:
                    return False
                # can't be after dot
                if seen_dot:
                    return False
                seen_sign = True

            case ".":
                # can't be after E
                if seen_e:
                    return False
                # can't be more than one
                if seen_dot:
                    return False
                seen_dot = True
            case "0" | "1" | "2" | "3" | "4" | "5" | "6" | "7" | "8" | "9":
                if seen_e or seen_dot:
                    # all digits allowed after E or dot
                    seen_digit = True
                    continue
                # leading zero not allowed
                if seen_digit and seen_first_is_zero:
                    return False
                # leading zero but could 0.123
                if not seen_digit and t == "0":
                    seen_first_is_zero = True
                seen_digit = True
            case "e" | "E":
                # must be at least one digit
                if not seen_digit:
                    return False
                # can't have more than one E
                if seen_e:
                    return False
                # reset state to post E
                seen_e = True
                seen_digit = False
                seen_sign = False
                seen_dot = False

            case _:
                return False

    return seen_digit


def check_int(buf: bytes, start: int, end: int) -> bool:
    # optional minus
    # at least one digit
    # no leading zeroes

    seen_sign = seen_digit = seen_first_is_zero = False

    for t in buf[start:end]:
        # should only be in ascii range of utf8
        if t < 32 or t > 127:
            return False
        t = chr(t)

        match t:
            case "-":
                if seen_sign:
                    return False
                # can't be after digits
                if seen_digit:
                    return False
                seen_sign = True
            case "0" | "1" | "2" | "3" | "4" | "5" | "6" | "7" | "8" | "9":
                # leading zero not allowed but could be plain 0
                if seen_digit and seen_first_is_zero:
                    return False
                if not seen_digit and t == "0":
                    seen_first_is_zero = True
                seen_digit = True
            case _:
                return False

    return seen_digit


def check_int5hex(buf: bytes, start: int, end: int) -> bool:
    # optional sign
    # zero
    # x
    # at least one hex digit

    seen_sign = seen_x = seen_leading_zero = seen_digit = False

    for t in buf[start:end]:
        # should only be in ascii range of utf8
        if t < 32 or t > 127:
            return False
        t = chr(t)

        match t:
            case "-" | "+":
                if seen_sign:
                    return False
                # can't be after x / leading zero / digits
                if seen_x or seen_leading_zero or seen_digit:
                    return False
                seen_sign = True
            case (
                "0"
                | "1"
                | "2"
                | "3"
                | "4"
                | "5"
                | "6"
                | "7"
                | "8"
                | "9"
                | "a"
                | "A"
                | "b"
                | "B"
                | "c"
                | "C"
                | "d"
                | "D"
                | "e"
                | "E"
                | "f"
                | "F"
            ):
                if t == "0":
                    if not seen_x and not seen_leading_zero:
                        seen_leading_zero = True
                        continue
                if not seen_x:
                    return False
                seen_digit = True
            case "x" | "X":
                if seen_x:
                    return False
                if not seen_leading_zero:
                    return False
                seen_x = True
            case _:
                return False

    return seen_digit


# here for easy breakpoint
def invalid_string():
    # print(f"  invalid string at line {inspect.stack()[1].lineno}")
    return 0, 0


def decode_utf8_string(sin: bytes, sout: PyUnicode | None, escapes: int = 0) -> tuple[int, int]:
    # this function expresses what will happen when converted to C

    # C params:
    # sin: const unsigned char * and size_t length - must be at least one byte
    # sout: NULL or PyUnicode
    # escapes: how to handle backslash - 0 means ignore
    #     1 means JSON
    #     2 means JSON5
    # returns length in codepoints and maxchar
    # length and maxchar set to 0 on error

    # a first pass is used to check validity and calculate the
    # length in codepoints and maxchar, then the caller allocates a
    # corresponding PyUnicode and calls again to fill in because that
    # is how the CPython API works

    # at least one byte must be present
    assert len(sin)
    assert escapes in {0, 1, 2}
    # next input byte index to read
    sin_index = 0
    # next output codepoint index to write
    sout_index = 0

    max_char = 1

    def get_hex(num_digits: int) -> int:
        nonlocal sin_index
        if sin_index + num_digits > len(sin):
            return -1
        val = 0
        while num_digits:
            c = sin[sin_index]
            sin_index += 1
            if ord("0") <= c <= ord("9"):
                c = c - ord("0")
            elif ord("A") <= c <= ord("F"):
                c = 10 + c - ord("A")
            elif ord("a") <= c <= ord("f"):
                c = 10 + c - ord("a")
            else:
                return -1
            num_digits -= 1
            val = (val << 4) + c
        return val

    while sin_index < len(sin):
        b = sin[sin_index]
        sin_index += 1

        if b & 0b1000_0000 == 0:  # 0x80
            if b == ord("\\") and escapes:
                # there must be at least one more char
                if sin_index == len(sin):
                    return invalid_string()

                b = chr(sin[sin_index])
                sin_index += 1

                # JSON escapes
                if b in r"\"":
                    # left as is
                    pass
                elif b in "bfnrtv":
                    b = {"b": "\b", "f": "\f", "n": "\n", "r": "\r", "t": "\t", "v": "\v"}[b]
                elif escapes == 2 and b == "0":
                    b = "\0"
                    # but it must be followed by a non-digit
                    if sin_index < len(sin) and sin[sin_index] in b"0123456789":
                        return invalid_string()
                elif escapes == 2 and (b == "x" or b == "X"):
                    b = get_hex(2)
                elif escapes == 2 and b == "'":
                    # json5 can backslash single quote
                    pass
                elif b == "u":
                    b = get_hex(4)
                    if b < 0:
                        return invalid_string()
                    if 0xD800 <= b <= 0xDBFF:
                        # find second part of surrogate pair
                        if sin_index + 6 <= len(sin) and sin[sin_index] == ord("\\") and sin[sin_index + 1] == ord("u"):
                            sin_index += 2
                            second = get_hex(4)
                            if second < 0:
                                return invalid_string()
                            b = ((b - 0xD800) << 10) + (second - 0xDC00) + 0x10_000
                        else:
                            return invalid_string()
                elif escapes == 2:
                    # JSON5 backslash LineTerminatorSequence gets swallowed
                    # 2028 and 2029 are outside of ascii and hence multi-byte UTF8 sequence
                    if b == "\n":
                        continue
                    if b == chr(0xe2) and sin_index + 1 < len(sin) and sin[sin_index] == 0x80 and sin[sin_index + 1] in {0xa8, 0xa9}:
                        sin_index += 2
                        continue
                    if b == "\r":
                        # if followed by \n then swallow that too
                        if sin_index < len(sin) and chr(sin[sin_index]) == "\n":
                            sin_index += 1
                            continue
                        continue
                    return invalid_string()
                else:
                    # not a valid escape
                    return invalid_string()

                b = ord(b) if isinstance(b, str) else b

                if not acceptable_codepoint(b):
                    return invalid_string()

            max_char = max(max_char, b)
            if sout is not None:
                sout.WRITE(sout_index, b)
            sout_index += 1
            continue

        # utf8 multi-byte sequences
        if b & 0b1111_1000 == 0b1111_0000:
            codepoint = b & 0b0000_0111
            remaining = 3
        elif b & 0b1111_0000 == 0b1110_0000:
            codepoint = b & 0b0000_1111
            remaining = 2
        elif b & 0b1110_0000 == 0b1100_0000:
            codepoint = b & 0b0001_1111
            remaining = 1
        else:
            # not valid utf8
            return invalid_string()

        encoding_len = 1 + remaining

        if sin_index + remaining > len(sin):
            # not enough continuation bytes
            return invalid_string()

        while remaining:
            codepoint <<= 6
            b = sin[sin_index]
            sin_index += 1
            if b & 0b1100_0000 != 0b1000_0000:
                # not a valid continuation byte
                return invalid_string()
            codepoint += b & 0b0011_1111
            remaining -= 1

        if not acceptable_codepoint(codepoint):
            return invalid_string()

        # check for overlong encoding
        if (
            codepoint < 0x80
            or (0x80 <= codepoint <= 0x7FF and encoding_len != 2)
            or (0x800 <= codepoint <= 0xFFFF and encoding_len != 3)
        ):
            return invalid_string()

        max_char = max(max_char, codepoint)
        if sout is not None:
            sout.WRITE(sout_index, codepoint)
        sout_index += 1

    return sout_index, max_char


def acceptable_codepoint(codepoint: int) -> bool:
    # 0 is allowed, surrogate pair ranges are not valid
    # the builtin json decoder will allow a standalone surrogate
    # but python won't allow a surrogate when constructing a string.
    # python accepting lone surrogates https://bugs.python.org/issue11489
    #
    # "abc\ud83edef"  -- python rejects
    # json.loads(r'"abc \ud83e def"')  -- json.loads accepts but
    # it is really invalid and we reject
    #
    # related compatibility note is that sqlite encodes NaN json as null

    if 0xD800 <= codepoint <= 0xDBFF or 0xDC00 <= codepoint <= 0xDFFF:
        return False

    if codepoint < 0 or codepoint > 0x10_FFFF:
        return False

    return True
