#!/usr/bin/env python3

# This code uses Python's optional typing annotations.  You can
# ignore them and do not need to use them.  If you do use them
# then you must include this future annotations line first.
from __future__ import annotations

import functools
import json
import pprint

import apsw

### json_basics: JSON basics
# `JSON <https://www.json.org/json-en.html>`__ is a text format for
# representing data.  It supports text (str in Python), booleans (True
# and False in Python), null (None in Python), numbers (int and float
# in Python), arrays (list in Python), and objects (dict in Python).
#
# The format is human and machine readable.  Here we will use the
# Python standard :mod:`json` module to see what it looks like
# with a small piece of data.

data = {
    "hello": True,
    "numbers": [1, 2.2, "seven"],
    "hot": None,
}

# Get it as JSON
print("As json")
as_json = json.dumps(data)
print(as_json)

# That will be ugly so indent it
print("\nAs pretty json")
print(json.dumps(data, indent=4))

### blabblah: fault injection testing
# blah blah



### end: the end
# ene end

del apsw
