#!/usr/bin/env python3

# This code uses Python's optional typing annotations.  You can
# ignore them and do not need to use them.  If you do use them
# then you must include this future annotations line first.
from __future__ import annotations

from pprint import pprint
import functools

import apsw
import apsw.ext


### session_check: Is Session available?
# Session must be enabled in SQLite at compile time, and in APSW
# at its compile time.  (PyPI builds always have both enabled)

print("Session in SQLite:", "ENABLE_SESSION" in apsw.compile_options)

print("  Session in APSW:", hasattr(apsw, "Session"))

### session_end: Cleanup
# We can now close the connections, but it is optional.  APSW automatically
# cleans up sessions etc when their corresponding connections are closed.

if False:
    connection.close()


