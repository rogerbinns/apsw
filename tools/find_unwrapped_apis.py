#!/usr/bin/env python3

import urllib.request
import tempfile
import apsw, apsw.ext
import subprocess

basesqurl = "https://sqlite.org/"
with tempfile.NamedTemporaryFile() as f:
    f.write(urllib.request.urlopen(basesqurl + "toc.db").read())
    f.flush()

    db = apsw.Connection(f.name)
    db.rowtrace = apsw.ext.DataClassRowFactory(dataclass_kwargs={"frozen": True})

    functions = set(row.name for row in db.execute("""
        SELECT * FROM toc WHERE type = 'function' AND status = 0 AND name NOT LIKE '%16%'
            AND name NOT LIKE 'sqlite3_str_%'
    """))

# functions we won't wrap
nowrap = {
    # memory allocation
    "sqlite3_malloc",
    "sqlite3_msize",
    "sqlite3_realloc",
    "sqlite3_realloc64",

    # mutex stuff
    "sqlite3_mutex_alloc",
    "sqlite3_mutex_free",
    "sqlite3_mutex_held",
    "sqlite3_mutex_notheld",
    "sqlite3_mutex_try",

    # we use the -64 suffix versions
    "sqlite3_changes",
    "sqlite3_total_changes",

    # not useful
    "sqlite3_next_stmt",
    "sqlite3_os_end",
    "sqlite3_os_init",
    "sqlite3_unlock_notify",
    "sqlite3_close_v2",
    "sqlite3_snprintf",
    "sqlite3_vmprintf",
    "sqlite3_vsnprintf",
    "sqlite3_stmt_busy",

    # deprecated but not marked as such
    "sqlite3_trace",

    # windows only, no one has asked for it
    "sqlite3_win32_set_directory",
    "sqlite3_win32_set_directory8",
}

for f in sorted(functions):
    res = subprocess.run(["git", "grep", "-wFq", f])
    assert res.returncode >= 0
    if res.returncode == 0:  # found
        if f in nowrap:
            print(f"function { f } is in nowrap, but also in source")
        continue
    if f not in nowrap:
        print("missing", f)