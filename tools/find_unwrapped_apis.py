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
    db.row_trace = apsw.ext.DataClassRowFactory(dataclass_kwargs={"frozen": True})

    functions = set(row.name for row in db.execute("""
        SELECT * FROM toc WHERE type = 'function' AND status = 0 AND name NOT LIKE '%16%'
            AND name NOT LIKE 'sqlite3_str_%'
    """))

# functions we won't wrap
nowrap = {
    # memory allocation
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
    "sqlite3_malloc",
    "sqlite3_column_int",
    "sqlite3_bind_int",
    "sqlite3_value_int",
    "sqlite3_bind_blob",
    "sqlite3_bind_text",
    "sqlite3_result_int",
    "sqlite3_result_text",
    "sqlite3_result_zeroblob",
    "sqlite3_result_blob",
    "sqlite3_status",

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
    "sqlite3_keyword_check",  # sqlite3_keyword_name is used
    "sqlite3_compileoption_used",  # sqlite3_compileoption_get is used
    "sqlite3_set_auxdata",
    "sqlite3_get_auxdata",
    "sqlite3_value_dup",
    "sqlite3_value_encoding",
    "sqlite3_value_free",
    "sqlite3_value_frombind",
    "sqlite3_value_numeric_type",
    "sqlite3_value_pointer",
    "sqlite3_bind_parameter_index",  # we don't need to do this direction
    "sqlite3_context_db_handle",
    "sqlite3_create_filename",
    "sqlite3_free_filename",
    "sqlite3_filename_database",
    "sqlite3_database_file_object",
    "sqlite3_db_handle",
    "sqlite3_errcode",  # extended is used
    "sqlite3_errstr",
    "sqlite3_get_table",
    "sqlite3_free_table",
    "sqlite3_result_value",
    "sqlite3_bind_value",
    "sqlite3_auto_extension",  # connection_hooks are used
    "sqlite3_cancel_auto_extension",
    "sqlite3_reset_auto_extension",
    "sqlite3_result_error_nomem",
    "sqlite3_result_error_toobig",
    "sqlite3_libversion_number",  # more useful in C
    "sqlite3_version",  # we provide sqlite3_libversion which is the same

    # deprecated but not marked as such (v2+ exists)
    "sqlite3_trace",
    "sqlite3_profile",
    "sqlite3_wal_checkpoint",
    "sqlite3_create_collation",
    "sqlite3_create_module",
    "sqlite3_prepare",
    "sqlite3_prepare_v2",

    # windows only, no one has asked for it
    "sqlite3_win32_set_directory",
    "sqlite3_win32_set_directory8",

    # shouldn't be exposed to scripting
    "sqlite3_get_clientdata",
    "sqlite3_set_clientdata",

    # we don't do subtypes / pointer
    "sqlite3_value_subtype",
    "sqlite3_result_subtype",
    "sqlite3_bind_pointer",
    "sqlite3_result_pointer",

    # requires non-default compile options
    "sqlite3_normalized_sql",
    "sqlite3_test_control",
    "sqlite3_stmt_scanstatus",
    "sqlite3_stmt_scanstatus_reset",
    "sqlite3_stmt_scanstatus_v2",
    "sqlite3_snapshot_cmp",
    "sqlite3_snapshot_free",
    "sqlite3_snapshot_get",
    "sqlite3_snapshot_open",
    "sqlite3_snapshot_recover",
    "sqlite3_preupdate_blobwrite",
    "sqlite3_preupdate_count",
    "sqlite3_preupdate_depth",
    "sqlite3_preupdate_hook",
    "sqlite3_preupdate_new",
    "sqlite3_preupdate_old",
}

for f in sorted(functions):
    # we have to exclude this file ...
    res = subprocess.run(["git", "grep", "-wFq", f, "--", ":^tools/find_unwrapped_apis.py", "*.c"])
    assert res.returncode >= 0
    if res.returncode == 0:  # found
        if f in nowrap:
            # these aren't wrapped but are used in fts.c
            if f not in {"sqlite3_bind_pointer", "sqlite3_prepare", "sqlite3_result_error_nomem"}:
                print(f"function { f } is in nowrap, but also in source")
        continue
    if f not in nowrap:
        print("missing", f)