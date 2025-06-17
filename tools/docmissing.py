# python
#
# See the accompanying LICENSE file.
#
# Find things that haven't been documented and should be or have been
# but don't exist.

import glob, sys

import apsw

import names
import pathlib

# check shell knows all pragmas
import apsw.shell

con = apsw.Connection("")
all_pragmas = set(con.execute("pragma pragma_list").get)
exclude_pragmas = {
    # deprecated
    "count_changes",
    "empty_result_callbacks",
    "full_column_names",
    "legacy_file_format",
    "short_column_names",
    "temp_store_directory",
    # test only for debug builds
    "lock_status",
    "parser_trace",
    "sql_trace",
    # undocumented
    "stats",
}
for pragma in all_pragmas:
    if pragma in exclude_pragmas or pragma.startswith("vdbe_"):
        continue
    check = (pragma, f"{pragma}=", f"{pragma}(", f"{pragma};")
    assert any(c in apsw.shell.Shell._pragmas for c in check), f"pragma {pragma} not in apsw.shell.Shell._pragmas"

# check all pragmas are known to sqlite
for pragma in apsw.shell.Shell._pragmas:
    for c in "=(;":
        if pragma.endswith(c):
            pragma = pragma[:-1]
    assert pragma in all_pragmas or pragma in exclude_pragmas, f"{pragma} is in shell but not known to SQLite"


retval = 0

classes = {}

for filename in glob.glob("doc/*.rst"):
    for line in pathlib.Path(filename).read_text().splitlines():
        line = line.strip().split()

        if len(line) >= 2:
            if line[0] == ".." and line[1] in ("method::", "automethod::", "attribute::"):
                funcname = line[2].split("(")[0].strip()

                if "." in funcname:
                    klass, funcname = funcname.split(".", 1)
                else:
                    klass = "apsw"
                if klass not in classes:
                    classes[klass] = []
                classes[klass].append(funcname)

# ok, so we know what was documented.  Now lets see what exists

con = apsw.Connection("testdb")
cur = con.cursor()
cur.execute("create table x(y); insert into x values(x'abcdef1012');select * from x")
blob = con.blob_open("main", "x", "y", con.last_insert_rowid(), 0)
vfs = apsw.VFS("aname", "")
vfsfile = apsw.VFSFile(
    "", con.db_filename("main"), [apsw.SQLITE_OPEN_MAIN_DB | apsw.SQLITE_OPEN_CREATE | apsw.SQLITE_OPEN_READWRITE, 0]
)
session = apsw.Session(con, "main")

# virtual tables aren't real - just check their size hasn't changed
for n, e in (("VTModule", 3), ("VTTable", 17), ("VTCursor", 7)):
    if len(classes[n]) != e:
        sys.exit(f"Expexted len({n}) to be {e} not {len(classes[n])}")
    del classes[n]

for name, obj in (
    ("Connection", con),
    ("Cursor", cur),
    ("Blob", blob),
    ("VFS", vfs),
    ("VFSFile", vfsfile),
    ("apsw", apsw),
    ("VFSFcntlPragma", apsw.VFSFcntlPragma),
    ("zeroblob", apsw.zeroblob(3)),
    ("Session", session),
    ("PreUpdate", apsw.PreUpdate),
):
    if name not in classes:
        retval = 1
        print("class", name, "not found")
        continue

    for c in classes[name]:
        if not hasattr(obj, c) and not (name, c) == ("Cursor", "description_full") and c != "fork_checker":
            retval = 1
            print("%s.%s in documentation but not object" % (name, c))
    for c in dir(obj):
        if c.startswith("__"):
            continue
        # old renamed names?
        if name in names.renames and c in names.renames[name].values():
            continue
        if name == "apsw":
            # ignore imports
            if getattr(getattr(apsw, c), "__module__", name) != name:
                continue
            # ignore constants and modules
            if type(getattr(apsw, c)) in (type(3), type(sys)):
                continue
            # ignore debugging thingies
            if c.startswith("test_") or c in ("faultdict", "_fini"):
                continue
            # ignore the exceptions
            if isinstance(getattr(apsw, c), type) and issubclass(getattr(apsw, c), Exception):
                continue
            # ignore classes !!!
            if c in (
                "Connection",
                "VFS",
                "VFSFile",
                "zeroblob",
                "Shell",
                "URIFilename",
                "Cursor",
                "Blob",
                "Backup",
                "IndexInfo",
                "VFSFcntlPragma",
                "FTS5Tokenizer",
                "FTS5ExtensionApi",
                "Session",
                "Changeset",
                "ChangesetBuilder",
                "TableChange",
                "Rebaser",
                "PreUpdate",
            ):
                continue
            # ignore mappings !!!
            if c.startswith("mapping_"):
                continue
        if c not in classes[name] and not c.startswith("_") and c != "apsw_fault_inject":
            retval = 1
            print("%s.%s on object but not in documentation" % (name, c))

sys.exit(retval)
