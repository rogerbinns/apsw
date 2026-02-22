#!/usr/bin/env python3

# Package up all the SQLite extensions and binaries #547
# most likely exposed as apsw.sqlite_extras

# for windows we have to get and extract the full source, followed by
# getting and extracting the amalgamation over the top.  It is too
# painful to build the source otherwise.  may as well do this for
# all platforms based on if fetch is --sqlite or --all --sqlite

# this code works through building and documenting them first
# for later refactoring

from typing import Literal
import dataclasses


@dataclasses.dataclass
class Extra:
    name: str
    "unique name for extra"
    description: str
    "what it is"
    doc: str | None = None
    "documentation link relative to sqlite websites.  if none the source will be used"
    lib_sqlite: bool = False
    "true if needing to link against sqlite3.c"
    lib_sqlite_stdio: bool = False
    "true if needing sqlite3_stdio"
    sources: list[str] = dataclasses.field(default_factory=list[str])
    "list of files making source for this extra relative to sqlite3 directory"
    type: Literal["extension"] | Literal["executable"] = "extension"
    "what kind of extra"
    defines: list | None = None
    "additional defines needed"

    def __post_init__(self):
        if self.type == "extension" and not self.sources:
            self.sources = [f"ext/misc/{self.name}.c"]
        assert len(self.sources)
        assert not any("\\" in source for source in self.sources)
        if self.doc:
            assert not self.doc.startswith("/")
            self.doc = f"https://sqlite.org/{self.doc}"
        else:
            self.doc = "https://sqlite.org/src/file?ci=trunk&name=" + "%2f".join(self.sources[0].split("/"))


extras = [
    Extra(
        name="amatch",
        description="Approximate matches virtual table",
    ),
    Extra(
        name="anycollseq",
        description="Fake fallback collating function for any unknown collating sequence",
    ),
    Extra(
        name="appendvfs",
        description="A VFS shim that allows an SQLite database to be appended onto the end of some other file, such as an executable",
    ),
    Extra(
        name="base64",
        description="Convert either direction between base64 blob and text",
    ),
    Extra(
        name="base85",
        description="Convert either direction between base85 blob and text",
    ),
    Extra(
        name="btreeinfo",
        description="btreeinfo virtual table that shows information about all btrees in an SQLite database file",
    ),
    Extra(
        name="cksumvfs",
        description="A VFS shim that writes a checksum on each page of an SQLite database file",
    ),
    Extra(
        name="closure",
        description="A virtual table that finds the transitive closure of a parent/child relationship in a real table",
    ),
    Extra(
        name="completion",
        description="A virtual table that returns suggested completions for a partial SQL input",
    ),
    Extra(
        name="csv",
        description="A virtual table for reading CSV files",
    ),
    Extra(
        name="decimal",
        description="Routines to implement arbitrary-precision decimal math",
    ),
    Extra(
        name="eval",
        description="Implements SQL function eval() which runs SQL statements recursively",
    ),
    Extra(
        name="fileio",
        description="Implements SQL functions readfile() and writefile(), and eponymous virtual type 'fsdir'",
    ),
    # ::TODO:: fossildelta once RBU extension is wrapped
    Extra(
        name="fuzzer",
        description="Virtual table that generates variations on an input word at increasing edit distances from the original",
    ),
    Extra(
        name="ieee754",
        description="functions for the exact display* and input of IEEE754 Binary64 floating-point numbers",
    ),
    Extra(
        name="nextchar",
        description="Finds all valid 'next' characters for a string given a vocabulary",
    ),
    Extra(
        name="noop",
        description="Implements noop() functions useful for testing",
    ),
    Extra(
        name="prefixes",
        description="Table valued function providing all prefixes of a string",
    ),
    Extra(
        name="randomjson",
        description="Generates random json objects",
    ),
    Extra(
        name="regexp",
        description="Compact reasonably efficient posix extended regular expression matcher",
    ),
    Extra(
        name="rot13",
        description="rot13 function and collating sequence",
    ),
    Extra(
        name="sha1",
        description="SHA1 hash and query results hash",
    ),
    Extra(
        name="sha3",
        description="SHA3 hash and query results hash",
    ),
    Extra(
        name="spellfix",
        description="Search a large vocabulary for close matches",
        doc="spellfix1.html",
    ),
    Extra(
        name="stmt",
        description="Virtual table with information about all prepared statements on a connection",
    ),
    Extra(
        name="stmtrand",
        description="Function that returns the same sequence of random integers is returned for each invocation of the statement",
    ),
    Extra(
        name="tmstmpvfs",
        description="VFS shim that writes timestamps and other tracing information to the reserved bytes of each page, and also generates corresponding log files",
    ),
    # totype: hard codes byte order detection on processors from 2013
    Extra(
        name="uint",
        description="UINT collating sequence",
    ),
    Extra(
        name="unionvtab",
        description="Virtual table combining underlying tables from other databases",
    ),
    Extra(
        name="uuid",
        description="uuid functions",
    ),
    # vfslog: has to be compiled into the amalgamation
    Extra(
        name="vfsstat",
        description="VFS shim tracking call statistics",
    ),
    # vfstrace: has to be used with C code
    Extra(
        name="vtablog",
        description="Virtual table printing diagnostic information for interactive analysis and debugging",
    ),
    # vtshim: not useful
    # wholenumber: use generate_series
    # zipfile: requires libz
    Extra(
        name="zorder",
        description="Functions for z-order (Morton code) transformations",
    ),
    Extra(
        name="sqlite3_dbdump",
        type="executable",
        description="Converts the content of a SQLite database into UTF-8 text SQL statements that can be used to exactly recreate the original database",
        sources=["ext/misc/dbdump.c"],
        defines=[("DBDUMP_STANDALONE", 1)],
        lib_sqlite=True,
    ),
    Extra(
        name="sqlite3_diff",
        type="executable",
        sources=["tool/sqldiff.c"],
        description="Displays content differences between SQLite databases",
        doc="sqldiff.html",
        lib_sqlite_stdio=True,
        lib_sqlite=True,
    ),
    Extra(
        name="sqlire3_normalize",
        type="executable",
        sources=["ext/misc/normalize.c"],
        description="Normalizes SQL text so private information can be removed, and to identify structurally identical queries",
        defines=[("SQLITE_NORMALIZE_CLI", 1)],
        lib_sqlite=True,
    ),
    Extra(
        name="sqlite3_scrub",
        type="executable",
        sources=["ext/misc/scrub.c"],
        defines=[("SCRUB_STANDALONE", 1)],
        description="Makes a backup zeroing out all deleted content",
        lib_sqlite=True,
    ),
    Extra(
        name="sqlite3_shell",
        type="executable",
        sources=["shell.c"],
        description="Command line shell",
        doc="cli.html",
        lib_sqlite=True,
    ),
    Extra(
        name="sqlite3_expert",
        type="executable",
        sources=["ext/expert/expert.c", "ext/expert/sqlite3expert.c"],
        description="A simple system to propose useful indexes given a database and a set of SQL queries",
        lib_sqlite=True,
    ),
    Extra(
        name="sqlite3_rsync",
        type="executable",
        sources=["tool/sqlite3_rsync.c"],
        description="Database Remote-Copy Tool",
        doc="rsync.html",
        lib_sqlite=True,
    ),
]

import os
import pathlib
import tempfile
import re
import setuptools._distutils.ccompiler as ccompiler
from setuptools._distutils.sysconfig import customize_compiler

import logging

logging.basicConfig(level=logging.DEBUG, format="    %(message)s")


def c_quote(value: str, quote='"'):
    # I originally tried to backslash escape double quotes but rc
    # required double backslashes and there was no way to make
    # everyone happy except to ban them
    assert '"' not in value
    return quote + value + quote


def make_windows_resource(**fields):
    assert "FileDescription" in fields
    source = (pathlib.Path() / "sqlite3" / "src" / "sqlite3.rc").read_text()
    out: list[str] = []
    seen_value = False
    for line in source.splitlines():
        if line.strip().startswith("IDI_SQLITE ICON"):
            out.append(r'IDI_SQLITE ICON "sqlite3\\art\\sqlite370.ico"')
            continue
        if line.strip().startswith("VALUE"):
            seen_value = True
            parts = [p.strip() for p in line.split()]
            assert parts[0] == "VALUE"
            assert parts[1].startswith('"') and parts[1].endswith('",')
            name = parts[1][1:-2]
            if name in fields:
                v = fields.pop(name)
                if name == "FileDescription":
                    v += " (APSW packaged)"
                out.append(f'      VALUE "{name}", {c_quote(v)}')
            else:
                out.append(line)
            continue

        if seen_value and line.strip() == "END":
            seen_value = False
            for k, v in fields.items():
                out.append(f'      VALUE "{k}", {c_quote(v)}')
        out.append(line)
    return "\r\n".join(out) + "\r\n"


unix_resource_header = """
#ifdef APSW_SUPPORTS_ATTRIBUTE
#if defined(__APPLE__)
/* Mach-O section */
__attribute__((section("__TEXT,__apsw_info"), used))
#else
/* ELF note section */
__attribute__((section(".note.apsw"), used))
#endif
#endif
const char apsw_resource_metadata[] =
    "APSW-Note: Unmodified SQLite project artifact. Packaged by APSW for convenience.\\n"
"""


def resource_file(extra: Extra):
    if compiler.compiler_type == "msvc":
        with open(build_dir / f"{extra.name}.rc", "wt") as f:
            f.write(
                make_windows_resource(
                    FileDescription=f"SQLite {extra.description}",
                    InternalName=f"sqlite3 - {extra.name}",
                    Comment="Unmodified SQLite project artifact. Packaged by APSW for convenience",
                )
            )

    else:
        with open(build_dir / f"{extra.name}_rsrc.c", "wt") as f:
            f.write(f"""{unix_resource_header}
                "Copyright: https://sqlite.org/copyright.html\\n"
                "Description: {c_quote(extra.description, quote="")}\\n"
                "Documentation: {extra.doc}\\n"
            """)
            for k, v in sorted(Version.items()):
                f.write(f'"{k}: {v}\\n"')
            f.write(";\n")
    f.close()
    return f.name


Version = {
    "SQLITE_VERSION": None,
    "SQLITE_SOURCE_ID": None,
    "SQLITE_SCM_BRANCH": None,
    "SQLITE_SCM_TAGS": None,
    "SQLITE_SCM_DATETIME": None,
}

pat = r"#define\s+(" + "|".join(Version.keys()) + r')\s+"(.*)"\s*$'
for line in pathlib.Path("sqlite3/sqlite3.c").read_text(encoding="utf8").splitlines():
    if mo := re.match(pat, line):
        Version[mo.group(1)] = mo.group(2)

for k, v in Version.items():
    if v is None:
        raise Exception(f"Version {k} not found")

compiler = ccompiler.new_compiler(verbose=True)
customize_compiler(compiler)

compiler.add_include_dir("sqlite3")

# where the build artifacts go
build_dir = pathlib.Path() / "build" / "sqlite_extras"
compiler.mkpath(str(build_dir))

# where the final binaries go
output_dir = pathlib.Path() / "apsw" / "sqlite_extras_binaries"
compiler.mkpath(str(output_dir))

# figure out if attribute is understood
if compiler.compiler_type != "msvc":
    with tempfile.NamedTemporaryFile(mode="wt", suffix=".c") as f:
        f.write(unix_resource_header + ";")
        f.flush()

        try:
            compiler.compile([f.name], output_dir=str(build_dir), macros=[("APSW_SUPPORTS_ATTRIBUTE", 1)])
            compiler.macros.append(("APSW_SUPPORTS_ATTRIBUTE", 1))
            print("Attribute section and used supported")
        except ZeroDivisionError:  # ::TODO:: fix this
            print("Attribute section and used NOT SUPPORTED")

# build sqlite3 library
print(">>> sqlite3 library")
lib_enables = "CARRAY COLUMN_METADATA DBPAGE_VTAB DBSTAT_VTAB FTS4 FTS5 GEOPOLY MATH_FUNCTIONS PERCENTILE PREUPDATE_HOOK RTREE SESSION".split()

macros = [(f"SQLITE_ENABLE_{enable}", 1) for enable in lib_enables]
cfg = pathlib.Path("sqlite3") / "sqlite_cfg.h"
if cfg.exists():
    macros.append(("SQLITE_CUSTOM_INCLUDE", "sqlite_cfg.h"))
macros.append(("SQLITE_THREADSAFE", 1))
lib_objs = compiler.compile([str(pathlib.Path("sqlite3") / "sqlite3.c")], output_dir=str(build_dir), macros=macros)

# sqlite stdio
print(">>> sqlite3 stdio library")
lib_stdio_objs = compiler.compile(
    [str(pathlib.Path("sqlite3") / "ext" / "misc" / "sqlite3_stdio.c")], output_dir=str(build_dir)
)
lib_stdio_include = pathlib.Path("sqlite3") / "ext" / "misc"


## ::TODO:: build up a list of failures and the reason and output them at the end
## ::TODO:: figure out what exceptions to catch for a failed compile or link
for extra in extras:
    print(f">>> {extra.name}")
    missing = []
    for source in extra.sources:
        if not (pathlib.Path("sqlite3") / source).exists():
            missing.append(source)
    if missing:
        print("  Skipping due to missing source:", missing)
        print()
        continue

    resource = resource_file(extra)
    include_dirs = [str(lib_stdio_include)] if extra.lib_sqlite_stdio else None

    objs = compiler.compile(
        [str(pathlib.Path("sqlite3") / filename) for filename in extra.sources] + [resource],
        output_dir=str(build_dir),
        include_dirs=include_dirs,
        macros=extra.defines,
    )

    if extra.lib_sqlite:
        objs.extend(lib_objs)

    match extra.type:
        case "extension":
            # ::TODO:: do we want sqlext vs platform native dll extension?
            # dll means properties show up in explorer
            out_name = f"{extra.name}.sqlite_extension"
            out_name = f"{extra.name}{compiler.shared_lib_extension}"
            compiler.link_shared_object(objs, out_name, output_dir=str(build_dir))

        case "executable":
            out_name = f"{extra.name}{compiler.exe_extension if compiler.exe_extension else ''}"
            match compiler.compiler_type:
                case "msvc":
                    libraries = None
                case _:
                    libraries = ["m"]
            compiler.link_executable(objs, extra.name, output_dir=str(build_dir), libraries=libraries)
            # macos xattr -d com.apple.quarantine str(build_dir / out_name)

        case _:
            raise NotImplementedError

    try:
        os.remove(str(output_dir / out_name))
    except FileNotFoundError:
        pass

    # ::todo:: strip before moving
    compiler.move_file(str(build_dir / out_name), str(output_dir / out_name))

    print()
