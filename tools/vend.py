#!/usr/bin/env python3

# Package up all the SQLite extensions and binaries #547
# most likely exposed as apsw.sqlite_extras

# for windows we have to get and extract the full source, followed by
# getting and extracting the amalgamation over the top.  It is too
# painful to build the source otherwise.  may as well do this for
# all platforms based on if fetch is --sqlite or --all --sqlite

# this code works through building and documenting them first
# for later refactoring

# doc for source file pattern is https://www.sqlite.org/src/file?ci=trunk&name=ext%2Fmisc%2Frandomjson.c

from typing import Literal
import dataclasses


@dataclasses.dataclass
class Extra:
    name: str
    "unique name for extra"
    type: Literal["extension"] | Literal["executable"]
    "what kind of extra"
    sources: list[str]
    "list of files making source for this extra relative to sqlite3 directory"
    description: str
    "what it is"
    doc: str | None = None
    "documentation link relative to sqlite websites.  if none the source will be used"
    lib_sqlite: bool = False
    "true if needing to link against sqlite3.c"

    def __post_init__(self):
        assert len(self.sources)
        assert not any("\\" in source for source in self.sources)
        if self.doc:
            assert not self.doc.startswith("/")
            self.doc = f"https://sqlite.org/{self.doc}"
        else:
            self.doc = "https://sqlite.org/src/file?ci=trunk&name=" + "%2f".join(self.sources[0].split("/"))


extras = [
    Extra(
        name="randomjson",
        type="extension",
        sources=["ext/misc/randomjson.c"],
        description="Generates random json objects",
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
                out.append(f'      VALUE "{name}", "{v}"')
            else:
                out.append(line)
            continue

        if seen_value and line.strip() == "END":
            seen_value = False
            for k, v in fields.items():
                out.append(f'      VALUE "{k}", "{v}"')
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
                "Description: {extra.description}\\n"
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

    objs = compiler.compile(
        [str(pathlib.Path("sqlite3") / filename) for filename in extra.sources] + [resource],
        output_dir=str(build_dir),
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
