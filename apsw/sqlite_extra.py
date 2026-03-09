#!/usr/bin/env python3

from __future__ import annotations

import importlib.resources
import json
import pathlib

import apsw

class NotAvailable(Exception):
    """Raised if a specified item is not available in this installation

    It can be because it did not compile, the APSW installation
    didn't include it, or APSW is a zipped / frozen package but
    requires filesystem installation.
    """
    pass

def load(db: apsw.Connection, extension: str):
    """Loads the extension into the provided database

    :meth:`Extension loading <apsw.Connection.enable_load_extension>` will
    also be turned on.
    """
    db.enable_load_extension(True)
    entry, path = _get_entry(extension)
    if entry["type"] != "extension":
        raise ValueError(f"{extension} is a {entry['type']} not a loadable extension")
    db.load_extension(path)

def has(name: str) -> str | None:
    "Returns 'executable' or 'extension' if extra name is available else None"
    try:
        entry, _ = _get_entry(name)
        return entry['type']
    except Exception:
        return None

def path(name:str):
    "Filesystem path for named extra including extension"
    return _get_entry(name)[1]

# we could in theory know what this specific platform used
# but it is easier just to try everything
_exts = ("", ".so", ".dll", ".exe", ".dylib")

def _get_entry(name: str):
    extras = json.loads(importlib.resources.files(apsw).joinpath("sqlite_extra.json").read_text(encoding="utf8"))
    if name not in extras:
        raise LookupError(f"{name=} is not a known extra")
    bin_dir = importlib.resources.files(apsw).joinpath("sqlite_extra_binaries")
    if not isinstance(bin_dir, pathlib.Path):
        raise NotAvailable("extras can only be provided from filesystem storage")
    for ext in _exts:
        if (fn:=(bin_dir / name).with_suffix(ext)).exists():
            return extras[name], str(fn)
    raise NotAvailable(f"{name} is not included")


if __name__ == '__main__':
    import sys, os

    def usage():
        sys.exit("""\
python3 -m apsw.sqlite_extra --list
    Lists which extensions and executables are available from this
    installation

python3 -m apsw.sqlite_extra --path `name`
    Prints the path for extra `name` including file
    extension

python3 -m apsw.sqlite_extra `name` args...
    Invokes extra `name` with the supplied arguments
                 """)

    if len(sys.argv) == 1:
        usage()

    match sys.argv[1]:
        case "--help":
            usage()

        case "--list":
            extras = json.loads(importlib.resources.files(apsw).joinpath("sqlite_extra.json").read_text(encoding="utf8"))
            for name in extras:
                try:
                    extra, path = _get_entry(name)
                    print(f"{name:20} {extra['type']:13} {extra['description']}")
                except NotAvailable:
                    pass

        case "--path":
            if len(sys.argv) != 3:
                sys.exit("--path takes one name")
            try:
                entry, path = _get_entry(sys.argv[2])
                print(path)
                sys.exit(0)
            except Exception as exc:
                sys.exit(str(exc))

        case _:
            try:
                entry, path = _get_entry(sys.argv[1])
                if entry["type"] != "executable":
                    raise Exception(f"'{sys.argv[1]}' is not an executable.  Extensions need to be loaded into SQLite ")
                os.execl(path, *sys.argv[1:])
            except Exception as exc:
                print("Use --help to get help", file=sys.stderr)
                sys.exit(str(exc))