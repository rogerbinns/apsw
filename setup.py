#!/usr/bin/env python

import os
import sys

from distutils.core import setup, Extension

# python 2 and 3 print equivalent
def write(*args):
    # py2 won't allow keyword arg on end, so work around it
    dest=sys.stdout
    if args[-1]==sys.stderr:
        dest=args[-1]
        args=args[:-1]
    dest.write(" ".join(args)+"\n")
    dest.flush()

py3=sys.version_info>=(3,0)

# Should we automatically fetch SQLite amalgamation?
fetch=None
argv2=[]
for v in sys.argv:
    if v=="--fetch-sqlite" or v.startswith("--fetch-sqlite="):
        fetch=v
    else:
        argv2.append(v)
sys.argv=argv2

if fetch:
    if py3:
        import urllib.request
        urlopen=urllib.request.urlopen
        import io
        bytesio=io.BytesIO
    else:
        import urllib2
        urlopen=urllib2.urlopen
        import cStringIO
        bytesio=cStringIO.StringIO

    # Do we want the 'zip' or full blown version with configure?
    simple=sys.platform in ('win32', 'win64')

    import re
    import zipfile
    URL="http://sqlite.org/download.html"
    if fetch=="--fetch-sqlite":
        # grab index page to figure out latest version available
        write("Fetching", URL)
        page=urlopen(URL).read()
        if py3:
            page=page.decode("iso8859_1")
        match=re.search('"sqlite-amalgamation-3_([0-9]+)_([0-9]+).zip"', page)
        if match:
            ver="3."+match.group(1)+"."+match.group(2)
        else:
            write("Unable to determine current SQLite version.  Use --fetch-sqlite=VERSION", sys.stderr)
            write("to set version (for example --fetch-sqlite=3.6.2", sys.stderr)
            sys.exit(17)
    else:
        ver=fetch[len("--fetch-sqlite="):]
    if simple:
        ver=ver.replace(".", "_")
        AURL="http://sqlite.org/sqlite-amalgamation-%s.zip" % (ver,)
    else:
        AURL="http://www.sqlite.org/sqlite-amalgamation-%s.tar.gz" % (ver,)
    write("Fetching", AURL)
    data=urlopen(AURL).read()
    data=bytesio(data)
    if simple:
        zip=zipfile.ZipFile(data, "r")
        for name in "sqlite3.c", "sqlite3.h", "sqlite3ext.h":
            write("Extracting", name)
            # If you get an exception here then the archive doesn't contain the files it should
            open(name, "wb").write(zip.read(name))
    else:
        # we need to run configure to get various -DHAVE_foo flags on non-windows platforms
        import tarfile
        tar=tarfile.open("nonexistentname to keep old python happy", 'r', data)
        configmember=None
        for member in tar.getmembers():
            tar.extract(member)
            if member.path.endswith("/configure"):
                configmember=member
        tar.close()
        if os.path.exists('sqlite3'):
            for dirpath, dirnames, filenames in os.walk('sqlite3', topdown=False):
                for file in filenames:
                    os.remove(os.path.join(dirpath, file))
                for dir in dirnames:
                    os.rmdir(os.path.join(dirpath, dir))
            os.rmdir('sqlite3')
        # the directory name has changed a bit with each release so try to work out what it is
        if not configmember:
            write("Unable to determine directory it extracted to.", dest=sys.stderr)
            sys.exit(19)
        dirname=configmember.path.split('/')[0]
        os.rename(dirname, 'sqlite3')
        os.chdir('sqlite3')
        write("Running configure to work which flags to compile SQLite with")
        res=os.system("./configure >/dev/null")
        defline=None
        for line in open("Makefile"):
            if line.startswith("DEFS = "):
                defline=line
                break
        if not defline:
            write("Unable to determine compile flags.  Edit the top of sqlite3/sqlite3.c to manually set.", dest=sys.stderr)
            sys.exit(18)
        defs=[]
        import shlex
        for part in shlex.split(defline):
            if part.startswith("-DHAVE"):
                part=part[2:]
                if '=' in part:
                    part=part.split('=', 1)
                else:
                    part=(part, '')
                defs.append(part)
        op=open("sqlite3-fixed.c", "wt")
        for define in defs:
            op.write('#define %s %s\n' % tuple(define))
        op.write(open('sqlite3.c', 'rt').read())
        op.close()
        os.rename("sqlite3-fixed.c", "sqlite3.c")
        os.chdir("..")

depends=["apswversion.h", "pointerlist.c", "statementcache.c", "traceback.c"]
define_macros=[]

# We always want threadsafe
define_macros.append( ('SQLITE_THREADSAFE', '1') )

# We don't want assertions
if "--debug" not in sys.argv:
    define_macros.append( ('NDEBUG', '1') )

# This includes the functionality marked as experimental in SQLite 3.
# Comment out the line to exclude them
define_macros.append( ('EXPERIMENTAL', '1') )

# If you compiled SQLite omitting functionality then specify the same
# defines here.  For example this exlcudes loadable extensions.
#
# define_macros.append( ('SQLITE_OMIT_LOAD_EXTENSION', '1') )

include_dirs=[]
library_dirs=[]

# Look for amalgamation in our directory or in sqlite3 subdirectory
amalgamation=(
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "sqlite3.c"),
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "sqlite3", "sqlite3.c")
    )

usingamalgamation=False
for path in amalgamation:
    if os.path.exists(path):
        if sys.platform=="win32":
            # double quotes get consumed by windows arg processing
            define_macros.append( ('APSW_USE_SQLITE_AMALGAMATION', '\\"'+path+'\\"') )
        else:
            define_macros.append( ('APSW_USE_SQLITE_AMALGAMATION', '"'+path+'"') )
        libraries=[]
        usingamalgamation=True
        depends.append(path)
        write("SQLite: Using amalgamation", path)
        break
    
if not usingamalgamation:
    # if there is a sqlite3 subdirectory then use that, otherwise
    # the system sqlite will be used
    if os.path.exists("sqlite3"):
        include_dirs=["sqlite3"]
        library_dirs=["sqlite3"]
        write("SQLite: Using include/libraries in sqlite3 subdirectory")
    else:
        write("SQLite: Using system sqlite include/libraries")

    libraries=['sqlite3']

    

# setuptools likes to define NDEBUG even when we want debug stuff
if "--debug" in sys.argv:
    define_macros.append( ('APSW_NO_NDEBUG', 1) ) # double negatives are bad
    define_macros.append( ('SQLITE_DEBUG', 1) ) # also does NDEBUG mangling


# work out version number
version=open("apswversion.h", "rtU").read().split()[2].strip('"')

setup(name="apsw",
      version=version,
      description="Another Python SQLite Wrapper",
      long_description=\
"""A Python wrapper for the SQLite embedded relational database engine.
In contrast to other wrappers such as pysqlite it focuses on being
a minimal layer over SQLite attempting just to translate the
complete SQLite API into Python.""",
      author="Roger Binns",
      author_email="rogerb@rogerbinns.com",
      url="http://code.google.com/p/apsw/",
      download_url="http://code.google.com/p/apsw/downloads/list",
      classifiers=[
    "Development Status :: 5 - Production/Stable",
    "Intended Audience :: Developers",
    "License :: OSI Approved :: zlib/libpng License",
    "Operating System :: OS Independent",
    "Programming Language :: C",
    "Topic :: Database :: Front-Ends",
    ],
      keywords=["database", "sqlite"],
      license="OSI Approved :: zlib/libpng License",

      ext_modules=[Extension("apsw",
                             ["apsw.c"],
                             include_dirs=include_dirs,
                             library_dirs=library_dirs,
                             libraries=libraries,
                             define_macros=define_macros,
                             depends=depends)])

