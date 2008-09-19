import os
import sys

from distutils.core import setup, Extension

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
    # python 2 and 3 print equivalent
    def write(*args):
        # py2 won't allow keyword arg on end, so work around it
        dest=sys.stdout
        if args[-1]==sys.stderr:
            dest=args[-1]
            args=args[:-1]
        dest.write(" ".join(args)+"\n")
        dest.flush()
    import urllib2
    import re
    import cStringIO
    import zipfile
    URL="http://sqlite.org/download.html"
    if fetch=="--fetch-sqlite":
        # grab index page to figure out latest version available
        write("Fetching", URL)
        page=urllib2.urlopen(URL).read()
        match=re.search('"sqlite-amalgamation-3_([0-9]+)_([0-9]+).zip"', page)
        if match:
            ver="3."+match.group(1)+"."+match.group(2)
        else:
            write("Unable to determine current SQLite version.  Use --fetch-sqlite=VERSION", sys.stderr)
            write("to set version (for example --fetch-sqlite=3.6.2", sys.stderr)
            sys.exit(17)
    else:
        ver=fetch[len("--fetch-sqlite="):]
    ver=ver.replace(".", "_")
    AURL="http://sqlite.org/sqlite-amalgamation-%s.zip" % (ver,)
    write("Fetching", AURL)
    data=urllib2.urlopen(AURL).read()
    data=cStringIO.StringIO(data)
    zip=zipfile.ZipFile(data, "r")
    for name in "sqlite3.c", "sqlite3.h", "sqlite3ext.h":
        write("Extracting", name)
        # If you get an exception here then the archive doesn't contain the files it should
        open(name, "wb").write(zip.read(name))


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
        break
    
if not usingamalgamation:
    # if there is a sqlite3 subdirectory then use that, otherwise
    # the system sqlite will be used
    if os.path.exists("sqlite3"):
        include_dirs=["sqlite3"]
        library_dirs=["sqlite3"]

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

