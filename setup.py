import os

from distutils.core import setup, Extension

define_macros=[]

# Excludes the assertions
define_macros.append( ('NDEBUG', '1') )
# We always want threadsafe
define_macros.append( ('SQLITE_THREADSAFE', '1') )

# This includes the functionality marked as experimental in SQLite 3.
# Comment out the line to exclude them
define_macros.append( ('EXPERIMENTAL', '1') )

include_dirs=[]
library_dirs=[]

# if sqlite3.c (amalgamation is in the sqlite3 sub-directory then that is used)
amalgamation=os.path.join(os.path.dirname(os.path.abspath(__file__)), "sqlite3", "sqlite3.c")

if os.path.exists(amalgamation):
    define_macros.append( ('APSW_USE_SQLITE_AMALGAMATION', '"'+amalgamation+'"') )
    libraries=[]
else:
    # if there is a sqlite3 subdirectory then use that, otherwise
    # the system sqlite will be used
    if os.path.exists("sqlite3"):
        include_dirs=["sqlite3"]
        library_dirs=["sqlite3"]

    libraries=['sqlite3']

# work out version number
version=open("apswversion.h", "rtU").read().split()[2].strip('"')

setup(name="apsw",
      version=version,
      author="Roger Binns",
      author_email="rogerb@rogerbinns.com",
      description="Another Python SQLite Wrapper",

      ext_modules=[Extension("apsw",
                             ["apsw.c"],
                             include_dirs=include_dirs,
                             library_dirs=library_dirs,
                             libraries=libraries,
                             define_macros=define_macros)])

