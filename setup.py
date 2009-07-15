#!/usr/bin/env python

import os
import sys
import shlex
import glob

from distutils.core import setup, Extension, Command

##
## Do your customizations here or by creating a setup.cfg as documented at
## http://www.python.org/doc/2.5.2/dist/setup-config.html
##

include_dirs=['src']
library_dirs=[]
define_macros=[]
libraries=[]

# This includes the functionality marked as experimental in SQLite 3.
# Comment out the line to exclude them
define_macros.append( ('EXPERIMENTAL', '1') )

##
## End of customizations
##

# python 2 and 3 print equivalent
def write(*args):
    # py2 won't allow keyword arg on end, so work around it
    dest=sys.stdout
    if args[-1]==sys.stderr:
        dest=args[-1]
        args=args[:-1]
    dest.write(" ".join(args)+"\n")
    dest.flush()


addicuinclib=False
# Look for various omits and enables
argv2=[]
for v in sys.argv:
    if v.startswith("--enable="):
        what=v[len("--enable="):]
        define_macros.append( ('SQLITE_ENABLE_'+what.upper(), 1) )
        if what.upper()=="ICU":
            addicuinclib=True
        os.putenv("APSW_TEST_"+what.upper(), "1")
        # See issue #55 where I had left of the 3 in fts3.  This code
        # tries to catch misspelling the name of an extension.
        # However the SQLITE_ENABLE prefix is also used by other
        # options - see http://www.sqlite.org/compile.html but almost
        # all of those have _ in them, so our abbreviated and
        # hopefully future proof test
        if "_" not in what.lower() and \
           "memsys" not in what.lower() and \
           what.lower() not in ("fts3", "rtree", "icu", "iotrace"):
            write("Unknown enable "+what, sys.stderr)
            sys.exit(1)
    elif v.startswith("--omit="):
        define_macros.append( ('SQLITE_OMIT_'+v[len("--omit="):].upper(), 1) )
    else:
        argv2.append(v)
sys.argv=argv2



py3=sys.version_info>=(3,0)

# Run test suite
class run_tests(Command):

    description="Run test suite"


    # I did originally try using 'verbose' as the option but it turns
    # out that is builtin and defaults to 1 (--quiet is also builtin
    # and forces verbose to 0)
    user_options=[
        ("show-tests", "s", "Show each test being run"),
        ]

    # see if you can find boolean_options documented anywhere
    boolean_options=['show-tests']

    def initialize_options(self):
        self.show_tests=0

    def finalize_options(self):
        pass
    
    def run(self):
        import unittest
        import tests
        tests.setup()
        suite=unittest.TestLoader().loadTestsFromModule(tests)
        # verbosity of zero doesn't print anything, one prints a dot
        # per test and two prints each test name
        result=unittest.TextTestRunner(verbosity=self.show_tests+1).run(suite)
        if not result.wasSuccessful():
            sys.exit(1)

# A hack we dont't document
class build_test_extension(Command):
    description="Compiles APSW test loadable extension"


    user_options=[]

    def initialize_options(self):
        pass
    def finalize_options(self):
        pass
    def run(self):
        os.system("gcc -fPIC -shared -o testextension.sqlext -Isqlite3 -I. src/testextension.c")

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
    # A function for verifying downloads
    def verifyurl(url, data):
        d=["%s" % (len(data),)]
        try:
            import hashlib
            d.append(hashlib.sha1(data).hexdigest())
            d.append(hashlib.md5(data).hexdigest())
        except ImportError:
            import sha
            d.append(sha.new(data).hexdigest())
            import md5
            d.append(md5.new(data).hexdigest())

        write("Length:", d[0], " SHA1:", d[1], " MD5:", d[2])
        sums=os.path.join(os.path.dirname(__file__), "checksums")
        for line in open(sums, "rtU"):
            line=line.strip()
            if len(line)==0 or line[0]=="#":
                continue
            l=[l.strip() for l in line.split()]
            if len(l)!=4:
                write("Invalid line in checksums file:", line, sys.stderr)
                raise ValueError("Bad checksums file")
            if l[0]==url:
                if l[1:]==d:
                    write("Checksums verified")
                    return
                if l[1]!=d[0]:
                    write("Length does not match.  Expected", l[1], "download was", d[0])
                if l[2]!=d[1]:
                    write("SHA does not match.  Expected", l[2], "download was", d[1])
                if l[3]!=d[2]:
                    write("MD5 does not match.  Expected", l[3], "download was", d[2])
                write("The download does not match the checksums distributed with APSW.\n"
                      "The download should not have changed since the checksums were\n"
                      "generated.  The cause could be anything from network corruption\n"
                      "to a malicious attack.")
                raise ValueError("Checksums do not match")
        # no matching line
        write("(Not verified.  No match in checksums file)")


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
        match=re.search('"sqlite-amalgamation-3([0-9_]+).zip"', page)
        if match:
            ver="3."+match.group(1)[1:].replace("_", ".")
        else:
            write("Unable to determine current SQLite version.  Use --fetch-sqlite=VERSION", sys.stderr)
            write("to set version (for example --fetch-sqlite=3.6.2", sys.stderr)
            sys.exit(17)
    else:
        ver=fetch[len("--fetch-sqlite="):]
    if simple:
        ver=ver.replace(".", "_")
        AURL="http://www.sqlite.org/sqlite-amalgamation-%s.zip" % (ver,)
    else:
        AURL="http://www.sqlite.org/sqlite-amalgamation-%s.tar.gz" % (ver,)
    write("Fetching", AURL)
    data=urlopen(AURL).read()
    verifyurl(AURL, data)
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
            # find first file named configure
            if not configmember and member.name.endswith("/configure"):
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
        dirname=configmember.name.split('/')[0]
        os.rename(dirname, 'sqlite3')
        os.chdir('sqlite3')
        write("Running configure to work out SQLite compilation flags")
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
        for part in shlex.split(defline):
            if part.startswith("-DHAVE"):
                part=part[2:]
                if '=' in part:
                    part=part.split('=', 1)
                else:
                    part=(part, )
                defs.append(part)
        op=open("sqlite3-fixed.c", "wt")
        for define in defs:
            op.write('#define %s %s\n' % tuple(define))
        op.write(open('sqlite3.c', 'rt').read())
        op.close()
        os.rename("sqlite3-fixed.c", "sqlite3.c")
        os.chdir("..")

# We depend on every .[ch] file in src
depends=[f for f in glob.glob("src/*.[ch]") if f!="src/apsw.c"]


# We don't want assertions
if "--debug" not in sys.argv:
    define_macros.append( ('NDEBUG', '1') )


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
        usingamalgamation=True
        depends.append(path)
        # we also add the directory to include path since icu tries to use it
        include_dirs.append(os.path.dirname(path))
        write("SQLite: Using amalgamation", path)
        amalgamationpath=path
        break
    
if not usingamalgamation:
    # if there is a sqlite3 subdirectory then use that, otherwise
    # the system sqlite will be used
    if os.path.exists("sqlite3"):
        include_dirs.append("sqlite3")
        library_dirs.append("sqlite3")
        write("SQLite: Using include/libraries in sqlite3 subdirectory")
    else:
        write("SQLite: Using system sqlite include/libraries")

    libraries.append('sqlite3')

# setuptools likes to define NDEBUG even when we want debug stuff
if "--debug" in sys.argv:
    define_macros.append( ('APSW_NO_NDEBUG', '1') ) # double negatives are bad
    define_macros.append( ('SQLITE_DEBUG', '1') ) # also does NDEBUG mangling

# add stuff for 3rd party components?
if addicuinclib:
    foundstuff=False
    for part in shlex.split(os.popen("icu-config --cppflags", "r").read()):
        if part.startswith("-I"):
            include_dirs.append(part[2:])
            foundstuff=True
        elif part.startswith("-D"):
            part=part[2:]
            if '=' in part:
                part=part.split('=', 1)
            else:
                part=(part, '1')
            define_macros.append(part)
            foundstuff=True

    for part in shlex.split(os.popen("icu-config --ldflags", "r").read()):
        if part.startswith("-L"):
            library_dirs.append(part[2:])
            foundstuff=True
        elif part.startswith("-l"):
            libraries.append(part[2:])
            foundstuff=True

    if not foundstuff:
        write("ICU: Unable to determine includes/libraries for ICU using icu-config")
        write("ICU: You will need to manually edit setup.py to set them")

# Now do some chicanery.  If a source distribution is requested and
# --fetch-sqlite was requested then make sure the sqlite amalgamation
# ends up as part of the source distribution.

if "sdist" in sys.argv:
    pos=sys.argv.index("sdist")+1
    # Make sure the manifest is regenerated
    sys.argv.insert(pos, "--force-manifest")
    if fetch and usingamalgamation:
        # Use a temporary file for the manifest
        tmpmanifest="MANIFEST.in.tmp"
        sys.argv.insert(pos, "--template="+tmpmanifest)
        try:
            os.remove(tmpmanifest)
        except:
            pass
        min=open("MANIFEST.in", "rU")
        mout=open(tmpmanifest, "wt")
        for line in min:
            mout.write(line)
        min.close()
        # os.path.relpath emulation
        amalrelpath=amalgamationpath[len(os.path.dirname(os.path.abspath(__file__)))+1:]
        mout.write("include "+amalrelpath+"\n")
        # also include headers and extension headers
        mout.write("include "+amalrelpath.replace("sqlite3.c", "sqlite3.h")+"\n")
        mout.write("include "+amalrelpath.replace("sqlite3.c", "sqlite3ext.h")+"\n")
        mout.close()

# work out version number
version=open("src/apswversion.h", "rtU").read().split()[2].strip('"')

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
                             ["src/apsw.c"],
                             include_dirs=include_dirs,
                             library_dirs=library_dirs,
                             libraries=libraries,
                             define_macros=define_macros,
                             depends=depends)],


      cmdclass={'test': run_tests,
                'build_test_extension': build_test_extension}
      )

