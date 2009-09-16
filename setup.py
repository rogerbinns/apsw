#!/usr/bin/env python

# See the accompanying LICENSE file.

import os
import sys
import shlex
import glob
import re
import zipfile, tarfile

from distutils.core import setup, Extension, Command
from distutils.command import build_ext, build, sdist

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
    # py2 won't allow optional keyword arg on end, so work around it
    dest=sys.stdout
    if args[-1]==sys.stderr:
        dest=args[-1]
        args=args[:-1]
    dest.write(" ".join(args)+"\n")
    dest.flush()

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

fetch_parts=[]

class fetch(Command):
    description="Automatically downloads SQLite and components"
    user_options=[
        ("version=", None, "Which version of SQLite/components to get (default current)"),
        ("sqlite", None, "Download SQLite amalgamation"),
        ("asyncvfs", None, "Download the asynchronous vfs"),
        ("genfkey", None, "Download the generate foreign key triggers code"),
        ("all", None, "Download all downloadable components"),
        ]
    boolean_options=['sqlite', 'asyncvfs', 'genfkey', 'all']

    def initialize_options(self):
        self.version=None
        self.sqlite=False
        self.asyncvfs=False
        self.genfkey=False
        self.all=False

    def finalize_options(self):
        # If all is selected then turn on all components
        global fetch_parts
        if self.all:
            for i in self.boolean_options:
                    setattr(self, i, True)
        for i in self.boolean_options:
            if i!="all":
                fetch_parts.append(i)

    def run(self):
        # work out the version
        if self.version is None:
            write("  Getting download page to work out current SQLite version")
            page=self.download("http://www.sqlite.org/download.html", text=True, checksum=False)
            match=re.search('"sqlite-amalgamation-3([0-9_]+).zip"', page)
            if match:
                self.version="3."+match.group(1)[1:].replace("_", ".")
            else:
                write("Unable to determine current SQLite version.  Use --version=VERSION", sys.stderr)
                write("to set version - eg setup.py fetch --version=3.6.18", sys.stderr)
                sys.exit(17)
            write("    Version is "+self.version)
        # now get each selected component
        downloaded=0

        ## The amalgamation
        if self.sqlite:
            write("  Getting the SQLite amalgamation")
            simple=sys.platform in ('win32', 'win64')

            if simple:
                ver=self.version.replace(".", "_")
                AURL="http://www.sqlite.org/sqlite-amalgamation-%s.zip" % (ver,)
            else:
                AURL="http://www.sqlite.org/sqlite-amalgamation-%s.tar.gz" % (self.version,)

            data=self.download(AURL)

            if simple:
                zip=zipfile.ZipFile(data, "r")
                for name in "sqlite3.c", "sqlite3.h", "sqlite3ext.h":
                    write("Extracting", name)
                    # If you get an exception here then the archive doesn't contain the files it should
                    open(name, "wb").write(zip.read(name))
            else:
                # we need to run configure to get various -DHAVE_foo flags on non-windows platforms
                # if you get an exception here it is likely that you don't have the python zlib module
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
                write("    Running configure to work out SQLite compilation flags")
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
            downloaded+=1

        if self.asyncvfs:
            write("  Getting the async vfs extension")
            data=self.download("http://www.sqlite.org/sqlite-%s.tar.gz" % (self.version,))
            tar=tarfile.open("nonexistentname to keep old python happy", 'r', data)
            lookfor=("sqlite3async.c", "sqlite3async.h")
            found=[0]*len(lookfor)
            for member in tar.getmembers():
                for i,n in enumerate(lookfor):
                    if member.name.endswith("/ext/async/"+n):
                        self.fixupasyncvfs(n, tar.extractfile(member))
                        found[i]+=1
            tar.close()
            if found!=[1]*len(lookfor):
                for i,f in enumerate(lookfor):
                    if found[i]!=1:
                        write("Found %d of %s - should have been exactly one" % (found[i], f), sys.stderr)
                raise ValueError("Unable to correctly get asyncvfs parts")
            downloaded+=1

        if self.genfkey:
            write("  Getting genfkey code")
            data=self.download("http://www.sqlite.org/sqlite-%s.tar.gz" % (self.version,))
            tar=tarfile.open("nonexistentname to keep old python happy", 'r', data)
            found=0
            for member in tar.getmembers():
                if member.name.endswith("/src/shell.c"):
                    self.extractgenfkey(tar.extractfile(member))
                    found+=1
            tar.close()
            if found!=1:
                write("Found shell.c %d times - should have been exactly once" % (found,), sys.stderr)
                raise ValueError("Unable to correctly get genfkey")
            downloaded+=1

        if not downloaded:
            write("You didn't specify any components to fetch.  Use")
            write("   setup.py fetch --help")
            write("for a list and details")
            raise ValueError("No components downloaded")

    def fixupasyncvfs(self, fname, code):
        n=os.path.join(os.path.dirname(__file__), fname)
        # see http://www.sqlite.org/src/info/084941461f
        afs=re.compile(r"^(int asyncFileSize\()")
        proto=re.compile(r"^(\w+\s+sqlite3async_(initialize|shutdown|control|run)\()")
        o=open(n, "wt")
        try:
            for line in code:
                line=afs.sub(r"static \1", line)
                line=proto.sub(r"SQLITE3ASYNC_API \1", line)
                o.write(line)
        except:
            o.close()
            os.remove(n)
            raise

    def extractgenfkey(self, code):
        write("extractgenfkey %d" % (len(code.read()),))
    
    # A function for verifying downloads
    def verifyurl(self, url, data):
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

        write("    Length:", d[0], " SHA1:", d[1], " MD5:", d[2])
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
                    write("    Checksums verified")
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
        write("    (Not verified.  No match in checksums file)")        

    # download a url
    def download(self, url, text=False, checksum=True):
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

        write("    Fetching", url)
        page=urlopen(url).read()

        if text:
            if py3:
                page=page.decode("iso8859_1")

        if checksum:
            self.verifyurl(url, page)

        if not text:
            page=bytesio(page)

        return page

# We allow enable/omit to be specified to build and then pass them to build_ext
build_enable=None
build_omit=None

bparent=build.build
class apsw_build(bparent):
    user_options=bparent.user_options+\
                  [ ("enable=", None, "Enable SQLite options (comma seperated list)"),
                    ("omit=", None, "Omit SQLite functionality (comma seperated list)"),
                    ]

    
    def initialize_options(self):
        v=bparent.initialize_options(self)
        self.enable=None
        self.omit=None
        return v

    def finalize_options(self):
        global build_enable, build_omit
        build_enable=self.enable
        build_omit=self.omit
        return bparent.finalize_options(self)

def findamalgamation():
    amalgamation=(
        os.path.join(os.path.dirname(os.path.abspath(__file__)), "sqlite3.c"),
        os.path.join(os.path.dirname(os.path.abspath(__file__)), "sqlite3", "sqlite3.c")
        )
    for path in amalgamation:
        if os.path.exists(path):
            return path
    return None

def findasyncvfs():
    path=os.path.join(os.path.dirname(os.path.abspath(__file__)), "sqlite3async.c")
    if os.path.exists(path):
        return path
    return None

beparent=build_ext.build_ext
class apsw_build_ext(beparent):

    user_options=beparent.user_options+\
                  [ ("enable=", None, "Enable SQLite options (comma seperated list)"),
                    ("omit=", None, "Omit SQLite functionality (comma seperated list)"),
                    ]

    
    def initialize_options(self):
        v=beparent.initialize_options(self)
        self.enable=build_enable
        self.omit=build_omit
        return v
   
    def finalize_options(self):
        v=beparent.finalize_options(self)

        ext=self.extensions[0]

        if not ext.define_macros: ext.define_macros=[]
        if not ext.depends: ext.depends=[]
        if not ext.include_dirs: ext.include_dirs=[]
        if not ext.library_dirs: ext.library_dirs=[]
        if not ext.libraries: ext.libraries=[]

        # Fixup debug setting
        if self.debug:
            # distutils forces NDEBUG even with --debug so overcome that
            ext.define_macros.append( ('APSW_NO_NDEBUG', '1') ) # double negatives are bad
            ext.define_macros.append( ('SQLITE_DEBUG', '1') ) # also does NDEBUG mangling
        else:
            ext.define_macros.append( ('NDEBUG', '1') )

        # SQLite 3
        # Look for amalgamation in our directory or in sqlite3 subdirectory

        path=findamalgamation()
        if path:
            if sys.platform=="win32":
                # double quotes get consumed by windows arg processing
                ext.define_macros.append( ('APSW_USE_SQLITE_AMALGAMATION', '\\"'+path+'\\"') )
            else:
                ext.define_macros.append( ('APSW_USE_SQLITE_AMALGAMATION', '"'+path+'"') )
            ext.depends.append(path)
            # we also add the directory to include path since icu tries to use it
            ext.include_dirs.append(os.path.dirname(path))
            write("SQLite: Using amalgamation", path)
        else:
            d=os.path.join(os.path.dirname(os.path.abspath(__file__)), "sqlite3")
            if os.path.isdir(d):
                write("SQLite: Using include/libraries in sqlite3 subdirectory")
                ext.include_dirs.append(d)
                ext.library_dirs.append(d)
            else:
                write("SQLite: Using system sqlite include/libraries")
            ext.libraries.append('sqlite3')

        # enables
        addicuinclib=False
        if self.enable:
            for e in self.enable.split(","):
                e=e.strip()
                ext.define_macros.append( ("SQLITE_ENABLE_"+e.upper(), 1) )
                if e.upper()=="ICU":
                    addicuinclib=True
                os.putenv("APSW_TEST_"+e.upper(), "1")
                # See issue #55 where I had left off the 3 in fts3.  This code
                # tries to catch misspelling the name of an extension.
                # However the SQLITE_ENABLE prefix is also used by other
                # options - see http://www.sqlite.org/compile.html but almost
                # all of those have _ in them, so our abbreviated and
                # hopefully future proof test
                if "_" not in e.lower() and \
                       "memsys" not in e.lower() and \
                       e.lower() not in ("fts3", "rtree", "icu", "iotrace", "stat2"):
                    write("Unknown enable "+e, sys.stderr)
                    raise ValueError("Bad enable "+e)

        # omits
        if self.omit:
            for e in self.omit.split(","):
                e=e.strip()
                ext.define_macros.append( ("SQLITE_OMIT_"+e.upper(), 1) )

        # icu
        if addicuinclib:
            foundicu=False
            for part in shlex.split(os.popen("icu-config --cppflags", "r").read()):
                if part.startswith("-I"):
                    ext.include_dirs.append(part[2:])
                    foundicu=True
                elif part.startswith("-D"):
                    part=part[2:]
                    if '=' in part:
                        part=part.split('=', 1)
                    else:
                        part=(part, '1')
                    ext.define_macros.append(part)
                    foundicu=True

            for part in shlex.split(os.popen("icu-config --ldflags", "r").read()):
                if part.startswith("-L"):
                    ext.library_dirs.append(part[2:])
                    foundicu=True
                elif part.startswith("-l"):
                    ext.libraries.append(part[2:])
                    foundicu=True

            if foundicu:
                write("ICU: Added includes, flags and libraries from icu-config")
            else:
                write("ICU: Unable to determine includes/libraries for ICU using icu-config")
                write("ICU: You will need to manually edit setup.py or setup.cfg to set them")

        # asyncvfs
        path=findasyncvfs()
        if path:
            if sys.platform=="win32":
                # double quotes get consumed by windows arg processing
                ext.define_macros.append( ('APSW_USE_SQLITE_ASYNCVFS_C', '\\"'+path+'\\"') )
                ext.define_macros.append( ('APSW_USE_SQLITE_ASYNCVFS_H', '\\"'+path[:-1]+'h\\"') )
            else:
                ext.define_macros.append( ('APSW_USE_SQLITE_ASYNCVFS_C', '"'+path+'"') )
                ext.define_macros.append( ('APSW_USE_SQLITE_ASYNCVFS_H', '"'+path[:-1]+'h"') )
            write("AsyncVFS: "+path)

        # done ...
        return v
   
    def run(self):
        v=beparent.run(self)
        return v

sparent=sdist.sdist
class apsw_sdist(sparent):

    def initialize_options(self):
        sparent.initialize_options(self)
        # Make sure the manifest is regenerated
        self.force_manifest=True

        # Now do some chicanery.  If a source distribution is requested and
        # fetch --sqlite was requested then make sure the sqlite amalgamation
        # ends up as part of the source distribution.
        if fetch_parts:
            # Use a temporary file for the manifest
            tmpmanifest="MANIFEST.in.tmp"
            self.template=tmpmanifest
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
            if "sqlite" in fetch_parts:
                amalgamationpath=findamalgamation()
                amalrelpath=amalgamationpath[len(os.path.dirname(os.path.abspath(__file__)))+1:]
                mout.write("include "+amalrelpath+"\n")
                # also include headers and extension headers
                mout.write("include "+amalrelpath.replace("sqlite3.c", "sqlite3.h")+"\n")
                mout.write("include "+amalrelpath.replace("sqlite3.c", "sqlite3ext.h")+"\n")

            if "asyncvfs" in fetch_parts:
                asyncpath=findasyncvfs()
                asyncpath=asyncpath[len(os.path.dirname(os.path.abspath(__file__)))+1:]
                mout.write("include "+asyncpath+"\n")
                mout.write("include "+asyncpath[:-1]+"h\n")

            mout.close()


# We depend on every .[ch] file in src
depends=[f for f in glob.glob("src/*.[ch]") if f!="src/apsw.c"]
for f in (findamalgamation(), findasyncvfs()):
    if f:
        depends.append(f)


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
                'build_test_extension': build_test_extension,
                'fetch': fetch,
                'build_ext': apsw_build_ext,
                'build': apsw_build,
                'sdist': apsw_sdist}
      )

