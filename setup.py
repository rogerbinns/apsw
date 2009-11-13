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
        shared="shared"
        if sys.platform.startswith("darwin"):
            shared="bundle"
        res=os.system("gcc -fPIC -%s -o testextension.sqlext -Isqlite3 -I. src/testextension.c" % (shared,))
        if res!=0:
            raise RuntimeError("Building test extension failed")


# deal with various python version compatibility issues with how
# to treat returned web data as lines of text
def fixupcode(code):
    if sys.version_info<(2,5):
        if type(code)!=str:
            code=code.read()
        return [l+"\n" for l in code.split("\n")]
    if sys.version_info>=(3,0):
        if type(code)!=bytes:
            code=code.read()
        if type(code)==bytes:
            code=code.decode("iso8859-1")
        return [l+"\n" for l in code.split("\n")]
    return code

fetch_parts=[]

class fetch(Command):
    description="Automatically downloads SQLite and components"
    user_options=[
        ("version=", None, "Which version of SQLite/components to get (default current)"),
        ("missing-checksum-ok", None, "Continue on a missing checksum (default abort)"),
        ("sqlite", None, "Download SQLite amalgamation"),
        ("asyncvfs", None, "Download the asynchronous vfs"),
        ("all", None, "Download all downloadable components"),
        ]
    fetch_options=['sqlite', 'asyncvfs']
    boolean_options=fetch_options+['all', 'missing-checksum-ok']

    def initialize_options(self):
        self.version=None
        self.sqlite=False
        self.asyncvfs=False
        self.all=False
        self.missing_checksum_ok=False

    def finalize_options(self):
        # If all is selected then turn on all components
        global fetch_parts
        if self.all:
            for i in self.fetch_options:
                    setattr(self, i, True)
        for i in self.fetch_options:
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
            if self.version=="fossil":
                write("  Getting current trunk from fossil")
            else:
                write("  Getting the SQLite amalgamation")
            simple=sys.platform in ('win32', 'win64')

            if simple:
                ver=self.version.replace(".", "_")
                AURL="http://www.sqlite.org/sqlite-amalgamation-%s.zip" % (ver,)
            else:
                AURL="http://www.sqlite.org/sqlite-amalgamation-%s.tar.gz" % (self.version,)

            checksum=True
            if self.version=="fossil":
                simple=False
                AURL="http://www.sqlite.org/src/zip/sqlite3.zip?uuid=trunk"
                checksum=False

            data=self.download(AURL, checksum=checksum)

            if simple:
                zip=zipfile.ZipFile(data, "r")
                for name in "sqlite3.c", "sqlite3.h", "sqlite3ext.h":
                    write("Extracting", name)
                    # If you get an exception here then the archive doesn't contain the files it should
                    open(name, "wb").write(zip.read(name))
                zip.close()
            else:
                # we need to run configure to get various -DHAVE_foo flags on non-windows platforms
                # delete existing sqlite3 directory if it exists, but save sqlite3config.h if it exists
                sqlite3config_h=None
                if os.path.exists("sqlite3/sqlite3config.h"):
                    sqlite3config_h=open("sqlite3/sqlite3config.h", "rb").read()
                if os.path.exists('sqlite3'):
                    for dirpath, dirnames, filenames in os.walk('sqlite3', topdown=False):
                        for file in filenames:
                            os.remove(os.path.join(dirpath, file))
                        for dir in dirnames:
                            os.rmdir(os.path.join(dirpath, dir))
                    os.rmdir('sqlite3')
                if self.version=="fossil":
                    zip=zipfile.ZipFile(data, "r")
                    for name in zip.namelist():
                        # extract
                        if name.endswith("/"):
                            os.mkdir(name)
                        else:
                            open(name, "wb").write(zip.read(name))
                    zip.close()
                else:
                    # if you get an exception here it is likely that you don't have the python zlib module
                    import zlib
                    tar=tarfile.open("nonexistentname to keep old python happy", 'r', data)
                    configmember=None
                    for member in tar.getmembers():
                        tar.extract(member)
                        # find first file named configure
                        if not configmember and member.name.endswith("/configure"):
                            configmember=member
                    tar.close()
                    # the directory name has changed a bit with each release so try to work out what it is
                    if not configmember:
                        write("Unable to determine directory it extracted to.", dest=sys.stderr)
                        sys.exit(19)
                    dirname=configmember.name.split('/')[0]
                    os.rename(dirname, 'sqlite3')
                os.chdir('sqlite3')
                if self.version=="fossil":
                    write("    Building amalgamation from fossil")
                    res=os.system("make TOP=. -f Makefile.linux-gcc sqlite3.c && cp src/sqlite3ext.h .")
                    defs=[]
                    if sqlite3config_h:
                        open("sqlite3config.h", "wb").write(sqlite3config_h)
                else:
                    write("    Running configure to work out SQLite compilation flags")
                    res=os.system("./configure >/dev/null")
                    defline=None
                    for line in open("Makefile"):
                        if line.startswith("DEFS = "):
                            defline=line
                            break
                    if not defline:
                        write("Unable to determine compile flags.  Create sqlite3/sqlite3config.h to manually set.", sys.stderr)
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
                if res!=0:
                    raise ValueError("Command execution failed")
                if defs:
                    op=open("sqlite3config.h", "wt")
                    op.write("""
/* This file was generated by parsing how configure altered the Makefile
   which isn't used when building python extensions.  It is specific to the
   machine and developer components on which it was run. */
   \n""")
                               
                    for define in defs:
                        op.write('#define %s %s\n' % tuple(define))
                    op.close()
                os.chdir("..")
            downloaded+=1

        if self.asyncvfs:
            write("  Getting the async vfs extension")
            if self.version=="fossil":
                AURL="http://www.sqlite.org/src/zip/sqlite3.zip?uuid=trunk"
            else:
                AURL="http://www.sqlite.org/sqlite-%s.tar.gz" % (self.version,)
            data=self.download(AURL, checksum=not self.version=="fossil")

            if self.version=="fossil":
                archive=zipfile.ZipFile(data, "r")
                members=archive.namelist()
                extractfile=archive.read
            else:
                import zlib
                archive=tarfile.open("nonexistentname to keep old python happy", 'r', data)
                members=[a.name for a in archive.getmembers()]
                extractfile=archive.extractfile
                
            lookfor=("sqlite3async.c", "sqlite3async.h")
            found=[0]*len(lookfor)
            for member in members:
                for i,n in enumerate(lookfor):
                    if member.endswith("/ext/async/"+n):
                        self.fixupasyncvfs(n, extractfile(member))
                        found[i]+=1
                        
            archive.close()
            if found!=[1]*len(lookfor):
                for i,f in enumerate(lookfor):
                    if found[i]!=1:
                        write("Found %d of %s - should have been exactly one" % (found[i], f), sys.stderr)
                raise ValueError("Unable to correctly get asyncvfs parts")
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
            for line in fixupcode(code):
                line=afs.sub(r"static \1", line)
                line=proto.sub(r"SQLITE3ASYNC_API \1", line)
                o.write(line)
            o.close()
        except:
            o.close()
            os.remove(n)
            raise

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
        if not self.missing_checksum_ok:
            raise ValueError("No checksum available.  Use --missing-checksum-ok option to continue")

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
build_enable_all_extensions=False

bparent=build.build
class apsw_build(bparent):
    user_options=bparent.user_options+\
                  [ ("enable=", None, "Enable SQLite options (comma seperated list)"),
                    ("omit=", None, "Omit SQLite functionality (comma seperated list)"),
                    ("enable-all-extensions", None, "Enable all SQLite extensions"),
                    ]
    boolean_options=bparent.boolean_options+["enable-all-extensions"]
    
    def initialize_options(self):
        v=bparent.initialize_options(self)
        self.enable=None
        self.omit=None
        self.enable_all_extensions=build_enable_all_extensions
        return v

    def finalize_options(self):
        global build_enable, build_omit, build_enable_all_extensions
        build_enable=self.enable
        build_omit=self.omit
        build_enable_all_extensions=self.enable_all_extensions
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

def find_in_path(name):
    for loc in os.getenv("PATH").split(os.pathsep):
        f=os.path.abspath(os.path.join(loc, name))
        if os.path.exists(f) or os.path.exists(f.lower()) or os.path.exists(f.lower()+".exe"):
            return f
    return None

beparent=build_ext.build_ext
class apsw_build_ext(beparent):

    user_options=beparent.user_options+\
                  [ ("enable=", None, "Enable SQLite options (comma seperated list)"),
                    ("omit=", None, "Omit SQLite functionality (comma seperated list)"),
                    ("enable-all-extensions", None, "Enable all SQLite extensions"),
                    ]
    boolean_options=beparent.boolean_options+["enable-all-extensions"]

    
    def initialize_options(self):
        v=beparent.initialize_options(self)
        self.enable=build_enable
        self.omit=build_omit
        self.enable_all_extensions=build_enable_all_extensions
        return v
   
    def finalize_options(self):
        v=beparent.finalize_options(self)

        if self.enable_all_extensions:
            exts=["fts3", "fts3_parenthesis", "rtree"]
            if find_in_path("icu-config"):
                exts.append("icu")
            if not self.enable:
                self.enable=",".join(exts)
            else:
                self.enable=self.enable+","+",".join(exts)

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

        # fork checker?
        if hasattr(os, "fork"):
            ext.define_macros.append( ('APSW_FORK_CHECKER', '1') )

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
            load_extension=True
        else:
            load_extension=False
            d=os.path.join(os.path.dirname(os.path.abspath(__file__)), "sqlite3")
            if os.path.isdir(d):
                write("SQLite: Using include/libraries in sqlite3 subdirectory")
                ext.include_dirs.append(d)
                ext.library_dirs.append(d)
            else:
                write("SQLite: Using system sqlite include/libraries")
            ext.libraries.append('sqlite3')

        s3config=os.path.join(os.path.dirname(os.path.abspath(__file__)), "sqlite3", "sqlite3config.h")
        if os.path.exists(s3config):
            if sys.platform=="win32":
               ext.define_macros.append( ('APSW_USE_SQLITE_CONFIG', '\\"'+s3config+'\\"') )
            else:
                ext.define_macros.append( ('APSW_USE_SQLITE_CONFIG', '"'+s3config+'"') )

        # enables
        addicuinclib=False
        if self.enable:
            for e in self.enable.split(","):
                e=e.strip()
                if e.lower()=="load_extension":
                    load_extension=True
                    continue
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
                if e.lower()=="load_extension":
                    load_extension=False
                ext.define_macros.append( ("SQLITE_OMIT_"+e.upper(), 1) )

        if not load_extension:
            ext.define_macros.append( ("SQLITE_OMIT_LOAD_EXTENSION", 1) )

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

        # shell
        if not os.path.exists("src/shell.c") or \
               os.path.getmtime("src/shell.c")<os.path.getmtime("tools/shell.py") or \
               os.path.getmtime(__file__)>os.path.getmtime("src/shell.c"):
            create_c_file("tools/shell.py", "src/shell.c")

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
                if os.path.exists("sqlite3/sqlite3config.h"):
                    mout.write("include sqlite3/sqlite3config.h\n")

            if "asyncvfs" in fetch_parts:
                asyncpath=findasyncvfs()
                asyncpath=asyncpath[len(os.path.dirname(os.path.abspath(__file__)))+1:]
                mout.write("include "+asyncpath+"\n")
                mout.write("include "+asyncpath[:-1]+"h\n")

            mout.close()

def create_c_file(src, dest):
    # Transforms Python src into C dest as a sequence of strings.
    # Because of the pathetic microsoft compiler we have to break it
    # up into small chunks
    out=["/* Automatically generated by setup.py from "+src+" */", ""]
    percents=1
    size=0
    for line in open(src, "rtU"):
        if "if__name__=='__main__':" in line.replace(" ",""):
            break
        if line.strip().startswith('#'): # full line comment
            continue
        if line.strip()=="import apsw":
            continue
        size=size+len(line)
        comma=size>32000
        if comma:
            size=0
            percents+=1
        line=line.replace("\\", "\\\\").\
              replace('"', '\\"')
        out.append('  "'+line.rstrip()+'\\n"')
        if comma:
            out[-1]=out[-1]+","
    if out[-1].endswith(","):
        out[-1]=out[-1][:-1]
    out[1]='"%s",' % ("%s" * percents,)
    open(dest, "wt").write("\n".join(out))


# We depend on every .[ch] file in src
depends=[f for f in glob.glob("src/*.[ch]") if f!="src/apsw.c"]
for f in (findamalgamation(), findasyncvfs()):
    if f:
        depends.append(f)
# we produce a .c file from this
depends.append("tools/shell.py")

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
    "License :: OSI Approved :: Any",
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

