#!/usr/bin/python

# See the accompanying LICENSE file.

"""This file runs the test suite against several versions of SQLite
and Python to make sure everything is ok in the various combinations.
It only runs on a UNIX like environment.

You should make sure that wget is using a proxy so you don't hit the
upstream sites repeatedly and have ccache so that compiles are
quicker.

All the work is done in parallel rather than serially.  This allows
for it to finish a lot sooner.

"""

import os
import re
import sys
import threading
import Queue
import optparse
import traceback

def run(cmd):
    status=os.system(cmd)
    if os.WIFEXITED(status):
        code=os.WEXITSTATUS(status)
        if code==0:
            return
        raise Exception("Exited with code "+`code`+": "+cmd)
    raise Exception("Failed with signal "+`os.WTERMSIG(status)`+": "+cmd)

def dotest(pyver, logdir, pybin, pylib, workdir, sqlitever):
    couchenv=""
    if couchp(pyver):
        couchenv="\"APSW_COUCHDB="+couchdb+'"'
    run("cd %s ; env %s LD_LIBRARY_PATH=%s %s setup.py fetch --version=%s --all build_test_extension build_ext --inplace --force --enable-all-extensions test -v >%s 2>&1" % (workdir, couchenv, pylib, pybin, sqlitever, os.path.abspath(os.path.join(logdir, "buildruntests.txt"))))

def runtest(workdir, pyver, ucs, sqlitever, logdir):
    pybin, pylib=buildpython(workdir, pyver, ucs, os.path.abspath(os.path.join(logdir, "pybuild.txt")))
    dotest(pyver, logdir, pybin, pylib, workdir, sqlitever)

def threadrun(queue):
    while True:
        d=queue.get()
        if d is None:
            return
        try:
            runtest(**d)
            sys.stdout.write(".")
            sys.stdout.flush()
        except:
            # uncomment to debug problems with this script
            #traceback.print_exc()
            print "\nFAILED", d
        
def main(PYVERS, UCSTEST, SQLITEVERS, concurrency):
    print "Test starting"
    os.system("rm -rf apsw.so megatestresults 2>/dev/null ; mkdir megatestresults")
    print "  ... removing old work directory"
    workdir=os.path.abspath("work")
    os.system("rm -rf %s/* 2>/dev/null ; mkdir -p %s" % (workdir, workdir))
    os.system('rm -rf $HOME/.local/lib/python*/site-packages/apsw* 2>/dev/null')
    print "      done"

    queue=Queue.Queue()
    threads=[]

    for pyver in PYVERS:
        for ucs in UCSTEST:
            if pyver=="system":
                if ucs!=2: continue
                ucs=0
            for sqlitever in SQLITEVERS:
                print "Python",pyver,"ucs",ucs,"   SQLite",sqlitever
                workdir=os.path.abspath(os.path.join("work", "py%s-ucs%d-sq%s" % (pyver, ucs, sqlitever)))
                logdir=os.path.abspath(os.path.join("megatestresults", "py%s-ucs%d-sq%s" % (pyver, ucs, sqlitever)))
                run("mkdir -p %s/src %s/tools %s" % (workdir, workdir, logdir))
                run("cp *.py checksums "+workdir)
                run("cp tools/*.py "+workdir+"/tools/")
                run("cp src/*.c src/*.h "+workdir+"/src/")

                queue.put({'workdir': workdir, 'pyver': pyver, 'ucs': ucs, 'sqlitever': sqlitever, 'logdir': logdir})

    threads=[]
    for i in range(concurrency):
        queue.put(None) # exit sentinel
        t=threading.Thread(target=threadrun, args=(queue,))
        t.start()
        threads.append(t)

    print "All builds started, now waiting for them to finish (%d concurrency)" % (concurrency,)
    for t in threads:
        t.join()
    print "\nFinished"

def getpyurl(pyver):
    dirver=pyver
    if 'a' in dirver:
        dirver=dirver.split('a')[0]
    elif 'b' in dirver:
        dirver=dirver.split('b')[0]
    elif 'rc' in dirver:
        dirver=dirver.split('rc')[0]
    if pyver>'2.3.0':
        # Upper or lower case 'p' in download filename is somewhat random
        p='P'
        if pyver in ("3.1rc2",):
            p='p'
        return "http://python.org/ftp/python/%s/%sython-%s.tar.bz2" % (dirver,p,pyver)
    if pyver=='2.3.0':
        pyver='2.3'
        dirver='2.3'
    return "http://python.org/ftp/python/%s/Python-%s.tgz" % (dirver,pyver)

def buildpython(workdir, pyver, ucs, logfilename):
    if pyver=="system": return "/usr/bin/python", ""
    url=getpyurl(pyver)
    if url.endswith(".bz2"):
        tarx="j"
    else:
        tarx="z"
    if pyver=="2.3.0": pyver="2.3"    
    run("cd %s ; mkdir pyinst ; ( echo \"Getting %s\"; wget -q %s -O - | tar xf%s -  ) > %s 2>&1" % (workdir, url, url, tarx, logfilename))
    # See https://bugs.launchpad.net/ubuntu/+source/gcc-defaults/+bug/286334
    if pyver.startswith("2.3"):
        # https://bugs.launchpad.net/bugs/286334
        opt='BASECFLAGS=-U_FORTIFY_SOURCE'
    else:
        opt=''
    if pyver.startswith("3.0"):
        full="full" # 3.1 rc 1 doesn't need 'fullinstall'
    else:
        full=""
    run("cd %s ; cd *ython-%s ; ./configure %s --disable-ipv6 --enable-unicode=ucs%d --prefix=%s/pyinst  >> %s 2>&1; make >>%s 2>&1; make  %sinstall >>%s 2>&1 ; make clean >/dev/null" % (workdir, pyver, opt, ucs, workdir, logfilename, logfilename, full, logfilename))
    suf=""
    if pyver>="3.1":
        suf="3"
    pybin=os.path.join(workdir, "pyinst", "bin", "python"+suf)
    # couchdb
    if couchp(pyver):
        run("(cd %s ; wget -q -O - '%s' | tar xfz - ; cd setuptools* ; %s setup.py install ; `dirname \"%s\"`/easy_install CouchDB ) >>%s 2>&1" % 
            (workdir, 'http://pypi.python.org/packages/source/s/setuptools/setuptools-0.6c11.tar.gz#md5=7df2a529a074f613b509fb44feefe74e',
             pybin, pybin, logfilename))
    return pybin, os.path.join(workdir, "pyinst", "lib")
    
# Default versions we support
PYVERS=(
    '3.1.2',
    '2.7b1',
    '2.6.5',
    '2.5.4',
    '2.4.6',
    '2.3.7',
    'system',
    # '2.2.3',  - apsw not supported on 2.2 as it needs GILstate
    )

SQLITEVERS=(
    '3.6.23.1',
    '3.6.23',
   )

def couchp(pyver):
    # should we try to support couchdb?
    if couchdb and pyver=="system":
        try:
            import couchdb as ignored
            import httplib2
            return True
        except ImportError:
            return False
    return pyver<"3" and pyver>="2.4" and couchdb

if __name__=='__main__':
    nprocs=0
    try:
        # try and work out how many processors there are - this works on linux
        for line in open("/proc/cpuinfo", "rt"):
            line=line.split()
            if line and line[0]=="processor":
                nprocs+=1
    except:
        pass
    # well there should be at least one!
    if nprocs==0:
        nprocs=1

    concurrency=nprocs*2

    parser=optparse.OptionParser()
    parser.add_option("--pyvers", dest="pyvers", help="Which Python versions to test against [%default]", default=",".join(PYVERS))
    parser.add_option("--sqlitevers", dest="sqlitevers", help="Which SQLite versions to test against [%default]", default=",".join(SQLITEVERS))
    parser.add_option("--fossil", dest="fossil", help="Also test current SQLite FOSSIL version [%default]", default=False, action="store_true")
    parser.add_option("--ucs", dest="ucs", help="Unicode character widths to test in bytes [%default]", default="2,4")
    parser.add_option("--tasks", dest="concurrency", help="Number of simultaneous builds/tests to run [%default]", default=concurrency)
    parser.add_option("--couchdb", dest="couchdb", help="URL to couchdb server", default=None)

    options,args=parser.parse_args()

    if args:
        parser.error("Unexpected options "+str(options))

    pyvers=options.pyvers.split(",")
    sqlitevers=options.sqlitevers.split(",")
    if options.fossil:
        sqlitevers.append("fossil")
    ucstest=[int(x) for x in options.ucs.split(",")]
    concurrency=int(options.concurrency)
    sqlitevers=[x for x in sqlitevers if x]
    couchdb=options.couchdb
    main(pyvers, ucstest, sqlitevers, concurrency)
