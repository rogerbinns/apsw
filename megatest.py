#!/usr/bin/python

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

def run(cmd):
    status=os.system(cmd)
    if os.WIFEXITED(status):
        code=os.WEXITSTATUS(status)
        if code==0:
            return
        raise Exception("Exited with code "+`code`+": "+cmd)
    raise Exception("Failed with signal "+`os.WTERMSIG(status)`+": "+cmd)


def dotest(logdir, pybin, workdir, sqlitever):
    buildsqlite(workdir, sqlitever, os.path.abspath(os.path.join(logdir, "sqlitebuild.txt")))
    buildapsw(os.path.abspath(os.path.join(logdir, "buildapsw.txt")), pybin, workdir)
    # now the actual tests
    run("cd %s ; %s tests.py >%s 2>&1" % (workdir, pybin, os.path.abspath(os.path.join(logdir, "runtests.txt"))))


def runtest(workdir, pyver, ucs, sqlitever, logdir):
    pybin=buildpython(workdir, pyver, ucs, os.path.abspath(os.path.join(logdir, "pybuild.txt")))
    dotest(logdir, pybin, workdir, sqlitever)

def main():
    print "Test starting"
    os.system("rm -rf apsw.so megatestresults 2>/dev/null ; mkdir megatestresults")
    print "  ... removing old work directory"
    workdir=os.path.abspath("work")
    os.system("rm -rf %s 2>/dev/null ; mkdir %s" % (workdir, workdir))
    print "      done"
    threads=[]

    for pyver in PYVERS:
        for ucs in (2,4):
            if pyver=="system":
                if ucs!=2: continue
                ucs=0
            for sqlitever in SQLITEVERS:
                print "Python",pyver,"ucs",ucs,"   SQLite",sqlitever
                workdir=os.path.abspath(os.path.join("work", "py%s-ucs%d-sq%s" % (pyver, ucs, sqlitever)))
                logdir=os.path.abspath(os.path.join("megatestresults", "py%s-ucs%d-sq%s" % (pyver, ucs, sqlitever)))
                run("mkdir -p %s %s" % (workdir, logdir))
                run("cp *.py *.c *.h "+workdir)

                t=threading.Thread(target=runtest, kwargs={'workdir': workdir, 'pyver': pyver, 'ucs': ucs, 'sqlitever': sqlitever, 'logdir': logdir})
                t.start()
                threads.append(t)

    print "All builds started, now waiting for them to finish"
    for t in threads:
        t.join()
                


def getpyurl(pyver):
    dirver=pyver
    if pyver>'2.3.0':
        return "http://www.python.org/ftp/python/%s/Python-%s.tar.bz2" % (dirver,pyver)
    if pyver=='2.3.0':
        pyver='2.3'
        dirver='2.3'
    return "http://www.python.org/ftp/python/%s/Python-%s.tgz" % (dirver,pyver)

def sqliteurl(sqlitever):
    return "http://sqlite.org/sqlite-%s.tar.gz" % (sqlitever,)

def buildpython(workdir, pyver, ucs, logfilename):
    if pyver=="system": return "/usr/bin/python"
    
    url=getpyurl(pyver)
    if url.endswith(".bz2"):
        tarx="j"
    else:
        tarx="z"
    if pyver=="2.3.0": pyver="2.3"    
    run("cd %s ; mkdir pyinst ; wget -q %s -O - | tar xf%s -  > %s 2>&1" % (workdir, url, tarx, logfilename))
    run("cd %s ; cd Python-%s ; ./configure --enable-unicode=ucs%d --prefix=%s/pyinst >> %s 2>&1; make >>%s 2>&1; make  install >>%s 2>&1" % (workdir, pyver, ucs, workdir, logfilename, logfilename, logfilename))

    return os.path.join(workdir, "pyinst", "bin", "python")
    
def buildsqlite(workdir, sqlitever, logfile):
    os.system("rm -rf %s/sqlite3 2>/dev/null" % (workdir,))
    if sqlitever=="cvs":
        run("cd %s ; cvs -d :pserver:anonymous@www.sqlite.org:/sqlite checkout sqlite > %s 2>&1; mv sqlite sqlite3" % (workdir, logfile,))
    else:
        run("cd %s ; wget -q %s -O - | tar xfz - > %s 2>&1; mv sqlite-%s sqlite3" % (workdir, sqliteurl(sqlitever), logfile, sqlitever))
    run('cd %s/sqlite3 ; env CC="gcc -fPIC" CFLAGS="-DHAVE_DLOPEN" ./configure --enable-threadsafe --disable-tcl >> %s 2>&1; make >> %s 2>&1; cp .libs/*.a .; ranlib *.a 2>/dev/null; cp src/sqlite3ext.h .' % (workdir,logfile,logfile))
    if sys.platform.startswith("darwin"):
        run('cd %s ; gcc -fPIC -bundle -o testextension.sqlext -Isqlite3 testextension.c' % (workdir,))
    else:
        run('cd %s ; gcc -fPIC -shared -o testextension.sqlext -Isqlite3 testextension.c' % (workdir,))

def buildapsw(outputfile, pybin, workdir):
    run("cd %s ; %s setup.py build >>%s 2>&1" % (workdir, pybin, outputfile))
    if pybin=="/usr/bin/python":
        run("cd %s ; cp build/*/apsw.so ." % (workdir,))
    else:
        run("cd %s ; %s setup.py install >>%s 2>&1" % (workdir, pybin, outputfile))





PYVERS=(
    '2.5',
    '2.4.4',
    '2.3.6',
    'system',
    # '2.2.3',  - apsw not supported on 2.2 as it needs GILstate
    )

SQLITEVERS=(
#    'cvs',
    '3.3.10',
    '3.3.11',
    '3.3.12',
   )


if __name__=='__main__':
    main()
