#!/usr/bin/python

"""This file runs the test suite against several versions of SQLite
and Python to make sure everything is ok in the various combinations.
It only runs on a UNIX like environment.

You should make sure that wget is using a proxy so you don't hit the
upstream sites repeatedly and have ccache so that compiles are
quicker.  """

import os
import re

def run(cmd):
    print cmd
    status=os.system(cmd)
    print status
    if os.WIFEXITED(status):
        code=os.WEXITSTATUS(status)
        if code==0:
            return
        raise Exception("Exited with code "+`code`)
    raise Exception("Failed with signal "+`os.WTERMSIG(status)`)


def dotest(outputdir, pybin, workdir, pyver, sqlitever):
    buildsqlite(workdir, sqlitever)
    buildapsw("%s/py%s-sqlite%s-build.txt" % (outputdir,pyver,sqlitever), pybin)

    # now the actual tests
    run("env APSW_TEST_ITERATIONS=250 %s tests.py 2>&1 | tee %s/py%s-sqlite%s.txt" % (pybin, outputdir, pyver, sqlitever))


def main():
    os.system("rm -rf apsw.so megatestresults 2>/dev/null ; mkdir megatestresults")
    for pyver in PYVERS:
        for ucs in (2,4):
            workdir=os.path.abspath("work")
            os.system("rm -rf %s 2>/dev/null ; mkdir %s" % (workdir, workdir))
            pybin=buildpython(workdir, pyver, ucs)
            for sqlitever in SQLITEVERS:
                # get rid of any existing apsw in the Python
                os.system("find %s -name '*apsw*' | xargs -r rm")
                dotest(os.path.abspath("megatestresults"), pybin, workdir, "%s-ucs%d" % (pyver, ucs), sqlitever)


def getpyurl(pyver):
    dirver=pyver
    if pyver>'2.2.3':
        return "http://www.python.org/ftp/python/%s/Python-%s.tar.bz2" % (dirver,pyver)
    else:
        return "http://www.python.org/ftp/python/%s/Python-%s.tgz" % (dirver,pyver)

def sqliteurl(sqlitever):
    return "http://sqlite.org/sqlite-%s.tar.gz" % (sqlitever,)

def buildpython(workdir, pyver, ucs):
    url=getpyurl(pyver)
    if url.endswith(".bz2"):
        tarx="j"
    else:
        tarx="z"
    
    run("cd %s ; mkdir pyinst ; wget %s -O - | tar xf%s -" % (workdir, url, tarx))
    run("cd %s ; cd Python-%s ; ./configure --enable-unicode=ucs%d --prefix=%s/pyinst ; make -j 3 ; make  install" % (workdir, pyver, ucs, workdir))

    return os.path.join(workdir, "pyinst", "bin", "python")
    
def buildsqlite(workdir, sqlitever):
    os.system("rm -rf sqlite3 2>/dev/null")
    run("cd %s ; wget %s -O - | tar xfz - ; mv sqlite-%s ../sqlite3" % (workdir, sqliteurl(sqlitever), sqlitever))
    run('cd sqlite3 ; env CC="gcc -fPIC" ./configure --enable-threadsafe --disable-tcl ; make -j 3 ; cp .libs/*.a .')

def buildapsw(outputfile, pybin):
    os.system("rm -rf build dist 2>/dev/null")
    run("%s setup.py build 2>&1 | tee %s ; %s setup.py install" % (pybin,outputfile,pybin))





PYVERS=(
    '2.5',
    '2.4.4',
    '2.4.3',
    '2.3.5',
    '2.3.6',
    # '2.2.3',  - apsw not supported on 2.2 as it needs GILstate
    )

SQLITEVERS=(
    '3.3.8',
    '3.3.7',
#    '3.3.6',  - not supported as 3.3.7 has new column return value api
#    '3.3.4',
    )


if __name__=='__main__':
    main()
