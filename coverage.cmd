@rem measure code coverage on Windows with MinGW
@echo off
set pyver=23
if not "%1" == "" set pyver=%1
set pydir=c:\python%pyver%
@rem clean up any files from a previous run
if exist apsw.gcda del apsw.gcda
if exist apsw.c.gcov del apsw.c.gcov
if exist traceback.c.gcov del traceback.c.gcov
if exist apsw.gcno del apsw.gcno
@rem make a def file
if exist apsw.def del apsw.def
echo LIBRARY apsw.pyd >apsw.def
echo EXPORTS             >>apsw.def
echo initapsw              >>apsw.def
@rem do the compilation and linking
@echo on
gcc -ftest-coverage -fprofile-arcs -mdll -Wall -DEXPERIMENTAL -Isqlite3 -I%pydir%\include -I%pydir%\PC -c apsw.c
gcc -ftest-coverage -fprofile-arcs -shared -s apsw.o apsw.def -Lsqlite3 -L%pydir%\libs -L%pydir%\PCBuild -lsqlite3 -lpython%pyver% -o apsw.pyd
@del apsw.def
%pydir%\python tests.py
gcov apsw.c