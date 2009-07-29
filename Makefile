
VERSION=3.6.17-r1
VERDIR=apsw-$(VERSION)

# setup.py options for windows dist
WINOPTS=--enable=fts3 --enable=fts3_parenthesis --enable=rtree

SOURCEFILES = \
	src/apsw.c \
	src/apswbuffer.c \
	src/apswversion.h \
	src/backup.c \
	src/blob.c \
	src/connection.c \
	src/cursor.c \
	src/exceptions.c \
	src/pyutil.c \
	src/statementcache.c \
        src/traceback.c  \
	src/testextension.c  \
	src/util.c \
	src/vfs.c \
	src/vtable.c

OTHERFILES = \
	checksums \
	mingwsetup.bat  \
	setup.py  \
	tools/speedtest.py \
	tools/apswtrace.py \
	tests.py

GENDOCS = \
	doc/blob.rst \
	doc/vfs.rst \
	doc/vtable.rst \
	doc/connection.rst \
	doc/cursor.rst \
	doc/apsw.rst \
	doc/backup.rst

.PHONY : all docs doc header linkcheck publish showsymbols compile-win source upload

all: header docs

doc: docs

docs: $(GENDOCS) doc/example.rst doc/.static
	env PYTHONPATH=. python tools/docmissing.py
	env PYTHONPATH=. python tools/docupdate.py $(VERSION)
	make VERSION=$(VERSION) -C doc clean html htmlhelp 

doc/example.rst: example-code.py tools/example2rst.py src/apswversion.h
	rm -f dbfile
	env PYTHONPATH=. python tools/example2rst.py

doc/.static:
	mkdir -p doc/.static

# This is probably gnu make specific but only developers use this makefile
$(GENDOCS): doc/%.rst: src/%.c tools/code2rst.py
	python tools/code2rst.py $< $@

linkcheck:
	make VERSION=$(VERSION) -C doc linkcheck 

publish: docs
	if [ -d ../apsw-publish ] ; then rm -f ../apsw-publish/* ../apsw-publish/_static/* ../apsw-publish/_sources/* ; \
	rsync -av doc/build/html/ ../apsw-publish/ ;  cd ../apsw-publish ; set -x ; \
	svn -q propset svn:mime-type text/html `find . -name \*.html` ; \
	svn -q propset svn:mime-type text/javascript `find . -name \*.js` ; \
	svn -q propset svn:mime-type "text/plain; charset=UTF-8" `find . -name \*.txt` ; \
	svn -q propset svn:mime-type image/png `find . -name \*.png` ; \
	svn -q propset svn:mime-type text/css `find . -name \*.css` ; \
	fi

header:
	echo "#define APSW_VERSION \"$(VERSION)\"" > src/apswversion.h


# the funky test stuff is to exit successfully when grep has rc==1 since that means no lines found.
showsymbols:
	test -f apsw.so # ensure file exists
	set +e; nm --extern-only --defined-only apsw.so | egrep -v ' (__bss_start|_edata|_end|_fini|_init|initapsw)$$' ; test $$? -eq 1 || false

# You need to use the MinGW version of make. 
compile-win:
	-del /q apsw.pyd
	cmd /c del /s /q dist
	cmd /c del /s /q build
	-cmd /c md dist
	c:/python23/python setup.py build --compile=mingw32 install $(WINOPTS) test bdist_wininst
	c:/python24/python setup.py build --compile=mingw32 install $(WINOPTS) test bdist_wininst
	c:/python25/python setup.py build --compile=mingw32 install $(WINOPTS) test bdist_wininst
# See http://bugs.python.org/issue3308 if 2.6+ or 3.0+ fail to run with
# missing symbols/dll issues
	c:/python26/python setup.py build --compile=mingw32 install $(WINOPTS) test bdist_wininst
	c:/python30/python setup.py build --compile=mingw32 install $(WINOPTS) test bdist_wininst
        # They went out of their way to prevent mingw from working with 3.1.  You
        # have to install msvc.  Google for "visual c++ express edition".
	c:/python31/python setup.py build install $(WINOPTS) test bdist_wininst

source: docs
	rm -rf $(VERDIR)
	mkdir -p $(VERDIR)/src
	cp  $(SOURCEFILES) $(VERDIR)/src/
	cp $(OTHERFILES) $(VERDIR)
	cd $(VERDIR) ; mkdir doc ; cd doc ; unzip ../../dist/apswdoc-$(VERSION).zip
	-rm -f dist/$(VERDIR).zip
	zip -9 -r dist/$(VERDIR).zip $(VERDIR)

upload:
	test -f tools/googlecode_upload.py
	test -f dist/$(VERDIR).zip
	test -f dist/$(VERDIR).win32-py2.3.exe
	test -f dist/$(VERDIR).win32-py2.4.exe
	test -f dist/$(VERDIR).win32-py2.5.exe
	test -f dist/$(VERDIR).win32-py2.6.exe
	test -f dist/$(VERDIR).win32-py3.0.exe
	test -f dist/$(VERDIR).win32-py3.1.exe
	test -f dist/$(VERDIR).chm
	python tools/googlecode_upload.py -p apsw -s "$(VERSION) Windows Python 3.1 (Binary)" -l "Type-Installer,OpSys-Windows" dist/$(VERDIR).win32-py3.1.exe
	python tools/googlecode_upload.py -p apsw -s "$(VERSION) Windows Python 3.0 (Binary)" -l "Type-Installer,OpSys-Windows" dist/$(VERDIR).win32-py3.0.exe
	python tools/googlecode_upload.py -p apsw -s "$(VERSION) Windows Python 2.6 (Binary)" -l "Type-Installer,OpSys-Windows" dist/$(VERDIR).win32-py2.6.exe
	python tools/googlecode_upload.py -p apsw -s "$(VERSION) Windows Python 2.5 (Binary)" -l "Type-Installer,OpSys-Windows" dist/$(VERDIR).win32-py2.5.exe
	python tools/googlecode_upload.py -p apsw -s "$(VERSION) Windows Python 2.4 (Binary)" -l "Type-Installer,OpSys-Windows" dist/$(VERDIR).win32-py2.4.exe
	python tools/googlecode_upload.py -p apsw -s "$(VERSION) Windows Python 2.3 (Binary)" -l "Type-Installer,OpSys-Windows" dist/$(VERDIR).win32-py2.3.exe
	python tools/googlecode_upload.py -p apsw -s "$(VERSION) (Documentation only - Compiled Help Format)" -l "Type-Docs" dist/$(VERDIR).chm
	python tools/googlecode_upload.py -p apsw -s "$(VERSION) (Source, includes HTML documentation)" -l "Type-Source,OpSys-All" dist/$(VERDIR).zip
