
VERSION=3.6.4-r1
VERDIR=apsw-$(VERSION)

# setup.py options for windows dist
WINOPTS=--enable=fts  --enable=rtree

SOURCEFILES = \
	src/apsw.c \
	src/apswbuffer.c \
	src/apswversion.h \
	src/blob.c \
	src/exceptions.c \
	src/pointerlist.c \
	src/pyutil.c \
	src/statementcache.c \
        src/traceback.c  \
	src/testextension.c  \
	src/util.c \
	src/vfs.c \
	src/vtable.c

OTHERFILES = \
	mingwsetup.bat  \
	setup.py  \
	speedtest.py \
	tests.py

GENDOCS = \
	doc/blob.rst \
	doc/vfs.rst \
	doc/vtable.rst \
	doc/connection.rst \
	doc/cursor.rst

all: header docs

docs: $(GENDOCS) doc/example.rst
	env PYTHONPATH=. python tools/docmissing.py
	make VERSION=$(VERSION) -C doc clean html htmlhelp 

doc/example.rst: example-code.py tools/example2rst.py
	env PYTHONPATH=. python tools/example2rst.py

# This is probably gnu make specific but only developers use this makefile
$(GENDOCS): doc/%.rst: src/%.c tools/code2rst.py
	python tools/code2rst.py $< $@

linkcheck:
	make http_proxy=http://192.168.1.25:8080 VERSION=$(VERSION) -C doc linkcheck 

header:
	echo "#define APSW_VERSION \"$(VERSION)\"" > src/apswversion.h

tidytoc:
	python coloursrc.py
	awk ' BEGIN {p=1} /<!--toc-->/ {p=0; print;next} /<!--endtoc-->/ {p=1} {if(p) print} ' < apsw.html > tmpfile
	hypertoc --gen_anchors tmpfile >tmpfile2
	hypertoc --gen_toc --inline --toc_tag '!--toc--' \
	  --toc_label "" tmpfile2 | \
	sed 's@\(<li><a href="#dbapinotes">\)@</ul></td><td valign="top"><ul>\1@' | \
	grep -v '"list-style: none;"'> tmpfile
	-tidy -q -indent -asxhtml -wrap 120 <tmpfile >apsw.html
	@rm -f tmpfile tmpfile2 .tmpop-*
	linkchecker -q --no-status --ignore-url="http://www.sqlite.org/cvstrac/tktview?tn=[0-9][0-9][0-9][0-9]" apsw.html

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
	c:/python23/python setup.py build --compile=mingw32 install $(WINOPTS)
	c:/python23/python tests.py
	c:/python23/python setup.py build --compile=mingw32 bdist_wininst $(WINOPTS)
	c:/python23/python example-code.py

	c:/python24/python setup.py build --compile=mingw32 install $(WINOPTS)
	c:/python24/python tests.py
	c:/python24/python setup.py build --compile=mingw32 bdist_wininst $(WINOPTS)

	c:/python25/python setup.py build --compile=mingw32 install $(WINOPTS)
	c:/python25/python tests.py
	c:/python25/python setup.py build --compile=mingw32 bdist_wininst $(WINOPTS)

# See http://bugs.python.org/issue3308 if these fail to run with
# missing symbols
	c:/python26/python setup.py build --compile=mingw32 install $(WINOPTS)
	c:/python26/python tests.py
	c:/python26/python setup.py build --compile=mingw32 bdist_wininst $(WINOPTS)

# Beta release currently
	c:/python30/python setup.py build --compile=mingw32 install $(WINOPTS)
	c:/python30/python tests.py
	c:/python30/python setup.py build --compile=mingw32 bdist_wininst $(WINOPTS)


source:
	rm -rf $(VERDIR)
	mkdir -p $(VERDIR)/src
	cp  $(SOURCEFILES) $(VERDIR)/src/
	cp $(OTHERFILES) $(VERDIR)
	rm -rf dist
	mkdir dist
	zip -9 -r dist/$(VERDIR).zip $(VERDIR)

upload:
	test -f tools/googlecode_upload.py
	test -f apsw.html
	test -f dist/$(VERDIR).zip
	test -f dist/$(VERDIR).win32-py2.3.exe
	test -f dist/$(VERDIR).win32-py2.4.exe
	test -f dist/$(VERDIR).win32-py2.5.exe
	test -f dist/$(VERDIR).win32-py2.6.exe
	-rm -f $(VERDIR).html
	python tools/googlecode_upload.py -p apsw -s "$(VERSION) Windows Python 2.6 (Binary)" -l "Type-Installer,OpSys-Windows" dist/$(VERDIR).win32-py2.6.exe
	python tools/googlecode_upload.py -p apsw -s "$(VERSION) Windows Python 2.5 (Binary)" -l "Type-Installer,OpSys-Windows" dist/$(VERDIR).win32-py2.5.exe
	python tools/googlecode_upload.py -p apsw -s "$(VERSION) Windows Python 2.4 (Binary)" -l "Type-Installer,OpSys-Windows" dist/$(VERDIR).win32-py2.4.exe
	python tools/googlecode_upload.py -p apsw -s "$(VERSION) Windows Python 2.3 (Binary)" -l "Type-Installer,OpSys-Windows" dist/$(VERDIR).win32-py2.3.exe
	python tools/googlecode_upload.py -p apsw -s "$(VERSION) (Source)" -l "Type-Source,OpSys-All" dist/$(VERDIR).zip
	cp apsw.html $(VERDIR).html
	python tools/googlecode_upload.py -p apsw -s "$(VERSION) (Documentation)" -l "Type-Docs" $(VERDIR).html
