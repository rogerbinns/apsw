
VERSION=3.6.4-r1
VERDIR=apsw-$(VERSION)

SOURCEFILES = \
	src/apsw.c \
	src/apswbuffer.c \
	src/apswversion.h \
	src/blob.c \
	src/pointerlist.c \
	src/statementcache.c \
        src/traceback.c  \
	src/testextension.c 

OTHERFILES = \
	mingwsetup.bat  \
	setup.py  \
	speedtest.py \
	tests.py

all: header docs

# The various tools and sphinx generate a prodigious amount of output which
# we send to dev null.  latex is whiny
docs:
	python example2rst.py
	python code2rst.py src/blob.c doc/blob.rst
	make VERSION=$(VERSION) -C doc clean html htmlhelp  # >/dev/null

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
	c:/python23/python setup.py build --compile=mingw32 install 
	c:/python23/python tests.py
	c:/python23/python setup.py build --compile=mingw32 bdist_wininst
	c:/python23/python example-code.py
	c:/python24/python setup.py build --compile=mingw32 install 
	c:/python24/python tests.py
	c:/python24/python setup.py build --compile=mingw32 bdist_wininst
	c:/python25/python setup.py build --compile=mingw32 install 
	c:/python25/python tests.py
	c:/python25/python setup.py build --compile=mingw32 bdist_wininst
# See http://bugs.python.org/issue3308 if these fail to run with
# missing symbols
	c:/python26/python setup.py build --compile=mingw32 install 
	c:/python26/python tests.py
	c:/python26/python setup.py build --compile=mingw32 bdist_wininst
# Beta release currently
	c:/python30/python setup.py build --compile=mingw32 install 
	c:/python30/python tests.py
	c:/python30/python setup.py build --compile=mingw32 bdist_wininst


source:
	rm -rf $(VERDIR)
	mkdir -p $(VERDIR)/src
	cp  $(SOURCEFILES) $(VERDIR)/src/
	cp $(OTHERFILES) $(VERDIR)
	rm -rf dist
	mkdir dist
	zip -9 -r dist/$(VERDIR).zip $(VERDIR)

upload:
	test -f googlecode_upload.py
	test -f apsw.html
	test -f dist/$(VERDIR).zip
	test -f dist/$(VERDIR).win32-py2.3.exe
	test -f dist/$(VERDIR).win32-py2.4.exe
	test -f dist/$(VERDIR).win32-py2.5.exe
	test -f dist/$(VERDIR).win32-py2.6.exe
	-rm -f $(VERDIR).html
	python googlecode_upload.py -p apsw -s "$(VERSION) Windows Python 2.6 (Binary)" -l "Type-Installer,OpSys-Windows" dist/$(VERDIR).win32-py2.6.exe
	python googlecode_upload.py -p apsw -s "$(VERSION) Windows Python 2.5 (Binary)" -l "Type-Installer,OpSys-Windows" dist/$(VERDIR).win32-py2.5.exe
	python googlecode_upload.py -p apsw -s "$(VERSION) Windows Python 2.4 (Binary)" -l "Type-Installer,OpSys-Windows" dist/$(VERDIR).win32-py2.4.exe
	python googlecode_upload.py -p apsw -s "$(VERSION) Windows Python 2.3 (Binary)" -l "Type-Installer,OpSys-Windows" dist/$(VERDIR).win32-py2.3.exe
	python googlecode_upload.py -p apsw -s "$(VERSION) (Source)" -l "Type-Source,OpSys-All" dist/$(VERDIR).zip
	cp apsw.html $(VERDIR).html
	python googlecode_upload.py -p apsw -s "$(VERSION) (Documentation)" -l "Type-Docs" $(VERDIR).html
