
SQLITEVERSION=3.6.23.1
APSWSUFFIX=-r1

VERSION=$(SQLITEVERSION)$(APSWSUFFIX)
VERDIR=apsw-$(VERSION)

# These control Debian packaging
DEBSUFFIX=1ppa1
DEBMAINTAINER="Roger Binns <rogerb@rogerbinns.com>"
PPAUPLOAD=ppa:ubuntu-rogerbinns/apsw

GENDOCS = \
	doc/blob.rst \
	doc/vfs.rst \
	doc/vtable.rst \
	doc/connection.rst \
	doc/cursor.rst \
	doc/apsw.rst \
	doc/backup.rst

.PHONY : all docs doc header linkcheck publish showsymbols compile-win source source_nocheck upload tags clean ppa dpkg

all: header docs

clean: 
	make PYTHONPATH="`pwd`" VERSION=$(VERSION) -C doc clean
	rm -rf dist build work/* megatestresults
	for i in '*.pyc' '*.pyo' '*~' '*.o' '*.so' '*.dll' '*.pyd' '*.gcov' '*.gcda' '*.gcno' '*.orig' '*.tmp' 'testdb*' 'testextension.sqlext' ; do \
		find . -type f -name "$$i" -print0 | xargs -0t --no-run-if-empty rm -f ; done

doc: docs

docs: build_ext $(GENDOCS) doc/example.rst doc/.static
	env PYTHONPATH=. http_proxy= python tools/docmissing.py
	env PYTHONPATH=. http_proxy= python tools/docupdate.py $(VERSION)
	make PYTHONPATH="`pwd`" VERSION=$(VERSION) -C doc clean html htmlhelp 

doc/example.rst: example-code.py tools/example2rst.py src/apswversion.h
	rm -f dbfile
	env PYTHONPATH=. python tools/example2rst.py

doc/.static:
	mkdir -p doc/.static

# This is probably gnu make specific but only developers use this makefile
$(GENDOCS): doc/%.rst: src/%.c tools/code2rst.py
	env PYTHONPATH=. http_proxy= python tools/code2rst.py $< $@

build_ext:
	python setup.py fetch --version=$(SQLITEVERSION) --all build_ext --inplace --force --enable-all-extensions

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
	rm -f apsw.so
	python setup.py build_ext --inplace --force --enable-all-extensions
	test -f apsw.so
	set +e; nm --extern-only --defined-only apsw.so | egrep -v ' (__bss_start|_edata|_end|_fini|_init|initapsw)$$' ; test $$? -eq 1 || false

WINBUILD=fetch --version=$(SQLITEVERSION) --all build --enable-all-extensions --compile=mingw32 install build_test_extension test bdist_wininst
WINMSBUILD=fetch --version=$(SQLITEVERSION) --all build --enable-all-extensions install build_test_extension test bdist_wininst

# You need to use the MinGW version of make.  See
# http://bugs.python.org/issue3308 if 2.6+ or 3.0+ fail to run with
# missing symbols/dll issues.  For Python 3.1 they went out of their
# way to prevent mingw from working.  You have to install msvc.
# Google for "visual c++ express edition" and hope the right version
# is still available.

compile-win:
	-del /q apsw.pyd
	cmd /c del /s /q dist
	cmd /c del /s /q build
	-cmd /c md dist
	c:/python23/python setup.py $(WINBUILD)
	c:/python24/python setup.py $(WINBUILD)
	c:/python25/python setup.py $(WINBUILD)
	c:/python26/python setup.py $(WINBUILD)
	c:/python31/python setup.py $(WINMSBUILD)

source_nocheck: docs
	python setup.py sdist --formats zip --add-doc

# Make the source and then check it builds and tests correctly.  This will catch missing files etc
source: source_nocheck
	mkdir -p work
	rm -rf work/$(VERDIR)
	cd work ; unzip -q ../dist/$(VERDIR).zip
# Make certain various files do/do not exist
	for f in doc/vfs.html doc/_sources/pysqlite.txt tools/apswtrace.py ; do test -f work/$(VERDIR)/$$f ; done
	for f in sqlite3.c sqlite3/sqlite3.c debian/control ; do test ! -f work/$(VERDIR)/$$f ; done
# Test code works
	cd work/$(VERDIR) ; python setup.py fetch --version=$(SQLITEVERSION) --all build_ext --inplace --enable-all-extensions build_test_extension test

upload:
	@if [ -z "$(GC_USER)" ] ; then echo "Specify googlecode user by setting GC_USER environment variable" ; exit 1 ; fi
	@if [ -z "$(GC_PASSWORD)" ] ; then echo "Specify googlecode password by setting GC_PASSWORD environment variable" ; exit 1 ; fi
	test -f tools/googlecode_upload.py
	test -f dist/$(VERDIR).zip
	test -f dist/$(VERDIR).win32-py2.3.exe
	test -f dist/$(VERDIR).win32-py2.4.exe
	test -f dist/$(VERDIR).win32-py2.5.exe
	test -f dist/$(VERDIR).win32-py2.6.exe
	test -f dist/$(VERDIR).win32-py3.1.exe
	test -f dist/$(VERDIR).chm
	-rm -f dist/$(VERDIR)-sigs.zip dist/*.asc
	for f in dist/* ; do gpg --use-agent --armor --detach-sig "$$f" ; done
	cd dist ; zip -m $(VERDIR)-sigs.zip *.asc
	python tools/googlecode_upload.py --user "$(GC_USER)" --password "$(GC_PASSWORD)" -p apsw -s "$(VERSION) GPG signatures for all files" -l "Type-Signatures,OpSys-All" dist/$(VERDIR)-sigs.zip
	python tools/googlecode_upload.py --user "$(GC_USER)" --password "$(GC_PASSWORD)" -p apsw -s "$(VERSION) Windows Python 3.1 (Binary)" -l "Type-Installer,OpSys-Windows" dist/$(VERDIR).win32-py3.1.exe
	python tools/googlecode_upload.py --user "$(GC_USER)" --password "$(GC_PASSWORD)" -p apsw -s "$(VERSION) Windows Python 2.6 (Binary)" -l "Type-Installer,OpSys-Windows" dist/$(VERDIR).win32-py2.6.exe
	python tools/googlecode_upload.py --user "$(GC_USER)" --password "$(GC_PASSWORD)" -p apsw -s "$(VERSION) Windows Python 2.5 (Binary)" -l "Type-Installer,OpSys-Windows" dist/$(VERDIR).win32-py2.5.exe
	python tools/googlecode_upload.py --user "$(GC_USER)" --password "$(GC_PASSWORD)" -p apsw -s "$(VERSION) Windows Python 2.4 (Binary)" -l "Type-Installer,OpSys-Windows" dist/$(VERDIR).win32-py2.4.exe
	python tools/googlecode_upload.py --user "$(GC_USER)" --password "$(GC_PASSWORD)" -p apsw -s "$(VERSION) Windows Python 2.3 (Binary)" -l "Type-Installer,OpSys-Windows" dist/$(VERDIR).win32-py2.3.exe
	python tools/googlecode_upload.py --user "$(GC_USER)" --password "$(GC_PASSWORD)" -p apsw -s "$(VERSION) (Documentation only - Compiled Help Format)" -l "Type-Docs" dist/$(VERDIR).chm
	python tools/googlecode_upload.py --user "$(GC_USER)" --password "$(GC_PASSWORD)" -p apsw -s "$(VERSION) (Source, includes HTML documentation)" -l "Type-Source,OpSys-All" dist/$(VERDIR).zip

tags:
	rm -f TAGS
	ctags-exuberant -e --recurse --exclude=build --exclude=work .

debian/copyright: LICENSE
	cp LICENSE debian/copyright

# :TODO: fix this
debian/changelog: doc/changes.rst
	touch debian/changelog

dpkg: clean doc debian/copyright debian/changelog
	rm -rf debian-build
	tools/mkdebianchangelog.py $(VERSION) $(DEBSUFFIX) $(DEBMAINTAINER)
	python setup.py fetch --all --version=$(SQLITEVERSION) sdist --formats bztar --add-doc
	mkdir -p debian-build
	cp dist/$(VERDIR).tar.bz2 debian-build/python-apsw_$(VERSION).orig.tar.bz2
	cd debian-build ; tar xvfj *.tar.bz2 ; cd $(VERDIR) ; rsync -av ../../debian .
	cd debian-build/$(VERDIR) ; debuild -S && sudo pbuilder build ../*.dsc

ppa: dpkg
	cd debian-build ; dput $(PPAUPLOAD) *_source.changes
