
SQLITEVERSION=3.7.3
APSWSUFFIX=-r1

VERSION=$(SQLITEVERSION)$(APSWSUFFIX)
VERDIR=apsw-$(VERSION)

# These control Debian packaging
DEBSUFFIX=1ppa1
DEBMAINTAINER="Roger Binns <rogerb@rogerbinns.com>"
DEBSERIES=maverick lucid karmic jaunty
PPAUPLOAD=ppa:ubuntu-rogerbinns/apsw

# Some useful info
#
# To use a different SQLite version: make SQLITEVERSION=1.2.3 blah blah
#
# build_ext      - builds extension in current directory fetching sqlite
# test           - builds extension in place then runs test suite
# doc            - makes the doc
# source         - makes a source zip in dist directory after running code through test suite
# dpkg-bin       - produces binary debian packages for each DEBSERIES
# dpkg           - produces debian source package for each DEBSERIES
# ppa            - calls dpkg and then uploads to PPAUPLOAD

GENDOCS = \
	doc/blob.rst \
	doc/vfs.rst \
	doc/vtable.rst \
	doc/connection.rst \
	doc/cursor.rst \
	doc/apsw.rst \
	doc/backup.rst

.PHONY : all docs doc header linkcheck publish showsymbols compile-win source source_nocheck upload tags clean ppa dpkg dpkg-bin coverage valgrind

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
	env PYTHONPATH=. http_proxy= python tools/code2rst.py $(SQLITEVERSION) $< $@

build_ext:
	python setup.py fetch --version=$(SQLITEVERSION) --all build_ext --inplace --force --enable-all-extensions

coverage:
	python setup.py fetch --version=$(SQLITEVERSION) --all && env APSW_PY_COVERAGE=t tools/coverage.sh

test: build_ext
	python tests.py

# Needs a debug python.  Look at the final numbers at the bottom of
# l6, l7 and l8 and see if any are growing
valgrind: /space/pydebug/bin/python
	python setup.py fetch --version=$(SQLITEVERSION) --all && \
	  env PATH=/space/pydebug/bin:$$PATH SHOWINUSE=t APSW_TEST_ITERATIONS=6 tools/valgrind.sh 2>&1 | tee l6 && \
	  env PATH=/space/pydebug/bin:$$PATH SHOWINUSE=t APSW_TEST_ITERATIONS=7 tools/valgrind.sh 2>&1 | tee l7 && \
	  env PATH=/space/pydebug/bin:$$PATH SHOWINUSE=t APSW_TEST_ITERATIONS=8 tools/valgrind.sh 2>&1 | tee l8 

linkcheck:
	make VERSION=$(VERSION) -C doc linkcheck 

publish: docs
	if [ -d ../apsw-publish ] ; then rm -f ../apsw-publish/* ../apsw-publish/_static/* ../apsw-publish/_sources/* ; \
	rsync -a doc/build/html/ ../apsw-publish/ ;  cd ../apsw-publish ; hg status ; \
	fi

header:
	echo "#define APSW_VERSION \"$(VERSION)\"" > src/apswversion.h


# the funky test stuff is to exit successfully when grep has rc==1 since that means no lines found.
showsymbols:
	rm -f apsw.so
	python setup.py fetch --all --version=$(SQLITEVERSION) build_ext --inplace --force --enable-all-extensions
	test -f apsw.so
	set +e; nm --extern-only --defined-only apsw.so | egrep -v ' (__bss_start|_edata|_end|_fini|_init|initapsw)$$' ; test $$? -eq 1 || false

# Getting Visual Studio 2008 Express to work for 64 compilations is a
# pain, so use this builtin hidden command
WIN64HACK=win64hackvars
WINBPREFIX=fetch --version=$(SQLITEVERSION) --all build --enable-all-extensions
WINBSUFFIX=install build_test_extension test
WINBINST=bdist_wininst
WINBMSI=bdist_msi

# You need to use the MinGW version of make.  See
# http://bugs.python.org/issue3308 if 2.6+ or 3.0+ fail to run with
# missing symbols/dll issues.  For Python 3.1 they went out of their
# way to prevent mingw from working.  You have to install msvc.
# Google for "visual c++ express edition 2008" and hope the right version
# is still available.

compile-win:
	-del /q apsw.pyd
	cmd /c del /s /q dist
	cmd /c del /s /q build
	-cmd /c md dist
	c:/python23/python setup.py $(WINBPREFIX) --compile=mingw32 $(WINBSUFFIX) $(WINBINST)
	c:/python24/python setup.py $(WINBPREFIX) --compile=mingw32 $(WINBSUFFIX) $(WINBINST)
	c:/python25/python setup.py $(WINBPREFIX) --compile=mingw32 $(WINBSUFFIX) $(WINBINST)
	c:/python26/python setup.py $(WINBPREFIX) $(WINBSUFFIX) $(WINBINST)
	c:/python26/python setup.py $(WINBPREFIX) $(WINBSUFFIX) $(WINBMSI)
	c:/python26-64/python setup.py $(WIN64HACK) $(WINBPREFIX) $(WINBSUFFIX) $(WINBINST)
	c:/python27/python setup.py $(WINBPREFIX) $(WINBSUFFIX) $(WINBINST)
	c:/python27-64/python setup.py  $(WIN64HACK) $(WINBPREFIX) $(WINBSUFFIX) $(WINBINST)
	c:/python31/python setup.py $(WINBPREFIX) $(WINBSUFFIX) $(WINBINST)
	c:/python31-64/python setup.py  $(WIN64HACK) $(WINBPREFIX) $(WINBSUFFIX) $(WINBINST)

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
	test -f dist/$(VERDIR).win32-py2.7.exe
	test -f dist/$(VERDIR).win32-py3.1.exe
	test -f dist/$(VERDIR).chm
	-rm -f dist/$(VERDIR)-sigs.zip dist/*.asc
	for f in dist/* ; do gpg --use-agent --armor --detach-sig "$$f" ; done
	cd dist ; zip -m $(VERDIR)-sigs.zip *.asc
	python tools/googlecode_upload.py --user "$(GC_USER)" --password "$(GC_PASSWORD)" -p apsw -s "$(VERSION) GPG signatures for all files" -l "Type-Signatures,OpSys-All" dist/$(VERDIR)-sigs.zip
	python tools/googlecode_upload.py --user "$(GC_USER)" --password "$(GC_PASSWORD)" -p apsw -s "$(VERSION) Windows Python 3.1 (Binary)" -l "Type-Installer,OpSys-Windows" dist/$(VERDIR).win32-py3.1.exe
	python tools/googlecode_upload.py --user "$(GC_USER)" --password "$(GC_PASSWORD)" -p apsw -s "$(VERSION) Windows Python 2.7 (Binary)" -l "Type-Installer,OpSys-Windows" dist/$(VERDIR).win32-py2.7.exe
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
	python setup.py fetch --all --version=$(SQLITEVERSION) sdist --formats bztar --add-doc
	rm -rf debian-build
	mkdir -p debian-build
	cp dist/$(VERDIR).tar.bz2 debian-build/python-apsw_$(VERSION).orig.tar.bz2 
	set -ex ; \
	for series in $(DEBSERIES) ; do \
	   tools/mkdebianchangelog.py $(VERSION) $(DEBSUFFIX)~$${series}1 $(DEBMAINTAINER) $$series ; \
	   cd debian-build ; rm -rf $(VERDIR); tar xfj  python-apsw_$(VERSION).orig.tar.bz2 ; \
	   cd $(VERDIR) ; rsync -av ../../debian . ; \
	   debuild -S ; \
	   cd ../.. ; \
	done


# This idiotic tool won't understand giving --distribution to --build
# and just do the right thing (ie getting that distro etc) so we have
# to keep remaking them
dpkg-bin: dpkg
	set -ex ; \
	cd debian-build ; \
	for series in $(DEBSERIES) ; do \
	  sudo pbuilder create --http-proxy "$(http_proxy)" --distribution $${series} ; \
	  sudo pbuilder build --http-proxy "$(http_proxy)" --distribution $${series} *~$${series}1.dsc ; \
	done
# Look in /var/cache/pbuilder/result/ to find the output .deb files

ppa: dpkg
	cd debian-build ; for f in *_source.changes ; do dput $(PPAUPLOAD) $$f ; done
