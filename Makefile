
SQLITEVERSION=3.39.0
APSWSUFFIX=.0

RELEASEDATE="5 June 2022"

VERSION=$(SQLITEVERSION)$(APSWSUFFIX)
VERDIR=apsw-$(VERSION)
VERWIN=apsw-$(VERSION)

PYTHON=python3

# Some useful info
#
# To use a different SQLite version: make SQLITEVERSION=1.2.3 blah blah
#
# build_ext      - builds extension in current directory fetching sqlite
# test           - builds extension in place then runs test suite
# doc            - makes the doc
# source         - makes a source zip in dist directory after running code through test suite

GENDOCS = \
	doc/blob.rst \
	doc/vfs.rst \
	doc/vtable.rst \
	doc/connection.rst \
	doc/cursor.rst \
	doc/apsw.rst \
	doc/backup.rst

.PHONY : all docs doc header linkcheck publish showsymbols compile-win source source_nocheck release tags clean ppa dpkg dpkg-bin coverage valgrind valgrind1 tagpush pydebug test fulltest test_debug

all: header docs

tagpush:
	git tag -af $(SQLITEVERSION)$(APSWSUFFIX)
	git push --tags

clean:
	make PYTHONPATH="`pwd`" VERSION=$(VERSION) -C doc clean
	rm -rf dist build work/* megatestresults apsw.egg-info __pycache__ :memory: .mypy_cache .ropeproject
	mkdir dist
	for i in 'vgcore.*' '.coverage' '*.pyc' '*.pyo' '*~' '*.o' '*.so' '*.dll' '*.pyd' '*.gcov' '*.gcda' '*.gcno' '*.orig' '*.tmp' 'testdb*' 'testextension.sqlext' ; do \
		find . -type f -name "$$i" -print0 | xargs -0t --no-run-if-empty rm -f ; done

doc: docs

docs: build_ext $(GENDOCS) doc/example.rst doc/.static
	env PYTHONPATH=. $(PYTHON) tools/docmissing.py
	env PYTHONPATH=. $(PYTHON) tools/docupdate.py $(VERSION)
	make PYTHONPATH="`pwd`" VERSION=$(VERSION) RELEASEDATE=$(RELEASEDATE) -C doc clean html
	-tools/spellcheck.sh

doc/example.rst: example-code.py tools/example2rst.py src/apswversion.h
	rm -f dbfile
	env PYTHONPATH=. $(PYTHON) tools/example2rst.py

doc/.static:
	mkdir -p doc/.static

# This is probably gnu make specific but only developers use this makefile
$(GENDOCS): doc/%.rst: src/%.c tools/code2rst.py
	env PYTHONPATH=. $(PYTHON) tools/code2rst.py $(SQLITEVERSION) $< $@

src/apsw.docstrings: $(GENDOCS) tools/rst2docstring.py src/types.py
	env PYTHONPATH=. $(PYTHON) tools/rst2docstring.py src/apsw.docstrings $(GENDOCS)

build_ext:
	env $(PYTHON) setup.py fetch --version=$(SQLITEVERSION) --all build_ext --inplace --force --enable-all-extensions

build_ext_debug:
	env $(PYTHON) setup.py fetch --version=$(SQLITEVERSION) --all build_ext --inplace --force --enable-all-extensions --debug

coverage:
	env $(PYTHON) setup.py fetch --version=$(SQLITEVERSION) --all && env APSW_PY_COVERAGE=t tools/coverage.sh

test: build_ext
	env PYTHONHASHSEED=random $(PYTHON) tests.py

test_debug: $(PYDEBUG_DIR)/bin/python3
	$(MAKE) build_ext_debug PYTHON=$(PYDEBUG_DIR)/bin/python3
	env PYTHONHASHSEED=random APSWTESTPREFIX=$(PYDEBUG_WORKDIR) $(PYDEBUG_DIR)/bin/python3 tests.py -v

fulltest: test test_debug

linkcheck:
	make RELEASEDATE=$(RELEASEDATE) VERSION=$(VERSION) -C doc linkcheck

publish: docs
	if [ -d ../apsw-publish ] ; then rm -f ../apsw-publish/* ../apsw-publish/_static/* ../apsw-publish/_sources/* ; \
	rsync -a doc/build/html/ ../apsw-publish/ ;  cd ../apsw-publish ; git status ; \
	fi

header:
	echo "#define APSW_VERSION \"$(VERSION)\"" > src/apswversion.h

stubtest: build_ext
	env PYTHONPATH=. $(PYTHON) -m mypy.stubtest --allowlist tools/stubtest.allowlist apsw

# the funky test stuff is to exit successfully when grep has rc==1 since that means no lines found.
showsymbols:
	rm -f apsw`$(PYTHON) -c "import sysconfig; print(sysconfig.get_config_var('EXT_SUFFIX'))"`
	$(PYTHON) setup.py fetch --all --version=$(SQLITEVERSION) build_ext --inplace --force --enable-all-extensions
	test -f apsw`$(PYTHON) -c "import sysconfig; print(sysconfig.get_config_var('EXT_SUFFIX'))"`
	set +e; nm --extern-only --defined-only apsw`$(PYTHON) -c "import sysconfig; print(sysconfig.get_config_var('EXT_SUFFIX'))"` | egrep -v ' (__bss_start|_edata|_end|_fini|_init|initapsw|PyInit_apsw)$$' ; test $$? -eq 1 || false

# Windows compilation
WINBPREFIX=fetch --version=$(SQLITEVERSION) --all build_ext --enable-all-extensions --inplace build
WINBSUFFIX=build_test_extension test
WINBINST=bdist_wininst
WINBWHEEL=bdist_wheel

compile-win:
	-del /q apsw\\*.pyd
	-del /q dist\\*.egg
	-del /q testextension.*
	-cmd /c del /s /q __pycache__
	-cmd /c del /s /q sqlite3
	cmd /c del /s /q dist
	cmd /c del /s /q build
	-cmd /c md dist
	c:/python310-32/python setup.py $(WINBPREFIX) $(WINBSUFFIX) $(WINBWHEEL)
	c:/python310/python setup.py $(WINBPREFIX) $(WINBSUFFIX) $(WINBWHEEL)
	c:/python39-32/python setup.py $(WINBPREFIX) $(WINBSUFFIX) $(WINBINST) $(WINBWHEEL)
	c:/python39/python setup.py $(WINBPREFIX) $(WINBSUFFIX) $(WINBINST) $(WINBWHEEL)
	c:/python38/python setup.py $(WINBPREFIX) $(WINBSUFFIX) $(WINBINST) $(WINBWHEEL)
	c:/python38-64/python setup.py $(WINBPREFIX) $(WINBSUFFIX) $(WINBINST) $(WINBWHEEL)
	c:/python37/python setup.py $(WINBPREFIX) $(WINBSUFFIX) $(WINBINST) $(WINBWHEEL)
	c:/python37-64/python setup.py $(WINBPREFIX) $(WINBSUFFIX) $(WINBINST) $(WINBWHEEL)
	del dist\\*.egg

setup-wheel:
	c:/python310/python -m ensurepip
	c:/python310/python -m pip install --upgrade wheel setuptools
	c:/python310-32/python -m ensurepip
	c:/python310-32/python -m pip install --upgrade wheel setuptools
	c:/python39/python -m ensurepip
	c:/python39/python -m pip install --upgrade wheel setuptools
	c:/python39-32/python -m ensurepip
	c:/python39-32/python -m pip install --upgrade wheel setuptools
	c:/python38/python -m ensurepip
	c:/python38/python -m pip install --upgrade wheel setuptools
	c:/python38-64/python -m ensurepip
	c:/python38-64/python -m pip install --upgrade wheel setuptools
	c:/python37/python -m ensurepip
	c:/python37/python -m pip install --upgrade wheel setuptools
	c:/python37-64/python -m ensurepip
	c:/python37-64/python -m pip install --upgrade wheel setuptools


source_nocheck: docs
	env APSW_USE_DISTUTILS=t $(PYTHON) setup.py sdist --formats zip --add-doc

# Make the source and then check it builds and tests correctly.  This will catch missing files etc
source: source_nocheck
	mkdir -p work
	rm -rf work/$(VERDIR)
	cd work ; unzip -q ../dist/$(VERDIR).zip
# Make certain various files do/do not exist
	for f in doc/vfs.html doc/_sources/pysqlite.txt tools/apswtrace.py ; do test -f work/$(VERDIR)/$$f ; done
	for f in sqlite3.c sqlite3/sqlite3.c debian/control ; do test ! -f work/$(VERDIR)/$$f ; done
# Test code works
	cd work/$(VERDIR) ; env APSW_USE_DISTUTILS=t $(PYTHON) setup.py fetch --version=$(SQLITEVERSION) --all build_ext --inplace --enable-all-extensions build_test_extension test

release:
	test -f dist/$(VERDIR).zip
	test -f dist/$(VERWIN).win32-py3.7.exe
	test -f dist/$(VERWIN)-cp37-cp37m-win32.whl
	test -f dist/$(VERWIN).win-amd64-py3.7.exe
	test -f dist/$(VERWIN)-cp37-cp37m-win_amd64.whl
	test -f dist/$(VERWIN).win32-py3.8.exe
	test -f dist/$(VERWIN)-cp38-cp38-win32.whl
	test -f dist/$(VERWIN).win-amd64-py3.8.exe
	test -f dist/$(VERWIN)-cp38-cp38-win_amd64.whl
	test -f dist/$(VERWIN).win32-py3.9.exe
	test -f dist/$(VERWIN)-cp39-cp39-win32.whl
	test -f dist/$(VERWIN).win-amd64-py3.9.exe
	test -f dist/$(VERWIN)-cp39-cp39-win_amd64.whl
	test -f dist/$(VERWIN)-cp310-cp310-win32.whl
	test -f dist/$(VERWIN)-cp310-cp310-win_amd64.whl
	-rm -f dist/$(VERDIR)-sigs.zip dist/*.asc
	for f in dist/* ; do gpg --use-agent --armor --detach-sig "$$f" ; done
	cd dist ; zip -m $(VERDIR)-sigs.zip *.asc

tags:
	rm -f TAGS
	ctags-exuberant -e --recurse --exclude=build --exclude=work .

# building a python debug interpreter

PYDEBUG_VER=3.10.2
PYDEBUG_DIR=/space/pydebug
PYVALGRIND_VER=$(PYDEBUG_VER)
PYVALGRIND_DIR=/space/pyvalgrind
# This must end in slash
PYDEBUG_WORKDIR=/space/apsw/work/

# Build a debug python including address sanitizer.  Extensions it builds are also address sanitized
pydebug:
	set -x && cd "$(PYDEBUG_DIR)" && find . -delete && \
	curl https://www.python.org/ftp/python/$(PYDEBUG_VER)/Python-$(PYDEBUG_VER).tar.xz | tar xfJ - && \
	cd Python-$(PYDEBUG_VER) && \
	./configure --with-address-sanitizer --without-pymalloc --prefix="$(PYDEBUG_DIR)" \
	CPPFLAGS="-DPyDict_MAXFREELIST=0 -DPyFloat_MAXFREELIST=0 -DPyTuple_MAXFREELIST=0 -DPyList_MAXFREELIST=0" && \
	env PATH="/usr/lib/ccache:$$PATH" ASAN_OPTIONS=detect_leaks=false make -j install

pyvalgrind:
	set -x && cd "$(PYVALGRIND_DIR)" && find . -delete && \
	curl https://www.python.org/ftp/python/$(PYVALGRIND_VER)/Python-$(PYVALGRIND_VER).tar.xz | tar xfJ - && \
	cd Python-$(PYVALGRIND_VER) && \
	./configure --with-valgrind --without-pymalloc --prefix="$(PYVALGRIND_DIR)" \
	CPPFLAGS="-DPyDict_MAXFREELIST=0 -DPyFloat_MAXFREELIST=0 -DPyTuple_MAXFREELIST=0 -DPyList_MAXFREELIST=0" && \
	env PATH="/usr/lib/ccache:$$PATH" make -j install

# Look at the final numbers at the bottom of l6, l7 and l8 and see if any are growing
valgrind: $(PYVALGRIND_DIR)/bin/python3
	$(PYVALGRIND_DIR)/bin/python3 setup.py fetch --version=$(SQLITEVERSION) --all && \
	  env APSWTESTPREFIX=$(PYDEBUG_WORKDIR) PATH=$(PYVALGRIND_DIR)/bin:$$PATH SHOWINUSE=t APSW_TEST_ITERATIONS=6 tools/valgrind.sh 2>&1 | tee l6 && \
	  env APSWTESTPREFIX=$(PYDEBUG_WORKDIR) PATH=$(PYVALGRIND_DIR)/bin:$$PATH SHOWINUSE=t APSW_TEST_ITERATIONS=7 tools/valgrind.sh 2>&1 | tee l7 && \
	  env APSWTESTPREFIX=$(PYDEBUG_WORKDIR) PATH=$(PYVALGRIND_DIR)/bin:$$PATH SHOWINUSE=t APSW_TEST_ITERATIONS=8 tools/valgrind.sh 2>&1 | tee l8

# Same as above but does just one run
valgrind1: $(PYVALGRIND_DIR)/bin/python3
	$(PYVALGRIND_DIR)/bin/python3 setup.py fetch --version=$(SQLITEVERSION) --all && \
	  env APSWTESTPREFIX=$(PYDEBUG_WORKDIR) PATH=$(PYVALGRIND_DIR)/bin:$$PATH SHOWINUSE=t APSW_TEST_ITERATIONS=1 tools/valgrind.sh
