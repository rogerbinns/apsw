
SQLITEVERSION=3.40.1
APSWSUFFIX=.0

RELEASEDATE="15 January 2023"

VERSION=$(SQLITEVERSION)$(APSWSUFFIX)
VERDIR=apsw-$(VERSION)
VERWIN=apsw-$(VERSION)

PYTHON=python3

GENDOCS = \
	doc/blob.rst \
	doc/vfs.rst \
	doc/vtable.rst \
	doc/connection.rst \
	doc/cursor.rst \
	doc/apsw.rst \
	doc/backup.rst

.PHONY : help all tagpush clean doc docs build_ext build_ext_debug coverage pycoverage test test_debug fulltest linkcheck unwrapped \
		 publish stubtest showsymbols compile-win setup-wheel source_nocheck source release pydebug pyvalgrind valgrind valgrind1 \
		 fossil

help: ## Show this help
	@egrep -h '\s##\s' $(MAKEFILE_LIST) | sort | \
	awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

all: src/apswversion.h src/apsw.docstrings apsw/__init__.pyi test docs ## Update generated files, build, test, make doc

tagpush: ## Tag with version and push
	git tag -af $(SQLITEVERSION)$(APSWSUFFIX)
	git push --tags

clean: ## Cleans up everything
	make PYTHONPATH="`pwd`" VERSION=$(VERSION) -C doc clean
	rm -rf dist build work/* megatestresults apsw.egg-info __pycache__ apsw/__pycache__ :memory: .mypy_cache .ropeproject htmlcov "System Volume Information" doc/docdb.json
	mkdir dist
	for i in 'vgcore.*' '.coverage' '*.pyc' '*.pyo' '*~' '*.o' '*.so' '*.dll' '*.pyd' '*.gcov' '*.gcda' '*.gcno' '*.orig' '*.tmp' 'testdb*' 'testextension.sqlext' ; do \
		find . -type f -name "$$i" -print0 | xargs -0t --no-run-if-empty rm -f ; done
	rm -f doc/typing.rstgen doc/example.rst $(GENDOCS)

doc: docs ## Builds all the doc

docs: build_ext $(GENDOCS) doc/example.rst doc/.static doc/typing.rstgen
	env PYTHONPATH=. $(PYTHON) tools/docmissing.py
	env PYTHONPATH=. $(PYTHON) tools/docupdate.py $(VERSION)
	make PYTHONPATH="`pwd`" VERSION=$(VERSION) RELEASEDATE=$(RELEASEDATE) -C doc clean html
	tools/spellcheck.sh

doc/example.rst: example-code.py tools/example2rst.py src/apswversion.h
	rm -f dbfile
	env PYTHONPATH=. $(PYTHON) -sS tools/example2rst.py
	rm -f dbfile

doc/typing.rstgen: src/apswtypes.py tools/types2rst.py
	-rm -f doc/typing.rstgen
	$(PYTHON) tools/types2rst.py

doc/.static:
	mkdir -p doc/.static

# This is probably gnu make specific but only developers use this makefile
$(GENDOCS): doc/%.rst: src/%.c tools/code2rst.py
	env PYTHONPATH=. $(PYTHON) tools/code2rst.py $(SQLITEVERSION) doc/docdb.json $< $@

apsw/__init__.pyi src/apsw.docstrings: $(GENDOCS) tools/gendocstrings.py src/apswtypes.py
	env PYTHONPATH=. $(PYTHON) tools/gendocstrings.py doc/docdb.json src/apsw.docstrings

build_ext: src/apswversion.h  ## Fetches SQLite and builds the extension
	env $(PYTHON) setup.py fetch --version=$(SQLITEVERSION) --all build_ext -DSQLITE_ENABLE_COLUMN_METADATA --inplace --force --enable-all-extensions

src/faultinject.h: tools/genfaultinject.py
	-rm src/faultinject.h
	tools/genfaultinject.py src/faultinject.h

build_ext_debug: src/apswversion.h src/faultinject.h ## Fetches SQLite and builds the extension in debug mode
	env $(PYTHON) setup.py fetch --version=$(SQLITEVERSION) --all build_ext --inplace --force --enable-all-extensions --debug

coverage:  src/faultinject.h ## Coverage of the C code
	env $(PYTHON) setup.py fetch --version=$(SQLITEVERSION) --all && tools/coverage.sh

PYCOVERAGEOPTS=--source apsw --append

pycoverage:  ## Coverage of the Python code
	-rm -rf .coverage htmlcov
	$(PYTHON) -m coverage run $(PYCOVERAGEOPTS) -m apsw.tests
	$(PYTHON) -m coverage run $(PYCOVERAGEOPTS) -m apsw ":memory:" .quit
	$(PYTHON) -m coverage run $(PYCOVERAGEOPTS) -m apsw.speedtest --apsw --sqlite3
	$(PYTHON) -m coverage run $(PYCOVERAGEOPTS) -m apsw.trace -o /dev/null --sql --rows --timestamps --thread example-code.py >/dev/null
	$(PYTHON) -m coverage report -m
	$(PYTHON) -m coverage html --title "APSW python coverage"
	$(PYTHON) -m webbrowser -t htmlcov/index.html

test: build_ext ## Standard testing
	env $(PYTHON) -m apsw.tests

test_debug: $(PYDEBUG_DIR)/bin/python3  src/faultinject.h ## Testing in debug mode and sanitizer
	$(MAKE) build_ext_debug PYTHON=$(PYDEBUG_DIR)/bin/python3
	env APSWTESTPREFIX=$(PYDEBUG_WORKDIR) $(PYDEBUG_DIR)/bin/python3 -m apsw.tests -v

fulltest: test test_debug

linkcheck:  ## Checks links from doc
	make RELEASEDATE=$(RELEASEDATE) VERSION=$(VERSION) -C doc linkcheck

unwrapped:  ## Find SQLite APIs that are not wrapped by APSW
	env PYTHONPATH=. $(PYTHON) tools/find_unwrapped_apis.py

publish: docs
	rsync -a --delete --exclude=.git --exclude=.nojekyll doc/build/html/ ../apsw-publish/ ;  cd ../apsw-publish ; git status

src/apswversion.h: Makefile
	echo "#define APSW_VERSION \"$(VERSION)\"" > src/apswversion.h

stubtest: build_ext  ## Verifies type annotations with mypy
	$(PYTHON) -m mypy.stubtest --allowlist tools/stubtest.allowlist apsw
	$(PYTHON) -m mypy example-code.py
	$(PYTHON) -m mypy --strict example-code.py

fossil: ## Grabs latest trunk from SQLite source control, extracts and builds in sqlite3 directory
	-mv sqlite3/sqlite3config.h .
	-rm -rf sqlite3
	mkdir sqlite3
	set -e ; cd sqlite3 ; curl --output - https://www.sqlite.org/src/tarball/sqlite.tar.gz | tar xfz - --strip-components=1
	set -e ; cd sqlite3 ; ./configure --quiet --enable-all ; make sqlite3.c sqlite3
	-mv sqlite3config.h sqlite3/

# the funky test stuff is to exit successfully when grep has rc==1 since that means no lines found.
showsymbols:  ## Finds any C symbols that aren't static(private)
	rm -f apsw/__init__`$(PYTHON) -c "import sysconfig; print(sysconfig.get_config_var('EXT_SUFFIX'))"`
	$(PYTHON) setup.py fetch --all --version=$(SQLITEVERSION) build_ext --inplace --force --enable-all-extensions
	test -f apsw/__init__`$(PYTHON) -c "import sysconfig; print(sysconfig.get_config_var('EXT_SUFFIX'))"`
	set +e; nm --extern-only --defined-only apsw/__init__`$(PYTHON) -c "import sysconfig; print(sysconfig.get_config_var('EXT_SUFFIX'))"` | egrep -v ' (__bss_start|_edata|_end|_fini|_init|initapsw|PyInit_apsw)$$' ; test $$? -eq 1 || false

# Windows compilation
WINBPREFIX=fetch --version=$(SQLITEVERSION) --all build_ext --enable-all-extensions --inplace build
WINBSUFFIX=build_test_extension test
WINBWHEEL=bdist_wheel
# config used in CI
WINCICONFIG=set APSW_TEST_FSYNC_OFF=set &

compile-win:  ## Builds and tests against all the Python versions on Windows
	-del /q apsw\\*.pyd
	-del /q dist\\*.egg
	-del /q testextension.*
	-cmd /c del /s /q __pycache__
	-cmd /c del /s /q sqlite3
	cmd /c del /s /q dist
	cmd /c del /s /q build
	-cmd /c md dist
	c:/python311-32/python setup.py $(WINBPREFIX) $(WINBSUFFIX) $(WINBWHEEL)
	$(WINCICONFIG) c:/python311-32/python -m apsw.tests
	c:/python311/python setup.py $(WINBPREFIX) $(WINBSUFFIX) $(WINBWHEEL)
	$(WINCICONFIG) c:/python311/python -m apsw.tests
	c:/python310-32/python setup.py $(WINBPREFIX) $(WINBSUFFIX) $(WINBWHEEL)
	$(WINCICONFIG) c:/python310-32/python -m apsw.tests
	c:/python310/python setup.py $(WINBPREFIX) $(WINBSUFFIX) $(WINBWHEEL)
	$(WINCICONFIG) c:/python310/python -m apsw.tests
	c:/python39-32/python setup.py $(WINBPREFIX) $(WINBSUFFIX) $(WINBWHEEL)
	$(WINCICONFIG) c:/python39-32/python -m apsw.tests
	c:/python39/python setup.py $(WINBPREFIX) $(WINBSUFFIX) $(WINBWHEEL)
	$(WINCICONFIG) c:/python39/python -m apsw.tests
	c:/python38/python setup.py $(WINBPREFIX) $(WINBSUFFIX) $(WINBWHEEL)
	$(WINCICONFIG) c:/python38/python -m apsw.tests
	c:/python38-64/python setup.py $(WINBPREFIX) $(WINBSUFFIX) $(WINBWHEEL)
	$(WINCICONFIG) c:/python38-64/python -m apsw.tests
	c:/python37/python setup.py $(WINBPREFIX) $(WINBSUFFIX) $(WINBWHEEL)
	$(WINCICONFIG) c:/python37/python -m apsw.tests
	c:/python37-64/python setup.py $(WINBPREFIX) $(WINBSUFFIX) $(WINBWHEEL)
	$(WINCICONFIG) c:/python37-64/python -m apsw.tests
	c:/python36/python setup.py $(WINBPREFIX) $(WINBSUFFIX) $(WINBWHEEL)
	$(WINCICONFIG) c:/python36/python -m apsw.tests
	c:/python36-64/python setup.py $(WINBPREFIX) $(WINBSUFFIX) $(WINBWHEEL)
	$(WINCICONFIG) c:/python36-64/python -m apsw.tests

setup-wheel:  ## Ensures all Python Windows version have wheel support
	c:/python311/python -m ensurepip
	c:/python311/python -m pip install --upgrade wheel setuptools
	c:/python311-32/python -m ensurepip
	c:/python311-32/python -m pip install --upgrade wheel setuptools
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
	c:/python36/python -m ensurepip
	c:/python36/python -m pip install --upgrade wheel setuptools
	c:/python36-64/python -m ensurepip
	c:/python36-64/python -m pip install --upgrade wheel setuptools


source_nocheck: docs src/apswversion.h
	$(PYTHON) setup.py sdist --formats zip --add-doc

source: source_nocheck # Make the source and then check it builds and tests correctly.  This will catch missing files etc
	mkdir -p work
	rm -rf work/$(VERDIR)
	cd work ; unzip -q ../dist/$(VERDIR).zip
# Make certain various files do/do not exist
	for f in doc/vfs.html doc/_sources/pysqlite.txt apsw/trace.py ; do test -f work/$(VERDIR)/$$f ; done
	for f in sqlite3.c sqlite3/sqlite3.c debian/control src/faultinject.h ; do test ! -f work/$(VERDIR)/$$f ; done
# Test code works
	cd work/$(VERDIR) ; $(PYTHON) setup.py fetch --version=$(SQLITEVERSION) --all build_ext --inplace --enable-all-extensions build_test_extension test

release: ## Signs built source file(s)
	test -f dist/$(VERDIR).zip
	-rm -f dist/$(VERDIR)-sigs.zip dist/*.asc
	for f in dist/* ; do gpg --use-agent --armor --detach-sig "$$f" ; done
	cd dist ; zip -m $(VERDIR)-sigs.zip *.asc

# building a python debug interpreter
PYDEBUG_VER=3.11.2
PYDEBUG_DIR=/space/pydebug
PYVALGRIND_VER=$(PYDEBUG_VER)
PYVALGRIND_DIR=/space/pyvalgrind
# This must end in slash
PYDEBUG_WORKDIR=/space/apsw-test/

pydebug: ## Build a debug python including address sanitizer.  Extensions it builds are also address sanitized
	set -x && cd "$(PYDEBUG_DIR)" && find . -delete && \
	curl https://www.python.org/ftp/python/$(PYDEBUG_VER)/Python-$(PYDEBUG_VER).tar.xz | tar xfJ - && \
	cd Python-$(PYDEBUG_VER) && \
	./configure --with-address-sanitizer --with-undefined-behavior-sanitizer --without-pymalloc --with-pydebug --prefix="$(PYDEBUG_DIR)" \
	CPPFLAGS="-DPyDict_MAXFREELIST=0 -DPyFloat_MAXFREELIST=0 -DPyTuple_MAXFREELIST=0 -DPyList_MAXFREELIST=0" && \
	env PATH="/usr/lib/ccache:$$PATH" ASAN_OPTIONS=detect_leaks=false make -j install

pyvalgrind: ## Build a debug python with valgrind integration
	set -x && cd "$(PYVALGRIND_DIR)" && find . -delete && \
	curl https://www.python.org/ftp/python/$(PYVALGRIND_VER)/Python-$(PYVALGRIND_VER).tar.xz | tar xfJ - && \
	cd Python-$(PYVALGRIND_VER) && \
	./configure --with-valgrind --without-pymalloc  --with-pydebug --prefix="$(PYVALGRIND_DIR)" \
	CPPFLAGS="-DPyDict_MAXFREELIST=0 -DPyFloat_MAXFREELIST=0 -DPyTuple_MAXFREELIST=0 -DPyList_MAXFREELIST=0" && \
	env PATH="/usr/lib/ccache:$$PATH" make -j install

valgrind: $(PYVALGRIND_DIR)/bin/python3 src/faultinject.h ## Runs multiple iterations with valgrind to catch leaks
	$(PYVALGRIND_DIR)/bin/python3 setup.py fetch --version=$(SQLITEVERSION) --all && \
	  env APSWTESTPREFIX=$(PYDEBUG_WORKDIR) PATH=$(PYVALGRIND_DIR)/bin:$$PATH SHOWINUSE=t APSW_TEST_ITERATIONS=6 tools/valgrind.sh 2>&1 | tee l6 && \
	  env APSWTESTPREFIX=$(PYDEBUG_WORKDIR) PATH=$(PYVALGRIND_DIR)/bin:$$PATH SHOWINUSE=t APSW_TEST_ITERATIONS=7 tools/valgrind.sh 2>&1 | tee l7 && \
	  env APSWTESTPREFIX=$(PYDEBUG_WORKDIR) PATH=$(PYVALGRIND_DIR)/bin:$$PATH SHOWINUSE=t APSW_TEST_ITERATIONS=8 tools/valgrind.sh 2>&1 | tee l8

valgrind1: $(PYVALGRIND_DIR)/bin/python3 src/faultinject.h ## valgrind check (one iteration)
	$(PYVALGRIND_DIR)/bin/python3 setup.py fetch --version=$(SQLITEVERSION) --all && \
	  env APSWTESTPREFIX=$(PYDEBUG_WORKDIR) PATH=$(PYVALGRIND_DIR)/bin:$$PATH SHOWINUSE=t APSW_TEST_ITERATIONS=1 tools/valgrind.sh

valgrind_no_fetch: $(PYVALGRIND_DIR)/bin/python3 src/faultinject.h ## valgrind check (one iteration) - does not fetch SQLite, using existing directory
	  env APSWTESTPREFIX=$(PYDEBUG_WORKDIR) PATH=$(PYVALGRIND_DIR)/bin:$$PATH SHOWINUSE=t APSW_TEST_ITERATIONS=1 tools/valgrind.sh