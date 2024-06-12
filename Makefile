
SQLITEVERSION=3.46.0
APSWSUFFIX=.0

RELEASEDATE="24 May 2024"

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
		 fossil doc-depends dev-depends docs-no-fetch compile-win-one langserver

help: ## Show this help
	@egrep -h '\s##\s' $(MAKEFILE_LIST) | sort | \
	awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

all: src/apswversion.h src/apsw.docstrings apsw/__init__.pyi src/constants.c src/stringconstants.c test docs ## Update generated files, build, test, make doc

tagpush: ## Tag with version and push
	test "`git branch --show-current`" = master
	git tag -af $(SQLITEVERSION)$(APSWSUFFIX)
	git push --tags

clean: ## Cleans up everything
	$(MAKE) PYTHONPATH="`pwd`" VERSION=$(VERSION) -C doc clean
	rm -rf dist build work/* megatestresults apsw.egg-info __pycache__ apsw/__pycache__ :memory: .mypy_cache .ropeproject htmlcov "System Volume Information" doc/docdb.json
	mkdir dist
	for i in 'vgcore.*' '.coverage' '*.pyc' '*.pyo' '*~' '*.o' '*.so' '*.dll' '*.pyd' '*.gcov' '*.gcda' '*.gcno' '*.orig' '*.tmp' 'testdb*' 'testextension.sqlext' ; do \
		find . -type f -name "$$i" -print0 | xargs -0 --no-run-if-empty rm -f ; done
	rm -f doc/typing.rstgen doc/example.rst doc/renames.rstgen $(GENDOCS)
	-rm -rf sqlite3/

doc: docs ## Builds all the doc

docs: build_ext docs-no-fetch

docs-no-fetch: $(GENDOCS) doc/example.rst doc/.static doc/typing.rstgen doc/renames.rstgen
	rm -f testdb
	env PYTHONPATH=. $(PYTHON) tools/docmissing.py
	env PYTHONPATH=. $(PYTHON) tools/docupdate.py $(VERSION)
	$(MAKE) PYTHONPATH="`pwd`" VERSION=$(VERSION) RELEASEDATE=$(RELEASEDATE) -C doc clean html
	tools/spellcheck.sh
	rst2html.py --strict --verbose --exit-status 1 README.rst >/dev/null

doc/example.rst: example-code.py tools/example2rst.py src/apswversion.h
	rm -f dbfile
	env PYTHONPATH=. $(PYTHON) -sS tools/example2rst.py
	rm -f dbfile

doc/typing.rstgen: src/apswtypes.py tools/types2rst.py
	-rm -f doc/typing.rstgen
	$(PYTHON) tools/types2rst.py

doc/renames.rstgen: tools/names.py tools/renames.json
	-rm -f doc/renames.rstgen
	env PYTHONPATH=. $(PYTHON) tools/names.py rst-gen > doc/renames.rstgen


doc/.static:
	mkdir -p doc/.static

doc-depends: ## pip installs packages needed to build doc
	$(PYTHON) -m pip install -U --upgrade-strategy eager sphinx sphinx_rtd_theme

dev-depends: ## pip installs packages useful for development (none are necessary except setuptools)
	$(PYTHON) -m pip install -U --upgrade-strategy eager build wheel setuptools pip
	$(PYTHON) -m pip install -U --upgrade-strategy eager mypy pdbpp coverage flake8 ruff

# This is probably gnu make specific but only developers use this makefile
$(GENDOCS): doc/%.rst: src/%.c tools/code2rst.py
	env PYTHONPATH=. $(PYTHON) tools/code2rst.py $(SQLITEVERSION) doc/docdb.json $< $@

apsw/__init__.pyi src/apsw.docstrings: $(GENDOCS) tools/gendocstrings.py src/apswtypes.py
	env PYTHONPATH=. $(PYTHON) tools/gendocstrings.py doc/docdb.json src/apsw.docstrings

src/constants.c: Makefile tools/genconstants.py src/apswversion.h
	-rm -f src/constants.c
	env PYTHONPATH=. $(PYTHON) tools/genconstants.py > src/constants.c

src/stringconstants.c: Makefile tools/genstrings.py src/apswversion.h
	-rm -f src/stringconstants.c
	$(PYTHON) tools/genstrings.py > src/stringconstants.c

build_ext: src/apswversion.h  apsw/__init__.pyi src/apsw.docstrings ## Fetches SQLite and builds the extension
	env $(PYTHON) setup.py fetch --version=$(SQLITEVERSION) --all build_ext -DSQLITE_ENABLE_COLUMN_METADATA --inplace --force --enable-all-extensions
	env $(PYTHON) setup.py build_test_extension

src/faultinject.h: tools/genfaultinject.py
	-rm src/faultinject.h
	tools/genfaultinject.py src/faultinject.h

build_ext_debug: src/apswversion.h src/faultinject.h ## Fetches SQLite and builds the extension in debug mode
	env $(PYTHON) setup.py fetch --version=$(SQLITEVERSION) --all build_ext --inplace --force --enable-all-extensions --debug

coverage:  src/faultinject.h ## Coverage of the C code
	env $(PYTHON) setup.py fetch --version=$(SQLITEVERSION) --all && tools/coverage.sh

PYCOVERAGEOPTS=--source apsw --append

pycoverage:  ## Coverage of the Python code
	-rm -rf .coverage htmlcov dbfile
	$(PYTHON) -m coverage run $(PYCOVERAGEOPTS) -m apsw.tests
	$(PYTHON) -m coverage run $(PYCOVERAGEOPTS) -m apsw ":memory:" .exit
	$(PYTHON) -m coverage run $(PYCOVERAGEOPTS) -m apsw.speedtest --apsw --sqlite3
	$(PYTHON) -m coverage run $(PYCOVERAGEOPTS) -m apsw.trace -o /dev/null --sql --rows --timestamps --thread example-code.py >/dev/null
	$(PYTHON) -m coverage report -m
	$(PYTHON) -m coverage html --title "APSW python coverage"
	$(PYTHON) -m webbrowser -t htmlcov/index.html

test: build_ext ## Standard testing
	env $(PYTHON) -m apsw.tests
	env PYTHONPATH=. $(PYTHON) tools/names.py run-tests
	env $(PYTHON) setup.py build_ext -DSQLITE_ENABLE_COLUMN_METADATA --inplace --force --enable-all-extensions --apsw-no-old-names
	env $(PYTHON) -m apsw.tests
	rm apsw/__init__.pyi
	$(MAKE) apsw/__init__.pyi

test_debug: $(PYDEBUG_DIR)/bin/python3  src/faultinject.h ## Testing in debug mode and sanitizer
	$(MAKE) build_ext_debug PYTHON=$(PYDEBUG_DIR)/bin/python3
	env APSWTESTPREFIX=$(PYDEBUG_WORKDIR) $(PYDEBUG_DIR)/bin/python3 -m apsw.tests -v

fulltest: test test_debug

linkcheck:  ## Checks links from doc
	env PYTHONPATH="`pwd`" $(MAKE) RELEASEDATE=$(RELEASEDATE) VERSION=$(VERSION) -C doc linkcheck

unwrapped:  ## Find SQLite APIs that are not wrapped by APSW
	env PYTHONPATH=. $(PYTHON) tools/find_unwrapped_apis.py

publish: docs
	rsync -a --delete --exclude=.git --exclude=.nojekyll doc/build/html/ ../apsw-publish/ ;  cd ../apsw-publish ; git status

src/apswversion.h: Makefile
	echo "#define APSW_VERSION \"$(VERSION)\"" > src/apswversion.h

stubtest: build_ext  ## Verifies type annotations with mypy
	-$(PYTHON) -m mypy.stubtest --allowlist tools/stubtest.allowlist apsw
	$(PYTHON) -m mypy --allow-redefinition example-code.py
	$(PYTHON) -m mypy --allow-redefinition --strict example-code.py

fossil: ## Grabs latest trunk from SQLite source control, extracts and builds in sqlite3 directory
	-mv sqlite3/sqlite3config.h .
	-rm -rf sqlite3
	mkdir sqlite3
	set -e ; cd sqlite3 ; curl --output - https://www.sqlite.org/src/tarball/sqlite.tar.gz | tar xfz - --strip-components=1
	set -e ; cd sqlite3 ; ./configure --quiet --enable-all ; $(MAKE) sqlite3.c sqlite3
	-mv sqlite3config.h sqlite3/

# the funky test stuff is to exit successfully when grep has rc==1 since that means no lines found.
showsymbols:  ## Finds any C symbols that aren't static(private)
	rm -f apsw/__init__`$(PYTHON) -c "import sysconfig; print(sysconfig.get_config_var('EXT_SUFFIX'))"`
	$(PYTHON) setup.py fetch --all --version=$(SQLITEVERSION) build_ext --inplace --force --enable-all-extensions
	test -f apsw/__init__`$(PYTHON) -c "import sysconfig; print(sysconfig.get_config_var('EXT_SUFFIX'))"`
	set +e; nm --extern-only --defined-only apsw/__init__`$(PYTHON) -c "import sysconfig; print(sysconfig.get_config_var('EXT_SUFFIX'))"` | egrep -v ' (__bss_start|_edata|_end|_fini|_init|initapsw|PyInit_apsw)$$' ; test $$? -eq 1 || false

compile-win:  ## Builds and tests against all the Python versions on Windows
	-del /q apsw\\*.pyd
	-del /q dist\\*.egg
	-del /q testextension.*
	-del /q *.whl
	-del /q setup.apsw
	-cmd /c del /s /q __pycache__
	-cmd /c del /s /q sqlite3
	-cmd /c del /s /q dist
	-cmd /c del /s /q build
	-cmd /c del /s /q .venv
	-cmd /c md dist
	$(MAKE) compile-win-one PYTHON=c:/python312/python
	$(MAKE) compile-win-one PYTHON=c:/python312-32/python
	$(MAKE) compile-win-one PYTHON=c:/python311/python
	$(MAKE) compile-win-one PYTHON=c:/python311-32/python
	$(MAKE) compile-win-one PYTHON=c:/python310-32/python
	$(MAKE) compile-win-one PYTHON=c:/python310/python
	$(MAKE) compile-win-one PYTHON=c:/python39-32/python
	$(MAKE) compile-win-one PYTHON=c:/python39/python
	$(MAKE) compile-win-one PYTHON=c:/python38/python
	$(MAKE) compile-win-one PYTHON=c:/python38-64/python

# I did try to make this use venv but then the pip inside the venv and
# other packages were skipped due to metadata issues
compile-win-one:  ## Does one Windows build - set PYTHON variable
	$(PYTHON) -m pip install --upgrade --upgrade-strategy eager pip wheel setuptools
	$(PYTHON) -m pip uninstall -y apsw
	copy tools\\setup-pypi.cfg setup.apsw
	$(PYTHON)  -m pip --no-cache-dir wheel -v .
	cmd /c FOR %i in (*.whl) DO $(PYTHON)  -m pip --no-cache-dir install --force-reinstall %i
	$(PYTHON) setup.py build_test_extension
	$(PYTHON) -m apsw.tests
	-del /q setup.apsw *.whl

# We ensure that only master can be made source, and that the
# myriad caches everywhere are removed (they end up in the examples
# doc)
source_nocheck: src/apswversion.h
	test "`git branch --show-current`" = master
	find . -depth -name '.*cache' -type d -exec rm -r "{}" \;
	env APSW_NO_GA=t $(MAKE) doc
	$(PYTHON) setup.py sdist --formats zip --add-doc

source: source_nocheck # Make the source and then check it builds and tests correctly.  This will catch missing files etc
	mkdir -p work
	rm -rf work/$(VERDIR)
	cd work ; unzip -q ../dist/$(VERDIR).zip
# Make certain various files do/do not exist
	for f in doc/vfs.html doc/_sources/pysqlite.txt apsw/trace.py src/faultinject.h; do test -f work/$(VERDIR)/$$f ; done
	for f in sqlite3.c sqlite3/sqlite3.c debian/control ; do test ! -f work/$(VERDIR)/$$f ; done
# Test code works
	cd work/$(VERDIR) ; $(PYTHON) setup.py fetch --version=$(SQLITEVERSION) --all build_ext --inplace --enable-all-extensions build_test_extension test

release: ## Signs built source file(s)
	test "`git branch --show-current`" = master
	test -f dist/$(VERDIR).zip
	-rm -f dist/$(VERDIR).cosign-bundle
	cosign sign-blob --yes --bundle dist/$(VERDIR).cosign-bundle dist/$(VERDIR).zip

# building a python debug interpreter
PYDEBUG_VER=3.12.4
PYDEBUG_DIR=/space/pydebug
PYVALGRIND_VER=$(PYDEBUG_VER)
PYVALGRIND_DIR=/space/pyvalgrind
# This must end in slash
PYDEBUG_WORKDIR=/space/apsw-test/

pydebug: ## Build a debug python including address sanitizer.  Extensions it builds are also address sanitized
	set -x && cd "$(PYDEBUG_DIR)" && find . -delete && \
	curl https://www.python.org/ftp/python/`echo $(PYDEBUG_VER) | sed 's/[abr].*//'`/Python-$(PYDEBUG_VER).tar.xz | tar xfJ - && \
	cd Python-$(PYDEBUG_VER) && \
	./configure --with-address-sanitizer --with-undefined-behavior-sanitizer --without-pymalloc --with-pydebug --prefix="$(PYDEBUG_DIR)" \
	--without-freelists --with-assertions && \
	env ASAN_OPTIONS=detect_leaks=false $(MAKE) -j install
	$(MAKE) dev-depends PYTHON=$(PYDEBUG_DIR)/bin/python3

pyvalgrind: ## Build a debug python with valgrind integration
	set -x && cd "$(PYVALGRIND_DIR)" && find . -delete && \
	curl https://www.python.org/ftp/python/`echo $(PYVALGRIND_VER) | sed 's/[abr].*//'`/Python-$(PYVALGRIND_VER).tar.xz | tar xfJ - && \
	cd Python-$(PYVALGRIND_VER) && \
	./configure --with-valgrind --without-pymalloc  --with-pydebug --prefix="$(PYVALGRIND_DIR)" \
	--without-freelists --with-assertions && \
	$(MAKE) -j install
	$(MAKE) dev-depends PYTHON=$(PYVALGRIND_DIR)/bin/python3


valgrind: $(PYVALGRIND_DIR)/bin/python3 src/faultinject.h ## Runs multiple iterations with valgrind to catch leaks
	$(PYVALGRIND_DIR)/bin/python3 setup.py fetch --version=$(SQLITEVERSION) --all && \
	  env APSWTESTPREFIX=$(PYDEBUG_WORKDIR) PATH=$(PYVALGRIND_DIR)/bin:$$PATH APSW_TEST_ITERATIONS=6 tools/valgrind.sh 2>&1 | tee l6 && \
	  env APSWTESTPREFIX=$(PYDEBUG_WORKDIR) PATH=$(PYVALGRIND_DIR)/bin:$$PATH APSW_TEST_ITERATIONS=7 tools/valgrind.sh 2>&1 | tee l7 && \
	  env APSWTESTPREFIX=$(PYDEBUG_WORKDIR) PATH=$(PYVALGRIND_DIR)/bin:$$PATH APSW_TEST_ITERATIONS=8 tools/valgrind.sh 2>&1 | tee l8

valgrind1: $(PYVALGRIND_DIR)/bin/python3 src/faultinject.h ## valgrind check (one iteration)
	$(PYVALGRIND_DIR)/bin/python3 setup.py fetch --version=$(SQLITEVERSION) --all && \
	  env APSWTESTPREFIX=$(PYDEBUG_WORKDIR) PATH=$(PYVALGRIND_DIR)/bin:$$PATH APSW_TEST_ITERATIONS=1 tools/valgrind.sh

valgrind_no_fetch: $(PYVALGRIND_DIR)/bin/python3 src/faultinject.h ## valgrind check (one iteration) - does not fetch SQLite, using existing directory
	  env APSWTESTPREFIX=$(PYDEBUG_WORKDIR) PATH=$(PYVALGRIND_DIR)/bin:$$PATH APSW_TEST_ITERATIONS=1 tools/valgrind.sh

langserver:  ## Language server integration json
	$(PYTHON) tools/gencompilecommands.py > compile_commands.json

megatest-build: ## Builds and updates podman container for running megatest
	podman build --squash-all -t apsw-megatest -f tools/apsw-megatest-build

MEGATEST_ARGS=
megatest-run: ## Runs megatest in container
	podman run -i --tty -v "`pwd`/../apsw-test:/megatest/apsw-test" -v "`pwd`:/megatest/apsw" -v "$$HOME/.ccache:/megatest/ccache" apsw-megatest $(MEGATEST_ARGS)