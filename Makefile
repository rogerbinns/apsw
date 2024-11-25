
SQLITEVERSION=3.47.1
APSWSUFFIX=.0

RELEASEDATE="25 November 2024"

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
	doc/backup.rst \
	doc/fts.rst

.PHONY : help all tagpush clean doc docs build_ext build_ext_debug coverage pycoverage test test_debug fulltest linkcheck unwrapped \
		 publish stubtest showsymbols compile-win setup-wheel source_nocheck source release pydebug \
		 fossil doc-depends dev-depends docs-no-fetch compile-win-one langserver

help: ## Show this help
	@egrep -h '\s##\s' $(MAKEFILE_LIST) | sort | \
	awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

all: src/apswversion.h src/apsw.docstrings apsw/__init__.pyi src/constants.c src/stringconstants.c  test docs ## Update generated files, build, test, make doc

tagpush: ## Tag with version and push
	test "`git branch --show-current`" = master
	git tag -af $(SQLITEVERSION)$(APSWSUFFIX)
	git push --tags

clean: ## Cleans up everything
	$(MAKE) PYTHONPATH="`pwd`" VERSION=$(VERSION) -C doc clean
	rm -rf dist build work/* megatestresults apsw.egg-info __pycache__ apsw/__pycache__ :memory: .mypy_cache .ropeproject htmlcov "System Volume Information" doc/docdb.json
	for i in 'vgcore.*' '.coverage*' '*.pyc' '*.pyo' '*~' '*.o' '*.so' '*.dll' '*.pyd' '*.gcov' '*.gcda' '*.gcno' '*.orig' '*.tmp' 'testdb*' 'testextension.sqlext' ; do \
		find . -type f -name "$$i" -print0 | xargs -0 --no-run-if-empty rm -f ; done
	rm -f doc/typing.rstgen doc/example.rst doc/example-fts.rst doc/renames.rstgen $(GENDOCS)
	rm -f compile_commands.json setup.apsw work
	-rm -rf sqlite3/

doc: docs ## Builds all the doc

docs: build_ext docs-no-fetch

docs-no-fetch: $(GENDOCS) doc/example.rst doc/example-fts.rst doc/.static doc/typing.rstgen doc/renames.rstgen
	rm -f testdb
	env PYTHONPATH=. $(PYTHON) tools/docmissing.py
	env PYTHONPATH=. $(PYTHON) tools/docupdate.py $(VERSION) $(RELEASEDATE)
	$(MAKE) PYTHONPATH="`pwd`" VERSION=$(VERSION) RELEASEDATE=$(RELEASEDATE) -C doc clean html
	tools/spellcheck.sh
	rst2html5 --strict --verbose --exit-status 1 README.rst >/dev/null

doc/example.rst: examples/main.py tools/example2rst.py src/apswversion.h
	rm -f dbfile
	env PYTHONPATH=. $(PYTHON) -sS tools/example2rst.py examples/main.py doc/example.rst
	rm -f dbfile

doc/example-fts.rst: examples/fts.py tools/example2rst.py src/apswversion.h
	-rm -f recipes.db*
	cp ../apsw-extended-testing/recipes.db .
	env PYTHONPATH=. $(PYTHON) -sS tools/example2rst.py examples/fts.py doc/example-fts.rst
	rm -f recipes.db*

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
	$(PYTHON) -m pip install -U --upgrade-strategy eager mypy pdbp coverage ruff

# This is probably gnu make specific but only developers use this makefile
$(GENDOCS): doc/%.rst: src/%.c tools/code2rst.py  tools/tocupdate.sql
	env PYTHONPATH=. $(PYTHON) tools/code2rst.py $(SQLITEVERSION) doc/docdb.json $< $@

apsw/__init__.pyi src/apsw.docstrings: $(GENDOCS) tools/gendocstrings.py src/apswtypes.py  tools/tocupdate.sql
	env PYTHONPATH=. $(PYTHON) tools/gendocstrings.py doc/docdb.json src/apsw.docstrings

src/constants.c: Makefile tools/genconstants.py src/apswversion.h tools/tocupdate.sql
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
	tools/coverage.sh

PYCOVERAGEOPTS=--source apsw -p

pycoverage:  ## Coverage of all the Python code
	-rm -rf .coverage .coverage.* htmlcov dbfile
	$(PYTHON) -m coverage run $(PYCOVERAGEOPTS) -m apsw.tests
	$(PYTHON) -m coverage run $(PYCOVERAGEOPTS) -m apsw ":memory:" .exit
	$(PYTHON) -m coverage run $(PYCOVERAGEOPTS) -m apsw.speedtest --iterations 2 --scale 2 --unicode 25 --apsw --sqlite3
	$(PYTHON) -m coverage run $(PYCOVERAGEOPTS) -m apsw.trace -o /dev/null --sql --rows --timestamps --thread examples/main.py >/dev/null
	$(PYTHON) -m coverage combine
	$(PYTHON) -m coverage report -m
	$(PYTHON) -m coverage html --title "APSW python coverage"
	$(PYTHON) -m webbrowser -t htmlcov/index.html

ftscoverage: ## Coverage of Python code for FTS support
	-rm -rf .coverage .coverage.* htmlcov dbfile
	$(PYTHON) -m coverage run $(PYCOVERAGEOPTS) -m apsw.ftstests
	$(PYTHON) -m coverage combine
	$(PYTHON) -m coverage report -m
	$(PYTHON) -m coverage html --title "APSW FTS python coverage"
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

stubtest: ## Verifies type annotations with mypy
	-$(PYTHON) -m mypy.stubtest --allowlist tools/stubtest.allowlist apsw
	env PYTHONPATH=. $(PYTHON) -m mypy --allow-redefinition examples/main.py
	env PYTHONPATH=. $(PYTHON) -m mypy --allow-redefinition examples/fts.py

# set this to a commit id to grab that instead
FOSSIL_URL="https://www.sqlite.org/src/tarball/sqlite.tar.gz"
fossil: ## Grabs latest trunk from SQLite source control, extracts and builds in sqlite3 directory
	-mv sqlite3/sqlite3config.h .
	-rm -rf sqlite3
	mkdir sqlite3
	set -e ; cd sqlite3 ; curl --output - $(FOSSIL_URL) | tar xfz - --strip-components=1
	set -e ; cd sqlite3 ; ./configure --quiet --all --disable-tcl ; $(MAKE) sqlite3.c sqlite3
	-mv sqlite3config.h sqlite3/

# the funky test stuff is to exit successfully when grep has rc==1 since that means no lines found.
showsymbols:  ## Finds any C symbols that aren't static(private)
	rm -f apsw/__init__`$(PYTHON) -c "import sysconfig; print(sysconfig.get_config_var('EXT_SUFFIX'))"`
	$(PYTHON) setup.py fetch --all --version=$(SQLITEVERSION) build_ext --inplace --force --enable-all-extensions
	test -f apsw/__init__`$(PYTHON) -c "import sysconfig; print(sysconfig.get_config_var('EXT_SUFFIX'))"`
	set +e; nm --extern-only --defined-only apsw/__init__`$(PYTHON) -c "import sysconfig; print(sysconfig.get_config_var('EXT_SUFFIX'))"` | egrep -v ' (__bss_start|_edata|_end|_fini|_init|initapsw|PyInit_apsw)$$' ; test $$? -eq 1 || false
	test -f apsw/_unicode`$(PYTHON) -c "import sysconfig; print(sysconfig.get_config_var('EXT_SUFFIX'))"`
	set +e; nm --extern-only --defined-only apsw/_unicode`$(PYTHON) -c "import sysconfig; print(sysconfig.get_config_var('EXT_SUFFIX'))"` | egrep -v ' (__bss_start|_edata|_end|_fini|_init|PyInit__unicode)$$' ; test $$? -eq 1 || false

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
	$(MAKE) compile-win-one PYTHON=c:/python313/python
	$(MAKE) compile-win-one PYTHON=c:/python313-32/python
	$(MAKE) compile-win-one PYTHON=c:/python312/python
	$(MAKE) compile-win-one PYTHON=c:/python312-32/python
	$(MAKE) compile-win-one PYTHON=c:/python311/python
	$(MAKE) compile-win-one PYTHON=c:/python311-32/python
	$(MAKE) compile-win-one PYTHON=c:/python310-32/python
	$(MAKE) compile-win-one PYTHON=c:/python310/python
	$(MAKE) compile-win-one PYTHON=c:/python39-32/python
	$(MAKE) compile-win-one PYTHON=c:/python39/python

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
	rm -rf doc/build/html/_static/fonts/ doc/build/html/_static/css/fonts/ doc/build/apsw.1
	rst2man doc/cli.rst doc/build/apsw.1
	$(PYTHON) setup.py sdist --formats zip --add-doc

source: source_nocheck # Make the source and then check it builds and tests correctly.  This will catch missing files etc
	mkdir -p work
	rm -rf work/$(VERDIR)
	cd work ; unzip -q ../dist/$(VERDIR).zip
# Make certain various files do/do not exist
	for f in man/cli.1 doc/vfs.html doc/_sources/pysqlite.txt apsw/trace.py src/faultinject.h; do test -f work/$(VERDIR)/$$f ; done
	for f in sqlite3.c sqlite3/sqlite3.c debian/control ; do test ! -f work/$(VERDIR)/$$f ; done
# Test code works
	cd work/$(VERDIR) ; $(PYTHON) setup.py fetch --version=$(SQLITEVERSION) --all build_ext --inplace --enable-all-extensions build_test_extension test

release: ## Signs built source file(s)
	test "`git branch --show-current`" = master
	test -f dist/$(VERDIR).zip
	-rm -f dist/$(VERDIR).cosign-bundle
	cosign sign-blob --yes --bundle dist/$(VERDIR).cosign-bundle dist/$(VERDIR).zip

src/_unicodedb.c: tools/ucdprops2code.py ## Update generated Unicode database lookups
	-rm -f $@
	$(PYTHON) tools/ucdprops2code.py $@

# building a python debug interpreter
PYDEBUG_VER=3.12.7
PYDEBUG_DIR=/space/pydebug
PYTHREAD_VER=$(PYDEBUG_VER)
PYTHREAD_DIR=/space/pythread
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

pythread: ## Build a debug python including thread sanitizer.  Extensions it builds are also thread sanitized
	set -x && cd "$(PYTHREAD_DIR)" && find . -delete && \
	curl https://www.python.org/ftp/python/`echo $(PYTHREAD_VER) | sed 's/[abr].*//'`/Python-$(PYTHREAD_VER).tar.xz | tar xfJ - && \
	cd Python-$(PYDEBUG_VER) && \
	env CFLAGS=-fsanitize=thread LDFLAGS=-fsanitize=thread TSAN_OPTIONS=report_bugs=0 ./configure  --without-pymalloc --with-pydebug --prefix="$(PYTHREAD_DIR)" --without-freelists  && \
	$(MAKE) -j install
	$(MAKE) dev-depends PYTHON=$(PYTHREAD_DIR)/bin/python3

langserver:  ## Language server integration json
	$(PYTHON) tools/gencompilecommands.py > compile_commands.json

megatest-build: ## Builds and updates podman container for running megatest
	podman build --squash-all -t apsw-megatest -f tools/apsw-megatest-build

MEGATEST_ARGS=
megatest-run: ## Runs megatest in container
	podman run --pids-limit=-1 -i --tty -v "`pwd`/../apsw-test:/megatest/apsw-test" -v "`pwd`:/megatest/apsw" -v "$$HOME/.ccache:/megatest/ccache" apsw-megatest $(MEGATEST_ARGS)

megatest-shell: ## Runs a shell in the megatest container
	podman run -i --tty -v "`pwd`/../apsw-test:/megatest/apsw-test" -v "`pwd`:/megatest/apsw" -v "$$HOME/.ccache:/megatest/ccache" --entrypoint /bin/bash apsw-megatest
