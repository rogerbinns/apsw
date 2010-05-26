# Makefile for Sphinx documentation
#

# The Microsoft Help Compiler can't cope with a directory starting
# with dot such as the default used by sphinx - .build so we use build
# without the leading dot.

# You can set these variables from the command line.
SPHINXOPTS    =
SPHINXBUILD   = sphinx-build -q
PAPER         =

# Internal variables.
PAPEROPT_a4     = -D latex_paper_size=a4
PAPEROPT_letter = -D latex_paper_size=letter
ALLSPHINXOPTS   = -d build/doctrees $(PAPEROPT_$(PAPER)) $(SPHINXOPTS) .

.PHONY: help clean html web pickle htmlhelp latex changes linkcheck

help:
	@echo "Please use \`make <target>' where <target> is one of"
	@echo "  html      to make standalone HTML files"
	@echo "  pickle    to make pickle files (usable by e.g. sphinx-web)"
	@echo "  htmlhelp  to make HTML files and a HTML help project"
	@echo "  latex     to make LaTeX files, you can set PAPER=a4 or PAPER=letter"
	@echo "  changes   to make an overview over all changed/added/deprecated items"
	@echo "  linkcheck to check all external links for integrity"

clean:
	-rm -rf build/* 
	-rm -f ../dist/apsw*.chm ../dist/apsw*.pdf ../dist/apswdoc-*.zip

html:
	mkdir -p build/html build/doctrees ../dist
	$(SPHINXBUILD) -b html $(ALLSPHINXOPTS) build/html
	cd build/html ; zip -9rq ../../../dist/apswdoc-$(VERSION).zip *

pickle:
	mkdir -p build/pickle build/doctrees
	$(SPHINXBUILD) -b pickle $(ALLSPHINXOPTS) build/pickle
	@echo
	@echo "Build finished; now you can process the pickle files or run"
	@echo "  sphinx-web build/pickle"
	@echo "to start the sphinx-web server."

web: pickle

# See http://code.google.com/p/htmlhelp/wiki/HHW4Wine for how to get
# the Microsoft help compiler on Linux.  It also always has a non-zero
# exit code.

htmlhelp:
	-rm -f htmlhelp
	mkdir -p build/htmlhelp build/doctrees ../dist
	$(SPHINXBUILD) -b htmlhelp $(ALLSPHINXOPTS) build/htmlhelp
	wine "c:/program files/html help workshop/hhc.exe" build/htmlhelp/apsw.hhp || true
	cp build/htmlhelp/apsw.chm ../dist/apsw-$(VERSION).chm

latex:
	mkdir -p build/latex build/doctrees ../dist
	$(SPHINXBUILD) -b latex $(ALLSPHINXOPTS) build/latex
	make -C build/latex all-pdf
	cp build/latex/apsw.pdf ../dist/apsw-$(VERSION).pdf

changes:
	mkdir -p build/changes build/doctrees
	$(SPHINXBUILD) -b changes $(ALLSPHINXOPTS) build/changes
	@echo
	@echo "The overview file is in build/changes."

linkcheck:
	mkdir -p build/linkcheck build/doctrees
	$(SPHINXBUILD) -b linkcheck $(ALLSPHINXOPTS) build/linkcheck
	@echo
	@echo "Link check complete; look for any errors in the above output " \
	      "or in build/linkcheck/output.txt."

pdf:
	mkdir -p build/pdf build/doctrees
	$(SPHINXBUILD) -b pdf $(ALLSPHINXOPTS) build/pdf

epub:
	mkdir -p build/epub build/doctrees
	$(SPHINXBUILD) -b epub $(ALLSPHINXOPTS) build/epub
