
VERSION=3.3.5-r1
VERDIR=apsw-$(VERSION)

all: header toc colour

header:
	echo "#define APSW_VERSION \"$(VERSION)\"" > apswversion.h

toc:
	awk ' BEGIN {p=1} /<!--toc-->/ {p=0; print;next} /<!--endtoc-->/ {p=1} {if(p) print} ' < apsw-source.html > tmpfile
	hypertoc --gen_anchors tmpfile >tmpfile2
	hypertoc --gen_toc --inline --toc_tag '!--toc--' \
	  --toc_label "" tmpfile2 | \
	sed 's@\(<li><a href="#dbapinotes">\)@</ul></td><td valign="top"><ul>\1@' | \
	grep -v '"list-style: none;"' >apsw-source.html
	@rm -f tmpfile tmpfile2

colour:
	python coloursrc.py

# You need to use the MinGW version of make.  It needs the doubled up slashes
distrib-win:
	cmd //c del //s //q dist
	cmd //c del //s //q build
	cmd //c del //s //q $(VERDIR)
	-cmd //c md $(VERDIR)
	-cmd //c md dist
	cmd //c copy apsw.html $(VERDIR)
	cmd //c copy apsw.c $(VERDIR)
	cmd //c copy apswversion.h $(VERDIR)
	cmd //c copy setup.py $(VERDIR)
	cmd //c copy tests.py $(VERDIR)
	cmd //c copy mingwsetup.bat $(VERDIR)
	zip -9 -r dist/$(VERDIR).zip $(VERDIR)
	c:/python23/python setup.py build --compile=mingw32 bdist_wininst
	c:/python24/python setup.py build --compile=mingw32 bdist_wininst
