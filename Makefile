
VERSION=3.3.7-r1
VERDIR=apsw-$(VERSION)

all: header tidytoc

header:
	echo "#define APSW_VERSION \"$(VERSION)\"" > apswversion.h

tidytoc:
	python coloursrc.py
	awk ' BEGIN {p=1} /<!--toc-->/ {p=0; print;next} /<!--endtoc-->/ {p=1} {if(p) print} ' < apsw.html > tmpfile
	hypertoc --gen_anchors tmpfile >tmpfile2
	hypertoc --gen_toc --inline --toc_tag '!--toc--' \
	  --toc_label "" tmpfile2 | \
	sed 's@\(<li><a href="#dbapinotes">\)@</ul></td><td valign="top"><ul>\1@' | \
	grep -v '"list-style: none;"'> tmpfile
	-tidy -indent -asxhtml -wrap 80 <tmpfile >apsw.html
	@rm -f tmpfile tmpfile2 .tmpop-*

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
