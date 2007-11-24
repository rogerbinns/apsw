
VERSION=3.5.2-r1
VERDIR=apsw-$(VERSION)

SOURCE=apsw.c apsw.html apswversion.h mingwsetup.bat pointerlist.c \
	  setup.py statementcache.c testextension.c tests.py traceback.c 

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
# otherwise it goes off and modifies them, surrounding colons and almost any
# other punctuation it sees!
distrib-win:
	cmd //c del //s //q dist
	cmd //c del //s //q build
	cmd //c del //s //q $(VERDIR)
	-cmd //c md $(VERDIR)
	-cmd //c md dist
	c:/python23/python setup.py build --compile=mingw32 bdist_wininst
	c:/python24/python setup.py build --compile=mingw32 bdist_wininst
	c:/python25/python setup.py build --compile=mingw32 bdist_wininst
	pscp dist/*.exe rogerb@initd.org://var//www//pub//software//pysqlite//apsw//$(VERSION)//

distrib-lin:
	rm -rf $(VERDIR)
	mkdir $(VERDIR)
	cp  $(SOURCE) $(VERDIR)
	rm -rf dist
	mkdir dist
	zip -9 -r dist/$(VERDIR).zip $(VERDIR)
	ssh initd.org mkdir -p /var/www/pub/software/pysqlite/apsw/$(VERSION)
	scp dist/$(VERDIR).zip apsw.html initd.org:/var/www/pub/software/pysqlite/apsw/$(VERSION)/
