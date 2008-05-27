
VERSION=3.5.9-r1
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
	-tidy -q -indent -asxhtml -wrap 120 <tmpfile >apsw.html
	@rm -f tmpfile2 .tmpop-*
	echo rm -f tmpfile
	linkchecker -q --no-status --ignore-url="http://www.sqlite.org/cvstrac/tktview?tn=[0-9][0-9][0-9][0-9]" apsw.html

# You need to use the MinGW version of make.  It needs the doubled up slashes
# otherwise it goes off and modifies them, surrounding colons and almost any
# other punctuation it sees!
compile-win:
	cmd /c del /s /q dist
	cmd /c del /s /q build
	-cmd /c md dist
	c:/python23/python setup.py build --compile=mingw32 install 
	c:/python23/python tests.py
	c:/python23/python setup.py build --compile=mingw32 bdist_wininst
	c:/python24/python setup.py build --compile=mingw32 install 
	c:/python24/python tests.py
	c:/python24/python setup.py build --compile=mingw32 bdist_wininst
	c:/python25/python setup.py build --compile=mingw32 install 
	c:/python25/python tests.py
	c:/python25/python setup.py build --compile=mingw32 bdist_wininst
	c:/python26/python setup.py build --compile=mingw32 install 
	c:/python26/python tests.py
	c:/python26/python setup.py build --compile=mingw32 bdist_wininst

distrib-win: compile-win
	pscp dist/*.exe rogerb@initd.org://var//www//pub//software//pysqlite//apsw//$(VERSION)//

compile-lin:
	rm -rf $(VERDIR)
	mkdir $(VERDIR)
	cp  $(SOURCE) $(VERDIR)
	rm -rf dist
	mkdir dist
	zip -9 -r dist/$(VERDIR).zip $(VERDIR)

distrib-lin: compile-lin
	ssh initd.org mkdir -p /var/www/pub/software/pysqlite/apsw/$(VERSION)
	scp dist/$(VERDIR).zip apsw.html initd.org:/var/www/pub/software/pysqlite/apsw/$(VERSION)/
