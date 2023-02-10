#!/bin/sh

# https://github.com/rogerbinns/apsw/pull/229

WRONG="paramaters|paramater|partifular|seperately|desribes|seperate|seperated|begining|targetted|libary|interpretter|entireity|publically"
WRONG="$WRONG|noone|commiting|statment|statments|simulataneous|exection|wierd|valueable|tempory|datatabases|mutliple|implment|contraints"
WRONG="$WRONG|exeception|implemtation|multple|exeception|implment|implmentation|commited|unintentially|explicity|ouput|overal|unraiseable"
WRONG="$WRONG|reumable"

git grep --color -Eniw "($WRONG)" | grep -v tools/spellcheck.sh

n=`git grep --color -Eniw "($WRONG)" | grep -v tools/spellcheck.sh | wc -l`

if [ $n -gt 0 ] ; then exit 1 ; else exit 0 ; fi
