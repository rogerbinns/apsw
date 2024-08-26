#!/bin/sh

# https://github.com/rogerbinns/apsw/pull/229

WRONG="paramaters|paramater|partifular|seperately|desribes|seperate|seperated|begining|targetted|libary|interpretter|entireity|publically"
WRONG="$WRONG|noone|commiting|statment|statments|simulataneous|exection|wierd|valueable|tempory|datatabases|mutliple|implment|contraints"
WRONG="$WRONG|exeception|implemtation|multple|exeception|implment|implmentation|commited|unintentially|explicity|ouput|overal|unraiseable"
WRONG="$WRONG|reumable|bestpractise|exlcudes"

# code-block should be used not code in rst
WRONG="$WRONG|code::"

git grep --color -Eniw "($WRONG)" | grep -v tools/spellcheck.sh | grep -v 'Splitting such as'

n=`git grep --color -Eniw "($WRONG)" | grep -v tools/spellcheck.sh | grep -v 'Splitting such as' | wc -l`

if [ $n -gt 0 ] ; then exit 1 ; else exit 0 ; fi
