/*
  A prepared statment cache for SQLite

  Copyright (C) 2007 Roger Binns <rogerb@rogerbinns.com>

  This software is provided 'as-is', without any express or implied
  warranty.  In no event will the authors be held liable for any
  damages arising from the use of this software.
 
  Permission is granted to anyone to use this software for any
  purpose, including commercial applications, and to alter it and
  redistribute it freely, subject to the following restrictions:
 
  1. The origin of this software must not be misrepresented; you must
     not claim that you wrote the original software. If you use this
     software in a product, an acknowledgment in the product
     documentation would be appreciated but is not required.

  2. Altered source versions must be plainly marked as such, and must
     not be misrepresented as being the original software.

  3. This notice may not be removed or altered from any source
     distribution.
 
*/


/* Statement cache 

   You should call statementcache_init with the number of entries you
   want.  Then change calls to sqlite3_prepare and sqlite3_finalize to
   call statementcache_prepare and statementcache_finalize prepending
   the StatementCache pointer as an initial argument.  Call
   statementcache_free to free up the cache.

*/


#ifndef STATEMENTCACHE_LINKAGE
#define STATEMENTCACHE_LINKAGE
#endif


#if 1
#define STATEMENTCACHE_MALLOC malloc
#define STATEMENTCACHE_REALLOC(x,y) do {free(x); x=malloc(y);} while(0)
#define STATEMENTCACHE_FREE    free
#else
#define STATEMENTCACHE_MALLOC  PyMem_Malloc
#define STATEMENTCACHE_REALLOC(x,y) do {PyMem_Free(x); x=PyMem_Malloc(y);} while(0)
#define STATEMENTCACHE_FREE    PyMem_Free
#define STATEMENTCACHE_MALLOC  sqlite3_malloc
#define STATEMENTCACHE_REALLOC(x,y) x=sqlite3_realloc(x,y)
#define STATEMENTCACHE_FREE    sqlite3_free
#endif

typedef struct _StatementCacheEntry
{
  unsigned inuse;
  sqlite3_stmt *stmt;
  int stringlength;
  char *sql;
  unsigned lru;
} StatementCacheEntry;

typedef struct _StatementCache
{
  unsigned nentries;
  StatementCacheEntry *entries;
  sqlite3* db;
  unsigned currentlru;
#ifdef SCSTATS
  int hits;
  int misses;
  int evictions;
  int full;
#endif
} StatementCache;

STATEMENTCACHE_LINKAGE
StatementCache* 
statementcache_init(sqlite3*db, unsigned nentries)
{
  StatementCache *sc=(StatementCache*)STATEMENTCACHE_MALLOC(sizeof(StatementCache));
  memset(sc, 0, sizeof(StatementCache));
  sc->nentries=nentries;
  sc->entries=STATEMENTCACHE_MALLOC(nentries*sizeof(StatementCacheEntry));
  memset(sc->entries, 0, nentries*sizeof(StatementCacheEntry));
  assert(db);
  sc->db=db;
  return sc;
}

STATEMENTCACHE_LINKAGE
int 
statementcache_free(StatementCache* sc)
{
  unsigned int i, notfreed=0;
  StatementCacheEntry *sce;

  if(!sc) return 0;

  for(i=0;i<sc->nentries;i++)
    {
      sce=&(sc->entries[i]);
      if(sce->inuse)
	{
	  notfreed++;
	  continue;
	}
      if(sce->stmt)
	{
#ifndef NDEBUG
	  int res= /* get rid of unused variable warnings */
#endif
          sqlite3_finalize(sce->stmt);
	  assert(res==SQLITE_OK);
	  sce->stmt=0;
	}
      if(sce->sql)
	{
	  STATEMENTCACHE_FREE(sce->sql);
	  sce->sql=0;
	}
    }
  if(notfreed)
    return notfreed;
#ifdef SCSTATS
  printf("SC: %d hits, %d misses, %d evictions, %d full\n", sc->hits, sc->misses, sc->evictions, sc->full);
#endif
  STATEMENTCACHE_FREE(sc->entries);
  STATEMENTCACHE_FREE(sc);
  return 0;
}

STATEMENTCACHE_LINKAGE
int  
statementcache_prepare(StatementCache *sc, 
		       sqlite3* db, 
		       const char *zSql, 
		       int *nBytes, 
		       sqlite3_stmt **ppStmt, 
		       const char **pzTail,
                       unsigned int *inuse)
{
  StatementCacheEntry *sce;
  int evict=-1, res, empty=-1;
  unsigned i, evictlru=4294967295U;

  assert(sc->db==db);

  if(*nBytes<0)
    *nBytes=strlen(zSql);

  /* find if we have a cached statement - we don't bother for over 10kb of text */
  if(*nBytes<10240)
    for(i=0;i<sc->nentries;i++)
      {
        sce=&(sc->entries[i]);
        if(sce->inuse)
          continue;

        if(!sce->stmt)
          {
            if(empty<0)
              empty=i;
            continue;
          }

        /* LRU */
        if(sce->lru<evictlru)
          {
            evict=i;
            evictlru=sce->lru;
          }

        if(sce->stringlength!=*nBytes)
          continue;
        if(memcmp(zSql, sce->sql, sce->stringlength))
          continue;
        /* ok, we can use this one */
        *ppStmt=sce->stmt;
        sce->inuse=1;
        *pzTail=zSql+sce->stringlength;
        *nBytes-=sce->stringlength;
#ifdef SCSTATS
        sc->hits++;
#endif
        return SQLITE_OK;
      }

#ifdef SCSTATS
  sc->misses++;
#endif
  if(evict<0 && empty<0)
    {
#ifdef SCSTATS
      sc->full++;
#endif
      sce=NULL;
    }
  else
    {
      if(empty>=0)
        {
          evict=empty;
        }
      else
        {
#ifdef SCSTATS
          sc->evictions++;
#endif
        }

      /* reserve the statement cache entry */
      sce=&(sc->entries[evict]);
      assert(sce->inuse==0);
      sce->inuse=1;
    }

  /* not in the cache */
  if(inuse)
    {
      assert(*inuse==0);
      *inuse=1;
    }
  assert(*(zSql+*nBytes)==0);
  Py_BEGIN_ALLOW_THREADS
    res=sqlite3_prepare_v2(db, zSql, *nBytes+1, ppStmt, pzTail);
  Py_END_ALLOW_THREADS;
  if(inuse)
    {
      assert(*inuse==1);
      *inuse=0;
    }

  if(res!=SQLITE_OK || !*ppStmt)
    {
      if(sce) sce->inuse=0;
      return res;
    }

  if(sce)
    {
      int oldlen=sce->stringlength;
      sce->stringlength=*pzTail-zSql;

      
      if(sce->stmt)
        {
          res=sqlite3_finalize(sce->stmt);
          assert(res==SQLITE_OK);
        }
      sce->stmt=*ppStmt;

      if(sce->sql)
        {
          if(sce->stringlength>oldlen)
            STATEMENTCACHE_REALLOC(sce->sql, sce->stringlength+1);
        }
      else
        /* SQLite reads off end, so we put an extra null on allocations*/      
        sce->sql=STATEMENTCACHE_MALLOC(sce->stringlength+1);

      memcpy(sce->sql, zSql, sce->stringlength);
      sce->sql[sce->stringlength]=0;
    }

  /* Update nBytes to be remaining string length */
  *nBytes-=*pzTail-zSql;

  return res;
}

STATEMENTCACHE_LINKAGE
int  
statementcache_finalize(StatementCache* sc, sqlite3_stmt *pStmt)
{
  StatementCacheEntry *sce;
  unsigned int i;
  int res;

  /* whitespace sql gives null stmt */
  if(!pStmt)
    return SQLITE_OK;

  for(i=0;i<sc->nentries;i++)
    {
      sce=&(sc->entries[i]);
      if(sce->stmt==pStmt)
	{
	  assert(sce->inuse);
	  sce->inuse=0;
	  res=sqlite3_reset(pStmt);
          sqlite3_clear_bindings(pStmt);
	  sc->currentlru++;
	  sce->lru=sc->currentlru;
	  return res;
	}
    }
  
  return sqlite3_finalize(pStmt);
}


