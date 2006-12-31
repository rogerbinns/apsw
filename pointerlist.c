/*
  A list of pointers using Python memory management functions

  Copyright (C) 2006 Roger Binns <rogerb@rogerbinns.com>

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

/* A poor imitation of STL :-) It would be nice if Python had
   something like this at the C level, but all of Python's data
   structures mess with reference counts. */

typedef struct 
{
  unsigned int numentries;
  unsigned int allocatedsize;
  unsigned int allocunits;
  void **items;
} pointerlist;

/* Initialize a pointerlist.  You should allocate one, memset it to
   all zeroes and then fill in the function pointers.  You can also
   set allocunits to how many items get allocated in each chunk */
static void
pointerlist_init(pointerlist *pl)
{
  assert(pl->numentries==0);
  assert(pl->allocatedsize==0);
  assert(pl->items==0);
  if(pl->allocunits<1)
    pl->allocunits=64;
}

static void
pointerlist_free(pointerlist *pl)
{
  if(pl->items)
    PyMem_Free(pl->items);
  memset(pl, 0, sizeof(pointerlist));
}

/* returns 0 on failure, non-zero on success */
static int
pointerlist_add(pointerlist *pl, void *item)
{
  int i;
  if(!item) return 0;

  if(!pl->items)
    {
      pl->items=PyMem_Malloc(sizeof(void*)*pl->allocunits);
      if(pl->items) return 0;
      pl->allocatedsize=pl->allocunits;
    }
  if(pl->numentries+1>=pl->allocatedsize)
    {
      pl->items=PyMem_Realloc(pl->items, sizeof(void*)*(pl->allocatedsize+pl->allocunits));
      memset(((char*)(pl->items))+sizeof(void*)*pl->allocatedsize, 0, sizeof(void*)*pl->allocunits);
      pl->allocatedsize+=pl->allocunits;
    }
  for(i=0;i<pl->allocatedsize;i++)
    {
      if(!pl->items[i])
	{
	  pl->items[i]=item;
	  pl->numentries+=1;
	  return 1;
	}
    }
  assert(0); /* can't get here! */
  return 0;
}

/* returns 0 if item not found, else non-zero on successful removal */
static int
pointerlist_remove(pointerlist *pl, void* item)
{
  int i;
  if(!pl->items) return 0;
  if(!item) return 0;

  for(i=0;i<pl->allocatedsize;i++)
    {
      if(pl->items[i]==item)
	{
	  pl->items[i]=0;
	  pl->numentries+=-1;
	  return 1;
	}
    }
  return 0;
}

/* Iterating over a pointerlist */

typedef struct
{
  pointerlist *pl;
  int itemnum;
} pointerlist_visit;

/* returns zero if there are more items, non-zero if no more are left */
static int
pointerlist_visit_finished(pointerlist_visit *plv)
{
  return plv->itemnum<plv->pl->allocatedsize;
}

/* moves to next member */
static int
pointerlist_visit_next(pointerlist_visit *plv)
{
  plv->itemnum+=1;
  for(;plv->itemnum<plv->pl->allocatedsize;plv->itemnum++)
    if(plv->pl->items[plv->itemnum])
      return 1;
  return 0;
}

static void
pointerlist_visit_begin(pointerlist *pl, pointerlist_visit *plv)
{
  plv->pl=pl;
  plv->itemnum=-1;
  pointerlist_visit_next(plv);
}


static void*
pointerlist_visit_get(pointerlist_visit *plv)
{
  return plv->pl->items[plv->itemnum];
}
