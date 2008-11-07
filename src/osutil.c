/*
  Operating system abstractions

  Copyright (C) 2008 Roger Binns <rogerb@rogerbinns.com>

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

/* Thread local storage for the SQLite error message.  SQLite does not
   do its error message as a per thread thing like errno, so we have
   to explicitly remember it on a per thread basis. We basically strdup the
   string.  */

#if defined(_WIN32) || defined(WIN32) || defined(__CYGWIN__) || defined(__MINGW32__) || defined(__BORLANDC__)

#include <windows.h>

static DWORD apswtlserrslot;

BOOL WINAPI 
DllMain(HANDLE hinstDLL, DWORD dwReason, LPVOID lpvReserved)
{

  switch(dwReason)
    {
    case DLL_PROCESS_ATTACH:
      apswtlserrslot=TlsAlloc();
      return (apswtlserrslot==0xFFFFFFFF)?FALSE:TRUE;
    case DLL_PROCESS_DETACH:
      return (TlsFree(apswtlserrslot)==0)?FALSE:TRUE;
    case DLL_THREAD_DETACH:
      free(TlsGetValue(apswtlserrslot));
      return TRUE;
    case DLL_THREAD_ATTACH:
      /* No code for thread attach since TlsGetValue() will return NULL until a value is set */
      return TRUE;
    }
  return TRUE;
}

/* we rely on dllmain for initialization */
static int
apsw_inittls(void)
{
  return 0;
}

static void
apsw_set_tls_error(const char *what)
{
  
  free(TlsGetValue(apswtlserrslot));
  TlsSetValue(apswtlserrslot, strdup(what));
}

static const char *
apsw_get_tls_error(void)
{
  return (const char *)TlsGetValue(apswtlserrslot);
}


#else
/* use pthreads */

#include <pthread.h>

/* the pthread docs say to do this once stuff */
static pthread_once_t apswtlserrslot_once = PTHREAD_ONCE_INIT;
static pthread_key_t apswtlserrslot;

static void
apsw_inittls_once(void)
{
  pthread_key_create(&apswtlserrslot, free);
}

static int
apsw_inittls(void)
{
  return pthread_once(&apswtlserrslot_once, apsw_inittls_once);
}

static void
apsw_set_tls_error(const char *what)
{
  free(pthread_getspecific(apswtlserrslot));
  pthread_setspecific(apswtlserrslot, strdup(what));
}

static const char *
apsw_get_tls_error(void)
{
  return (const char *)pthread_getspecific(apswtlserrslot);
}
#endif
