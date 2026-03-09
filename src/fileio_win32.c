/*

The fileio extension calls two functions that are in the SQLite
library but are not part of the sqlite extension api.  For our
extra build we link against libsqlite_tool but that isn't useful.

* APSW is using an amalgamation not that sqlite dll
* APSW doesn't include the qlite_extra_binaries directory in its
  runtime library path
* .. and it would result in two SQLites anyway
* .. which would confuse whose malloc is being used and just make
  things worse

So we link with this file containing the routines
*/

#if defined(WIN32) || defined(_WIN32)

#include "sqlite3ext.h"

/* this points the api pointers in fileio.c */
SQLITE_EXTENSION_INIT3

#include <windows.h>

/* These implementations are semantically the same as in the SQLite
source but directly use public functions directly */

LPWSTR
sqlite3_win32_utf8_to_unicode(const char *zText)
{
  LPWSTR zWideText = 0;
  int nChar;

  /* the conversion is two passes - first to figure out the
    size including null terminator */
  nChar = MultiByteToWideChar(CP_UTF8, 0, zText, -1, NULL, 0);
  if (!nChar)
    return 0;
  zWideText = sqlite3_malloc(nChar * sizeof(WCHAR));
  if (!zWideText)
    return 0;

  /* second pass - do the conversion */
  nChar = MultiByteToWideChar(CP_UTF8, 0, zText, -1, zWideText, nChar);
  if (!nChar)
  {
    sqlite3_free(zWideText);
    return 0;
  }
  return zWideText;
}

char *
sqlite3_win32_unicode_to_utf8(LPCWSTR zWideText)
{
  char *zText = 0;
  int nByte;

  /* same pattern as above - corresponding opposite API */
  nByte = WideCharToMultiByte(CP_UTF8, 0, zWideText, -1, 0, 0, 0, 0);
  if (!nByte)
    return 0;
  zText = sqlite3_malloc(nByte);
  if (!zText)
    return 0;

  nByte = WideCharToMultiByte(CP_UTF8, 0, zWideText, -1, zText, nByte, 0, 0);
  if (!nByte)
  {
    sqlite3_free(zText);
    return 0;
  }
  return zText;
}

#endif