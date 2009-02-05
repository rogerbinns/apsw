/*
  Another Python Sqlite Wrapper

  Wrap SQLite hot backup functionality

  Copyright (C) 2009 Roger Binns <rogerb@rogerbinns.com>

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

/** 
Backup
******

A backup object encapsulates copying one database to another.  You
call :meth:`Connection.backup` on the destination database to get the
backup object.  Call :meth:`~backup.step` to copy some pages
repeatedly dealing with errors as appropriate.  Finally
:meth:`~backup.finish` cleans up committing or rolling back and
releasing locks.

Here is an example usage using the **with** statement to ensure
:meth:`~backup.finish` is called::

  # copies source.main into db
  with db.backup("main", source, "main") as b:
      while not b.done:
          b.step(100)
          print b.remaining, b.pagecount

If you are not using **with** then you'll need to ensure
:meth:`~backup.finish` is called::

  # copies source.main into db
  b=db.backup("main", source, "main")
  try:
      while not b.done:
          b.step(100)
          print b.remaining, b.pagecount
  finally:
      b.finish()

*/
