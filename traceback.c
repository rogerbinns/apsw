/* Augment a traceback with dummy stack frames from C so you can tell
   why the code was called. */

/*
  This code was originally  from the Pyrex project:
  Copyright (C) 2004-2006 Greg Ewing <greg@cosc.canterbury.ac.nz>

  It has been lightly modified to be a part of APSW with permission from Greg.
  Changes Copyright (C) 2006 Roger Binns <rogerb@rogerbinns.com>

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

/* These are python header files */
#include "compile.h"
#include "frameobject.h"
#include "traceback.h"

/* Add a dummy frame to the traceback so the developer has a better idea of what C code was doing 

   @param filename: Use __FILE__ for this - it will be the filename reported in the frame
   @param lineno: Use __LINE__ for this - it will be the line number reported in the frame
   @param functionname: Name of the function reported
   @param localsformat: Format string for Py_BuildValue( that must specify a dictionary or NULL to make
                        an empty dictionary.  An example is "{s:i, s: s}" with the varargs then conforming
			to this format (the corresponding params could be "seven", 7, "foo", "bar"

*/
static void AddTraceBackHere(const char *filename, int lineno, const char *functionname, const char *localsformat, ...)
{
  PyObject *srcfile=0, *funcname=0, *empty_dict=0, *empty_tuple=0, *empty_string=0, *localargs=0;
  PyCodeObject *code=0;
  PyFrameObject *frame=0;
  va_list localargsva;

  va_start(localargsva, localsformat);
  srcfile=PyString_FromString(filename);
  funcname=PyString_FromString(functionname);
  empty_dict=PyDict_New();
  empty_tuple=PyTuple_New(0);
  empty_string=PyString_FromString("");

  localargs=localsformat?(Py_VaBuildValue((char *)localsformat, localargsva)):PyDict_New();
  if(localsformat)
    assert(localsformat[0]=='{');
  if(localargs)
    assert(PyDict_Check(localargs));

  /* did any fail? */
  if (!srcfile || !funcname || !empty_dict || !empty_tuple || !empty_string)
    goto end;

  /* make the dummy code object */
  code = PyCode_New(
     0,            /*int argcount,*/
     0,            /*int nlocals,*/
     0,            /*int stacksize,*/
     0,            /*int flags,*/
     empty_string, /*PyObject *code,*/
     empty_tuple,  /*PyObject *consts,*/
     empty_tuple,  /*PyObject *names,*/
     empty_tuple,  /*PyObject *varnames,*/
     empty_tuple,  /*PyObject *freevars,*/
     empty_tuple,  /*PyObject *cellvars,*/
     srcfile,     /*PyObject *filename,*/
     funcname,     /*PyObject *name,*/
     lineno,       /*int firstlineno,*/
     empty_string  /*PyObject *lnotab*/
   );
  if (!code) goto end;

  /* make the dummy frame */
  frame=PyFrame_New(
           PyThreadState_Get(), /*PyThreadState *tstate,*/
	   code,                /*PyCodeObject *code,*/
	   empty_dict,          /*PyObject *globals,*/
	   localargs            /*PyObject *locals*/
	   );
  if(!frame) goto end;

  /* add dummy frame to traceback */
  frame->f_lineno=lineno;
  PyTraceBack_Here(frame);
  
  /* this epilogue deals with success or failure cases */
 end:
  va_end(localargsva);
  Py_XDECREF(localargs);
  Py_XDECREF(srcfile);
  Py_XDECREF(funcname);
  Py_XDECREF(empty_dict); 
  Py_XDECREF(empty_tuple); 
  Py_XDECREF(empty_string); 
  Py_XDECREF(code); 
  Py_XDECREF(frame); 
}

