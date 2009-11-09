#

## This code provides a bridge between SQLite and Couchdb. It is
## implemented as a SQLite virtual table.  You just need to import the
## file which will automatically make it available to any new APSW
## connections.  You can also use -init or .read from the APSW Shell
## and it will become available within the shell session.

import apsw
import couchdb
import random
import sys
import pickle
try:
    from uuid import uuid4
except ImportError:
    # This happens on Python 2.4
    class dummyuuid:
        pass

    def uuid4():
        x="".join(["%02x" % (random.randint(0,255),) for _ in range(16)])
        o=dummyuuid
        o.hex=x
        return o

class Source:

    def __init__(self):
        self.rbatch=5000
        self.wbatch=5000
        self.deepupdate=True

    _cfgmap={"read-batch": "rbatch",
             "write-batch": "wbatch",
             "deep-update": "deepupdate"}

    def getconfig(self, item):
        if item not in self._cfgmap:
            return ValueError("Unknown config item "+str(item))
        return getattr(self, self._cfgmap[item])

    def setconfig(self, item, value):
        if item not in self._cfgmap:
            return ValueError("Unknown config item "+str(item))
        # currently all ints
        if sys.version_info<(3,):
            value=int(value)
        else:
            value=long(value)
        if item in ("read-batch", "write-batch"):
            if value<1:
                raise ValueError("%s value must be greater than zero: %d" % (item, value))
        elif item=="deep-update":
            if value not in (0,1):
                raise ValueError("%s value must be zero or one: %d" % (item, value))
        setattr(self, self._cfgmap[item], value)

    # javascript function that returns all keys of a doc in a view
    _allkeysfunc="""
    function(doc) {
    for(var i in doc)
       if(i.charAt(0)!='_')
           emit(null, i);
        }"""
    
    def Create(self, db, modulename, dbname, tablename, *args):
        "Called when a table is created"
        # args[0] must be url of couchdb authentication information.
        # For example http://user:pass@example.com
        # args[1] is db name'

        # sqlite provides the args still quoted etc.  We have to strip
        # them off.

        args=[self._unquote(a) for a in args]
        
        server=couchdb.Server(args[0])
        cdb=server[args[1]]

        # There is a nasty bug in httplib2 which is used by
        # python-couchdb that causes double submission of some
        # requests.  This is the workaround.  The code above is left
        # in because it produces better exceptions if supplied with
        # invalid data and the code below can be deleted once the
        # bug is fixed.
        # http://code.google.com/p/couchdb-python/issues/detail?id=85
        # http://code.google.com/p/httplib2/issues/detail?id=67
        a=args[0]
        if not a.endswith("/"):
            a=a+"/"
        a=a+args[1]
        cdb=couchdb.Database(a)

        cols=[]
        for c in args[2:]:
            if c!='+':
                cols.append(c)
            else:
                autocols=[row["value"] for row in cdb.query(self._allkeysfunc, limit=1000)]
                autocols.sort()
                for c in autocols:
                    if c not in cols:
                        cols.append(c)

        # use this for permanent tables
        maptable="%s.%s" % (self._fmt_sql_identifier(dbname),
                            self._fmt_sql_identifier(tablename+"_idmap"))
        # and this for temp
        maptable=self._fmt_sql_identifier(tablename+"_idmap")

        t=Table(self, db, cdb, cols, maptable)

        sql="create table ignored("+",".join([self._fmt_sql_identifier(c) for c in cols])+")"

        return sql, t

    Connect=Create

    def _fmt_sql_identifier(self, v):
        "Return the identifier quoted in SQL syntax if needed (eg table and column names)"
        if not len(v): # yes sqlite does allow zero length identifiers
            return '""'
        # double quote it unless there are any double quotes in it
        if '"' in v:
            return "[%s]" % (v,)
        return '"%s"' % (v,)

    def _unquote(self, val):
        "A somewhat lenient sql value/identifier parser"
        # remove whitespace
        v=val.strip()
        quote=None
        if v[0] in ('"', "'", '['):
            quote=v[0]
            if quote=='[': quote=']'
            v=v[1:]
        else:
            if len(v.split())>1:
                raise ValueError("You cannot specify column types or defaults: %s" % (v,))
            return v
        res=[]
        while len(v):
            if v[0]==quote:
                if quote!=']' and v[1:].startswith(quote):
                    res.append(quote)
                    v=v[2:]
                    continue
                # must be endquote
                v=v[1:]
                if len(v):
                    raise ValueError("Trailing text after endquote: "+repr(val))
                continue
            res.append(v[0])
            v=v[1:]
        return "".join(res)

class Table:

    def __init__(self, source, adb, cdb, cols, maptable):
        # A temporary table that maps between couchdb _id field for
        # each document and the rowid needed to implement a virtual
        # table. _rowid_ is the 64 bit int id autoassigned by SQLIte.
        # We also keep track of the _rev for each document so that
        # updates can be done
        adb.cursor().execute("create temporary table if not exists %s(_id UNIQUE, _rev)" % (maptable,))

        self.s=source
        self.adb=adb
        self.cdb=cdb
        self.cols=cols
        self.maptable=maptable
        self.pending_updates={}

    def Destroy(self):
        self.adb.cursor().execute("drop table if  exists "+self.maptable)

    def BestIndex(self, constraints, orderbys):
        # nothing to index
        if not constraints and not orderbys:
            return None
        # we don't have an index on rowid
        for col,op in constraints:
            if col<0:
                return None
        return [[(i,False) for i in range(len(constraints))], # constraintarg position for cursor.filter
                0,                                  # index number
                repr(constraints),                  # index string
                False,                              # orderby consumed
                1]                                  # cost

    def Open(self):
        return Cursor(self)

    def Rename(self, newname):
        raise NotImplementedError("Rename not supported")

    _binary_type = eval(("buffer", "bytes") [sys.version_info>=(3,0)])
    def _unpickle(self, fields):
        res=[]
        for f in fields:
            if isinstance(f, self._binary_type):
                res.append(pickle.loads(f))
            else:
                res.append(f)
        return res

    def UpdateInsertRow(self, rowid, fields):
        fields=self._unpickle(fields)
        if rowid is not None:
            raise ValueError("You cannot specify the rowid")
        _id=None
        if "_id" in self.cols:
            _id=fields[self.cols.index("_id")]
        if _id is None:
            # autogenerate
            _id=uuid4().hex
        data=dict(zip(self.cols, fields))
        data["_id"]=_id
        self.pending_updates[_id]=data

        if len(self.pending_updates)>=self.s.wbatch:
            self.flushpending()
            
        return self.getrowforid(_id)

    def UpdateDeleteRow(self, rowid):
        row=self.adb.cursor().execute("select _id,_rev from "+self.maptable+" where _rowid_=?", (rowid,)).fetchall()
        assert len(row)
        _id=row[0][0]
        _rev=row[0][1]
        d={"_id": _id, "_deleted": True}
        if _rev:
            d["_rev"]=_rev
        self.pending_updates[_id]=d
        if len(self.pending_updates)>=self.s.wbatch:
            self.flushpending()

    def UpdateChangeRow(self, rowid, newrowid, fields):
        fields=self._unpickle(fields)
        if newrowid!=rowid:
            raise ValueError("You cannot change the rowid")
        row=self.adb.cursor().execute("select _id,_rev from "+self.maptable+" where _rowid_=?", (rowid,)).fetchall()
        assert len(row)
        _id,_rev=row[0]
        if "_id" in self.cols and fields[self.cols.index("_id")]!=_id:
            raise ValueError("You cannot change the _id")
        if self.s.deepupdate:
            d=dict(self.cdb[_id])
            if d["_rev"]!=_rev:
                raise couchdb.client.ResourceConflict("Document has changed on server")
            for k,v in zip(self.cols, fields):
                if v is None and k not in d:
                    pass
                else:
                    d[k]=v
        else:
            d=dict(zip(self.cols, fields))
        d["_rev"]=_rev
        d["_id"]=_id
        self.pending_updates[_id]=d
        if len(self.pending_updates)>=self.s.wbatch:
            self.flushpending()
        
    def getrowforid(self, _id):
        return self.adb.cursor().execute("insert or ignore into "+self.maptable+"(_id) values(?);"
                                        "select _rowid_ from "+self.maptable+" where _id=?",
                                        (_id, _id)).fetchall()[0][0]

    def flushpending(self):
        if not len(self.pending_updates):
            return
        
        p=self.pending_updates.values()
        self.pending_updates={}
        fails=[]
        c=self.adb.cursor()
        for i, (success, docid, rev_or_exc) in enumerate(self.cdb.update(p)):
            if success:
                c.execute("update "+self.maptable+" set _rev=? where _id=?", (rev_or_exc, docid))
            else:
                fails.append("%s: %s\nData: %s" % (docid, rev_or_exc, p[i]))
        if fails:
                raise couchdb.client.ResourceConflict("Failed to create/update %d documents" % (len(fails),), fails)

    def Begin(self):
        self.flushpending()

    def Sync(self):
        self.flushpending()

    def Commit(self):
        self.flushpending()

    def Rollback(self):
        # Note that we probably already committed stuff anyway so we
        # can't do a true rollback
        self.pending_updates={}

class Cursor:

    opmap={
        apsw.SQLITE_INDEX_CONSTRAINT_EQ: '==',
        apsw.SQLITE_INDEX_CONSTRAINT_GE: '>=',
        apsw.SQLITE_INDEX_CONSTRAINT_GT: '>',
        apsw.SQLITE_INDEX_CONSTRAINT_LE: '<=',
        apsw.SQLITE_INDEX_CONSTRAINT_LT: '<',
        apsw.SQLITE_INDEX_CONSTRAINT_MATCH: None
        }

    _binary_type = eval(("buffer", "bytes") [sys.version_info>=(3,0)])
    _basestring = eval(("basestring", "str") [sys.version_info>=(3,0)])
    _sqlite_types=(_basestring, type(None), type(1), type(1.1))
    if sys.version_info<(3,0):
        _sqlite_types=_sqlite_types+(type(eval("100L")),)

    def _jsquoute(self, v):
        assert not isinstance(v, self._binary_type)
        if isinstance(v, self._basestring):
            return '"'+ \
                   v.replace("\\", "\\\\") \
                   .replace('"', '\\"') \
                   +'"'
        if v is None:
            return "null"
        # integer
        return str(v)
    
    def __init__(self, table):
        self.t=table

    def _quote(self, c):
        return c.replace("\\", "\\\\"). \
               replace("'", "\\'")

    def Filter(self, indexnum, indexname, args):
        # back to begining - we flush any outstanding changes so that we don't have to
        # merge pending updates with server data
        self.t.flushpending()
        q=None
        if indexname:
            idx=eval(indexname)
            q=[]
            for ((col,constraint), arg) in zip(idx, args):
                if isinstance(arg, self._binary_type):
                    v=pickle.loads(arg)
                    if constraint!=apsw.SQLITE_INDEX_CONSTRAINT_EQ:
                        raise ValueError("You can only do equality with blobs not "+self.opmap[constraint])
                    # The various python json modules encode to a
                    # string slightly differently so
                    # toJSON(doc['field']) can't be used.
                    q.append("areObjectsEqual(doc['%s'], %s)" % (
                        self._quote(self.t.cols[col]),
                        couchdb.json.encode(v)))
                else:
                    q.append("(doc['%s']%s%s)" % (self._quote(self.t.cols[col]), self.opmap[constraint], self._jsquoute(arg)))
            q=" && ".join(q)
            
        self.query=Query(self.t.cdb, self.t.cols, q, self.t.s.rbatch)

    def Eof(self):
        # Eof is called before next so we do all the work in eof
        r=self.query.eof()
        if not r:
            self._id, self._rev, self._values = self.query.current()
            self.t.adb.cursor().execute("update "+self.t.maptable+" set _rev=? where _rowid_=?", (self._rev, self.Rowid()))
        return r

    def Rowid(self):
        return self.t.getrowforid(self._id)

    def Column(self, which):
        if which<0:
            return self.Rowid()
        val=self._values[which]
        if isinstance(val, self._sqlite_types):
            return val
        return self._binary_type(pickle.dumps(val, -1))

    def Next(self):
        pass

    def Close(self):
        pass

class Query:
    """Encapsulates a couchdb query dealing with batching and EOF testing"""

    def _quote(self, c):
        return c.replace("\\", "\\\\"). \
               replace("'", "\\'")
    
    def __init__(self, cdb, cols, query=None, batch=3):
        self.cdb=cdb
        # what an incredibly stupid language that it takes this much
        # code just to determine if two objects are equal
        self.mapfn='''
function _areObjectsEqual(left,right) {
  if(left===right)                   return true;
  if (typeof left != typeof right)   return false;
  if(typeof left != 'object')        return left==right;
  if(left.length !== right.length)   return false;
  for(var i in left)
       if(!areObjectsEqual(left[i], right[i]))
         return false;
  for(var i in right)
       if(!areObjectsEqual(left[i], right[i]))
         return false;         
  return true;
}

function areObjectsEqual(left, right) {
  var res=_areObjectsEqual(left, right);
  log(""+res+" "+left+" - "+right);
  return res;
}

function(doc) {
  /*<QUERY>*/
     emit(null, [doc._rev, %s]);
            }''' % \
        (",".join(["doc['%s']===undefined?null:doc['%s']" % (self._quote(c),self._quote(c)) for c in cols]),)
        if query:
            self.mapfn=self.mapfn.replace("/*<QUERY>*/", "if("+query+")")
        res=cdb.query(self.mapfn, limit=batch)
        self.more=len(res)>=batch
        self.iter=iter(res)
        self.batch=batch

    def eof(self):
        while True:
            for self.curval in self.iter:
                return False

            if not self.more:
                self.curval=None
                return True

            # setup next batch
            res=self.cdb.query(self.mapfn, limit=self.batch, skip=1, startkey=None, startkey_docid=self.curval["id"])
            self.more=len(res)>=self.batch
            self.iter=iter(res)

    def current(self):
        return self.curval["id"], self.curval["value"][0], self.curval["value"][1:]

# register if invoked from shell
thesource=Source()
def register(db, thesource=thesource):
    db.createmodule("couchdb", thesource)
    db.createscalarfunction("couchdb_config", thesource.getconfig, 1)
    db.createscalarfunction("couchdb_config", thesource.setconfig, 2)

if 'shell' in locals() and hasattr(shell, "db") and isinstance(shell.db, apsw.Connection):
    register(shell.db)

apsw.connection_hooks.append(register)
    
del thesource
del register
