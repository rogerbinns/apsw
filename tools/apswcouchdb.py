#

## This code provides a bridge between SQLite and Couchdb. It is
## implemented as a SQLite virtual table.  You just need to import the
## file which will automatically make it available to any new APSW
## connections.  You can also use -init or .read from the APSW Shell
## and it will become available within the shell session.

import apsw
import couchdb
import random
from uuid import uuid4

couchdb

class Source:

    "Called when a table is created"
    def Create(self, db, modulename, dbname, tablename, *args):
        # args[0] must be url of couchdb authentication information.
        # For example http://user:pass@example.com
        # args[1] is db name'

        # sqlite provides the args still quoted etc.  We have to strip
        # them off.

        args=[eval(a.replace("\\", "\\\\")) for a in args]
        
        server=couchdb.Server(args[0])
        cdb=server[args[1]]

        cols=[]
        for c in args[2:]:
            if c!='+':
                cols.append(c)
            else:
                1/0

        # use this for permanent tables
        maptable="%s.%s" % (self._fmt_sql_identifier(dbname),
                            self._fmt_sql_identifier(tablename+"_idmap"))
        # and this for temp
        maptable=self._fmt_sql_identifier(tablename+"_idmap")

        t=Table(db, cdb, cols, maptable)

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


class Table:

    def __init__(self, adb, cdb, cols, maptable):
        # a temporary table that maps between couchdb _id field for
        # each document and the rowid needed to implement a virtual
        # table. _rowid_ is the 64 bit int id autoassigned by SQLIte.
        adb.cursor().execute("create temporary table if not exists %s(_id UNIQUE)" % (maptable,))

        self.adb=adb
        self.cdb=cdb
        self.cols=cols
        self.maptable=maptable
        self.pending_updates={}
        self.rbatch=1
        self.wbatch=1
        self.revcache={}

    def Destroy(self):
        self.adb.cursor().execute("drop table if  exists "+self.maptable)

    def BestIndex(self, *args):
        print "bestindex",`args`
        return None

    def Open(self):
        return Cursor(self)

    def Rename(self):
        raise Exception("Rename not supported")

    def UpdateInsertRow(self, rowid, fields):
        if rowid is not None:
            raise Exception("You cannot specify the rowid")
        _id=None
        if "_id" in self.cols:
            _id=fields[self.cols.index("_id")]
        if _id is None:
            # autogenerate
            _id=uuid4().hex
        data=dict(zip(self.cols, fields))
        data["_id"]=_id
        self.pending_updates[_id]=data

        if len(self.pending_updates)>=self.wbatch:
            self.flushpending()
            
        return self.getrowforid(_id)

    def UpdateDeleteRow(self, rowid):
        row=self.adb.cursor().execute("select _id from "+self.maptable+" where _rowid_=?", (rowid,)).fetchall()
        assert len(row)
        _id=row[0][0]
        # now find it in revcache
        _rev=self.revcache.get(_id, None)
        d={"_id": _id, "_deleted": True}
        if _rev:
            d["_rev"]=_rev
        self.pending_updates[_id]=d
        if len(self.pending_updates)>=self.wbatch:
            self.flushpending()

    def UpdateChangeRow(self, rowid, newrowid, fields):
        if newrowid!=rowid:
            raise Exception("You cannot change the rowid")
        # find id
        _id=None
        if "_id" in self.cols:
            _id=fields[self.cols.index("_id")]
        if _id is None:
            row=self.adb.cursor().execute("select _id from "+self.maptable+" where _rowid_=?", (rowid,)).fetchall()
            assert len(row)
            _id=row[0][0]

        # now find rev
        _rev=None
        if "_rev" in self.cols:
            _rev=fields[self.cols.index("_rev")]
        if _rev is None:
            _rev=self.revcache.get(_id)

        d=dict(zip(self.cols, fields))
        d["_rev"]=_rev
        d["_id"]=_id
        self.pending_updates[_id]=d
        if len(self.pending_updates)>=self.wbatch:
            self.flushpending()
        

    def revcache_add(self, _id, _rev):
        if len(self.revcache)>110:
            # remove 10 random members
            for i in random.sample(self.revcache.keys(), 10):
                del self.revcache[i]
        self.revcache[_id]=_rev

    def getrowforid(self, _id):
        return self.adb.cursor().execute("insert or ignore into "+self.maptable+" values(?);"
                                        "select _rowid_ from "+self.maptable+" where _id=?",
                                        (_id, _id)).fetchall()[0][0]

    def flushpending(self):
        if not len(self.pending_updates):
            return
        
        p=self.pending_updates.values()
        self.pending_updates={}
        fails=[]
        for i, (success, docid, rev_or_exc) in enumerate(self.cdb.update(p)):
            if not success:
                fails.append("%s: %s\nData: %s" % (docid, rev_or_exc, p[i]))
        if fails:
            raise Exception("Failed to create/update %d documents" % (len(fails),), fails)
        

    def Begin(self):
        print "begin"
        self.flushpending()

    def Sync(self):
        print "sync"
        self.flushpending()

    def Commit(self):
        print "commit"
        self.flushpending()

    def Rollback(self):
        print "rollback"
        self.flushpending()

class Cursor:
    def __init__(self, table):
        self.t=table

    def Filter(self, *args):
        # back to begining
        self.query=Query(self.t.cdb, self.t.cols)

    def Eof(self):
        # Eof is called before next so we do all the work in eof
        r=self.query.eof()
        if not r:
            self._id, self._rev, self._values = self.query.current()
            self.t.revcache_add(self._id, self._rev)
        return r

    def Rowid(self):
        return self.t.getrowforid(self._id)

    def Column(self, which):
        if which<0:
            return self.Rowid()
        return self._values[which]

    def Next(self):
        pass

    def Close(self):
        pass

class Query:
    """Encapsulates a couchdb query dealing with batching and EOF testing"""
    def __init__(self, cdb, cols, query=None, batch=3):
        self.cdb=cdb
        self.mapfn='''
        function(doc) {
          emit(null, [doc._rev, %s]);
        }''' % (",".join(["doc['%s']===undefined?null:doc['%s']" % (c,c) for c in cols]),)
        self.iter=iter(cdb.query(self.mapfn, limit=batch))
        self.returned=0
        self.batch=batch

    def eof(self):
        while True:
            for self.curval in self.iter:
                self.returned+=1
                return False

            if not self.returned:
                # iterator returned no rows so we are at the end
                self.curval=None
                return True

            # setup next batch
            self.iter=iter(self.cdb.query(self.mapfn, limit=self.batch, skip=1, startkey=None, startkey_docid=self.curval["id"]))
            self.returned=0

    def current(self):
        return self.curval["id"], self.curval["value"][0], self.curval["value"][1:]




# register if invoked from shell
thesource=Source()
def register(db, thesource=thesource):
    db.createmodule("couchdb", thesource)

if 'shell' in locals() and hasattr(shell, "db") and isinstance(shell.db, apsw.Connection):
    register(shell.db)

apsw.connection_hooks.append(register)
    
del thesource
del register
