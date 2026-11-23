# Enumerating the callbacks — completeness is the whole point

A callback you forget to list is a callback nobody attacks, so the matrix is only as good as its
inventory, and the inventory **grows every SQLite and apsw release**. Do not hand-maintain a list in
this file — it would be stale within a release. Derive the set *mechanically from the current tree*
each run, two sources, and record each callback's *shape* (that is what the adversary exploits).

## The two sources of callbacks

**1. SQLite-registered callbacks — every point where apsw hands SQLite a C trampoline that calls
Python.** Find them by the registration call in apsw's C: the `sqlite3_*hook` family
(commit/rollback/update/preupdate/wal), the `sqlite3_create_*` family (scalar/aggregate/window
functions, collations, virtual-table modules), the handler setters (busy, progress), trace/profile,
the authorizer, the collation-needed callback, autovacuum-pages, the virtual-table method table
(every method — create/connect/bestindex/filter/next/column/update/rename/find-function/integrity/…),
the FTS5 registration surface (tokenizers and auxiliary functions), the VFS and VFS-file method
tables, blob, and backup. Each registration installs a C function that, when SQLite fires it,
re-enters Python — that is a callback to attack. Grep for the registration primitives rather than
trusting memory; SQLite adds new ones (a hook here, a vtable method there) most releases, and a new
one is exactly the kind of surface that ships untested.

**2. apsw-level callbacks — Python callables apsw stores and later calls itself**, not via a SQLite
trampoline: the row and exec tracers, VFS/VFSFile method overrides a subclass provides, the fts5
tokenizer and auxiliary Python callables, the authorizer/busy/collation Python objects, and the
hooks on apsw's own encoders (e.g. a "produce a key/transform this object" callout while serialising).
Find these by the stored callable: apsw keeps a reference and invokes it later, so every
`Py_*Call*`/vectorcall on a stored Python object is an apsw-level callback.

Cross-check the two lists against apsw's own docs and against SQLite's (apsw ships its docs; SQLite
ships `toc.db`): a newly added callback in a release should fall out of that *delta*, not be
discovered later by a crash. This is the same enumeration the release-agreement check does for
constants and APIs (see `../release-check/`) — share the derivation.

## Record each callback's shape — the attack surface is in the shape

For every enumerated callback, capture (re-derived per release, not hard-coded):

- **Firing multiplicity** — once, once-per-row, once-per-byte, recursively. More fires = more chances
  for a mutation/free between them, and the place churn+GC bugs accumulate.
- **What it is handed** — Connection/Cursor/Blob/VTab objects, SQLite value pointers, Python
  containers (a bindings list, a row, a mapping being encoded). Anything the callback can reach it
  can also *destroy or mutate*.
- **What apsw holds across the call, borrowed vs owned** — the single most important field. A value
  apsw holds only **borrowed** across a callback is a use-after-free waiting for the callback to drop
  the owner's last reference; a pointer/length **derived from a mutable** (a list's buffer, a SQLite
  column pointer) is invalidated if the callback mutates the source even though the object still
  lives. (These are two different bugs with two different fixes — see
  `../verification/README.md`'s GIL-boundary note — so tag which one each callback risks.)
- **What the callback is *allowed* to do** per apsw's contract — may it reenter? close the owner?
  run SQL on the same connection? The matrix tests the full set of *legal* actions adversarially; an
  action apsw documents as forbidden should raise, not crash (see `threads-and-blocking.md` for the
  thread-affinity case).

The shape tells you what the callback *hands* you, but it does **not** bound the targets: the hostile
behaviours in `adversarial-reentrancy.md` and `threads-and-blocking.md` are applied to the objects and
arguments the shape exposes **and to every other live apsw object of every type** — see the next
section.

## Enumerate the object graph too — the targets are not just the owner

The callbacks are the *entry points*; the *targets* are every apsw object type and the links between
them. Build a second inventory alongside the callback list: the object types (Connection, Cursor,
Blob, Backup, the prepared statement and its bindings, VFS and VFSFile, session / changeset /
changegroup / rebaser, FTS5 objects, …) and, for each, the other objects it holds or is held by
**behind the scenes** — a cursor→connection→statement cache, a backup→two connections, a
blob/session→connection, bindings→statement, VFSFile→connection. That link map is where cross-object
lifetime bugs live, and it is what tells the attack generator which objects to point at from inside a
given callback: *all of them*, because a link you did not know about is a bug you cannot predict. Do
not scope a callback's attack to "its" object — being in a blob or VFS or exec-trace callback leaves
every connection, cursor, backup, and binding fully in scope. Re-derive the link map per release; the
relationships (and the object types) change.

So the generator is: for each callback, apply every hostile behaviour to every object in the graph,
on every thread — with the callback's own shape just telling you which objects are *most* directly
in reach and therefore the sharpest first targets, not the only ones.

## Keep the enumeration honest

- **Count what you reached, not what you listed.** Instrument the run (coverage, or a counter in each
  trampoline) and confirm every enumerated callback actually *fired* under attack. A callback that is
  in the list but never fired is untested and a false sense of coverage — the matrix must prove it
  drove each one.
- **Build with every extension enabled** (FTS5, RTREE, session, geopoly, …) or whole callback
  families silently do not exist to be enumerated — the same omission trap the rest of `tools/ai`
  warns about. A default build has no FTS5 module, so none of its tokenizer/aux callbacks are reached.
- **Re-enumerate from scratch each release.** New callback, new argument, new firing point — the delta
  against last release is itself a to-do list of surfaces to attack.
