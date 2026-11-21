# Pattern scanning — enumerate suspicious shapes, then triage

**Good for:** turning a hazard you understand into a *census* of every place it might occur.
Model checking tells you a shape is dangerous; pattern scanning finds all the sites, and you
(or another agent) triage each as safe or suspect. This is how the borrowed-ref / stale-data
hazards become an actionable worklist.

**Representative tool:** Semgrep (lightweight, syntactic + light dataflow, good C support —
it parses apsw's source essentially completely, including the `Py_*_ALLOW_THREADS` macros).
Alternatives: CodeQL (real dataflow/taint — follows a value across functions), or plain
ripgrep for a first sweep. Run several — they overlap, and agreement (or disagreement)
between a syntactic scan and a dataflow query is itself signal; the overlap is cross-checking,
not waste.

## Patterns worth encoding for apsw

- **Borrowed-reference APIs** (`PyList_GET_ITEM`, `PyWeakref_GetObject`, `PyDict_GetItem`,
  `PyTuple_GET_ITEM`, …). Each returns a value you do not own. The census here is large but
  mechanical; the suspect subset is those held across a GIL release or a callback, or used
  after the owner could be decref'd. Fix is uniform: the owning/`*Ref` variant, or incref.
- **Pointers into mutable sources** (`PyByteArray_AS_STRING`, a `sqlite3_column_/value_*`
  pointer, an apsw buffer that may realloc). Suspect when the derived pointer/length/index
  is held across a GIL release or reentrancy. Fix is *not* uniform — copy before release, or
  re-derive after; incref does not help. This is the harder, higher-value census.
- **Error-path shapes** — a NULL return with no exception set; a refcount not balanced on an
  early `goto`/return; the wrong exception type when several errors coincide. These match
  the historical AI-report bugs.

## The method

1. Write a few small rules for one hazard. Start permissive (find all candidate sites), then
   tighten (add the "held across a release/callback" condition) to cut false positives.
2. Run over `src/`. Expect many hits; most are safe (private, immutable, or guarded by a
   busy flag / the db mutex).
3. **Triage is the real work** and is where an agent adds value: for each hit decide whether
   the source is actually mutable/foreign and whether the value truly spans a release or
   callback. Record the suspects with the invariant each would break.

## Tips

- Syntactic rules that try to span a macro-with-brace (`... ALLOW_THREADS ... ALLOW_THREADS`)
  are brittle; it is often easier to enumerate the API uses and check the release boundary
  during triage than to encode the whole shape in one rule.
- Cross-reference every suspect against the known issues so you know which are new — but keep
  the whole census: it is a standing map of every hazard site and a regression check, and
  re-confirming the known sites each run is worthwhile, not waste.
