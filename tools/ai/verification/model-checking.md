# Model checking — concurrency, lifecycle, reentrancy

**Good for:** bugs that only appear in a particular *interleaving* — the GIL-release
hazards, close() racing an operation, a callback re-entering its owner, a finalizer running
mid-method. You cannot find these reliably by testing; a model checker explores every
ordering of a small abstract model and hands you a shortest counterexample trace.

**Representative tool:** TLA+ with the TLC checker (write the algorithm in PlusCal, which
translates to TLA+). Alternatives in the family: Alloy (relational/structural models — nice
for object-graph and refcount shapes), SPIN/Promela, or any current equivalent. Pick what
fits; the technique matters more than the tool.

## The method

1. **Model the state, not the code.** A handful of variables: the GIL, a db mutex, a
   handle that goes live→freed, a refcount, a "closed" flag, a version counter for a
   mutable source. One process per actor (a worker thread, a closer, a mutator). Each
   atomic step is one label — the gaps between labels are exactly where another actor can
   interleave, so put a label boundary wherever apsw releases the GIL or calls back into
   Python.
2. **State the invariant** you believe holds: no use-after-free, no use of a stale derived
   value, mutual exclusion actually holds, etc.
3. **Run the checker.** Either it exhausts the state space (invariant holds, for that
   bound) or it prints the interleaving that breaks it.
4. **Map the trace back to real code** and decide if it is reachable given apsw's actual
   guards (the GIL today, the db mutex, busy flags). A trace that needs the GIL removed is
   a free-threading finding; one that works under the GIL is a live bug.

## Where this has paid off in apsw

- The **GIL-release borrowed-ref** and **stale-derived-value** hazards both show up as
  short UAF/stale traces, and the fixes (own a strong ref; copy-or-re-derive) verify clean.
  The two are genuinely different and worth two separate models.
- **close()** vs a concurrent operation: a worker can proceed with no real lock once close
  nulls the db mutex, and the mutex assertion is blind at that moment. Treat close on its
  own — it is subtle and the "proper" fix (a lock outside the db mutex) has real costs the
  author has weighed; confirm the diagnosis rather than assuming a fix is wanted.

## Tips

- Keep models tiny (2–3 actors, symbolic values). Bigger models rarely find more and cost
  a lot; the bug usually needs only a couple of actors.
- Model one hazard per file. Mixing lifecycle + data-validity in one model muddies traces.
- Toggle the fix with a constant and re-run: showing the fixed model has *no* counterexample
  is half the value.
- Beware tool-syntax quirks (translators are picky about process/label forms). When the
  translator complains, simplify the smallest construct first.
