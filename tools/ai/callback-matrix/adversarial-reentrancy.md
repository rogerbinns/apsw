# Adversarial callbacks — the single-thread axis

Take each enumerated callback and, in place of the cooperative callback the standard suite installs,
run a **hostile** one that does the worst legal thing to apsw while apsw is mid-method with the GIL
released. This is where the confirmed-but-unfixed reentrancy bugs live, and where their next
siblings are. The behaviours below are a *parameterised axis*: apply each to every callback whose
shape (`enumerating-callbacks.md`) exposes the relevant object or argument.

## The catalogue of hostile behaviours

- **Reenter apsw — through any object, not just this one.** From inside the callback, call back into
  the Connection/Cursor/Blob/VTab it fired from *and sideways into every other live apsw object of
  every type*: run SQL on a different cursor, drive a backup, read another blob, mutate a different
  statement's bindings, operate a session, register/unregister a callback (including *this* one),
  invoke the same user function recursively. apsw must allow each cleanly or refuse it with an
  exception — never corrupt state. The behind-the-scenes links (see the README) mean an operation on
  a "different" object can reach the one you are inside.
- **Destroy mid-callback — any object, not just the owner.** `close()` or drop the last reference to
  the owning object *or any other live apsw object* — a cursor, a blob, a backup, a session, the
  connection underneath a *different* cursor — while apsw is still inside the method that fired the
  callback. This is the close-vs-in-flight hazard on a single thread, via reentrancy; the
  highest-value targets are objects linked behind the scenes to the one you are in, because freeing
  one can invalidate state the other still assumes.
- **Free the arguments.** Drop the last reference to, or clear, the very thing the callback was
  handed or that apsw is walking: clear the list a bindings converter is iterating, empty the mapping
  being encoded, delete the value apsw holds only borrowed. This is the exact mechanism of the known
  binding-list-mutation use-after-free class and the serialiser default-key class — a container is
  mutated between the moment apsw takes a borrowed pointer into it and the moment apsw uses it.
- **Mutate without freeing.** Change a length, reorder, grow/shrink a container so a cached
  pointer/length/index apsw derived from it is now wrong though the object still lives. Incref does
  **not** defend against this — the fix is copy-or-re-derive — so it is a distinct test from the
  free-the-arguments case above; keep them separate.
- **Raise, and return hostile values.** Raise an exception; return the wrong type; return an object
  whose conversion or `__del__` *itself* reenters apsw or frees something; return a value that
  triggers yet another callback. Error paths are where historical refcount/wrong-exception bugs
  cluster — the known exec-trace class is a borrowed callable used on an *error* path after the
  callback cleared it.
- **Churn and collect.** Do all of the above across **many iterations** with a `gc.collect()` inside
  the callback. Refcount and borrowed-reference bugs usually survive a single pass and only fail once
  the freed slot is actually reused — so a one-shot test is green and a thousand-iteration one
  crashes. Drive the count up; it is the one knob that most changes whether a real bug shows.

## Making a lifetime bug visible — and proving it is real

A use-after-free is silent until the freed memory is reused, so run under instrumentation and apply
the discipline that separates a genuine finding from an artifact:

- **ASan-instrumented unity build.** Build apsw as one translation unit with AddressSanitizer so a
  read of freed PyObject memory is reported with a stack, covering connection/cursor/fts/jsonb/
  session/blob together.
- **`PYTHONMALLOC=malloc`.** Essential: without it, CPython's pymalloc arenas satisfy the allocation
  from a pool ASan does not see, and the use-after-free surfaces as a wild segfault with no apsw
  frame instead of a clean ASan report. With it, ASan sees PyObject lifetimes and names the bug.
- **A no-apsw control.** Run the identical object-churn and GC *without* apsw in the loop. If the
  control also crashes, the bug is CPython's, not apsw's — do not report it.
- **Confirm on a stock build.** Reproduce the crash on an ordinary non-ASan build. A stock
  crash proves the finding is real and not an instrumentation artifact; a bug that only shows under
  ASan is latent (worth recording, lower severity) — say which.
- **Do not pin the victim.** The sharp trap: a probe that takes its own reference to the object it
  expects to be freed *prevents the free*, so the test reads clean and hides the bug. Leave the
  victim reachable only through the path the callback mutates — that is the whole point of the attack.

## Outcomes and feedback

Classify every red into the three outcomes from the README: a real apsw bug (stock crash/UAF/assert
in apsw frames — the prize), correct behaviour (a clean exception; apsw legitimately refusing an
illegal reentry is a pass), or a harness artifact (pinned victim, or the no-apsw control crashes
too). A confirmed finding becomes a standing reproducer described by its *essential conditions*
(which callback, which hostile behaviour, "many iterations + GC, victim held only via the mutated
path") rather than brittle line numbers, so it keeps catching the class as the code moves — the same
regression-guard rule as `../verification/`. Compose with `../verification/fault-injection.md`: an
*allocation failure inside* a hostile callback is a further axis, and with `threads-and-blocking.md`
for the same attacks mounted from another thread.
