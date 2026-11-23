# Threads and blocking — the cross-thread axis

The second half of the matrix: instead of (or as well as) a callback that reenters on its own thread,
**park the callback** and attack the same objects from *another* thread. This is the axis the user
named — "block that thread and call in from another thread" — and it exercises apsw's thread-affinity
and mutex discipline, which single-thread reentrancy cannot reach. Apply it to every enumerated
callback whose owner another thread can get a handle to.

## The core manoeuvre: block inside the callback, attack from outside

From inside a firing callback, block the thread on an event (do not return yet). While it is parked
there — apsw mid-method, GIL released, an operation in flight — have **every other thread** attack
**every live apsw object of every type**, not only the one the callback fired from:

- **`close()` any object** — the one the callback is inside, and every other connection, cursor, blob,
  backup, and session (the close-versus-in-flight race, now genuinely concurrent rather than
  reentrant),
- **run an operation** on any Connection/Cursor/Blob/Backup/session — especially ones linked behind
  the scenes to the parked object,
- **free** any object or a dependent,
- **register/unregister** callbacks on any object.

Being parked in a blob, VFS, or exec-trace callback puts **nothing** off-limits: the hidden links
between object types (see the README) are where the concurrent *cross-object* bugs live, so the
second thread should go after objects that look unrelated to the callback you are in, not just its
owner. Then release the event and let the callback return. Vary *which* thread wins the window, and
*which* object type it targets. Under the GIL this is largely serialised and most of these are
masked; the point of the manoeuvre is to hold the window open explicitly so the race is reliable
rather than vanishingly rare.

## Two expected results, and how to tell a bug from correct behaviour

apsw enforces thread-affinity and guards its SQLite calls with a database mutex, so the cross-thread
attacks have a **correct** outcome that must not be mistaken for a bug:

- **Calling an object from the wrong thread should raise `ThreadingViolationError`** — a clean Python
  exception — not crash and not hang. A `ThreadingViolationError` here is outcome 2 (a pass): apsw
  correctly refused. Assert that it is raised; its *absence* (the call proceeds, or crashes) is the
  finding.
- **A crash or a hang instead of that clean refusal is the bug.** The known liveness class is exactly
  this: a raw `sqlite3_*` call reached without the database-mutex guard that its siblings use, so
  instead of raising a threading violation it **deadlocks**. The whole family of setters/getters must
  be audited the same way — one unguarded member is a hang.

## Detect deadlocks with a watchdog, not a race detector

Lock-order inversions and missing-guard hangs are **liveness** failures, and thread sanitizers
(TSan) and happens-before race detectors (Helgrind) **do not flag them** — that is precisely why the
data-race track missed this whole class. The detector that works is a **watchdog timeout** wrapped
around each cross-thread scenario: a scenario that does not return within a generous bound is a
liveness bug (deadlock or livelock). Capture both threads' stacks at the timeout to name the lock
cycle. Pair this with `../verification/model-checking.md`: express the close-versus-operate (or
callback-reenters-from-another-thread) interleaving as a small model and let the checker enumerate
the orderings and produce the counterexample — the watchdog finds *that* it hangs, the model finds
*why*.

## Run the whole matrix with the GIL genuinely off

The GIL masks most of these races, so a passing run under the GIL proves little about them. Run the
entire cross-thread matrix on a **free-threaded build** with the GIL forced off — and **verify it is
actually off after `import apsw`**, because apsw re-enables the GIL by design, so a test that does
not re-check is silently vacuous. With the GIL genuinely off and an object shared across threads
doing close-versus-operate and callback-reenters-from-another-thread under sustained, high-iteration
load, the narrow windows that are invisible under the GIL become reproducible crashes. This is the
same catch-condition as the #637 / pillar-C regression guard in `../verification/README.md`; share
the harness. The race window is small, so a brief run passes — only a long adversarial run trips it,
and getting the effort high enough is the single thing that matters.

## Composition

The two axes multiply: every hostile behaviour in `adversarial-reentrancy.md` can be mounted from
another thread while the callback is parked, under the GIL and without it. Generate the full cross
product, instrument it the same way (ASan unity build, `PYTHONMALLOC=malloc`, no-apsw control, stock
confirmation), and classify into the three outcomes. A confirmed cross-thread finding leaves a
standing reproducer stated by its essential conditions — "block in callback X, `close()` from another
thread, GIL off, N iterations" — not by line numbers, so it survives the code moving.
