# Callback matrix for apsw

Exhaustively and **adversarially** exercise every callback apsw can invoke — each one driven by a
*hostile* Python callback — and verify apsw always degrades to a clean Python exception, never a
crash, use-after-free, assertion, `SystemError`, or deadlock. This directory is written for a
*fresh agent*: enough to understand *why* this is the richest remaining bug surface and *how to
build the tester*, deliberately **not** a rigid runbook. The exact set of callbacks, their
signatures, and the line numbers all drift every SQLite and apsw release — so this describes the
*method* and leaves the per-release specifics to be re-derived each time (see
`enumerating-callbacks.md`).

## Why this is where the bugs are

apsw is a thin C layer between two runtimes, and a callback is the one place where **foreign code
runs in the middle of an apsw method, with the GIL released and apsw's invariants mid-flight**. The
standard suite installs mostly *cooperative* callbacks — a scalar function that returns a number, a
collation that compares two strings — on one thread. The historical bug ledger (issue #637 and the
`C###` audit) concentrates exactly where the callback is *adversarial* instead:

- it **reenters** apsw — into the object it fired from, or *sideways* into any other live apsw
  object (connection, cursor, blob, backup, bindings, session) via their behind-the-scenes links,
- it **destroys or frees** an object — or the very arguments it was handed — mid-callback,
- it **mutates** the inputs apsw is still walking (a binding list, a mapping being encoded),
- it is **attacked from another thread** while parked inside the callback.

These are the escapes the data-path fuzzers and the standard suite structurally miss: a fuzzer
feeds *data*, it never installs a callback that turns around and attacks apsw. Four distinct,
confirmed, known-but-unfixed bug *classes* all live here — a binding-list converter whose list is
mutated mid-walk (borrowed element freed), an error-path traceback on a borrowed callable the
callback cleared, a hook slot NULLed before the replacement is published (reentrant `__del__`
calls through NULL), and an output callback that closes its own session mid-stream. The matrix
exists to find the next members of each class before users do.

## The reusable move

Pick a callback. Enumerate the hostile things its Python side can legally do (reenter, destroy,
mutate, raise, block-and-attack-cross-thread) — and apply them to **every apsw object of every type,
from every thread**, not just the object the callback fired from. Fire it many times with each
hostile behaviour, under instrumentation that makes a lifetime bug visible. Classify what comes back
into the three outcomes below. Then take the next callback. The matrix is the cross product
**{every callback} × {every adversarial behaviour} × {every apsw object as target, every type} ×
{single-thread reentrancy, cross-thread attack from every thread} × {GIL, no-GIL}** — mechanically
generated, not hand-written, because a callback you forget to list is a callback nobody attacks, and
an object type you treat as off-limits is a web of links nobody tests.

## The target set is the whole object graph, not the owner

Being inside a blob, VFS, exec-trace, or any other callback does **not** put connections, cursors,
backups, bindings, other blobs, sessions, changeset iterators, or any other object off-limits. apsw's
object types are linked behind the scenes — a cursor holds its connection and a prepared statement; a
backup ties two connections; a blob and a session belong to a connection; bindings belong to a
statement; a VFS file underlies a connection; an FTS5 object reaches its table and connection — and
those internal links are exactly where cross-object lifetime bugs hide. So from any callback, on any
thread, the legal targets are **all live apsw objects of every type**: `close()`, free, mutate, or
operate on an object that looks unrelated to the callback you entered through, and watch for the
hidden link that makes it related (freeing or closing the "unrelated" object invalidates state the
one you are inside still assumes, or vice versa). A matrix that only ever attacks the owning object
exercises a fraction of the graph and misses precisely the bugs that make this surface worth testing.

## Three outcomes (same discipline as `../verification/`)

Separate these every time or the signal drowns:

1. **A real apsw bug** — a crash, use-after-free, assertion abort, `SystemError`, or hang whose
   frames are in apsw. The highest-value catch. Reproduce on a **stock** build to prove it is not
   an instrumentation artifact.
2. **Correct, expected behaviour** — a clean Python exception, or a `ThreadingViolationError` when
   a callback attacks from the wrong thread. apsw faithfully refusing an illegal reentry is a
   *pass*, not a finding.
3. **A harness artifact** — the tester pinned the victim and prevented the very free it meant to
   trigger, or the no-apsw control crashes too (so it is CPython's bug, not apsw's). Rule these out
   before believing a red.

## Positive control: the matrix must be able to go red

A callback matrix that reports "all clean" is worthless until you have watched it catch a bug it
must catch. Use the known-but-unfixed reentrancy classes above as detect-the-known controls: point
the matrix at each class's callback with the matching hostile behaviour and confirm it reproduces
(a stock crash, or an ASan report with apsw frames). If a known class stays green, the matrix is
not reaching that callback or not applying that behaviour — fix the harness before trusting any
clean result elsewhere. This is the same positive-control rule the rest of `tools/ai` runs on.

## The files

- `enumerating-callbacks.md` — **start here.** How to mechanically derive the *complete* current
  set of callbacks (every SQLite-registered one and every apsw-level one) each release, and record
  each one's *shape* (how often it fires, what it is handed, what apsw holds borrowed vs owned
  across it) — because the shape is what the adversary exploits and the list is what must stay
  complete.
- `adversarial-reentrancy.md` — the single-thread axis: the catalogue of hostile behaviours a
  callback performs (reenter, destroy, mutate, raise, churn+GC), and the instrumented method
  (ASan unity build, `PYTHONMALLOC=malloc`, a no-apsw control, stock-build confirmation) that makes
  a lifetime bug visible and separates it from an artifact.
- `threads-and-blocking.md` — the cross-thread axis the method turns on: block a thread *inside* a
  callback and attack the same object from another thread; call from the wrong thread (expect
  `ThreadingViolationError`, not a crash); detect deadlocks with a watchdog, not a race detector;
  run the whole matrix with the GIL genuinely off.

Cross-check against `../verification/model-checking.md` (the interleavings behind a cross-thread
finding), `../verification/fault-injection.md` (failure *inside* a callback is another axis to
compose with these), and the release-agreement check (a newly added callback in a SQLite/apsw
release must show up in the enumeration delta, not be discovered by a crash).
