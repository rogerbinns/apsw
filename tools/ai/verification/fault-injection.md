# Fault injection — model every API's failure, not just its success

Most of apsw's hardest bugs live on **error paths that almost never run**: an
allocation deep inside SQLite fails, a CPython call returns `NULL`, a bind's
destructor fires on error — and the handling is subtly wrong. You cannot reach
these by normal testing because the failure almost never happens. You *make* it
happen, one failable call at a time, and check that apsw does the right thing.

The discipline is: **model every API apsw calls — CPython C-API, SQLite, libc,
and apsw's own internal helpers — for both the success and the failure case,
including the side effects of each.** The return value is only half the
contract. The side effects are where the traps are:

- **Failure must still set a Python exception.** A C function that returns
  `NULL`/`-1` on failure *without* setting an exception violates the CPython
  contract; the interpreter then raises `SystemError: ... returned NULL without
  setting an exception` (or, on a debug build, aborts on
  `assert((res!=NULL) ^ (PyErr_Occurred()))`). The mirror — returning a value
  *with* an exception still set — is equally a bug. This is a whole class, and
  it hides specifically on the rare allocation-failure path.
- **A call can steal — or *consume* — a reference, and the two differ on the
  failure branch.** `PyModule_AddObject` (pre-3.13) steals on success but *not*
  on failure, so the caller must own the ref on the failure branch and not on the
  success branch. But `PyList_SetItem`/`PyTuple_SetItem` are different: they
  consume the item ref on *every* path (steal on success, `Py_XDECREF` on
  failure) — the caller must *never* decref the item itself. Getting either
  backwards leaks or double-frees only when the call fails. This distinction also
  constrains how you may *fault* such a call (see the modelling note below): a
  consume-on-every-path API cannot be faulted by a skip-the-real-call injector
  without leaking the argument, because the real `Py_XDECREF` never runs.
- **Failure can consume/free a parameter, or leave output half-written.** Some
  SQLite calls run a destructor you passed even on certain errors; some leave an
  out-pointer partially initialized. Model what each call does to its arguments
  on the failure branch, not just what it returns.
- **Error paths acquire and must release.** A lock taken (or a resource
  allocated) before a fallible step that then raises must still be released.
  This is the classic "crown-jewel" error-path leak/deadlock.

apsw already ships a mature fault injector built on exactly this idea (every
failable call site can be made to fail, and the suite is coverage-driven so each
site is exercised). Treat what follows as how to build a **second, independent**
injector that reaches what a symbol-keyed one structurally cannot — and to
cross-check the one that exists. Build your own with whatever is current and
best; the points below are the principles, not a fixed recipe.

## Principle 1 — key faults by call stack, not by API name

A symbol-keyed injector ("make the next `sqlite3_malloc` fail") collapses every
call path through that symbol into one injection point, so to get per-call-path
resolution it has to *model apsw's own intermediate functions* by hand as extra
named sites. You can skip all of that: **capture the backtrace at each failable
call and treat each unique stack as its own injection site.** The stack *is* the
model — `PyMem_Realloc` reached from the JSONB encoder and the same call reached
from statement binding are then automatically distinct sites, failed
independently, with no hand-written intermediaries.

For this to work the site identity must be **stable across processes** (you
census sites in one run, then fail each in a separate run). Normalize every
return address to *module + offset* (e.g. via `dladdr`) before hashing the
stack, so the id does not depend on load addresses/ASLR. Memoize the
address→(module,offset) resolution — the symbolization call is slow and call
sites recur, so caching makes per-allocation keying amortized free.

## Principle 2 — fail *every* failable API, at every layer you can reach

Allocation is only one way a call fails. **Every API that can fail for any
reason should be made to fail in testing** — an allocation, yes, but also a disk
read/write/sync/truncate (`EIO`, `ENOSPC`), an `open`, a SQLite call returning
`SQLITE_BUSY`/`SQLITE_IOERR`/`SQLITE_CORRUPT`, a CPython call returning
`NULL`/`-1`. The error-handling path for each is distinct and almost never runs
on its own, so each needs its own forced failure, with the *correct* failure
mode and side effects (Principle 0's contract: the right return value *and* the
right exception/consumed-parameter/partial-output behavior for that specific
API).

No single interception reaches every layer — use the right mechanism per layer:

- **libc (allocators *and* I/O/syscalls), including inside bundled SQLite/ICU.**
  `LD_PRELOAD` interposes these. Route the interpreter's allocator to libc
  (`PYTHONMALLOC=malloc`) and one interposer then sees **CPython, SQLite, and ICU
  allocations at once** — and the same interposer fails `read`/`pwrite`/`fsync`/
  `ftruncate`/`open`/`mmap` to drive the `SQLITE_IOERR`/`SQLITE_FULL` disk-error
  paths, which are even less exercised than OOM. (Gotcha: SQLite is built with
  64-bit file offsets and calls the `*64` entry points — `pread64`/`pwrite64`/
  `open64`/`ftruncate64` — not the plain names; interpose both or you silently
  miss all of SQLite's I/O. A census showing zero `pread64` frames is the tell.)
  This is the surface a
  symbol-keyed injector structurally cannot reach (it can fail the *result* of a
  call apsw makes, not the *Nth internal allocation or read* SQLite performs
  inside it) — and it is the surface that produced a real, previously-unreported
  apsw finding via malloc-layer interposition.
- **SQLite's own interfaces.** SQLite is statically linked, so `LD_PRELOAD`
  cannot touch its internal *return values* — but SQLite is built to be faulted
  from the outside: install a custom allocator (`sqlite3_config`), a wrapper VFS
  that fails any I/O method, or use its built-in test controls to force
  `SQLITE_BUSY`/`IOERR`/`NOMEM` at the API boundary.
- **CPython C-API and the SQLite API at apsw's own call sites.** Also not
  `LD_PRELOAD`-reachable (static). This is precisely what apsw's *source-level*
  injector already covers — it macro-wraps a curated set of CPython + SQLite +
  apsw-internal calls and can fail each. Lean on it here; your out-of-process
  injector complements rather than replaces it, and running both cross-checks the
  overlap.

Enumerate the failable APIs apsw actually reaches (cross-check the source
injector's wrapped list against a census of calls — a gap there is an un-faulted
failure path), and make sure each is covered by *some* layer.

A happy consequence of failing at the allocation layer: the real allocation
never ran, so there is nothing to leak — which sidesteps most of the
"run-the-real-call-but-report-failure" gymnastics a return-layer injector needs
to stay leak-clean under a sanitizer. You still need that trick (invoke the real
function, then hand the caller an error) when you fault a call whose job is to
release/clean up — a `close`, a free-like API — because skipping the real work
leaks the resource; keep that mode for those (it is a few lines) and you will
need it far less than a pure return-layer injector does. The same mode is
mandatory for any call that **consumes a reference or frees a parameter on its
failure path** (the consume-on-every-path ref-stealers above; a SQLite call that
runs your destructor on error): skip-the-real-call faulting of one of these
leaks the argument, so it must either run-real-then-fail or be excluded from
faulting with a noted reason.

**Audit the failure *model* itself, not just build another injector.** A
source-level injector encodes, per wrapped API, a model: the failure return
value, whether it sets an exception, and what it does to its arguments. That
model is hand-maintained and can drift from — or never have matched — the API's
real behaviour (which, for an open-source dependency, you can read rather than
guess: the source is authoritative over the docs, and may do more than the docs
say). So one high-value, low-cost pass is to **diff the model against the actual
source** of every wrapped call: pull each API's real failure contract from its
implementation and confirm the injector represents it faithfully — a destroy/
consume-on-failure call modelled as a plain skip-real fault is a model bug (it
leaks, or exercises an error branch the real call can't reach), even when a
masking accident — the leaked object happening to be immortal, an unreachable
branch — keeps it from ever surfacing in the suite. The fix is to give it the
run-real-then-fail facade or exclude it with a reason, matching how its
siblings of the same class are already handled. Finding this needs no running at
all, only reading; it hardens the tool that hardens apsw.

## Principle 3 — synthesize the driver with coverage feedback (AFL-style)

Do not hand-write the workload that reaches the call sites — you will miss
surface. The injector already emits the coverage signal you need: the set of
unique sites a run touches. Use "**did this reach a new site?**" as the fitness
function, exactly as a coverage-guided fuzzer keeps inputs that hit new edges:

- Seed with a broad spread of API-exercising snippets (one per feature family).
- Grow the corpus by **introspection** — enumerate every class/method/function
  the binding exposes and call each with a menu of candidate arguments, each
  call guarded so one failure does not end the snippet.
- Keep any snippet that expands the global site set; stop when growth saturates.
- Record, for every site, which snippet first reached it ("owner"), so the
  injection sweep can re-reach that exact site by re-running its owner.

This turns "what Python code exercises all the paths?" from a manual chore into
a search the tool runs itself, and the surviving corpus is a reusable asset.

## Principle 4 — from one stack to many: a race scheduler

A faultable point is also a *schedule* point. One thread's backtrace is the state
of a sequential test; **the tuple of every thread's backtrace, each parked at a
faultable point, is the state of a concurrent one.** That gives a deterministic
race explorer instead of "run it hard and hope":

- When a thread reaches a schedule point, **park it** and register its stack.
  When two (or more) threads are parked, a controller **chooses the release
  order**. Run the same scenario under each order and you drive *both* sides of a
  race on demand — the lever the close-vs-operation / GIL-release class needs,
  which otherwise only shows up probabilistically at high iteration counts.
- **The release order is the thing you enumerate.** Identify a race point by the
  set of parked stacks; replay it, picking a different winner each run (both
  orders for a pair; a bounded randomized scheduler for more threads). This is the
  real-binary analogue of a model-checker counterexample — the model says the
  interleaving exists; this reproduces it on the actual code, deterministically.
- **Park only where it is safe** — never while the thread holds a lock others
  need to make progress, above all the GIL: parking a GIL-holding thread
  deadlocks every other Python thread. This makes the **build** the first design
  choice:
  - **Free-threaded build (no GIL) — the clean home, and the right target for
    the race class anyway.** With the GIL off, parking a thread at one of the
    libc faultable points you *already* interpose (an allocation/read/write)
    blocks nobody else, so your existing out-of-process injector doubles as the
    scheduler with a park/rendezvous controller bolted on — no need to touch the
    GIL at all. (apsw's free-threaded build re-enables the GIL by policy; force it
    off for this.)
  - **GIL build** — you would want to park at the GIL-release boundary
    (`PyEval_SaveThread`/`RestoreThread`), but a practical trap blocks the obvious
    route: if the interpreter is **statically linked** (libpython inside the
    `python` executable, the common case for `uv`/pyenv standalone builds, even
    when `Py_ENABLE_SHARED` is set — check `ldd`), the executable's own copy of
    those symbols wins symbol resolution over any `LD_PRELOAD`, so you cannot hook
    them from a preload (libc interposes fine because it loads *after* the
    preload; libpython-in-the-executable does not). Hooking the GIL boundary from
    outside the process therefore needs a genuinely **shared** `libpython.so`
    build. Prefer the free-threaded route above.
- **Compose it with fault injection.** Each schedule point is also a failure
  point, so the full space is (interleaving) × (which calls fail) — a race that
  only manifests when an allocation *also* fails on one side is reachable this way
  and almost nowhere else.

Build it aimed at the one or two known race windows first (deadlock-free parking
is fiddly), then widen. It pairs with the formal close model: the checker still
produces the counterexample, this turns it into a standing, deterministic repro.

Two hard-won park-safety rules, both learned by deadlocking on them: (1) do **not**
park inside an allocation — SQLite holds a global allocator mutex across its
`malloc`, so a thread parked there blocks every other thread's allocation; park at
**I/O** points instead (the VFS does not hold that mutex). (2) Arm the scheduler
only for the racing region and never park the orchestrating/main thread, or it
parks during setup and nothing starts. A robust pairing that needs no
pre-identified sites: park the first faultable point each of the first *two
distinct non-main threads* reaches, then release in the chosen order — this
reproduced deterministic A-before-B and B-before-A interleavings on a
free-threaded apsw with the GIL off.

**Match park granularity to the size of the window.** Faultable points
(allocations, I/O) are *coarse* schedule points — they pin a race only where it
straddles such a call. The sharpest lifecycle races are smaller than that: a
validity/liveness check and the use it was meant to guard sit in **one function,
a few statements apart**, with the invalidation (a field nulled under a lock the
reader never takes) landing in between. No call-boundary park can hold that
window open. The principle is a *granularity ladder* — function-entry → basic
block → source line/instruction — and you climb it only as far as the window
demands. Basic-block granularity is the rung that splits a within-function window
at compile time with no per-instruction tax: build the target with compiler
**coverage instrumentation that calls back on every basic block** (the same class
of instrumentation a coverage fuzzer uses) and park at the specific block,
resolving block→source through the instrumentation's own PC table. Same
park/release controller, finer point. Two dividends fall out: parking one block
*past* the suspect statement and seeing the fault vanish is positive proof you
have localised the exact window; and recording each thread's block trace while
armed yields the **causal path into the fault** — not merely that it crashed but
the sequence of code that led there, on both threads. Climb no further than
needed: line/instruction granularity (ptrace/DBI) costs a trap per step and is
rarely worth it once basic blocks isolate the window.

## Principle 5 — the oracle, and a debug target

For each site: run the owning workload once with that site's first occurrence
forced to fail, and classify the outcome.

- **Controlled (PASS):** a normal Python exception propagates (a `MemoryError`,
  or any apsw/ValueError/TypeError), or the program completes. The contract held.
- **Bug:** `SystemError: ... returned NULL without setting an exception` or
  `... returned a result with an exception set`; a fatal error /
  `_Py_CheckFunctionResult` abort; `SIGSEGV`; `SIGABRT` on an assertion; or a
  hang (an error-path deadlock — give each run a timeout).

Run against a Python built for analysis — **assertions on, pooling allocator and
freelists disabled** (see `dynamic-and-differential.md`) — so the interpreter's
own invariants and **apsw's own contract asserts** (apsw asserts the
success⊕exception invariant at its boundaries) fire, and so a clean run is
genuinely clean. A release build still catches the `SystemError` and the crashes
via the universal call-result check; the debug build adds the asserts and leak
accounting. Cross-run the flagged sites under a sanitizer build to catch
use-after-free and leak-on-error-path that a crash-only oracle misses.

## Principle 6 — triage apsw-owned vs upstream failures

A crash under injection is not automatically an apsw bug. Decide by the
**innermost non-library frame**: if the failing allocation's handling is in
apsw/SQLite code, it is yours; if the crash is deep in the interpreter's own
machinery (type creation during an `import`, say), it is an upstream
OOM-robustness issue — real, but file it against CPython, not apsw. (Expect
some of these: apsw imports modules at init, and the interpreter is not hardened
against OOM on every internal path.) Report as an apsw finding only what apsw
owns; forward the rest. Cross-reference every apsw-owned finding against the
existing C/Python audit lists so you know what is actually new.

## Why run this even when it finds nothing new

A full sweep that comes back clean is a *result*: it says every modelled failure
on that surface is handled with a clean exception, and it leaves a standing,
re-runnable guard. Re-run it after changes, widen the corpus as apsw grows, and
re-point it at freshly written C (a new codec, a new API) *before* it is
hardened — new error-handling code is where this bites. Related external work
worth mirroring or running alongside: coverage-guided extension fuzzers with
foreign-OOM injection, and static C-extension auditors that encode the
new/borrowed/stolen reference tables and the error-path contracts directly.

## Regression guard — essential conditions

Keep this as a standing check, described by conditions (so it survives code
edits and better tooling), not by site hashes or counts:

- **Every failable allocation on apsw's surface, failed one at a time, must
  yield a clean Python exception — never a `SystemError` (NULL/result vs
  exception mismatch), crash, assert, or hang.** Reach the surface with a
  coverage-grown corpus, key injection by normalized backtrace so each call path
  is tested independently, inject at the allocation layer (so SQLite/ICU-internal
  failures count), and run on an assertions-on, non-recycling build. Triage out
  upstream-owned crashes. Until an apsw-owned violation is fixed, this reproduces
  it; when it stops, the fix (or a coverage regression) is why — find out which.
