# Verification toolbox for apsw

A grab-bag of external tools and techniques for finding **logic and code errors** in apsw —
especially ones not already captured in the AI-report issues (#624 / #627 / #637) or the
existing suite. This directory is written for a *fresh agent*: enough to get oriented and
start, deliberately **not** a rigid runbook.

## Operating principle: run everything, cross-check, repeat

Compute is free here — the CPU, RAM, and storage are already paid for. These docs are **not**
about picking one tool or avoiding work; they are about doing *as much as possible* and
cross-checking the results. The goal is to make apsw better with every tool and technique
available, now and ongoing.

- **Run every applicable tool, not just the "best" one.** Overlap is a feature: when
  independent tools agree, that is confirmation; when they disagree, that is a flag worth
  chasing. A tool that only re-derives a known finding is still earning its keep — it is an
  independent check and a standing regression guard that will catch the next instance.
- **Re-running is worth it.** This is an ongoing practice, not a one-shot pass. Re-run after
  changes, and fold in new tools and techniques as they appear — better tools and models keep
  arriving. Diminishing returns are expected and completely fine; a continually cross-checked
  apsw is the point, not a single clever run.
- **Novel findings are a bonus, not the only prize.** Cross-reference findings against the
  known issue lists (there are **both** C and Python audits) so you know what is new — but the
  comprehensive, redundant, cross-checked coverage is itself the deliverable. Never skip a
  tool because it "probably won't find anything new."

## How to use this directory

Each file describes one *family* of tool, what class of bug it targets, how it maps onto
apsw, and roughly how to start. Treat them as starting points, not recipes:

- **Tools and models drift.** Versions, flags, rule syntax, even which tool is best will
  change. Reach for the current best tool in the family *and* the ones named here. Nothing
  has to match an exact version.
- **Interpret loosely, and diverge on purpose.** If two readings of a task are plausible, try
  both — different angles find different bugs. Running the same thing several ways, and past
  several agents, is good: the spread is the coverage.
- **The reusable move:** pick an invariant apsw must maintain, choose a checker that can hunt
  violations of it, express it at the right altitude (a model, a proof, a pattern, an
  instrumented run), run it, and cross-reference what comes back against the known issues and
  against the other tools' output. Then run the next tool.

## What apsw is (so you aim the tools right)

apsw is a thin, careful C extension wrapping SQLite, plus Python. The interesting bugs
cluster where C, Python's runtime, and SQLite meet. Productive things to aim at:

- **Concurrency / the GIL boundary.** apsw releases the GIL around SQLite calls, so other
  threads (and reentrant callbacks) run *inside* a method. Two distinct hazards live here:
  a **borrowed reference** whose owner is freed in the window (fix: own a strong ref), and
  a **value derived from a mutable source** — a pointer/length/index from a Python mutable,
  a SQLite column/value pointer, or an apsw buffer that may realloc — that is *invalidated*
  by mutation while the object stays alive (fix: copy or re-derive; incref does **not**
  help). These are different bugs with different fixes; keep them apart.
- **Object lifecycle & reentrancy.** close(), destructors/finalizers, cursors and other
  dependents, callbacks that re-enter the owner. See `close` especially — it is gnarly and
  best treated on its own.
- **Error paths.** Refcount deltas, wrong exception type when several errors coincide,
  NULL returns without an exception set. These are where the historical AI-report bugs live.
- **Binary format / parser invariants.** JSONB encode/decode: bounds and overflow on the
  extended-size length field, endianness, escape/UTF-8/surrogate validation, round-trips.

## Build apsw the way it ships (so the extensions are tested too)

Run the tools against apsw built as users actually get it — the extensions (FTS5, RTREE,
geopoly, session, …) are logically separate but are part of apsw's value (core *and* how
easily it exposes the extensions), so a default build that omits them leaves real surface
untested. A missing extension is silent: e.g. a default build has no FTS5 module at all, so
nothing exercises `apsw.fts5*`.

The reflective build is the PyPI/CI one. CI copies `tools/setup-pypi.cfg` to `setup.apsw`
(read as ini build directives) and builds with cibuildwheel; the config is:
`fetch=True` (download the matching SQLite release), `enable_all_extensions=True`,
`omit=icu`, `enable=COLUMN_METADATA`. The `omit=icu` is a **portability** decision, not a
blanket one: a distributed wheel can't assume the deployment platform has ICU, let alone a
compatible version. For a **local verification build the opposite holds** — whatever ICU is
present is stable and known — so include ICU (build all extensions *without* the omit) to
exercise the fullest surface (SQLite ICU collations/functions + apsw's ICU unicode path). If
the image lacks the ICU dev headers they can be staged without root (extract `libicu-dev`).
The
CI test command is `python -m apsw.tests -v` with `APSW_HEAVY_DURATION=2`; the deeper local
run is `tools/coverage.sh` (the fault injector `-m apsw.tests -vf`, then `apsw.tests.async_meta`
and `apsw.tests.large`). To mimic exactly, read `.github/workflows/build-pypi.yml` — note the
matrix already includes free-threaded `cp314t`/`cp315t`, and `codeql.yml` runs CodeQL (stock
default query set). So: build with that config, and point the verification tools at that
build, not a hand-picked subset.

## The files

- `ai-agent-audit.md` — **start here.** Pointing capable models at the code is the
  highest-yield tool and already produced the outstanding `C###`/`PY###` lists; the rest of
  this directory mostly complements or *checks* it. Read this for how to get marginal value
  (direct it; diversify) and how to pair it with the dispositive tools below.
- `model-checking.md` — find concurrency / lifecycle / reentrancy bugs by exploring all
  interleavings of a small model.
- `proof-assistants.md` — prove (or refute) pure invariants: parser/format soundness,
  bounds, overflow, round-trips.
- `pattern-scanning.md` — enumerate and flag suspicious code shapes across the whole tree,
  then triage.
- `c-analysis.md` — point-sensitive and whole-program analysis of the actual C.
- `dynamic-and-differential.md` — run the code under instrumentation, fuzz it, and compare
  it against an oracle (stdlib sqlite3 / other SQLite versions).
- `documented-rules.md` — SQLite's safety rules live only in prose; break one and you get
  silent corruption. Mine the docs (they ship in the tree) for the rule set, and make apsw
  enforce each in code or at least document it — re-derived per SQLite version as it drifts.
- `fault-injection.md` — model every API (CPython, SQLite, libc, apsw internals) for its
  *failure* as well as its success, including side effects (exception-on-failure, ref
  stealing, freed params). Build an injector that fails one call at a time — keyed by call
  stack and reaching foreign/intra-library allocations — and check apsw always degrades to a
  clean exception, never a crash or `SystemError`. Complements and cross-checks apsw's own
  fault injector.

## Regression guards — findings the suite must keep catching

Running the verifications is a regression guard, not just a one-time search. A confirmed
finding should leave behind a reproducer that keeps triggering until the bug is fixed; when
it stops, either the fix landed (record that) or the check regressed (find out why). Keep the
reproducers with the suite — but describe each by its **essential conditions, not brittle
coordinates**: exact line numbers and iteration counts drift as the code changes, so state
what conditions reveal the bug and let the current tools re-locate the specifics. (Point-in-
time specifics — faulting lines, the counts that happened to work — belong in the trial notes,
not here.)

- **Close vs. in-flight operation, under free-threading (#637 / pillar C).** Surfaces only
  with the GIL genuinely off and adversarial shared-object use, so the catch-conditions are
  the whole point: build a free-threaded Python, force the GIL off — and verify it is actually
  off *after* `import apsw` (apsw re-enables it by design, so otherwise the test is vacuous) —
  then share one Connection across threads doing close-versus-operate and run it hard. The race
  window is narrow, so a brief run passes; only a sustained, high-iteration adversarial run
  trips it (getting the effort high enough is the one thing that matters). Expected until
  fixed: a crash. Cross-check with the formal close model — the checker should still produce
  the matching counterexample, and the fixed variant should verify clean. When both stop
  flagging, the fix is in.
- **fts5query round-trip / semantic bugs.** Structure-aware round-trip plus a differential
  against the real FTS5 engine re-derive them quickly; keep running until fixed.
- **Allocation-failure handling across the whole surface (`fault-injection.md`).** Every
  failable allocation apsw reaches — including ones inside bundled SQLite/ICU — failed one at
  a time must produce a clean Python exception, never a `SystemError`, crash, assert, or hang.
  Reach the surface with a coverage-grown corpus, key injection by normalized backtrace, run
  on an assertions-on / non-recycling build, and triage out upstream (CPython/SQLite)-owned
  crashes. Re-point it at any newly written C before it is hardened.
- *(append each newly confirmed finding by its essential conditions + expected outcome)*

A finding earns a standing check, re-run automatically next time — written at the level of
"what conditions reveal it" so it survives code edits and better tools.

## Feeding results back

New, confirmed findings become issue items / gist entries in the usual flow (the author
writes product-code fixes; keep models/scripts out of the read-only source tree — put them
in your own working area). A finding is worth reporting when you can state the invariant it
breaks and, ideally, the smallest input or interleaving that breaks it.
