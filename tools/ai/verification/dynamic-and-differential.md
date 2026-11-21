# Dynamic & differential — run it, stress it, compare it

**Good for:** confirming the hazards the other tools *predict*, and for catching things no
static view will — real memory errors, real data races, and behaviour that disagrees with
an oracle. This is where a predicted interleaving becomes an actual crash.

## Instrumented runs (sanitizers)

Build with the sanitizers and run the suite. AddressSanitizer + UndefinedBehaviorSanitizer
catch use-after-free / out-of-bounds / UB on the paths the tests exercise.

**Setup that makes this trustworthy — tune the analysis Python so object lifetimes are
visible.** A stock Python hides exactly the memory bugs you are hunting, because several
layers *recycle* memory instead of freeing it, so a use-after-free or overrun lands on live,
reused memory and the sanitizer never sees a free. Defeat each layer when you build the Python
you analyze against:

- the **pooling allocator** (pymalloc) batches objects into big arenas → the tool sees one
  `malloc` per arena and intra-arena errors are invisible. Build without it (or force the
  allocator to plain `malloc`) so every object allocation hits `malloc` directly.
- **per-type freelists** (frames, tuples, floats, lists, dicts, …) hand a freed object's
  memory straight back out → a UAF lands on recycled-but-live memory. Build with freelists
  disabled so a free is a real free.
- a **debug build** adds assertions and refcount tracking (a whole-run refcount total is a
  near-free C-extension refleak check), and Python exposes **sanitizer/analysis build
  options** directly.

The exact configure flags drift by Python version — the principle is what matters: *make
every object free reach the allocator*. Then a clean sanitizer/valgrind run actually means
clean. (The project's debug Python is deliberately built along these lines — no pooling
allocator, freelists defeated — which is why the dynamic-cluster "clean" results above are
trustworthy rather than hollow.)

**ThreadSanitizer** is aimed squarely at the GIL-boundary hazards in `model-checking.md`, but
it comes with a trap that makes a naive "TSan found nothing" worthless, so treat it carefully:

- *Always run a positive control in the exact build you are testing.* A detector that reports
  "no races" is only informative once you have watched it report a race it must see — a trivial
  two-thread unsynchronized-write program, run under the same interpreter and options. If the
  control comes back silent, the detector is not looking, and every "clean" from it is hollow.
  This bit hard here: under a free-threaded interpreter TSan can report **zero** even on a
  blatant known race, because that runtime guards object access with its own per-object locks
  and critical sections (so there is no *memory-level* data race for TSan to see — the lost
  update is a higher-level logic race) and because such interpreters ship large TSan
  suppression lists. So a TSan zero on an apsw stressor may mean "the interpreter serialised the
  accesses," not "apsw is race-free" — do not read it as confirmation without the control.
- *Cross-check with lockset / happens-before detectors that need no recompile* (Valgrind's
  Helgrind and DRD). These flag a race whenever two accesses are reached under **disjoint lock
  sets**, even if the damaging interleaving did not occur in that run — more aggressive than
  TSan's observed-happens-before view, and they run on a plain (non-instrumented) build, so they
  are an independent third/fourth opinion. Filter their output by the *racing* frame, not by
  caller, or you hide real findings under the interpreter's own internal no-GIL races.
- *The deterministic race scheduler (`fault-injection.md`, Principle 4) is the dispositive
  tool* when the window is narrow: detectors that rely on the interleaving actually happening
  can miss a window that only opens under precise timing, whereas the scheduler forces it. Where
  a probabilistic detector and the scheduler disagree, trust the one that controls the schedule.

The project already has a debug/sanitizer build path; the leverage is writing the *multithreaded*
stressors (concurrent close, operate-while-closing, mutate-a-shared-object-during-a-call) that
drive those races — and then reading each detector's result through the cautions above rather
than at face value.

## Fault injection and property testing

apsw has a mature fault injector (every failable call can be made to fail) — the historical
error-path bugs live exactly where it reaches. Keep strengthening what it *asserts* (exact
exception type, refcount balance), not just that it ran. For the Python API, property-based
testing (generate structured inputs, assert invariants/round-trips) complements it; for the
binary format, coverage- or grammar-guided fuzzing (a JSON/JSONB generator, or a general
fuzzer over the decode entry points) explores corners hand-tests miss.

**Aim fuzzing where it pays off: new, unhardened, input-parsing surfaces.** These tools found
nothing on the JSONB codec precisely *because* it is mature and already audited — a hardened
target starves a fuzzer of signal. The high yield is on freshly written code that parses or
coerces external input and has not yet been through a static audit: a query-string /
named-binding / type-coercion layer, a new parser, a new import path. Point structure-aware
and property-based generators (seeded with real example inputs) at such a surface *as it
lands*, before it is hardened. A new API that re-expresses an existing one is doubly
attractive: the old path is a ready-made **differential oracle** (same inputs → compare
results), turning a fuzzer into a correctness checker, not just a crash finder.

**Confirm your instrumentation actually reaches the code under test — a "clean" run over code
you never instrumented is hollow.** Coverage-guided fuzzing and sanitizers only see the
translation units they were built into. A binding like apsw is several extensions: a sub-module
with its own parser can be a *separately built shared object*, not part of the main module's
compilation unit, so an instrumented build of the main module does not cover it and a fuzzer
pointed there explores nothing. Before trusting any "no crashes," verify reach: check that the
coverage signal actually grows against the target functions (or census the instrumented symbols),
and instrument each separately built unit in its own right. The same caution applies to the
analysis interpreter — confirm the GIL is really off *after* `import` for a free-threading test,
and that the sanitizer is active on the module (a positive control), before reading a clean
result as meaningful.

## Differential testing

Run apsw against an **oracle** and diff. Two that have been fruitful:

- **apsw vs the standard library's sqlite3 / vs SQLite's own functions** — same inputs,
  compare results and which side accepts/rejects. apsw is deliberately stricter in places
  (that is expected); the interesting output is a disagreement that is *not* a known
  deliberate divergence.
- **across SQLite versions** — build the in-development SQLite and diff its accept/emit
  behaviour against the released one, then check apsw still round-trips both. This is how you
  learn whether an upcoming release forces an apsw change (for JSONB it did not: every move
  was SQLite tightening toward what apsw already did).
- **across byte order** — binary-format bugs (a field written in native order, a cast that
  assumes endianness) are invisible on the little-endian hardware everyone develops on. Run the
  suite under a **big-endian emulator** (user-mode CPU emulation needs no VM), and — sharper
  than pass/fail — do a *cross-endian differential*: encode on one endianness, decode on the
  other, and assert the serialized bytes are identical. Byte-identical output across endianness
  is strong proof the format uses a fixed layout rather than native order. A bonus catch: a
  full-suite run under emulation also flags endianness-*fragile tests* — ones that hard-code a
  cross-ISA ordering (e.g. an exact floating-point tie-break) and fail on the other byte order
  though apsw itself is correct; those are test-robustness fixes, not apsw bugs, and worth
  separating out.

## The method

1. Turn a predicted hazard into a stressor and run it under the matching sanitizer.
2. For behaviour questions, pick an oracle, generate a broad corpus (random + adversarial),
   and diff; bucket disagreements and separate known-deliberate from new.
3. Minimize any failure to the smallest input/interleaving and hand it over as a test vector.

## Tips

- A clean sanitizer run only covers the paths you drove — pair it with the stressors the
  models suggest, or it will look safe while the race sits in an untested interleaving.
- Differential testing is only as good as the oracle; be explicit about where apsw is
  *meant* to differ so those cases do not drown the signal.
