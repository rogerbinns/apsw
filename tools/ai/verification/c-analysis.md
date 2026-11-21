# C analysis — reason about the actual code

**Good for:** bugs in the real C rather than in a model — null dereferences, use-after-free,
leaks on error paths, uninitialized reads, and (with the heavier tools) properties *proved*
about specific functions. Complements model checking (which abstracts) and pattern scanning
(which is syntactic).

## Two tiers

**Point-and-shoot static analyzers.** Clang's static analyzer (`scan-build` around the
build, or `clang --analyze`), clang-tidy, and cppcheck walk paths through the code and flag
likely defects. Low effort: wrap the normal build, read the report, triage. These are good
at the error-path family (leak/NULL-deref on an early return) and cheap to run regularly.
apsw already builds with clang available, so this is mostly free.

**Deeper reasoning.** When you want to *prove* something about a specific function — the
JSONB length/bounds arithmetic, a parser's termination, an index never going out of range —
bounded model checkers and deductive tools work on the real C: e.g. CBMC (bounded model
checking, great for "can this arithmetic overflow / can this index exceed the buffer within
N steps") or Frama-C (value analysis / weakest-precondition with annotations). More setup,
but they reason about the code the author actually ships, not a hand model.

## The method

1. Run the whole set of point-and-shoot analyzers over the build — several of them; they
   overlap, and where they agree you have confirmation, where they diverge you have a lead.
   Collect and triage all of it; keep the runs as a standing regression check.
2. For a specific worry you can phrase as "property P holds for function F on all inputs",
   reach for a bounded/deductive tool and isolate F (stub its callees). The JSONB extended-
   size decode and its bounds check are a natural first target — small, self-contained, and
   historically the site of a real overflow bug.
3. Separate *real* defects from analyzer noise by checking reachability against apsw's
   invariants (db mutex held, object not closed, etc.).

## Tips

- Analyzers drown you in the generated SQLite amalgamation if you let them — scope to
  apsw's own `src/`.
- These see the C after preprocessing, so the GIL macros and fault-injection shims are
  visible; a finding inside an `ALLOW_THREADS` region is worth extra attention because of
  concurrency, which single-threaded analyzers do not model.
- For data races specifically, a static C analyzer will not help — use the thread sanitizer
  (see `dynamic-and-differential.md`) or a model checker.
