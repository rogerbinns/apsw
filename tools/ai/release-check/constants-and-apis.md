# Constants and APIs — the symbol-level diff

The mechanical half of agreement: every constant and every function SQLite defines, checked for
presence, value, and correct exposure in apsw. Derive each list from the **current** `sqlite3.h` (and
the session surface in `sqlite3.c`) and diff against **runtime** apsw — never against apsw's own
generated files. Leave the actual names and values out of any script you keep; they change every
release, so re-extract them each time.

## Constants: presence, value, and bidirectional mapping

SQLite constants are `#define SQLITE_* <value>` (and `FTS5_*`) in the headers. Three checks, in order
of value:

- **Value equality, not just presence.** For every constant SQLite defines, `import apsw` and confirm
  apsw exposes it *with the same numeric value*. A constant that is present but wrong (a digit
  transposed, a flag bit shifted, picked up from the wrong release) is worse than a missing one and a
  pure presence check misses it. Parse the value from the header (watch for expressions —
  `A | B`, `(1<<N)`, references to another constant — evaluate them, don't string-compare).
- **The `mapping_*` dictionaries must be complete and bidirectional.** apsw exposes name↔value maps
  (`mapping_open_flags`, `mapping_extended_result_codes`, `mapping_authorizer_return_codes`,
  `mapping_db_config`, the session/changegroup ones, …). For each enumerated SQLite code *family*,
  confirm the matching `mapping_*` contains every member of that family at the right value, and that
  it round-trips both directions (name→value and value→name) — apsw and its users rely on these maps
  to translate codes, so a gap is a silent wrong-answer, not a crash.
- **The reverse direction: constants apsw exposes that SQLite no longer defines.** A constant SQLite
  *removed* or *renamed* in the new release but apsw still exposes is either a back-compat decision
  (documented — e.g. a deliberately re-added one) or stale. Diff both ways and classify each extra
  into the three outcomes.

Enumerate the **families** from SQLite's own grouping (the header comment blocks and `toc.db`'s
title/group column), and check each family as a unit — open flags, result codes and extended result
codes, authorizer action and return codes, conflict-resolution modes, datatypes, config and
db-config ops, limit categories, status and db-status params, function flags, prepare flags,
file-control ops, device characteristics, access flags, serialization and deserialization flags,
checkpoint modes, trace event codes, changeset/changegroup/session ops, FTS5 tokenize reasons and
token flags, and whatever new family the release introduced. A whole new family is the highest-value
catch: it means SQLite grew a feature and apsw may not expose any of it yet.

## Functions: wrapped, or deliberately unwrapped with a live reason

SQLite's public function set is the `sqlite3_*` declarations in the header. apsw already finds
unwrapped APIs via `make unwrapped` (`tools/gensqlitedebug.py`); the independent check derives the
same list from the header and diffs against what apsw's classes actually expose at runtime, then adds
the judgement the generator cannot make:

- **New functions this release.** Every `sqlite3_*` added since the previous version is a decision:
  wrap it, or deliberately not. An un-decided new function is the gap — surface it.
- **Re-validate the deliberately-unwrapped list.** apsw intentionally does not wrap some APIs (a
  reason exists in the code or an allowlist). A function that was reasonably skipped two releases ago
  may now be important (a new mainstream API, or one a newly added feature depends on). Re-check that
  each skip's reason still holds; do not rubber-stamp the allowlist.
- **Signature / parameter drift on functions apsw *does* wrap.** SQLite sometimes adds a parameter or
  a `_v2`/`_v3` variant, or changes what an argument means. A function apsw wraps whose signature or
  contract changed is a correctness bug even though its name is unchanged — the symbol diff shows the
  name still present and green, so this must be checked against the header declaration and the
  changelog (see `changelog-and-semantics.md`), not by name alone.

## How to derive ground truth cleanly

- **Build with every extension enabled** (FTS5, RTREE, session, geopoly, column-metadata, …) or the
  constants and functions those extensions define are absent from the runtime apsw you diff against,
  and you will "confirm agreement" on a surface that simply is not compiled in — a false pass. Match
  the reflective/CI build.
- **Pin the SQLite version explicitly** and confirm `apsw.compile_options` / the reported SQLite
  version is the release you think you are checking against — not a system SQLite that leaked in.
- **Evaluate header expressions, handle conditional `#define`s.** Constants guarded by
  `#ifdef SQLITE_ENABLE_*` only exist when that feature is compiled; make sure the extraction and the
  apsw build agree on which features are on, or you will report phantom gaps.
- **Diff both directions and record values**, so the result is "apsw is missing X; apsw has Y that
  SQLite dropped; apsw's value for Z disagrees" — each a different outcome — not a bare pass/fail.
