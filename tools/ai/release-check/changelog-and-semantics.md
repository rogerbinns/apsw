# Changelog and semantics — the agreement a symbol diff cannot see

A constant/API diff proves the *names and values* line up. It says nothing about **meaning**, and
SQLite changes meaning without changing names: a default flips, a function grows a parameter, a
pragma gains a mode, an API is deprecated, a behaviour that apsw documents or tests is altered. These
are the subtle release surprises — everything still compiles, every symbol still matches, and apsw is
nonetheless out of agreement with how the new SQLite behaves. The source for this check is prose, not
symbols: **SQLite's release notes / changelog** between the version apsw shipped against last and the
one it is about to ship against.

## Read the changelog as a checklist of required apsw responses

Walk the SQLite change history across the version gap and, for each entry, ask *does apsw need to do
anything?* — then confirm it did. The entries that matter:

- **New feature → new constants/APIs.** A new pragma, function, flag, virtual table, or option.
  Cross-reference the symbol diff (`constants-and-apis.md`): the feature's constants and entry points
  should already be flagged there, but the changelog tells you the feature *exists* and whether it is
  something apsw should surface or test, which a bare symbol list does not convey.
- **Changed default or changed behaviour.** The highest-value semantic catch: SQLite changes what a
  function returns in an edge case, flips a compile-time or runtime default, tightens parsing (the
  `DQS` double-quoted-string default is the classic example — a default that changed what a query
  does). If apsw's code, docs, or tests assume the old behaviour, that is a bug the symbol diff is
  blind to. Check apsw's tests especially: a test that hard-codes the old behaviour will either break
  (good — it caught it) or, worse, still pass against a stale expectation.
- **Deprecated or removed API.** An API apsw wraps that SQLite deprecated or removed needs a decision:
  keep wrapping with a note, or follow upstream. A removed API apsw still calls is a build or runtime
  break against the new release; a deprecated one is a future one. (This is also where a
  `tocupdate.sql` re-add originates — see `session-and-exceptions.md`.)
- **Changed signature / new `_v2`/`_v3` variant.** SQLite often adds a versioned variant rather than
  changing a function in place. Decide whether apsw should move to the new variant and whether the old
  one's behaviour shifted. The name apsw wraps is unchanged, so only the changelog (and the header
  declaration) reveals it.
- **Bug fixes that change observable results.** A SQLite bug fix can change query results, sort order,
  or error codes. If apsw has a test asserting the *old* (buggy) result, the fix will trip it; triage
  whether apsw's expectation should update to match SQLite's corrected behaviour.

## Keep it independent and version-anchored

- **Use SQLite's own release notes as ground truth**, and anchor to the exact two versions (the one
  apsw last shipped against — `doc/about.rst` / the `#error` floor in `src/apsw.c` record the minimum,
  and the previous release notes record what it shipped with — and the new target). Do not infer the
  changes from apsw's own commit history; that checks apsw against itself.
- **Confirm the apsw response at runtime or in tests**, not by reading the maintainer's generators. If
  a changed default should now be reflected, show it in `import apsw` behaviour or a test assertion,
  not in a source comment.
- **Three outcomes, as everywhere:** a real gap (apsw assumes an old default / wraps a removed API /
  lacks a test for a changed behaviour), a deliberate documented choice (apsw knowingly keeps the old
  shape — confirm the note exists and still applies), or a SQLite-side change that needs no apsw
  response (record that you checked it, so the next agent does not re-litigate it).

## The version-floor and policy cross-check

While you have both versions in hand, confirm the release's **version coupling** is self-consistent,
since it is the one place apsw's agreement with SQLite is a hard contract: the minimum SQLite in
`doc/about.rst`, the `#error` floor in `src/apsw.c`, and the SQLite the release actually bundles/tests
against must all agree, and apsw `X.Y.Z` must require SQLite `≥ X.Y`. apsw tracks SQLite
release-for-release and makes no effort at back-compat with older SQLite, so a mismatch here (a floor
not bumped when a new API was adopted, a bundled SQLite below the stated floor) is a release bug.
This complements the build-time version-skew check in `../platform-testing/system-sqlite-and-release.md`
— that one asks "does it build against each platform's SQLite"; this one asks "does the release's own
stated floor match what its code now requires."
