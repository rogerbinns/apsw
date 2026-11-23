# Session and the hand-maintained exceptions — where drift hides

The generated pipeline is only as good as the files it is generated from, and a few surfaces are
**hand-maintained** because SQLite's machine-readable sources do not cover them. Those are exactly
where a new release can drift silently: the generator keeps producing confidently from a list that is
now wrong. Check each one against SQLite *directly*, not against the hand-maintained list.

## Session — omitted from `toc.db`, so filled in by hand

SQLite's `toc.db` lists its APIs and constants but **leaves out the session extension**
(`sqlite3session_*`, `sqlite3changeset_*`, `sqlite3changegroup_*`, `sqlite3rebaser_*`, and the
`SQLITE_CHANGESET_*` / `SQLITE_SESSION_*` / `SQLITE_CHANGEGROUP_*` constants). apsw fills that gap by
hand (the maintainer maintains the full session surface in `tools/tocupdate.sql`). That hand-list is
precisely the thing that goes stale when SQLite adds a session API or constant, because nothing
*generates* it from upstream.

So derive the session surface independently from SQLite itself:

- **The session API is compiled into the amalgamation** `sqlite3.c` (under `SQLITE_ENABLE_SESSION`),
  not shipped as a separate `sqlite3session.h` in apsw's tree — so extract the `sqlite3session_*` /
  `sqlite3changeset_*` / `sqlite3changegroup_*` / `sqlite3rebaser_*` declarations and the
  session/changeset/changegroup constants from there (or from SQLite's published session
  documentation), and diff against what apsw wraps and exposes at runtime.
- **Watch for the streaming (`_strm`) variants and the version suffixes** (`_v2`, `_v3`): SQLite adds
  these over time and each is a separate entry the hand-list must carry. A new `_apply_v3` or a new
  `changegroup` function is the kind of thing that appears upstream and sits unwrapped.
- **Re-validate the experimental status.** The hand-list marks some session APIs (the rebaser)
  non-experimental on purpose; if SQLite promotes or changes the experimental status of a session
  API, that hand-decision needs revisiting.

Session is the worked example the maintainer named, but the principle generalises to **any surface
SQLite does not expose in `toc.db`**: find it, and check it against SQLite's source/docs directly.

## The `tocupdate.sql` quirk-patches — re-validate each against the new release

`tools/tocupdate.sql` does not only *add* the session surface; it *patches* SQLite's own quirks —
deletes a duplicated or useless constant, re-adds a constant a release removed but apsw must keep for
back-compat, re-classifies an experimental API. **Every one of those hand-patches is an assumption
about a specific SQLite version, and a new release can invalidate it.** For each patch, independently
confirm it is still right:

- **A re-added constant** (one SQLite removed that apsw keeps for back-compat): confirm the new
  release still omits it (so the re-add is still needed) and that nothing in the new release reuses
  the name or value for a different meaning (which would make the re-add actively wrong).
- **A deleted constant** (one apsw drops as useless/duplicated): confirm the new release has not given
  it a real meaning, which would make the deletion a now-missing constant.
- **A status re-classification**: confirm SQLite has not itself changed the status, which could make
  the override redundant or contradictory.

The failure mode to catch is a patch that was correct when written and is silently wrong now — the
generated pipeline will not notice, because it trusts the patch.

## FTS5 and other extension surfaces

FTS5 (and other extensions) contribute constants and callback/registration surfaces that are
partly hand-tracked too (some FTS5 constants live in the `tocupdate.sql` additions). Check the FTS5
tokenize-reason and token-flag constants, and the FTS5 API surface apsw exposes, against SQLite's
current FTS5 definitions the same way: upstream source/docs → runtime apsw. Any extension whose
constants or entry points are enumerated by hand rather than generated from `toc.db` belongs on this
list — enumerate which surfaces are hand-maintained *this* release (it changes), and check each.

## Why this file is separate from the symbol diff

`constants-and-apis.md` diffs the surfaces `toc.db` *does* cover, where the generated pipeline is
strong and the independent check is mostly confirmation. This file is the opposite: the surfaces
`toc.db` does **not** cover, where the generated pipeline is only as current as a human's last edit,
so the independent derivation from SQLite's own source is the *primary* signal, not a cross-check.
That is where a release is most likely to have silently drifted out of agreement.
