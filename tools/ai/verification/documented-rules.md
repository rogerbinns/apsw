# SQLite documented-rules enforcement — make the prose-only rules safe

SQLite has safety **rules that exist only in its documentation**: "don't use the destination
connection during a backup", "don't call back into SQLite from this callback", "this pointer
is invalid after the next step", "these objects aren't re-entrant", and so on. Break one and
you get **corruption or undefined behavior with no error** — the engine assumes the
application obeyed the rule (drh's "the application breaks the rules"). The rule set also
**drifts**: new APIs add rules, existing ones change across versions.

apsw's value is to not leave the user alone with that prose. For every such rule apsw should
either:
- **Enforce it in code** — detect the violation and raise a clean exception instead of
  letting corruption happen (the strong form); or
- **Document it** — mirror SQLite's warning in apsw's own docs for cases where enforcement is
  impractical (the fallback, for obscure/hard-to-detect rules).

A rule that is *neither* enforced nor documented is a gap — that is the thing to find.

## The method (re-run per SQLite version — the rules drift)

1. **Mine the SQLite docs for rules.** The SQLite documentation ships in the apsw tree (a
   `sqlite-doc-*.zip`). Extract it and scan for the signal phrases that mark a rule —
   "corrupt", "undefined behavior/result", "must not", "it is an error", "do not call",
   "not safe", "re-entran(t/cy)", "while … is in progress", "cannot/​may not be used". The
   C-API reference pages and the hook/callback pages carry the densest clusters.
2. **For each rule, record:** the API it governs, the consequence (corruption / UB / silent
   wrong result / clean error), and whether apsw **enforces**, **documents**, or **neither**.
3. **Close the gaps.** Where apsw can cheaply detect the violation, prefer code enforcement;
   otherwise add the documented warning. Enforcement that relies on the GIL is suspect — see
   the free-threading notes; prefer a check that holds without it.
4. **Keep it as a standing catalog** and re-derive it each SQLite upgrade, diffing against
   the previous version's rules to catch newly-added ones.

## What apsw already does (so you can see all three categories)

- **Enforces:** use-after-close (closed-checks + the dependents mechanism), cursor
  re-entrancy (the in-query flag → `ThreadingViolationError`), cross-thread use (the db-mutex
  + `ThreadingViolationError`), column/value pointer lifetime (converts to Python immediately,
  so the "pointer invalid after next step" rule can't bite the user).
- **Documents:** callback re-entrancy ("Calling back into SQLite can result in crashes,
  corrupt databases, or worse" on the autovacuum-pages callback), the VFS contract ("will
  result in memory corruption and crashes"), session-object non-re-entrancy.
- **Gap (example):** the backup rule — the destination connection must not be in use (e.g.
  have open read statements) during a backup; apsw guards only cross-thread busy, matching
  SQLite, which only rejects an open *transaction*. This is exactly the class that produced a
  real corruption report.

## Caveats

- A code check must raise *before* the unsafe call reaches SQLite, and must itself be correct
  under concurrency (don't trade a documented corruption for a racy check).
- Over-enforcement that rejects *legal* use is its own bug — enforce the rule as documented,
  no stricter, and test that legal patterns still work.
