# AI agent audit — point capable models at the code

**This is the highest-yield tool in this directory, and it is already proven on apsw:** the
outstanding `C###` and `PY###` bug lists (hundreds of findings across the C extension and all
59 Python files) were produced exactly this way — a capable model reading the source and
reasoning about defects, usually with a primary pass and an independent validation pass per
file. Nothing mechanical in this directory has matched that breadth.

So treat the other files here as **complements to, and checks on, AI audit**, not
replacements for it.

## Getting even more out of it: direct it (on top of the broad pass)

A broad "find bugs in apsw" re-run is always worth doing — it is an independent check, it
catches regressions and newly added code, and a newer model may see what an earlier pass
missed. On top of that, *directing* agents multiplies the chance of surfacing something the
prior audits did not:

- **Aim at what a static reader does worst.** Concurrency and ordering: the GIL-release
  window (a borrowed reference whose owner is freed; a pointer/length/index derived from a
  mutable source that is invalidated while the object stays alive — different bugs, different
  fixes), close()/reentrancy/finalizers, lock discipline. These benefit from an agent asked
  to reason about *interleavings*, not just read a function top to bottom.
- **Aim across layers.** The existing audits split C and Python; bugs that live in the
  *interaction* (a Python helper's assumption about C behaviour, or vice-versa) are
  under-covered. Ask an agent to follow one feature through both layers.
- **Aim at diffs and change.** Point an agent at what changed — a new SQLite version's
  behaviour, a recent commit, a newly added API — where no prior audit exists yet.
- **Give it a new frame.** Differential/metamorphic reasoning ("where could apsw and SQLite
  disagree, and is any disagreement *not* a deliberate divergence?"), invariant-first
  ("state the invariant this function must keep, then look for a violation").

## Diversity is the lever, not a bug

Running the **same** prompt past **several** agents is a feature: different models and even
the same model on different runs interpret an open task differently and cover different
ground (one reads row-major, another column-major). Do not over-constrain the prompt to force
one reading — spread a few framings and pool the results. Vague, suggestive prompts beat rigid
checklists here, and let better models (which keep arriving) do what they do best.

## Pair it with the dispositive tools

AI audit *proposes*; it also hallucinates plausible-but-wrong findings. The mechanical tools
in this directory are how you *dispose*:

- A claimed bounds/overflow or logic bug → turn it into a **proof or a refuting witness**
  (proof assistants / SMT) or a **crashing input** (fuzzing).
- A claimed race or lifecycle bug → turn it into a **model-checker counterexample** or a
  **sanitizer trace under a stressor**.
- A claimed leak/UAF → confirm under **ASAN / valgrind / a refcount check**.

Conversely, AI is the best tool for *triaging* mechanical output — explaining whether a
static-analyzer or type-checker hit is real, and cross-referencing it against the known
lists so you only report what is new.

## Caveats — not reasons to hold back

- Run it broadly *and* directed. Even on already-audited code a re-run is worth it: it is an
  independent check, it catches regressions and newly added code, and a fresh (often better)
  model may see what an earlier pass missed. Directing it (above) raises the odds of something
  new; do not skip the broad pass to "save" anything — compute is free, and diversity across
  many agents and framings is cheap coverage.
- The one real discipline is on the *output*, not the effort: every finding needs
  verification before it is reported — pair it with a dispositive tool or a written repro; do
  not forward raw AI claims as confirmed bugs. Hallucinated-but-plausible findings are the
  failure mode to guard against, which is exactly what the dispositive tools above are for.
