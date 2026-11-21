# Proof assistants / SMT — pure invariants

**Good for:** the self-contained, mostly-arithmetic parts where you want *certainty* rather
than a sampled test — above all the JSONB binary format. Decode/encode are pure functions;
their invariants (bounds, overflow-safety, endianness, round-trips, validator
soundness/completeness) can be stated and either proved for all inputs or refuted with a
witness.

**Representative tool:** Lean 4 (core library is enough for arithmetic; `omega` for linear
facts, `decide`/`native_decide` for concrete evaluation). Alternatives: Coq, Isabelle, or —
for a lighter touch — an SMT solver (Z3) driven directly or via Python bindings, which is
often faster to stand up for a single overflow/bounds question.

## The method

1. **Model the function as math.** Translate the C loop literally (e.g. the extended-size
   length accumulator `len = len*256 + byte` is a big-endian fold).
2. **State the property.** Examples that have mattered: an 8-byte length fits in 64 bits, so
   the *decode* never overflows — the hazard is in the arithmetic that *uses* it; a bounds
   check written as `offset + len` in wrapping 64-bit arithmetic is **unsound** (there is a
   witness that passes it yet points out of bounds — this is the historical C060 class);
   the subtraction form `len <= size - offset` is sound. Endianness: show the big-endian
   fold differs from the little-endian one (the latter was a real bug in another JSONB
   implementation).
3. **Prove or refute.** A proof covers all inputs; a refutation is a concrete witness you
   can turn into a test vector.

## Scope and honesty

Expect this to mostly **prove invariants hold** rather than surface new bugs — the format
has already been hammered empirically. That is still worth doing: a machine-checked
statement of *why* a fix is correct is durable, and the act of formalizing sometimes
exposes an unstated assumption. If you want bug-finding leverage here, aim at the pieces
that are easy to get subtly wrong across versions: the length/overflow arithmetic, the
depth limit (does detect use the same bound as decode?), surrogate-pair and UTF-8
validation boundaries, and encode∘decode round-trips.

## Tips

- Keep proofs dependency-light (no large math library) so a fresh agent can run a single
  file quickly.
- When a proof is fiddly, fall back to a concrete-evaluation check or an SMT query — a
  verified witness is often more useful to the author than a slick general proof.
