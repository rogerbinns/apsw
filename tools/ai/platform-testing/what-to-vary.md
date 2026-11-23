# What to vary — the axes that actually differ

Coverage on this axis is *distinct toolchains and runtimes reached*, not job count (see the
README). Below are the properties apsw can silently depend on, the kind of break each one
produces, and why a homogeneous CI never sees it. Pick targets so that each one moves at least
one of these knobs to a value the others did not — otherwise it is the same build again.

## libc — glibc vs musl vs bionic vs the BSD libcs

The single highest-value axis. The C library supplies headers, which functions exist, how
wide types are, locale and collation behaviour, and stdio/printf details.

- **musl (Alpine, Void-musl)** is the one that most often breaks glibc-only assumptions: fewer
  transitive header includes (so a missing `#include` surfaces here first — this is the
  `offsetof` class), no glibc-only functions, stricter/different locale handling, different
  `qsort` (not the same ordering for equal keys). If you reach only one non-default libc, reach
  musl.
- **bionic (Android/Termux)** is a third, different set of omissions and behaviours again, and a
  real user population. Grounded: apsw 3.54.1.0 builds clean and the full suite passes on bionic
  (via `termux/termux-docker`, Python 3.14 + clang 21, interpreter `/system/bin/linker64`), same
  skip profile as glibc — so bionic is cheap to keep as a standing target, not just a theoretical
  third libc. See `containers-and-vms.md` for the Termux harness specifics.
- **the BSD libcs** differ again and only exist on the BSDs (VM territory — see
  `containers-and-vms.md`).

What it catches: build breaks from missing transitive includes; link failures from
glibc-only symbols; runtime divergence from locale/sort/format differences. apsw has had the
*same query return different results* on Debian vs FreeBSD/OpenBSD reported from the wild — a
libc/platform behaviour difference, not a code change.

## Compiler and header vintage — the thing #640 actually turned on

The compiler and, crucially, **how recent its headers are**. Toolchains keep *removing*
unnecessary transitive includes as they improve; code that leaned on one breaks when it is
removed. This is orthogonal to libc.

- A **rolling / newest-toolchain distro (Arch, Gentoo ~stable, Fedora Rawhide)** carries the
  freshest gcc and the leanest headers — it is the one most likely to stop providing a
  transitive include you depend on. The author's own fix for the `offsetof` escape was "test
  build on Arch." Make a newest-gcc target a permanent fixture.
- An **older stable distro (Debian stable, an old manylinux)** is the opposite end and catches
  the reverse: use of a newer language/library feature the old compiler lacks.
- **A different compiler family (clang vs gcc)** rejects and warns on a different set, and its
  sanitizers/UBSan diagnostics differ. Build at least one target with clang.
- **Treat warnings as errors on at least one target** (`-Werror`, and the hardening warnings
  distros add by default — `_FORTIFY_SOURCE`, `-Wimplicit-function-declaration`). The
  `offsetof` escape was an *implicit-declaration warning* that only some builds elevated to an
  error; a target that elevates it is what turns the silent escape into a caught break.

What it catches: the `offsetof` class (missing includes surfaced by lean headers), new-feature
use that old compilers reject, undefined behaviour one compiler exploits and another does not.

## CPU architecture and endianness

Width and byte-order assumptions. apsw is C wrapping a binary-format library (SQLite, JSONB,
serialize), so this matters.

- **A big-endian arch (s390x)** catches native-byte-order assumptions in any binary format —
  this is the same concern as the cross-endian differential in `../verification/`; reaching a
  big-endian platform *runs the real suite* there instead of only emulating one call. Grounded, it
  earned its keep in an instructive way: it flagged an FTS5 ordering difference that looked like an
  endianness bug but, on running it down (diff the exact `float.hex()` values, don't trust the red),
  turned out to be a **platform-fragile test** — a `bm25 - subsequence` subtraction that is exactly
  `0.0` on x86 but a `2⁻⁵⁴` rounding residue on s390x, which reorders rows the test pinned by an
  exact-0.0 `order by` tie. Not an apsw bug and *not* byte-order (the rowids are identical crc32
  constants). So this axis flushes out both real binary-format breaks **and** tests that hard-code one
  ISA's float/ordering — report the latter as a test-robustness finding, and never promote a cross-arch
  red to "bug" without the exact values and mechanism. Full writeup: `containers-and-vms.md`.
- **A 64-bit non-x86 arch (arm64, ppc64le, riscv64)** catches `long`/`size_t`/pointer-width and
  alignment assumptions, and strict-alignment faults x86 forgives.
- Containers reach these through **user-mode CPU emulation** (one registered emulator, run the
  arm64/s390x image on an x86 host); real hardware or full-system VMs are higher fidelity but
  the emulated run already catches most width/endianness breaks. See `containers-and-vms.md`.

What it catches: sign/width bugs, alignment faults, endianness bugs in the binary formats,
`qsort`/float-format ordering that differs by arch (and so flushes out platform-fragile tests
that hard-code one ISA's ordering).

## Distro packaging and the Python it ships

How the distro *builds and bundles* Python and its tooling — this bites before apsw's own code
even compiles.

- **The distro's `python3` may omit what `setup.py` needs.** Debian's base `python3` ships
  **no `setuptools`** (and no `pip`); `setup.py` dies on `import setuptools` until you install
  `python3-setuptools`. Each distro splits this differently (`py3-setuptools`,
  `python-setuptools`, `python3-setuptools`). This *is* a platform break a user hits — catch it.
- **The distro's CFLAGS harden by default** (`_FORTIFY_SOURCE`, stack-protector, `-fPIC`,
  `-D_GLIBCXX_ASSERTIONS`, LTO). conda-forge's flags are what exposed `offsetof`. Build at
  least one target under a distro's real packaging flags, not bare `gcc`.
- **System-SQLite vs bundled.** A distro links apsw against *its* SQLite (a different version,
  with the distro's own compile options) rather than the amalgamation apsw ships. megatest has
  a `system` config for this; on the platform axis it also varies *which* SQLite.

What it catches: build-tooling breaks (the setuptools class), hardening-flag incompatibilities,
and version/option skew against a system SQLite.

## Python provenance and version — the overlap with megatest, handled

megatest already sweeps python.org CPython versions × GIL on one platform. Do **not** re-run
that sweep per distro — it is a combinatorial waste and not this axis. Instead take each
platform's **own** Python as given (distro python, Termux python, the newest a rolling distro
ships) and test against *that one*, because the distro python is built differently (its own
patches, flags, and bundled modules) from a python.org build — that difference is the coverage,
not the version number. Let megatest own the version matrix; let this own "the Python this
platform actually gives its users."

## Choosing a minimal high-value set

A small set that moves every knob beats a large set that repeats one. A good spanning set is
roughly: a **musl** target, a **newest-gcc** target, a **big-endian** target, a **clang +
-Werror** target, a **BSD** target, an **Android/Termux** target, and an **old-stable** target —
each the cheapest platform that holds that property. Add a **rolling/source-based** target for
the pending-release check. Everything beyond that is extra confirmation, which is welcome but is
no longer buying a new class.
