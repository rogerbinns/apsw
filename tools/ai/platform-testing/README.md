# Platform testing for apsw

Build and run apsw **and its test suite** on as many platforms as possible — the Linux
distributions, the BSDs, Android/Termux, source-based and rolling distros, and every CPU
architecture, libc, and compiler you can reach. This directory is written for a *fresh agent*:
enough to understand *why* this matters and *how to start*, deliberately **not** a rigid
runbook. Tools, image tags, and package names drift; the principles do not.

## Why this exists: it complements megatest, it does not repeat it

apsw already has `tools/megatest.py`. Megatest varies one axis hard — **Python version ×
SQLite version × 32/64-bit × build config × GIL/no-GIL × debug** — but it runs them all on a
*single* host: one OS, one libc, one compiler lineage. That is deep coverage of apsw's own
configurability and shallow coverage of the *platform*.

The platform is exactly where the escapes happen. The motivating case is real: apsw depended
on `offsetof` being pulled in transitively by another header, and for years it was — across
**200+ CI combinations** (manylinux, six Pythons, Windows/Linux/macOS, many SQLite configs).
Then one toolchain's headers stopped including it transitively and the build broke in the
wild (conda-forge), on code that had never changed. Every one of those 200+ combinations
shared the *same compiler/libc lineage*, so none of them varied the thing that actually
broke. The author's own remedy was "do a test build on Arch." That instinct — **reach a
different toolchain** — is this whole directory.

So the division of labour is clean:

- **megatest** owns *what apsw is configured as* (Python, SQLite, bits, flags), on one platform.
- **this** owns *what apsw is built and run on* (OS, libc, compiler, arch, packaging), at a
  fixed-enough config.

Neither subsumes the other. Run both.

## The governing idea: vary the toolchain, not the version count

A hundred builds that share a compiler-and-libc family are, for the class of bug that matters
here, **one build repeated a hundred times**. Coverage of this axis is measured in *distinct
toolchains and runtimes reached*, not in count of jobs. One Alpine (musl) build and one Arch
(newest gcc) build are worth more than fifty more glibc/stable-gcc builds. Always ask of a new
target: *what does this exercise that the ones I already ran do not?* If the answer is
"nothing," it is not coverage — see `what-to-vary.md` for the axes that actually differ and
the bug class each one catches.

## Isolation: an outer container, then nested containers and VMs

Mirror how megatest contains its blast radius. Run the harness itself **inside one outer
container**, and have it spawn each platform as a **nested container** (anything with a Linux
kernel: distros, musl, cross-arch via user-mode CPU emulation) or a **nested VM** (anything
without a Linux kernel: the BSDs, Android/Termux, other kernels). A broken build, a hung
test, a misbehaving emulator, or a distro that fights back is then sealed inside a throwaway
child that is deleted on exit — it cannot touch the host or the other targets. Podman runs
rootless and nested, which makes this practical; VMs cover what containers cannot. See
`containers-and-vms.md` for the mechanics at principle level.

## Positive control: prove the harness can see a break

A platform matrix that reports "all green" is only trustworthy once you have watched it go
**red** on a break it must catch — the same discipline as the rest of `../verification/`. Use
two different plants, because they test different things:

- **To self-test the harness** (does it actually compile, run, and report failure?), plant an
  *unconditional* break — an `#error`, a call to a function that exists nowhere — that every
  toolchain must reject. If any target stays green on that, your harness is not building or not
  reporting what it thinks; fix it before believing any result. (Capture the **full** compiler
  output when you do: a failure's first `error:` line names the cause, so never reduce the build
  log to a trailing tail that throws the diagnostic away — a real lesson from getting this wrong.)
- **To test the *diversity*** (are you reaching toolchains that differ?), plant the realistic
  break — remove the `#include <stddef.h>` that fixes the `offsetof` escape. Instructive result,
  learned the hard way by running it: in the **bundled** build this plant stays **green on every
  toolchain**, because the unity build includes the whole `sqlite3.c` amalgamation first and
  *that* drags in `stddef` transitively everywhere — which is precisely why #640 escaped 200+
  homogeneous amalgamation builds. So a green-everywhere here is **not** proof your targets are
  too alike; it is the amalgamation masking the plant, and if you stop here you will wrongly
  conclude your matrix is homogeneous. To actually turn this plant red you need a translation
  unit that does **not** pull in the whole amalgamation first — the **system-SQLite build**,
  where `apsw.c` includes only `sqlite3.h` (see `system-sqlite-and-release.md`). *That* build
  reproduces #640's condition: there a leaner toolchain reddens and an older one still providing
  `stddef` transitively does not. Run the diversity plant against the system-SQLite build, not the
  bundled one.

## What "run the tests" means here

Build apsw the way users on that platform actually get it, then run the real suite
(`python -m apsw.tests`), not just an import smoke. Build it as it ships — with the extensions
enabled (FTS5, RTREE, session, …), because a default build silently omits whole modules and
leaves that surface untested (same point as `../verification/`). Three outcomes are worth
separating, every time:

1. **A build/compile failure** — the highest-value catch here (the `offsetof` class):
   missing transitive includes, a type that is a different width, a compiler that rejects what
   another accepts, a libc without some function. These are what homogeneous CI cannot see.
2. **A test failure that is a real apsw bug** — behaviour that differs on this platform because
   apsw assumed something platform-specific (endianness, `qsort` stability, locale/collation,
   float formatting, `size_t`/`long` width, filesystem semantics). apsw has had genuine
   cross-OS result divergence reported from the wild; a differential across platforms (same
   input, compare outputs) is how you find it. Cross-reference `../verification/` — the
   big-endian / cross-endian work there is one slice of this.
3. **A platform-fragile *test*** — the test hard-codes an assumption (an exact float tie-break,
   a byte order, a locale) and fails though apsw is correct. That is a test-robustness fix, not
   an apsw bug; separate it out so it does not drown the signal, and record it.

## The north star: don't surprise the packager

The single most useful thing this matrix does is make sure **the apsw about to be released will not
surprise whoever packages it** on the platform its users are about to run. That one sentence sets
every choice below:

- **Pick the release most users will soon be on — current, leaning fresh.** Not the oldest still-
  supported, not an EOL snapshot you happen to have a VM image for (a stale Vagrant box is a trap:
  it tests a version almost nobody will run the new apsw against). If users are mid-migration,
  bias to the *newer* one, because that is where they are heading and where the packaging lands.
- **Build it the *packager's* way, not the convenient way.** The bundled-amalgamation build is the
  easy path and it is the least representative — almost no distro uses it (they link system SQLite
  with `use_system_sqlite_config` and their own flags; read each platform's recipe and reproduce
  it, see `system-sqlite-and-release.md`). The packager-faithful build is the headline result; the
  bundled build is only a control to diff against. The real `DQS=0` finding came from the faithful
  build and was invisible to the bundled one — that is the whole point.

Everything else — which libcs, which toolchains — serves this: reach the platforms whose packagers
would otherwise be the first to discover a break, and discover it for them first.

## The pending-release angle

Source-based and rolling distros (Arch, Gentoo, Nix, Void, …) carry build recipes for the
*released* apsw but not yet for the one about to ship. Before a release, build the pending
version the way each of those recipes will — their flags, their newest toolchains — and you
learn whether the release will break them *before* their users do. This is the cheapest place
to catch a packaging break, and it is forward-looking in a way megatest is not.

## How to use this directory

- `what-to-vary.md` — the axes that actually differ (libc, compiler/header vintage, CPU arch &
  endianness, distro packaging, Python provenance, build hardening flags), and the concrete
  bug class each one catches. Read this to choose targets that are coverage, not noise.
- `containers-and-vms.md` — the mechanics at principle level: nested containers for Linux
  targets, cross-arch emulation, VMs for the BSDs and Termux, how to get apsw into a target
  offline — primarily as **one self-contained `.pyz`** (source + amalgamation + an
  unpack→build→test driver) the target just runs, far less friction than copying a tree — and
  what to collect back.
- `system-sqlite-and-release.md` — the real payoff: building apsw against each platform's *own*
  SQLite and config (not the bundled amalgamation), separating version skew from config skew,
  the pending-release forward-compat check against distro recipes, and fetching downloads **once**
  then distributing them so the matrix stays kind to rate-limited mirrors.

Treat both as starting points, not recipes. The reusable move: pick a platform property apsw
must not silently depend on, reach a platform where that property differs, build and test
there, and compare against a platform where it does not. Then reach the next one.
