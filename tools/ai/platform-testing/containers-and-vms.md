# Containers and VMs — the mechanics, at principle level

How to actually reach each platform, contain the blast radius, get apsw in, build and test, and
collect the result. Specifics (image tags, package names, emulator names) drift and are given
only as illustration — the shape is what lasts.

## The isolation shape: outer container → nested children

Run the harness itself inside **one outer container**, and have it launch each target as a
throwaway child (`--rm`, deleted on exit). A broken build, a wedged test, a runaway emulator,
or a distro that corrupts its own package DB is then sealed inside a child that disappears —
nothing reaches the host or the sibling targets. This is the same containment megatest relies
on. Rootless, nested podman makes it work on a plain machine; it pulls images, runs
`--platform` emulated images, and nests without privilege. Children run in parallel — the
machine is big; cap concurrency by RAM/disk, not politeness.

## Two kinds of target

- **Anything with a Linux kernel → a nested container.** Every Linux distro (glibc and musl),
  and every CPU arch via user-mode emulation, is a container. Cheap, fast, parallel.
- **Anything without a Linux kernel → a nested VM.** The BSDs (FreeBSD/OpenBSD/NetBSD) and
  Android/Termux and any other-kernel target cannot be a Linux container — they need a VM
  (a full-system emulator, or a VM manager driving one). Slower and heavier; reserve them for
  the platforms containers cannot express, and keep one of each as a standing target rather
  than a broad sweep.

## Getting apsw into a target (offline, reproducibly)

- **One file beats many: hand the target a single self-contained `.pyz` — this is the primary way
  in.** Python runs a zip that has a `__main__.py`, so you can pack the source + the SQLite
  amalgamation + an unpack→build→run-the-suite driver into **one** file and `scp`/mount just that —
  then `python3 apsw-harness.pyz` (or `… system-config`) prints a one-line verdict. One file is far
  less friction than rsyncing a tree, and it is the natural shape for the **VM / bare-metal** targets
  (BSDs, Termux) where handing over one file is easy and a shared read-only tree is not. (This is the
  delivery megatest would use if it were written today: megatest copies a whole tree around because
  zipapp wasn't a tool reached for in its Python-2 era, not because copying is better — so prefer the
  `.pyz` and treat the tree-mount below as the fallback.) Pack **exactly `git ls-files`**, not the
  working tree: that is the "what ships" set, so a forgotten `git add` or an accidental dependence on
  an untracked working-tree file surfaces as a *build/test failure on the target* instead of a silent
  pass (the same reason megatest exports from git), and it inherently excludes the `*.so` /
  `*.sqlext` / `sqlite_cfg.h` / `build/` artifacts the "export only what ships" trap below warns
  about, because those are untracked. The **amalgamation is the one deliberate exception** — apsw
  fetches it, so it is *not* git-tracked; add it explicitly and label it, or the bundled build has no
  SQLite to compile. Grounded: a 4.9 MB `.pyz` built this way did unpack→bundled build→`Ran 244 OK
  (skipped=10)` in a bare `debian:bookworm-slim` with nothing but `python3`/`gcc`/`setuptools`
  present. Builder + driver: `~/apsw-ref/platform-testing/pyz/`.
- **The fallback — mount the source tree read-only.** Where a shared read-only mount *is* convenient
  (a Linux container on the same host) and you would rather not build a `.pyz`, export the tree once
  (the git-tracked files, the way megatest's copy step does) into a directory and mount it
  **read-only** into each child; the child copies it to a writable workdir and builds there.
  Read-only mount + copy means a child can never mutate your source, and every child builds the
  identical tree. Two traps apply to this path that the `.pyz`'s `git ls-files` set handles for you:
  - **Include the SQLite amalgamation in the export** so children need no network for the SQLite
    part — more reproducible and it works on an air-gapped or flaky-network target. apsw's build
    uses a present `sqlite3/` directly; only fall back to `setup.py fetch` (network) when you
    deliberately want to test the fetch path or a specific SQLite version.
  - **Export only what *ships* — a tree configured on one host carries artifacts that break another
    libc.** A real trap from grounding this: a `sqlite3/sqlite_cfg.h` left behind by a glibc
    `./configure` run defined `HAVE_PREAD64` / large-file support, and SQLite's
    `-D_HAVE_SQLITE_CONFIG_H=1` then forced those *on* inside a **musl** build, where `pread64` /
    `off64_t` are not declared — so the amalgamation build died with `'pread64' undeclared` on
    Alpine while every glibc target built clean. That is a *harness* artifact, not an apsw bug (the
    shipped amalgamation carries no such file); strip host-generated config headers from the export
    or you will manufacture cross-libc failures that look exactly like real findings. General rule:
    when a build breaks on one libc and not another, rule out your own export before believing it.
    A second instance of the same class, grounded on NetBSD: a stale **`testextension.sqlext`** (a
    prebuilt *Linux* shared object left in the host tree) rode along in the export. apsw's suite runs
    its optional load-extension test *iff that file exists*, so on NetBSD it tried to load a foreign
    binary and reported three errors that are not apsw bugs. Fix: strip build/test artifacts
    (`*.sqlext`, `*.so`, `testextension.*`, `build/`) from the export, or rebuild them natively on the
    target (`setup.py build_test_extension`) before running — and, again, suspect your own export
    first when a failure appears on exactly one platform.
- **For the pending-release / packaging check**, instead build the way the *distro's recipe*
  will: its sdist/wheel path, its flags, its system SQLite — that is what tells you whether the
  release will break that distro's packaging.

## Per-target recipe (the shape, not a script)

Inside each child, in order: **install the platform's toolchain and Python**, then **build apsw
as it ships**, then **run the real suite**. Each step has platform-specific traps worth knowing
because they *are* findings:

- **Install** varies per package manager and the invocation differs (`apt-get` / `apk` / `dnf` /
  `pacman` / `zypper` / `emerge` / `pkg`). Two traps that bite immediately: the shell
  (minimal images are **busybox `sh`, not bash** — write POSIX `sh`), and the Python tooling
  (the distro `python3` **may lack `setuptools`/`pip`** — install the distro's setuptools
  package or `setup.py` fails before touching apsw). These are real user-facing breaks; don't
  paper over them, record them.
- **Build** apsw with the extensions enabled (as `../verification/` insists), under the
  platform's real compiler and, on at least some targets, its hardening flags and
  warnings-as-errors — that elevation is what catches the `offsetof` class.
- **Test** by running `python -m apsw.tests` (the real suite), not an import smoke. Run it from
  a directory where the freshly built in-place extension is the one imported, and *not* from a
  directory whose contents shadow the `apsw` package on `sys.path` — a wrong-cwd import silently
  loads the source package without the compiled module and "passes" while testing nothing. Put
  a bound (timeout) on the run so one wedged target cannot stall the matrix.

## Cross-architecture by emulation

To run an arm64 / s390x / ppc64le / riscv64 image on an x86 host, register a user-mode CPU
emulator once (the usual binfmt approach), then launch the foreign-arch image like any other
container (`--platform` selects it). This is enough to catch width, alignment, and endianness
breaks and to run the suite big-endian — the same concern as the cross-endian differential in
`../verification/`, but running the *whole* suite rather than one call. Emulation is slow and
not perfectly faithful (timing, some syscalls); where fidelity matters, a full-system VM or real
hardware is better, but the emulated run already pays for itself. Confirm the emulator is
actually registered before trusting a foreign-arch "pass" — an unregistered arch silently falls
back or refuses, which is a hollow green.

Two traps grounding this surfaced. First, pulling a foreign-arch image with `--platform` can
**overwrite your local image tag** with the other arch's image (the tag is keyed by name, not by
arch), so a later run of the "same" tag silently executes emulated/big-endian and is mysteriously
~20× slower — pull foreign-arch images under a distinct name, or re-pull the native tag
afterwards. Second, emulation already earns its keep, and a grounded s390x run shows both the value
of the axis **and** the discipline it demands. A big-endian run flagged an FTS5 test
(`ftstests.FTS5Aux.testSubsequence`) whose result ordering differed from x86 — a plausible
class-2 endianness break. The right move is to **treat it as a candidate and run it down**, not to
report it. Diffing the full-precision values (print `float.hex()` on both arches, don't eyeball)
showed the real cause: the test orders rows by `bm25(yes) - subsequence(yes)` where several rows are
an **exact 0.0 float tie** on x86, broken by the rowid; on s390x that subtraction yields
`5.55e-17` (one IEEE-754 rounding residue, 2⁻⁵⁴) instead of exactly 0.0, so `order by boost desc`
reorders the tied rows and the test's hardcoded rowid order fails (`assertAlmostEqual(boost)` still
passes — only the rowid-order assertion fails). So it is a **platform-fragile test** (outcome 3, the
same class as the DQS case), **not** an endianness bug — and specifically **not** a byte-order
problem: the rowids are `zlib.crc32` constants, bit-identical on every arch. An earlier pass had
guessed "a rowid above 2³¹ sorts differently by byte order"; that guess was **wrong**, and only the
diff of exact values caught it. Lessons that generalize: (1) a cross-arch result difference is a
*candidate* until you have the exact values and the mechanism; emulators are imperfect on timing and
syscalls (a few extension loads failed under qemu that passed natively), so confirm by source/values,
not by the red alone; (2) tests that lean on *exact* floating-point equality to make an `order by`
tie deterministic are inherently platform-fragile — the fix is to quantize/epsilon the sort key or
compare equal-rank rows unordered, and that is a finding worth handing back even though it is not an
apsw bug.

## The BSDs and Termux

- **BSDs** (FreeBSD, OpenBSD, NetBSD): VM only (needs `/dev/kvm` for usable speed). They matter
  because their libc and kernel differ from every Linux target, and apsw has had genuine cross-OS
  result divergence between Linux and the BSDs reported from the wild — so run the suite there and
  **differential** the results against a Linux target (a disagreement that is not a documented
  deliberate difference is a finding). *Proven path (grounded):* FreeBSD publishes a
  **cloud-init `qcow2`** image — boot it headless under qemu+KVM, ssh in over a `hostfwd` port,
  install the toolchain + python with the cloud-init **`packages:`** list (not `runcmd`, which
  silently didn't run on the image tried; and skip `growpart`, which stalled the final stage), and
  build+test unprivileged as the default user. FreeBSD 15 built clean (clang) and passed the full
  suite. OpenBSD/NetBSD ship **no cloud image**, so they need a **fresh install driven over the
  serial console** — proven for both current releases, with these grounded specifics:
  - *Detach everything with `setsid`*, not just `nohup`. A plain `nohup … &` background qemu/driver
    gets reaped when the launching tool-shell's process group is torn down; `setsid` gives it its
    own session so it survives across turns. And drive the install from your **main** loop (a
    background wait-for-marker), never from a one-shot sub-agent — a sub-agent cannot babysit a
    multi-turn install and will resume and relaunch a *second* qemu onto the same files, and the
    two fight over one VM.
  - *OpenBSD* has a line-oriented installer: switch the loader console to the serial port first
    (type `set tty com0` into the VGA keyboard via the qemu monitor `sendkey`), then answer prompts
    by matching **only the current prompt line** (a scrollback window still holds earlier prompts
    and will mis-match). Serve the sets you fetched once over `http://10.0.2.2/…` (qemu slirp).
  - *NetBSD* uses the **curses** `sysinst`; drive it by reference-letter + Enter, reading the
    screen after each key. Its `boot-com.iso` is **boot-only** (a ramdisk with no sets), so install
    the sets over **HTTP from the CDN** (slirp even routes IPv6) or from a local mirror; choose MBR
    + **serial com0 bootblocks** so the installed system is reachable on the serial console.
  - Post-install, log in as root on the serial console once to inject an ssh key (and `doas`/enable
    root ssh), then do the build and test over ssh where output is clean.
- **Build it the platform's own way, not just bundled.** The highest-value BSD result came from the
  *faithful* build — the way the OS port builds it (`use_system_sqlite_config` against the port's
  SQLite, the port's flags), not the bundled amalgamation. That is what surfaced the `DQS=0`
  config-skew test failure (see `system-sqlite-and-release.md`); the bundled build was green and
  hid it. Read the platform's port recipe (its `Makefile`/`PKGBUILD`/`spec`) and reproduce its
  method; a background sweep of those recipes also tells you which platforms carry patches worth
  reproducing first.
- **Termux / Android (bionic libc)** — *proven path (grounded)*: a third libc (after glibc and musl)
  and a constrained userland, worth doing because real users run apsw there. The
  `termux/termux-docker` image is a genuine Termux environment — `uname` reports `Android`, the
  prefix is `/data/data/com.termux/files/usr`, and the Python binary's program interpreter is
  `/system/bin/linker64` (real bionic, verify that, not just `uname`). `pkg install python clang
  libsqlite` gives a current toolchain (seen: Python 3.14, clang 21); the bundled build compiles
  clean and the full suite passes with the same skip profile as glibc (`OK (skipped=10)`), no apsw
  bionic-portability break. Termux-specific harness traps (not apsw): there is **no `/tmp`** (write to
  `$TMPDIR`), **`/` is not writable** (build under `$HOME/work`), and it runs as a non-root uid so a
  few mode-0600 files in a root-owned source export are unreadable to the copy (harmless — they are
  docs/signing artifacts, not build inputs). A full-system Android VM is heavier and only needed if
  you must exercise the Android *framework*; for the libc/toolchain axis the container is enough.

## What to collect, and how to read it

Capture per target: the platform identity (OS, libc + version, compiler + version, arch, Python
version and provenance), and the three-way outcome from the README — **build failure** /
**real test failure** / **platform-fragile test**. A build failure on one toolchain and not
another is the highest-value result; keep the failing compiler output verbatim (it names the
missing include/symbol). For a test failure, minimise to the smallest input and differential it
against a passing platform to decide which of the three it is. A clean sweep is only meaningful
once the positive control (README) has shown the matrix can go red — report the planted-break
result alongside the green, or the green means little.
