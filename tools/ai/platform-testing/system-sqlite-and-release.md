# System SQLite, config matching, and the pending-release check

The highest-value thing this matrix proves is not just "apsw compiles here" — it is "**the apsw
about to be released still works with the SQLite this platform actually ships**." That is a
different and harder question, and it is where real-world breakage lives.

## Platforms ship their own SQLite, built their own way

Most distros do **not** use the amalgamation apsw bundles. They link apsw against **their**
`libsqlite3` — a version they chose, built with **their** compile options
(`SQLITE_ENABLE_*` / `SQLITE_OMIT_*` / threadsafe mode / default settings). Two platforms with
"the same" SQLite version can have materially different builds. apsw has to work against
whatever that is, and the pending release can regress it in two independent ways:

1. **Version skew — and this one is *policy*, not a bug.** apsw is "the Python expression of
   SQLite's C API": when SQLite adds constants/APIs, apsw adds them, and apsw **X.Y.Z requires
   SQLite ≥ X.Y** — the corresponding release or any *newer* one, never an older one. It makes
   **no effort at backwards compatibility with old SQLite** (`doc/about.rst`, "APSW and SQLite
   versions"), and enforces the floor with an `#error` in `apsw.c` (currently "at least 3.54").
   So building a pending apsw `--use-system-sqlite-config` against a distro whose `libsqlite3` is
   below that floor **fails to compile *by design*** — in one grounding run, Debian stable (3.46)
   and Alpine (3.53) failed exactly there while Fedora/Arch (3.54) built and passed. **Do not
   report that build failure as a finding; it is the documented contract.** It only tells you what
   you already know: that distro cannot adopt the new apsw until it first upgrades SQLite (which is
   *why* distros lag — apsw tracks SQLite release-for-release). The genuinely useful version-axis
   test is the inverse, below: build the SQLite the pending apsw *requires* (a release, or a
   **fossil** checkout when SQLite itself has not shipped yet) and confirm apsw works on the
   platform against **that**.
2. **Config skew.** A feature apsw assumes is present may be `SQLITE_OMIT_*`-ed out in the
   distro build, or a default apsw expects may be compiled to a different value. *Grounded
   example (this is the whole reason the dimension exists):* building apsw the FreeBSD-port way —
   `use_system_sqlite_config=True` against FreeBSD's `databases/sqlite3` port — the matched config
   carried **`SQLITE_DQS=0`** (FreeBSD compiles double-quoted string literals fully off, a
   hardening choice). The plain bundled build (SQLite's DQS default) passed 244/244; the
   config-matched build **failed one test** — `testBestPractice`, which does `execute('select
   "world"')` on a pre-bestpractice connection expecting "no error, just a deprecation log", but on
   a `DQS=0` library that raises `SQLError` immediately. apsw itself was correct (it faithfully
   surfaced the library's compile-time setting); the *test* assumed DQS-on. That is a
   platform-fragile test (outcome 3), invisible to every bundled build and every pip install, and
   surfaced only by matching a real platform's config. The lesson generalises: a one-line
   `compile_options` difference (`DQS`, an `OMIT`, a changed default) can flip a result, so the
   config-matched build is not redundant with the bundled one — run both and diff them.

   *Grounded across three BSDs (a sharp trap):* this reproduces **only** where the platform's
   **shared `libsqlite3.so`** is `DQS=0` — because that is what apsw links and what
   `use_system_sqlite_config` reads (setup.py enumerates `sqlite3_compileoption_get` on the `.so`
   with no allowlist). FreeBSD's shared lib is `DQS=0`; OpenBSD's and NetBSD's are not. On OpenBSD
   the `sqlite3` **CLI** is *statically* linked and ITS baked-in copy reports `DQS=0` via `pragma
   compile_options`, but that is irrelevant to apsw and misled a first pass into "OpenBSD is DQS=0"
   when it is not. **So determine a platform's SQLite config from the `.so` apsw will actually use**
   — load it (ctypes `sqlite3_compileoption_get`) or read `apsw.compile_options` after a
   system-config build — **never from the `sqlite3` CLI**, which can be a different build entirely.

   *Config skew can hit the BUILD too, not just behavior (grounded) — though keep the general lesson,
   not the one minor instance.* Because `set_config_from_system` copies **every** option off the `.so`
   with no allowlist, it also copies feature flags whose code needs extra **headers**: a `libsqlite3`
   built with `SQLITE_ENABLE_ICU` (seen on Void: SQLite 3.54.1, `ENABLE_ICU=True`) makes the
   config-matched amalgamation compile the ICU code, which `#include <unicode/utypes.h>` — without ICU
   dev headers the build dies `fatal error: unicode/utypes.h: No such file`. Installing `icu-devel`
   (+ `pkg-config`) fixes it, so it is a **missing build-dependency, not an apsw bug** (apsw faithfully
   honored the config). The **keeper is the general property**, which generalizes the DQS one: the
   faithful, config-matched build inherits the system SQLite's **build dependencies**, not only its
   runtime behavior, so a packager matching an extension-enabled system SQLite must also carry the
   matching `-devel` packages. The **ICU instance itself is near-theoretical and not worth acting on**:
   almost nobody enables `SQLITE_ENABLE_ICU` in SQLite, and even enabled it changes essentially nothing
   in SQL, callbacks, or apsw's API surface — and it is doubly unlikely here, needing *both*
   `use_system_sqlite_config` *and* an ICU-enabled system `.so` (among distros checked, those that use
   `use_system_sqlite_config` ship non-ICU SQLite, and the one with ICU on builds bundled). Treat it as
   a documented property of the config-matched build, not a finding to fix. (The *bundled* build has a
   mirror-image ICU wrinkle, also low-stakes: `--enable-all-extensions` self-gates ICU on whether
   `get_icu_config()` finds `icu-io` dev files, so a merely **transitive** libicu — Arch's base ships
   icu 78 — silently flips ICU *on*, and that build then fails to link under modern distro flags with
   `undefined symbol: ubrk_next_78` (`-Wl,--as-needed` drops the ICU libs). Note this is a **SQLite**
   build/link matter, not apsw: `ubrk_next` is in `sqlite3.c` under `#ifdef SQLITE_ENABLE_ICU`, and
   apsw's own source has **zero** ICU — ICU only affects the amalgamation's compile/link. So a
   platform-test harness's bundled build should **omit ICU by default** — nobody ships it and a stray
   libicu shouldn't decide the config — and exercise ICU only in an explicit, icu-dev-present mode.
   Grounded on Arch.)

Homogeneous CI (and megatest, which builds its *own* SQLite) never sees this, because it never
builds against a *foreign* SQLite it did not configure.

## How apsw already lets you match a platform's config

setup.py can read a platform's SQLite configuration directly from the installed library and
build apsw to match it — `--use-system-sqlite-config` drives a routine that `ctypes`-loads the
system `libsqlite3`, enumerates its compile options via `sqlite3_compileoption_get` (the same
list `PRAGMA compile_options` returns), and writes them into a `sqlite3config.h` that the build
then uses. So you do not have to guess a platform's config — you can extract it from the running
library and reproduce it exactly. (It also has a build mode that links against the system
`libsqlite3` itself rather than the amalgamation.) Study the current setup.py for the exact
option names; the mechanism is what matters and is stable, the flag spellings drift.

*This same convenience has a configure-time failure mode worth knowing (grounded: Gentoo #920953,
"apsw hangs on emerge").* `set_config_from_system` begins by calling
`ctypes.util.find_library("sqlite3")` to locate the `.so`. On glibc, CPython implements that by
shelling out to **`/sbin/ldconfig -p` by absolute path** (confirmed by `strace`; a `PATH` shim does
not intercept it). In an environment where `ldconfig -p` stalls — Gentoo's `emerge` sandbox is the
reported one — `find_library` blocks and the whole config-matched build hangs **at configure, before
a single file compiles**, which is exactly the reported symptom and easy to mistake for a slow build.
It reproduces cleanly by replacing `/sbin/ldconfig` with a hanging stub (the build then never
progresses; with a normal `ldconfig` the call returns in tens of milliseconds). The avoidance is that
loading the library directly — `ctypes.CDLL("libsqlite3.so.0")` — reads the same compile options with
no `ldconfig` at all, so this is a packaging-environment fragility with a clear fix surface, not an
apsw logic bug. When a `use_system_sqlite_config` build "hangs," look at its process tree for
`ldconfig` before assuming the compiler is slow.

## The two builds worth running per platform

On each platform, beyond the bundled-amalgamation build, run:

- **apsw against the platform's own `libsqlite3`, as the distro links it.** This is what the
  distro's users actually get. If the pending release breaks here, its users break on upgrade.
- **apsw against a *current* SQLite built with the platform's *own* config.** Extract the
  platform's compile options, build the SQLite apsw is about to ship with *those* options, and
  test that pairing. This isolates version skew from config skew: if the current-SQLite build
  passes but the system-SQLite build fails, the break is version skew (the platform's SQLite is
  too old); if the config-matched current build also fails, the break is in how apsw handles
  that *configuration*.

Because these targets are **disposable containers/VMs**, you can go further and simply
**overwrite the platform's system SQLite** with a config-matched current build and re-run the
suite — a destructive change that is perfectly safe in a throwaway child and tells you whether a
distro could carry the new pairing. Never do this outside a disposable target.

## The forward-looking payoff

Source-based and rolling distros (Arch, Gentoo, Nix, Void, …) have build recipes for the
*released* apsw, not the one about to ship. Run the pending version through each recipe's shape —
its flags, its SQLite, its newest toolchain — and you learn **before** release whether the
update will break that distro's packaging, while it is still cheap to fix. This is the one thing
in this whole directory that looks forward rather than back.

## Be a good citizen about downloads: fetch once, then distribute

The amalgamation and any other downloads come from mirrors with **rate limits**, and automated
agents hammering them is a real problem — do not have N containers each fetch the same file.
Fetch **once**, then distribute:

- apsw's build already **caches** every download (keyed by a hash of the URL, with a freshness
  window) under a per-user cache directory. Populate that cache **once** on the host, then share
  it into every child (mount it read-only, or copy it in, or point the cache's environment
  variable at the shared copy). Every child then gets a cache hit and never touches the network.
- Simpler still for the SQLite part: **include the already-fetched amalgamation in the source
  export** you mount into each child (this directory's `containers-and-vms.md` does exactly
  that), so the SQLite build needs no network at all.
- Only hit the network deliberately — once, to refresh the single cached copy, or when you are
  specifically testing the fetch path or pulling a SQLite version you do not yet have.

Treat "how many times did the matrix hit an upstream mirror" as a number to **minimise to one
per artifact**, not an afterthought. A matrix that is kind to mirrors can run as often as you
like; one that is not will get rate-limited and is antisocial besides.
