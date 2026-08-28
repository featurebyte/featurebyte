# Open Source / Third-Party Dependency License Policy

This document describes FeatureByte's policy for consuming third-party open-source
and free software dependencies in this repository.

## Scope

This policy applies to all third-party dependencies declared in `pyproject.toml`
and resolved in `uv.lock` — both the base package and its optional extras
(e.g. `featurebyte[server]`).

## Accepted licenses

Dependencies must be licensed under a permissive license. The current
authoritative allow-list is the `PERMISSIVE_LICENSES` value in
`Taskfile.lint.yaml`'s `licenses` task, which is enforced automatically in CI
on every pull request. As of this writing it includes (non-exhaustively):
Apache-2.0, BSD (2- and 3-clause), MIT, ISC, MPL-2.0, HPND, Python Software
Foundation License, Zope Public License, and Public Domain / Unlicense.

Strong-copyleft licenses (GPL, LGPL, AGPL) and any license not on the
allow-list are **not** accepted without explicit legal review and an
explicit, documented exception.

## Enforcement

- `task lint:licenses` runs `pip-licenses --allow-only=...` against the
  synced dependency environment and fails the build if any dependency's
  license is not on the allow-list. This runs on every pull request
  (`task lint:pr`), not just after merge, so a non-permissive dependency
  cannot land without being caught first.
- `task lint:safety` runs `pip-audit` (vulnerability, not license, scanning)
  on pushes to `main`.
- `task lint:sbom` generates a CycloneDX software bill of materials
  (`sbom.json`) for the distributed package's runtime + extras dependency
  tree. `task lint:notice` generates `THIRD-PARTY-NOTICES.txt`, attributing
  dependency licenses and authors. Both run on pushes to `main` and are
  published as CI build artifacts (see `.github/workflows/lint.yaml`) rather
  than committed to the repo, so they stay current with the lockfile instead
  of going stale between updates.

## Exceptions

If a dependency's license does not fit the allow-list but is needed, get
explicit legal sign-off before adding it, then add it to
`PERMISSIVE_LICENSES` (or `--ignore-packages`, if the dependency is
first-party or the classification is a known metadata error) with a comment
explaining the reasoning and who approved it — see the existing comments in
`Taskfile.lint.yaml`'s `licenses` task for the expected format.

## Resolved findings

This is the authoritative record for each ignored/overridden package below —
code comments in `Taskfile.lint.yaml`/`pyproject.toml` point here rather than
repeating this reasoning, to avoid the two drifting out of sync.

- **`pyphen`** (via `weasyprint`, used only for the Feature Job Setting
  Analysis PDF-export endpoint): tri-licensed GPL 2.0+ / LGPL 2.1+ / MPL 1.1
  — a genuine choice per its own bundled `LICENSE` file, not a compound
  requirement. Elected to consume it under LGPL 2.1+ / MPL 1.1 (not the GPL
  option), which is permitted for closed-source commercial use. The only
  hyphenation dictionary actually relevant here (`hyph_en_US.dic`) is
  separately BSD-style licensed per its own README, independent of
  `pyphen`'s tri-license. Re-check if additional non-English dictionaries
  are ever bundled/used — `pyphen`'s dictionaries come from LibreOffice and
  are not uniformly available under all three of GPL/LGPL/MPL. Ignored via
  `--ignore-packages` in the `licenses` task.
- **`rfc3987`** (GPLv3+, via `feast`'s `jsonschema[format]` requirement,
  part of `featurebyte[server]`'s runtime dependency graph — this shipped
  to anyone installing the `server` extra, and was undetected until the
  license gate was made to actually work): removed from the resolved graph
  entirely via `[tool.uv] override-dependencies` in `pyproject.toml`, which
  forces `jsonschema` to resolve without the `format` extra anywhere in the
  graph. Verified neither `feast` nor `featurebyte` uses
  `jsonschema.FormatChecker`, so this is not expected to be a functional
  regression — re-verify if `feast` is ever upgraded.
- **`chardet`** (LGPLv2+, transitive dependency of `cyclonedx-bom`, the
  dev-only SBOM tool — never distributed to customers): ignored via
  `--ignore-packages`.
- **`gssapi`**: not a real violation — its package metadata is broken
  (`License: LICENSE.txt`, a filename instead of an identifier), but the
  bundled `LICENSE.txt` is verbatim ISC License text, verified by reading
  it directly. Ignored via `--ignore-packages`.
- **`future`** (1.0.0, via `pyhive`, part of `featurebyte[server]`'s runtime
  dependency graph): package-level metadata is MIT and passes `task
  lint:licenses` without an ignore entry, but one bundled file,
  `future/backports/urllib/robotparser.py` (also mirrored at
  `future/moves/urllib/robotparser.py`), is separately dual-licensed
  GPL-2.0-only / PSF (Python-2.2) — `pip-licenses` reports only the
  package-level classifier and does not see this file-level fact. Elected
  the PSF/Python-2.2 option over GPLv2, per the choice the file's own
  header offers.

  Verified segregable in practice, not just "not imported by first-party
  code": `robotparser.py` only loads if something calls
  `future.standard_library.install_aliases()` (the only code path in
  `future/standard_library/__init__.py` that imports
  `future.backports.urllib.robotparser`). `pyhive` (the only reason
  `future` is a dependency at all) imports only `future.utils.iteritems`
  and `future.utils.with_metaclass` — never `standard_library` or
  `install_aliases`. Grepped the entire installed dependency tree for
  `install_aliases`: the only other hits are an unrelated same-named local
  variable in `jupyterlab/labextensions.py` (a dict of CLI flag aliases,
  nothing to do with `future`) and `libfuturize` (future's own bundled
  2-to-3 code-migration CLI tool, never invoked at runtime). Nothing in
  either resolved dependency graph (checked in both `featurebyte`'s
  Python 3.12 environment, `future` 1.0.0, and `featurebyte-app`'s Python
  3.10 environment, `future` 0.18.3) ever calls `install_aliases()`, so
  `robotparser.py` is present on disk but never actually executed by
  anything in Falcon's shipped build.

  Re-check both the election and this segregability finding if `future`
  is ever bumped or if `pyhive` starts using `future.standard_library`,
  since the file's license terms and its trigger condition are pinned to
  the versions checked here.
- **`autocommand`** (2.2.2, LGPL-3.0, vendored inside `setuptools`'s own
  `_vendor` bundle at `setuptools/_vendor/autocommand`): reached via
  `celerybeat-mongo`, a runtime dependency of `featurebyte[server]`, which
  depends on `setuptools`. It is not independently installed as its own
  top-level distribution (confirmed: `pip-licenses --packages autocommand`
  returns nothing), so it is invisible to `task lint:licenses` entirely —
  not a case of passing the gate on coarse metadata like `future`/`pillow`
  above, but of not being seen by the gate at all, since it's nested
  inside another package's vendored bundle rather than its own installed
  distribution. It does reach `docker/Dockerfile`'s `uv sync --all-groups
  --all-extras` build (the self-hostable SDK image), so it is a real,
  shipped dependency, not merely a build-time artifact. No first-party
  code imports `setuptools`/`pkg_resources`, and `autocommand` is pure
  Python with no compiled component, so it satisfies LGPL's
  source-availability-without-relinking case as shipped. Accepted as
  documented here rather than via `--ignore-packages`, since standard
  license-scanning tooling has no visibility into nested vendored
  sub-packages to ignore in the first place — re-check if `setuptools` is
  ever upgraded to a version that no longer vendors `autocommand`, or that
  vendors a different version of it.

  **Update (2026-08-28):** confirmed this SDK's Docker image (built from
  this repo's own `docker/Dockerfile`) is the same one distributed to
  on-prem customers who self-host the entire FeatureByte application - not
  merely a hypothetical self-hostable offering, following the same
  correction made to `frontend`'s `sharp-libvips` entry and to the
  MongoDB/SSPL position in `featurebyte-app`. Unlike those two, no
  additional notice-in-image fix is needed here: `docker/Dockerfile`'s
  runner stage does `COPY --from=builder /app/.venv /app/.venv`, copying
  the entire virtual environment wholesale - including every installed
  package's own bundled license files, such as
  `setuptools/_vendor/autocommand-2.2.2.dist-info/LICENSE`. That file is
  already present in the shipped image today, as a structural consequence
  of how this repo packages its Docker image (copy the whole `.venv`,
  unlike `frontend`'s Next.js build, which tree-shakes `node_modules` down
  to `.next/standalone` output and drops raw package license files in the
  process). No engineering fix required; this is lower-risk than it first
  appeared once actually checked against the real distribution model.
- **`pillow`** (12.3.0, direct runtime dependency): package-level metadata
  is `MIT-CMU` and passes `task lint:licenses`, but Pillow optionally
  bundles compiled-in codecs under several other licenses depending on the
  build. Verified via the actual linked libraries in the installed wheel
  (`otool -L` / `.dylibs` inventory) that this is the standard, unmodified
  official PyPI wheel: the GPL-3.0-or-later codec (`libimagequant`) and the
  LGPL-2.1-or-later codec (`fribidi`) are both **absent** — not merely
  unused, not bundled at all. FreeType *is* linked and is dual-licensed
  FTL / GPL-2.0-or-later; the official wheel builds it under the FTL
  option (the permissive one), which we are electing here explicitly for
  the first time — this was previously true by default but undocumented.
  Re-verify this election (and re-run the `otool -L`/`ldd` check) if
  `pillow` is ever built from source rather than installed from the
  official wheel, since a source build could pull in different optional
  codecs.

## Known open items

- No CLA / IP-assignment process currently exists for external contributions
  to this repository, despite `CONTRIBUTING.md` accepting external PRs.
