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

## Known open items

- `pyphen` (pulled in transitively via `weasyprint`, used only for the
  Feature Job Setting Analysis PDF-export endpoint) is triple-licensed
  GPLv2+ / LGPLv2+ / MPL-1.1. This is pending legal review to either
  formally elect the MPL-1.1 option or replace the dependency.
- **Resolved:** `rfc3987` (GPLv3+) used to be pulled in transitively via
  `feast`'s use of `jsonschema[format]` - and `feast` is part of
  `featurebyte[server]`'s runtime dependency graph, meaning it shipped to
  anyone who installed the `server` extra. This was undetected for an
  unknown period, since it was masked first by the license gate never
  having actually run at all, and then by `pip-licenses --allow-only`
  being fail-fast (reports only the first violation found, not all of
  them). Fixed via `[tool.uv] override-dependencies` in `pyproject.toml`,
  forcing `jsonschema` to resolve without the `format` extra anywhere in
  the graph - verified no code in `feast` or `featurebyte` itself actually
  uses `jsonschema.FormatChecker`, so this is not expected to be a
  functional regression, but re-verify if `feast` is ever upgraded.
- No CLA / IP-assignment process currently exists for external contributions
  to this repository, despite `CONTRIBUTING.md` accepting external PRs.
