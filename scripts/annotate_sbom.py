"""
Annotates a CycloneDX SBOM with FeatureByte-specific OSS due-diligence data:
distribution status, manner of use, modification status, and (where the raw
package metadata is wrong or a licensing election was made) a license note.

Adds these as `featurebyte:*` properties on each component, sourced from
`oss-annotations.yaml`'s repo-wide defaults plus any named per-package
overrides.
"""

from typing import Any, Dict

import copy
import json
import sys

import yaml

import lockfile_facts
from oss_pkgname import normalize


class StaleOverrideError(ValueError):
    """Raised when oss-annotations.yaml overrides a package no longer in the SBOM."""


_FLAGGABLE_FIELDS = ("modified", "manner")


def _suppress_unresolved_defaults(fields: Dict[str, Any], computed: Dict[str, Any]) -> None:
    """Drop a bulk-default field when `computed` flagged it unresolved without a value.

    Otherwise a generic default (e.g. 'modified: false') would silently stand in
    next to a '*_flag_reason' saying the opposite: that it couldn't be verified.
    """
    for field in _FLAGGABLE_FIELDS:
        if f"{field}_flag_reason" in computed and field not in computed:
            fields.pop(field, None)


def annotate_sbom(
    sbom: Dict[str, Any],
    annotations: Dict[str, Any],
    computed: Dict[str, Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Return a copy of `sbom` with `featurebyte:*` properties added to each component.

    Per-component fields are layered, later sources winning: repo-wide
    `defaults` < `computed` facts (mechanically derived from uv.lock, e.g. by
    lockfile_facts.compute_facts) < manual `overrides` (hand-curated
    exceptions in oss-annotations.yaml).
    """
    defaults = annotations["defaults"]
    computed = computed or {}
    overrides = {
        normalize(name): fields for name, fields in annotations.get("overrides", {}).items()
    }

    result = copy.deepcopy(sbom)
    components = result.get("components", [])
    matched = set()
    for component in components:
        normalized_name = normalize(component["name"])
        component_computed = computed.get(normalized_name, {})
        fields = dict(defaults)
        _suppress_unresolved_defaults(fields, component_computed)
        fields.update(component_computed)
        if normalized_name in overrides:
            fields.update(overrides[normalized_name])
            matched.add(normalized_name)

        properties = [
            {"name": f"featurebyte:{key}", "value": _stringify(value)}
            for key, value in fields.items()
        ]
        component.setdefault("properties", []).extend(properties)

    stale = set(overrides) - matched
    if stale:
        raise StaleOverrideError(
            "oss-annotations.yaml has overrides for packages not present in the SBOM "
            f"(removed dependency, rename, or typo?): {sorted(stale)}"
        )

    return result


def _stringify(value: Any) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def main() -> None:
    sbom_path, annotations_path = sys.argv[1], sys.argv[2]
    lock_path = sys.argv[3] if len(sys.argv) > 3 else None

    with open(sbom_path, encoding="utf-8") as file:
        sbom = json.load(file)
    with open(annotations_path, encoding="utf-8") as file:
        annotations = yaml.safe_load(file)

    computed = None
    if lock_path:
        computed = lockfile_facts.compute_facts(lockfile_facts.parse_lock(lock_path))

    result = annotate_sbom(sbom, annotations, computed=computed)

    with open(sbom_path, "w", encoding="utf-8") as file:
        json.dump(result, file, indent=2)
        file.write("\n")


if __name__ == "__main__":
    main()
