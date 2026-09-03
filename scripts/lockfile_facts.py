"""
Derives (B) manner-of-use and (C) modified facts for every dependency in
uv.lock, mechanically, rather than asserting them as a bulk default:

- modified: verified False when a package's lockfile `source` is a plain
  PyPI registry entry (no git/path/url override implying a fork or patch);
  otherwise left unresolved and flagged for manual review.
- manner: derived from the dependency graph itself - "direct dependency" for
  anything featurebyte's own pyproject.toml depends on directly, or "reached
  via <chain>" for transitive dependencies, tracing the shortest path back to
  a direct dependency. Any edge on that path with a platform-conditional
  marker (sys_platform, platform_machine, platform_system) is flagged for
  manual review, since that's exactly the pattern that hid nvidia-nccl-cu12
  behind a Linux-only marker in a past review.
"""

from pathlib import Path
from typing import Any, Dict, List, Optional, Set

from collections import deque

from oss_pkgname import normalize

try:
    import tomllib
except ModuleNotFoundError:
    import tomli as tomllib

PLATFORM_MARKER_KEYWORDS = (
    "sys_platform",
    "platform_machine",
    "platform_system",
    "platform_python_implementation",
)

_DIRECT_MANNER = (
    "Direct dependency of featurebyte (see pyproject.toml); imported as a Python "
    "library and invoked via direct function/API calls at runtime."
)


def parse_lock(path: Path) -> Dict[str, Any]:
    with open(path, "rb") as file:
        return tomllib.load(file)


def direct_dependency_names(lock: Dict[str, Any]) -> Set[str]:
    """Names of featurebyte's own direct dependencies (core + all extras)."""
    root = _find_root_package(lock)

    names = {normalize(dep["name"]) for dep in root.get("dependencies", [])}
    for extra_deps in root.get("optional-dependencies", {}).values():
        names.update(normalize(dep["name"]) for dep in extra_deps)

    return names


def _find_root_package(lock: Dict[str, Any]) -> Dict[str, Any]:
    for package in lock["package"]:
        if package.get("source") == {"editable": "."}:
            return package
    raise ValueError("No editable (self) package found in uv.lock")


def _modified_fact(source: Dict[str, Any]) -> Dict[str, Any]:
    if "registry" in source:
        return {
            "modified": False,
            "modified_basis": (
                "Verified: uv.lock records this package's source as the plain PyPI "
                "registry (no git/path/url override implying a fork or local patch)."
            ),
        }
    (source_kind,) = source.keys()
    return {
        "modified_flag_reason": (
            f"uv.lock records this package's source as '{source_kind}', not the plain "
            "PyPI registry - verify by hand whether this reflects a modification."
        ),
    }


def _is_platform_conditional(marker: Optional[str]) -> bool:
    if marker is None:
        return False
    return any(keyword in marker for keyword in PLATFORM_MARKER_KEYWORDS)


def _build_reverse_graph(lock: Dict[str, Any]) -> Dict[str, List[Dict[str, Any]]]:
    """Maps a dependency's normalized name to the edges of packages that depend on it.

    Includes each package's own `optional-dependencies` groups (e.g. pyspnego's
    "kerberos" extra pulling in gssapi) as edges from that package, not just its
    base `dependencies` - a package present in the lock at all means some
    dependent activated one of its extras, so these are real edges, not
    hypothetical ones.
    """
    reverse_graph: Dict[str, List[Dict[str, Any]]] = {}
    for package in lock["package"]:
        dependent = normalize(package["name"])
        edges = list(package.get("dependencies", []))
        for extra_deps in package.get("optional-dependencies", {}).values():
            edges.extend(extra_deps)
        for dep in edges:
            reverse_graph.setdefault(normalize(dep["name"]), []).append(
                {"dependent": dependent, "marker": dep.get("marker")}
            )
    return reverse_graph


def _transitive_manner(
    name: str, direct: Set[str], reverse_graph: Dict[str, List[Dict[str, Any]]]
) -> Dict[str, str]:
    """Shortest chain from `name` back to a direct dependency, via its dependents."""
    visited = {name}
    # each queue entry carries the first platform-conditional marker seen on the
    # path so far (or None), so a flag always names the edge that actually
    # triggered it - not whichever edge happens to close out the search.
    queue = deque([(name, None)])
    while queue:
        current, triggering_marker = queue.popleft()
        for edge in sorted(reverse_graph.get(current, []), key=lambda e: e["dependent"]):
            dependent = edge["dependent"]
            edge_marker = triggering_marker
            if edge_marker is None and _is_platform_conditional(edge["marker"]):
                edge_marker = edge["marker"]
            if dependent in direct:
                result = {
                    "manner": (
                        f"Transitive dependency, reached via {dependent} (a direct "
                        "dependency of featurebyte); imported as a Python library and "
                        "invoked via direct function/API calls at runtime."
                    )
                }
                if edge_marker is not None:
                    result["manner_flag_reason"] = (
                        "Reached via a platform-conditional dependency marker "
                        f"({edge_marker!r}) - verify manner/necessity for this "
                        "platform, as with the past nvidia-nccl-cu12 finding."
                    )
                return result
            if dependent not in visited:
                visited.add(dependent)
                queue.append((dependent, edge_marker))
    return {
        "manner_flag_reason": (
            "No reachability path found back to a direct dependency of featurebyte "
            "in uv.lock - verify this dependency is actually used."
        )
    }


def compute_facts(lock: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    """Per-package (normalized name) facts mechanically derived from uv.lock."""
    direct = direct_dependency_names(lock)
    reverse_graph = _build_reverse_graph(lock)

    facts: Dict[str, Dict[str, Any]] = {}
    for package in lock["package"]:
        if package.get("source") == {"editable": "."}:
            continue
        name = normalize(package["name"])
        package_facts = _modified_fact(package["source"])
        if name in direct:
            package_facts["manner"] = _DIRECT_MANNER
        else:
            package_facts.update(_transitive_manner(name, direct, reverse_graph))
        facts[name] = package_facts
    return facts
