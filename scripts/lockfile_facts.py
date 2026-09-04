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
  manual review, since a platform-restricted marker can obscure why a
  dependency is present at all.

Flag-reason text in this module is written to be readable by an external
reader (e.g. as part of due-diligence materials shared outside the company):
it states what was and wasn't mechanically verified, without referencing
internal incidents, tools, or people.
"""

from collections import deque
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

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
        data: Dict[str, Any] = tomllib.load(file)
        return data


def direct_dependency_names(lock: Dict[str, Any]) -> Set[str]:
    """Names of featurebyte's own direct dependencies (core + all extras)."""
    root = _find_root_package(lock)

    names = {normalize(dep["name"]) for dep in root.get("dependencies", [])}
    for extra_deps in root.get("optional-dependencies", {}).values():
        names.update(normalize(dep["name"]) for dep in extra_deps)

    return names


def _find_root_package(lock: Dict[str, Any]) -> Dict[str, Any]:
    packages: List[Dict[str, Any]] = lock["package"]
    for package in packages:
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
            "PyPI registry. Whether this reflects a modification from the upstream "
            "release has not been independently verified."
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
        for edge in sorted(reverse_graph.get(current, []), key=lambda e: str(e["dependent"])):
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
                        f"({edge_marker!r}); its necessity on that platform has not "
                        "been independently verified."
                    )
                return result
            if dependent not in visited:
                visited.add(dependent)
                queue.append((dependent, edge_marker))
    return {
        "manner_flag_reason": (
            "No reachability path was found back to a direct dependency of "
            "featurebyte in uv.lock; its continued use has not been independently "
            "verified."
        )
    }


def compute_facts(lock: Dict[str, Any]) -> Dict[Tuple[str, str], Dict[str, Any]]:
    """Per-(normalized name, version) facts mechanically derived from uv.lock.

    Keyed by version as well as name: uv.lock can resolve more than one version
    of the same package for different environment markers, and their `source`
    (hence `modified`) can legitimately differ between them - keying by name
    alone would let one version's facts silently overwrite another's.
    """
    direct = direct_dependency_names(lock)
    reverse_graph = _build_reverse_graph(lock)

    facts: Dict[Tuple[str, str], Dict[str, Any]] = {}
    for package in lock["package"]:
        if package.get("source") == {"editable": "."}:
            continue
        name = normalize(package["name"])
        package_facts = _modified_fact(package["source"])
        if name in direct:
            package_facts["manner"] = _DIRECT_MANNER
        else:
            package_facts.update(_transitive_manner(name, direct, reverse_graph))
        facts[(name, package["version"])] = package_facts
    return facts
