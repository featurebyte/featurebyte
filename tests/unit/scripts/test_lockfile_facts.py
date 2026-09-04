"""Tests for scripts/lockfile_facts.py."""

import importlib.util
import sys
from pathlib import Path

_SCRIPTS_DIR = Path(__file__).parents[3] / "scripts"
if str(_SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS_DIR))

_SCRIPT_PATH = _SCRIPTS_DIR / "lockfile_facts.py"
_spec = importlib.util.spec_from_file_location("lockfile_facts", _SCRIPT_PATH)
lockfile_facts = importlib.util.module_from_spec(_spec)
sys.modules["lockfile_facts"] = lockfile_facts
_spec.loader.exec_module(lockfile_facts)


def _lock(packages):
    return {"package": packages}


def _root(name="featurebyte", dependencies=None, optional_dependencies=None):
    return {
        "name": name,
        "version": "0.0.0",
        "source": {"editable": "."},
        "dependencies": dependencies or [],
        "optional-dependencies": optional_dependencies or {},
    }


def _pkg(name, source=None, dependencies=None):
    return {
        "name": name,
        "version": "1.0.0",
        "source": source or {"registry": "https://pypi.org/simple"},
        "dependencies": dependencies or [],
    }


def test_modified_verified_false_when_source_is_plain_registry():
    lock = _lock([_root(), _pkg("numpy")])

    facts = lockfile_facts.compute_facts(lock)

    assert facts[("numpy", "1.0.0")]["modified"] is False
    assert "plain PyPI registry" in facts[("numpy", "1.0.0")]["modified_basis"]


def test_modified_flagged_when_source_is_not_plain_registry():
    lock = _lock([_root(), _pkg("some-fork", source={"git": "https://github.com/x/y"})])

    facts = lockfile_facts.compute_facts(lock)

    assert "modified" not in facts[("some-fork", "1.0.0")]
    assert "git" in facts[("some-fork", "1.0.0")]["modified_flag_reason"]


def test_manner_direct_dependency():
    lock = _lock([_root(dependencies=[{"name": "numpy"}]), _pkg("numpy")])

    facts = lockfile_facts.compute_facts(lock)

    assert facts[("numpy", "1.0.0")]["manner"] == (
        "Direct dependency of featurebyte (see pyproject.toml); imported as a Python "
        "library and invoked via direct function/API calls at runtime."
    )


def test_manner_transitive_dependency_reached_via_direct_dep():
    lock = _lock(
        [
            _root(optional_dependencies={"server": [{"name": "weasyprint"}]}),
            _pkg("weasyprint", dependencies=[{"name": "pyphen"}]),
            _pkg("pyphen"),
        ]
    )

    facts = lockfile_facts.compute_facts(lock)

    assert facts[("pyphen", "1.0.0")]["manner"] == (
        "Transitive dependency, reached via weasyprint (a direct dependency of "
        "featurebyte); imported as a Python library and invoked via direct "
        "function/API calls at runtime."
    )


def test_manner_flag_reports_the_marker_that_actually_triggered_it():
    # the conditional marker sits on the *first* hop (pyspnego's kerberos extra),
    # not the final hop that reaches the direct dependency (requests-kerberos) -
    # the flag must report that first marker, not the unconditional final edge.
    lock = _lock(
        [
            _root(dependencies=[{"name": "requests-kerberos"}]),
            _pkg(
                "requests-kerberos",
                dependencies=[{"name": "pyspnego", "extra": ["kerberos"]}],
            ),
            {
                "name": "pyspnego",
                "version": "1.0.0",
                "source": {"registry": "https://pypi.org/simple"},
                "dependencies": [],
                "optional-dependencies": {
                    "kerberos": [{"name": "gssapi", "marker": "sys_platform != 'win32'"}]
                },
            },
            _pkg("gssapi"),
        ]
    )

    facts = lockfile_facts.compute_facts(lock)

    assert "sys_platform != 'win32'" in facts[("gssapi", "1.0.0")]["manner_flag_reason"]


def test_manner_flagged_when_reached_via_platform_conditional_marker():
    lock = _lock(
        [
            _root(dependencies=[{"name": "xgboost"}]),
            _pkg(
                "xgboost",
                dependencies=[
                    {
                        "name": "nvidia-nccl-cu12",
                        "marker": "platform_machine != 'aarch64' and sys_platform == 'linux'",
                    }
                ],
            ),
            _pkg("nvidia-nccl-cu12"),
        ]
    )

    facts = lockfile_facts.compute_facts(lock)

    assert "manner" in facts[("nvidia-nccl-cu12", "1.0.0")]
    assert "platform-conditional" in facts[("nvidia-nccl-cu12", "1.0.0")]["manner_flag_reason"]
    assert "sys_platform == 'linux'" in facts[("nvidia-nccl-cu12", "1.0.0")]["manner_flag_reason"]


def test_parse_lock_reads_real_toml_file(tmp_path):
    lock_path = tmp_path / "uv.lock"
    lock_path.write_text(
        """
[[package]]
name = "featurebyte"
version = "0.0.0"
source = { editable = "." }
dependencies = [{ name = "numpy" }]

[[package]]
name = "numpy"
version = "1.0.0"
source = { registry = "https://pypi.org/simple" }
"""
    )

    lock = lockfile_facts.parse_lock(lock_path)

    assert lockfile_facts.direct_dependency_names(lock) == {"numpy"}


def test_manner_reachable_via_dependents_activated_extra():
    # requests-kerberos (direct) depends on pyspnego[kerberos]; pyspnego's own
    # "kerberos" optional-dependencies group (not its base `dependencies`) is
    # what actually pulls in gssapi - mirrors the real featurebyte/uv.lock shape.
    lock = _lock(
        [
            _root(dependencies=[{"name": "requests-kerberos"}]),
            _pkg(
                "requests-kerberos",
                dependencies=[{"name": "pyspnego", "extra": ["kerberos"]}],
            ),
            {
                "name": "pyspnego",
                "version": "1.0.0",
                "source": {"registry": "https://pypi.org/simple"},
                "dependencies": [],
                "optional-dependencies": {"kerberos": [{"name": "gssapi"}]},
            },
            _pkg("gssapi"),
        ]
    )

    facts = lockfile_facts.compute_facts(lock)

    assert "manner_flag_reason" not in facts[("gssapi", "1.0.0")]
    assert facts[("gssapi", "1.0.0")]["manner"] == (
        "Transitive dependency, reached via requests-kerberos (a direct dependency "
        "of featurebyte); imported as a Python library and invoked via direct "
        "function/API calls at runtime."
    )


def test_compute_facts_keeps_facts_separate_for_different_versions_of_same_package():
    # uv.lock can resolve two versions of the same package for different
    # environment markers, with genuinely different `source` - keying facts by
    # name alone would let the second silently overwrite the first's facts.
    lock = _lock(
        [
            _root(dependencies=[{"name": "cffi"}]),
            {
                "name": "cffi",
                "version": "1.17.1",
                "source": {"registry": "https://pypi.org/simple"},
                "dependencies": [],
            },
            {
                "name": "cffi",
                "version": "2.1.1",
                "source": {"git": "https://github.com/x/y"},
                "dependencies": [],
            },
        ]
    )

    facts = lockfile_facts.compute_facts(lock)

    assert facts[("cffi", "1.17.1")]["modified"] is False
    assert "modified" not in facts[("cffi", "2.1.1")]
    assert "git" in facts[("cffi", "2.1.1")]["modified_flag_reason"]


def test_direct_dependency_names_includes_core_and_extras():
    lock = _lock(
        [
            _root(
                dependencies=[{"name": "numpy"}],
                optional_dependencies={"server": [{"name": "weasyprint"}]},
            ),
        ]
    )

    result = lockfile_facts.direct_dependency_names(lock)

    assert result == {"numpy", "weasyprint"}
