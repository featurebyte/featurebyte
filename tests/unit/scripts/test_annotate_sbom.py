"""Tests for scripts/annotate_sbom.py."""

import importlib.util
import json
import sys
from pathlib import Path

import pytest
import yaml

_SCRIPTS_DIR = Path(__file__).parents[3] / "scripts"
if str(_SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS_DIR))

_SCRIPT_PATH = _SCRIPTS_DIR / "annotate_sbom.py"
_spec = importlib.util.spec_from_file_location("annotate_sbom", _SCRIPT_PATH)
annotate_sbom = importlib.util.module_from_spec(_spec)
sys.modules["annotate_sbom"] = annotate_sbom
_spec.loader.exec_module(annotate_sbom)


DEFAULT_ANNOTATIONS = {
    "defaults": {
        "distributed": True,
        "distributed_rationale": "runtime dependency of the published featurebyte package",
        "manner": "imported as a Python library; invoked via direct function/API calls at runtime",
        "modified": False,
    },
    "overrides": {},
}


def _component(name: str, version: str = "1.0.0") -> dict:
    return {"type": "library", "name": name, "version": version}


def test_computed_facts_override_defaults_but_not_manual_overrides():
    annotations = {
        "defaults": DEFAULT_ANNOTATIONS["defaults"],
        "overrides": {"pyphen": {"license_note": "elected LGPL option"}},
    }
    computed = {
        "pyphen": {
            "modified": False,
            "modified_basis": "Verified: plain PyPI registry source.",
            "manner": "Transitive dependency, reached via weasyprint.",
        }
    }
    sbom = {"components": [_component("pyphen")]}

    result = annotate_sbom.annotate_sbom(sbom, annotations, computed=computed)

    properties = {p["name"]: p["value"] for p in result["components"][0]["properties"]}
    # computed fact wins over the generic default
    assert properties["featurebyte:manner"] == "Transitive dependency, reached via weasyprint."
    assert properties["featurebyte:modified_basis"] == "Verified: plain PyPI registry source."
    # manual override still wins over the computed fact
    assert properties["featurebyte:license_note"] == "elected LGPL option"


def test_unresolved_flag_suppresses_default_for_that_field():
    annotations = {"defaults": DEFAULT_ANNOTATIONS["defaults"], "overrides": {}}
    computed = {"orphan-pkg": {"manner_flag_reason": "No reachability path found."}}
    sbom = {"components": [_component("orphan-pkg")]}

    result = annotate_sbom.annotate_sbom(sbom, annotations, computed=computed)

    properties = {p["name"]: p["value"] for p in result["components"][0]["properties"]}
    # the bulk default 'manner' must not silently stand in once flagged unresolved
    assert "featurebyte:manner" not in properties
    assert properties["featurebyte:manner_flag_reason"] == "No reachability path found."
    # unrelated fields are unaffected
    assert properties["featurebyte:distributed"] == "true"


def test_main_annotates_sbom_file_in_place(tmp_path, monkeypatch):
    sbom_path = tmp_path / "sbom.json"
    annotations_path = tmp_path / "oss-annotations.yaml"
    sbom_path.write_text(json.dumps({"components": [_component("numpy")]}))
    annotations_path.write_text(yaml.safe_dump(DEFAULT_ANNOTATIONS))

    monkeypatch.setattr(
        sys, "argv", ["annotate_sbom.py", str(sbom_path), str(annotations_path)]
    )
    annotate_sbom.main()

    written = json.loads(sbom_path.read_text())
    properties = {p["name"]: p["value"] for p in written["components"][0]["properties"]}
    assert properties["featurebyte:distributed"] == "true"


def test_main_computes_lockfile_facts_when_uv_lock_given(tmp_path, monkeypatch):
    sbom_path = tmp_path / "sbom.json"
    annotations_path = tmp_path / "oss-annotations.yaml"
    lock_path = tmp_path / "uv.lock"
    sbom_path.write_text(json.dumps({"components": [_component("numpy")]}))
    annotations_path.write_text(yaml.safe_dump(DEFAULT_ANNOTATIONS))
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

    monkeypatch.setattr(
        sys,
        "argv",
        ["annotate_sbom.py", str(sbom_path), str(annotations_path), str(lock_path)],
    )
    annotate_sbom.main()

    written = json.loads(sbom_path.read_text())
    properties = {p["name"]: p["value"] for p in written["components"][0]["properties"]}
    assert properties["featurebyte:manner"] == (
        "Direct dependency of featurebyte (see pyproject.toml); imported as a Python "
        "library and invoked via direct function/API calls at runtime."
    )
    assert "plain PyPI registry" in properties["featurebyte:modified_basis"]


def test_raises_on_override_for_package_not_in_sbom():
    annotations = {
        "defaults": DEFAULT_ANNOTATIONS["defaults"],
        "overrides": {"rfc3987": {"license_note": "removed via jsonschema override"}},
    }
    sbom = {"components": [_component("numpy")]}

    with pytest.raises(annotate_sbom.StaleOverrideError, match="rfc3987"):
        annotate_sbom.annotate_sbom(sbom, annotations)


def test_license_note_omitted_when_component_uses_pure_defaults():
    sbom = {"components": [_component("numpy")]}

    result = annotate_sbom.annotate_sbom(sbom, DEFAULT_ANNOTATIONS)

    properties = {p["name"] for p in result["components"][0]["properties"]}
    assert "featurebyte:license_note" not in properties


def test_override_matching_is_case_and_separator_insensitive():
    annotations = {
        "defaults": DEFAULT_ANNOTATIONS["defaults"],
        "overrides": {"Typing_Extensions": {"license_note": "elected some option"}},
    }
    sbom = {"components": [_component("typing-extensions")]}

    result = annotate_sbom.annotate_sbom(sbom, annotations)

    properties = {p["name"]: p["value"] for p in result["components"][0]["properties"]}
    assert properties["featurebyte:license_note"] == "elected some option"


def test_override_replaces_specific_fields_keeps_others_default():
    annotations = {
        "defaults": DEFAULT_ANNOTATIONS["defaults"],
        "overrides": {
            "pyphen": {
                "manner": "imported by weasyprint, used only in the PDF-export endpoint",
                "license_note": "elected LGPL-2.1-or-later/MPL-1.1 option of tri-license, not GPL",
            }
        },
    }
    sbom = {"components": [_component("pyphen")]}

    result = annotate_sbom.annotate_sbom(sbom, annotations)

    properties = {p["name"]: p["value"] for p in result["components"][0]["properties"]}
    assert properties["featurebyte:manner"] == (
        "imported by weasyprint, used only in the PDF-export endpoint"
    )
    assert properties["featurebyte:license_note"] == (
        "elected LGPL-2.1-or-later/MPL-1.1 option of tri-license, not GPL"
    )
    # fields not overridden still fall back to the repo-wide defaults
    assert properties["featurebyte:distributed"] == "true"
    assert properties["featurebyte:modified"] == "false"


def test_defaults_applied_when_no_override():
    sbom = {"components": [_component("numpy")]}

    result = annotate_sbom.annotate_sbom(sbom, DEFAULT_ANNOTATIONS)

    properties = {p["name"]: p["value"] for p in result["components"][0]["properties"]}
    assert properties == {
        "featurebyte:distributed": "true",
        "featurebyte:distributed_rationale": "runtime dependency of the published featurebyte package",
        "featurebyte:manner": "imported as a Python library; invoked via direct function/API calls at runtime",
        "featurebyte:modified": "false",
    }
