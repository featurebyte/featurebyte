"""
Test treatment module
"""

import pytest

from featurebyte import (
    AssignmentDesign,
    AssignmentSource,
    Propensity,
    Treatment,
    TreatmentInterference,
    TreatmentTime,
    TreatmentTimeStructure,
    TreatmentType,
)
from featurebyte.enum import DBVarType
from featurebyte.exception import RecordCreationException


def _build_valid_binary_params() -> dict:
    """
    Helper to build a minimal valid binary treatment parameter set.
    """
    return dict(
        name="test_treatment",
        dtype=DBVarType.INT,
        treatment_type=TreatmentType.BINARY,
        source=AssignmentSource.RANDOMIZED,
        design=AssignmentDesign.SIMPLE_RANDOMIZATION,
        time=TreatmentTime.STATIC,
        time_structure=TreatmentTimeStructure.INSTANTANEOUS,
        interference=TreatmentInterference.NONE,
        treatment_labels=[0, 1],
        control_label=0,
        propensity=Propensity(
            granularity="global",
            knowledge="design-known",
            p_global=0.5,
        ),
    )


def test_treatment_create_and_delete(item_entity):
    """
    Test treatment create
    """
    params = _build_valid_binary_params()
    treatment = Treatment.create(**params)

    assert treatment.dtype == params["dtype"]
    assert treatment.treatment_type == params["treatment_type"]
    assert treatment.source == params["source"]
    assert treatment.design == params["design"]
    assert treatment.time == params["time"]
    assert treatment.time_structure == params["time_structure"]
    assert treatment.interference == params["interference"]
    assert treatment.treatment_labels == params["treatment_labels"]
    assert treatment.control_label == params["control_label"]
    assert treatment.propensity == params["propensity"]

    treatment_info = treatment.info()
    assert treatment_info == {
        "name": "test_treatment",
        "created_at": treatment_info["created_at"],
        "updated_at": treatment_info["updated_at"],
        "description": None,
        "dtype": "INT",
        "treatment_type": "binary",
        "source": "randomized",
        "design": "simple-randomization",
        "time": "static",
        "time_structure": "instantaneous",
        "interference": "none",
        "treatment_labels": [0, 1],
        "control_label": 0,
        "propensity": {"granularity": "global", "knowledge": "design-known", "p_global": 0.5},
    }

    assert treatment.saved
    treatment.delete()
    assert not treatment.saved


def test_treatment_conflict_with_dtype(item_entity):
    """
    Test treatment conflict with dtype
    """

    params = _build_valid_binary_params()
    params["dtype"] = DBVarType.VARCHAR
    with pytest.raises(RecordCreationException) as exc:
        treatment = Treatment.create(**params)
    expected = "treatment_label 0 has type int which is not compatible with dtype 'VARCHAR'. Expected types: str."
    assert expected in str(exc.value)


def test_treatment_conflict_with_treatment_type(item_entity):
    """
    Test treatment conflict with treatment_type
    """

    params = _build_valid_binary_params()
    params["treatment_type"] = TreatmentType.NUMERIC
    with pytest.raises(RecordCreationException) as exc:
        treatment = Treatment.create(**params)
    expected = "treatment_labels must be None for numeric treatments"
    assert expected in str(exc.value)

    params["treatment_labels"] = None
    params["control_label"] = None
    with pytest.raises(RecordCreationException) as exc:
        treatment = Treatment.create(**params)
    expected = "propensity must be None for numeric treatments"
    assert expected in str(exc.value)


def test_missing_control(item_entity):
    """
    Test treatment with control missing
    """

    params = _build_valid_binary_params()
    params["control_label"] = None
    with pytest.raises(RecordCreationException) as exc:
        treatment = Treatment.create(**params)
    expected = "control_label must be provided for non numeric treatments (treatment_type='binary')"
    assert expected in str(exc.value)


def test_missing_propensity(item_entity):
    """
    Test treatment with propensity missing
    """

    params = _build_valid_binary_params()
    params["propensity"] = None
    with pytest.raises(RecordCreationException) as exc:
        treatment = Treatment.create(**params)
    expected = "propensity must be provided for non numeric treatments (treatment_type='binary')"
    assert expected in str(exc.value)
