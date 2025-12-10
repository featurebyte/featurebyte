"""
Test treatment schema
"""

import pytest

from featurebyte.enum import DBVarType, TreatmentType
from featurebyte.models.treatment import (
    TreatmentTime,
    TreatmentTimeStructure,
)
from featurebyte.schema.treatment import TreatmentCreate


@pytest.mark.parametrize(
    "dtype, treatment_type, control_label, treatment_labels, expected_error",
    [
        # Valid cases
        (DBVarType.VARCHAR, TreatmentType.BINARY, "control", ["control", "treatment"], None),
        (DBVarType.CHAR, TreatmentType.BINARY, "control", ["control", "treatment"], None),
        (DBVarType.INT, TreatmentType.BINARY, 1, [0, 1], None),
        (DBVarType.BOOL, TreatmentType.BINARY, True, [True, False], None),
        (
            DBVarType.FLOAT,
            TreatmentType.NUMERIC,
            None,
            None,
            None,
        ),  # Regression without control label
        # No control label or labels for non continuous
        (DBVarType.VARCHAR, TreatmentType.BINARY, None, None, ValueError),
        (DBVarType.VARCHAR, TreatmentType.MULTI_ARM, None, None, ValueError),
        # Control label provided but labels missing
        (DBVarType.VARCHAR, TreatmentType.BINARY, "control", None, ValueError),
        # Invalid dtype combinations (validate_treatment_type)
        (DBVarType.FLOAT, TreatmentType.BINARY, 1.5, [1.5, 2], ValueError),
        (DBVarType.VARCHAR, TreatmentType.NUMERIC, 1.5, [1.5, 2], ValueError),
        # Continuous treatments must not have labels or control
        (DBVarType.FLOAT, TreatmentType.NUMERIC, None, [0.0, 1.0], ValueError),
        (DBVarType.FLOAT, TreatmentType.NUMERIC, 0.0, None, ValueError),
        # Binary treatment must have exactly 2 labels
        (DBVarType.VARCHAR, TreatmentType.BINARY, "control", ["control"], ValueError),
        (DBVarType.VARCHAR, TreatmentType.BINARY, "control", ["control", "t1", "t2"], ValueError),
        # Multi arm treatment must have at least 3 labels
        (DBVarType.VARCHAR, TreatmentType.MULTI_ARM, "control", ["control", "t1"], ValueError),
        # control_label must be in treatment_labels
        (
            DBVarType.VARCHAR,
            TreatmentType.BINARY,
            "not-in-labels",
            ["control", "treatment"],
            ValueError,
        ),
        # Type mismatch for labels and control label vs dtype
        (DBVarType.BOOL, TreatmentType.BINARY, "true", ["true", "false"], ValueError),
        (DBVarType.BOOL, TreatmentType.BINARY, 1, [0, 1], ValueError),
        (DBVarType.VARCHAR, TreatmentType.BINARY, 123, [123, 124], ValueError),
        (DBVarType.CHAR, TreatmentType.BINARY, True, [True, False], ValueError),
        (DBVarType.INT, TreatmentType.BINARY, "1", ["0", "1"], ValueError),
    ],
)
def test_treatment_create_labels_validation(
    dtype, treatment_type, control_label, treatment_labels, expected_error
):
    """
    Test treatment create schema validation for labels and label dtype compatibility
    """
    common_params = {
        "name": "treatment",
        "dtype": dtype,
        "treatment_type": treatment_type,
        "control_label": control_label,
        "treatment_labels": treatment_labels,
        "source": "randomized",
        "design": "simple-randomization",
    }
    if treatment_type in {TreatmentType.BINARY, TreatmentType.MULTI_ARM}:
        propensity = {
            "granularity": "global",
            "knowledge": "design-known",
            "p_global": 0.5 if treatment_type == TreatmentType.BINARY else None,
        }
        common_params.update({"propensity": propensity})

    if expected_error:
        with pytest.raises((ValueError, Exception)) as exc:
            TreatmentCreate(**common_params)
        assert exc.value is not None
    else:
        treatment_create = TreatmentCreate(**common_params)
        assert treatment_create.name == "treatment"
        assert treatment_create.dtype == dtype
        assert treatment_create.treatment_type == treatment_type
        assert treatment_create.control_label == control_label
        assert treatment_create.treatment_labels == treatment_labels


def test_numeric_treatment_cannot_have_propensity():
    """
    Numeric treatments must not define propensity
    """
    with pytest.raises(ValueError, match="propensity must be None for numeric treatments"):
        TreatmentCreate(
            name="treatment",
            dtype=DBVarType.FLOAT,
            treatment_type=TreatmentType.NUMERIC,
            control_label=None,
            treatment_labels=None,
            source="randomized",
            design="simple-randomization",
            propensity={
                "granularity": "global",
                "knowledge": "design-known",
                "p_global": 0.5,
            },
        )


def test_time_static_with_instantaneous_structure_is_valid():
    """
    For time='static', time_structure can be 'instantaneous'
    """
    treatment = TreatmentCreate(
        name="treatment",
        dtype=DBVarType.VARCHAR,
        treatment_type=TreatmentType.BINARY,
        control_label="control",
        treatment_labels=["control", "treatment"],
        source="randomized",
        design="simple-randomization",
        time=TreatmentTime.STATIC,
        time_structure=TreatmentTimeStructure.INSTANTANEOUS,
        propensity={
            "granularity": "global",
            "knowledge": "design-known",
            "p_global": 0.5,
        },
    )
    assert treatment.time == TreatmentTime.STATIC
    assert treatment.time_structure == TreatmentTimeStructure.INSTANTANEOUS


def test_time_static_with_invalid_structure_raises():
    """
    For time='static', time_structure must be NONE or INSTANTANEOUS
    """
    with pytest.raises(ValueError, match="For time='static'"):
        TreatmentCreate(
            name="treatment",
            dtype=DBVarType.VARCHAR,
            treatment_type=TreatmentType.BINARY,
            control_label="control",
            treatment_labels=["control", "treatment"],
            source="randomized",
            design="simple-randomization",
            time=TreatmentTime.STATIC,
            time_structure=TreatmentTimeStructure.CONTINUOUS_TIME,
            propensity={
                "granularity": "global",
                "knowledge": "design-known",
                "p_global": 0.5,
            },
        )


def test_time_continuous_with_continuous_structure_is_valid():
    """
    For time='continuous', time_structure must be CONTINUOUS_TIME
    """
    treatment = TreatmentCreate(
        name="treatment",
        dtype=DBVarType.FLOAT,
        treatment_type=TreatmentType.NUMERIC,
        control_label=None,
        treatment_labels=None,
        source="randomized",
        design="simple-randomization",
        time=TreatmentTime.CONTINUOUS,
        time_structure=TreatmentTimeStructure.CONTINUOUS_TIME,
    )
    assert treatment.time == TreatmentTime.CONTINUOUS
    assert treatment.time_structure == TreatmentTimeStructure.CONTINUOUS_TIME


def test_time_continuous_with_invalid_structure_raises():
    """
    For time='continuous', time_structure must not be NONE or INSTANTANEOUS
    """
    with pytest.raises(ValueError, match="For time='continuous'"):
        TreatmentCreate(
            name="treatment",
            dtype=DBVarType.FLOAT,
            treatment_type=TreatmentType.NUMERIC,
            control_label=None,
            treatment_labels=None,
            source="randomized",
            design="simple-randomization",
            time=TreatmentTime.CONTINUOUS,
            time_structure=TreatmentTimeStructure.NONE,
        )
