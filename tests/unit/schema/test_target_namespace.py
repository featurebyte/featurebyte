"""
Test target namespace schema
"""

import pytest
from bson import ObjectId

from featurebyte.enum import DBVarType, TargetType
from featurebyte.schema.target_namespace import TargetNamespaceCreate


@pytest.mark.parametrize(
    "dtype, target_type, positive_label, expected_error",
    [
        # Valid cases
        (DBVarType.VARCHAR, TargetType.CLASSIFICATION, "positive", None),
        (DBVarType.CHAR, TargetType.CLASSIFICATION, "positive", None),
        (DBVarType.INT, TargetType.CLASSIFICATION, 1, None),
        (DBVarType.BOOL, TargetType.CLASSIFICATION, True, None),
        (DBVarType.VARCHAR, TargetType.CLASSIFICATION, None, None),  # No positive label
        (DBVarType.FLOAT, TargetType.REGRESSION, None, None),  # Regression without positive label
        # Invalid target type
        (DBVarType.INT, TargetType.CLASSIFICATION, "positive", ValueError),
        (DBVarType.INT, None, 1, ValueError),
        # Invalid dtype
        (DBVarType.FLOAT, TargetType.CLASSIFICATION, 1.5, ValueError),
        # Type mismatch
        (DBVarType.BOOL, TargetType.CLASSIFICATION, "true", ValueError),
        (DBVarType.BOOL, TargetType.CLASSIFICATION, 1, ValueError),
        (DBVarType.VARCHAR, TargetType.CLASSIFICATION, 123, ValueError),
        (DBVarType.CHAR, TargetType.CLASSIFICATION, True, ValueError),
        (DBVarType.INT, TargetType.CLASSIFICATION, "1", ValueError),
    ],
)
def test_target_namespace_create_positive_label_validation(
    dtype, target_type, positive_label, expected_error
):
    """
    Test target namespace create schema validation for positive label
    """
    common_params = {
        "name": "target_namespace",
        "dtype": dtype,
        "entity_ids": [ObjectId()],
        "target_type": target_type,
        "positive_label": positive_label,
    }

    if expected_error:
        with pytest.raises((ValueError, Exception)) as exc:
            TargetNamespaceCreate(**common_params)
        # Check that some validation error was raised
        assert exc.value is not None
    else:
        target_namespace_create = TargetNamespaceCreate(**common_params)
        assert target_namespace_create.name == "target_namespace"
        assert target_namespace_create.dtype == dtype
        assert target_namespace_create.target_type == target_type
        assert target_namespace_create.positive_label == positive_label


def test_target_namespace_create_without_positive_label():
    """
    Test target namespace create schema without positive label
    """
    target_namespace_create = TargetNamespaceCreate(
        name="target_namespace",
        dtype=DBVarType.VARCHAR,
        entity_ids=[ObjectId()],
        target_type=TargetType.CLASSIFICATION,
    )
    assert target_namespace_create.name == "target_namespace"
    assert target_namespace_create.dtype == DBVarType.VARCHAR
    assert target_namespace_create.target_type == TargetType.CLASSIFICATION
    assert target_namespace_create.positive_label is None
