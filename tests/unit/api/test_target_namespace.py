"""
Test target namespace module
"""

import pytest

from featurebyte import TargetType
from featurebyte.api.target_namespace import TargetNamespace
from featurebyte.enum import DBVarType
from featurebyte.exception import RecordCreationException, RecordUpdateException


def test_target_namespace_create_and_delete(item_entity):
    """
    Test target namespace create
    """
    target_namespace = TargetNamespace.create(
        "target_namespace_1", primary_entity=["item"], dtype=DBVarType.INT, window="7d"
    )
    assert target_namespace.name == "target_namespace_1"
    assert target_namespace.window == "7d"
    assert target_namespace.entity_ids == [item_entity.id]
    assert target_namespace.target_type is None

    target_namespace.update_target_type(TargetType.CLASSIFICATION)
    assert target_namespace.target_type == TargetType.CLASSIFICATION

    target_namespace.update_positive_label(1)
    assert target_namespace.positive_label == 1

    with pytest.raises(RecordUpdateException) as exc:
        target_namespace.update_target_type(TargetType.REGRESSION)
    assert "Updating target type after setting it is not supported." in str(exc.value)

    namespace_info = target_namespace.info()
    assert namespace_info == {
        "name": "target_namespace_1",
        "description": None,
        "default_version_mode": "AUTO",
        "default_target_id": None,
        "target_type": "classification",
        "created_at": namespace_info["created_at"],
        "updated_at": namespace_info["updated_at"],
    }

    assert target_namespace.saved
    target_namespace.delete()
    assert not target_namespace.saved


def test_target_namespace_conflict_with_target_dtype(float_target, item_entity):
    """
    Test target namespace conflict with target
    """
    assert not float_target.saved
    namespace = TargetNamespace.create(
        name=float_target.name,
        primary_entity=[item_entity.name],
        dtype=DBVarType.INT,
    )
    assert namespace.dtype == DBVarType.INT
    assert float_target.dtype != namespace.dtype

    # check dtype conflict
    with pytest.raises(RecordCreationException) as exc:
        float_target.save()
    expected = (
        'TargetModel (name: "float_target") object(s) within the same namespace must '
        'have the same "dtype" value (namespace: "INT", TargetModel: "FLOAT").'
    )
    assert expected in str(exc.value)


def test_target_namespace_conflict_with_target_type(float_target, item_entity):
    """
    Test target namespace conflict with target
    """
    assert not float_target.saved
    namespace = TargetNamespace.create(
        name=float_target.name,
        primary_entity=[item_entity.name],
        dtype=DBVarType.INT,
        target_type=TargetType.CLASSIFICATION,
    )
    assert namespace.target_type == TargetType.CLASSIFICATION

    # check target_type conflict
    assert not float_target.saved
    with pytest.raises(RecordCreationException) as exc:
        float_target.update_target_type(TargetType.REGRESSION)
        float_target.save()

    expected = (
        "Target type regression is not consistent with namespace's target type classification"
    )
    assert expected in str(exc.value)


def test_positive_value_conflict_with_target_type(float_target, item_entity):
    """
    Test positive value conflict with target
    """
    assert not float_target.saved
    namespace = TargetNamespace.create(
        name=float_target.name,
        primary_entity=[item_entity.name],
        dtype=DBVarType.FLOAT,
        target_type=TargetType.REGRESSION,
    )
    assert namespace.target_type == TargetType.REGRESSION

    # check positive_label conflict
    assert not float_target.saved
    with pytest.raises(RecordCreationException) as exc:
        float_target.update_positive_label(1)
        float_target.save()

    expected = "Positive label can only be set for target namespace of type classification"
    assert expected in str(exc.value)


@pytest.mark.parametrize("scenario", ["update_before_save", "update_after_save"])
def test_target_namespace_update_with_target(
    float_target, item_entity, mock_api_object_cache, scenario
):
    """
    Test target namespace updated after target creation
    """
    assert not float_target.saved
    namespace = TargetNamespace.create(
        name=float_target.name,
        primary_entity=[item_entity.name],
        dtype=DBVarType.FLOAT,
    )
    assert namespace.default_target_id is None

    # save target & check namespace updated
    if scenario == "update_before_save":
        float_target.update_target_type(TargetType.REGRESSION)

    float_target.save()

    if scenario == "update_after_save":
        float_target.update_target_type(TargetType.REGRESSION)

    assert float_target.saved
    assert float_target.target_type == TargetType.REGRESSION
    assert namespace.default_target_id == float_target.id
    assert namespace.target_type == TargetType.REGRESSION


def test_target_namespace_create_with_positive_label(item_entity):
    """
    Test creating TargetNamespace with positive_label parameter
    """
    # Create a classification target namespace with positive_label
    namespace = TargetNamespace.create(
        "classification_target_with_label",
        primary_entity=["item"],
        dtype=DBVarType.VARCHAR,
        window="7d",
        target_type=TargetType.CLASSIFICATION,
        positive_label="positive",
    )

    # Verify positive_label was set
    assert namespace.cached_model.positive_label == "positive"
    assert namespace.cached_model.target_type == TargetType.CLASSIFICATION

    namespace.delete()


def test_target_namespace_create_with_positive_label_invalid_target_type(item_entity):
    """
    Test that creating TargetNamespace with positive_label for non-classification fails
    """
    # Try to create regression target namespace with positive_label (should fail)
    with pytest.raises(RecordCreationException) as exc:
        TargetNamespace.create(
            "regression_with_label",
            primary_entity=["item"],
            dtype=DBVarType.INT,
            window="7d",
            target_type=TargetType.REGRESSION,
            positive_label=1,
        )

    assert "Positive label can only be set for classification target type" in str(exc.value)


def test_target_namespace_positive_label_update_multiple_times_without_obs_tables(item_entity):
    """
    Test that positive_label can be updated multiple times when no observation tables exist
    """
    # Create a classification target namespace with initial positive_label
    namespace = TargetNamespace.create(
        "classification_target_multi_update",
        primary_entity=["item"],
        dtype=DBVarType.VARCHAR,
        window="7d",
        target_type=TargetType.CLASSIFICATION,
        positive_label="positive",
    )
    assert namespace.cached_model.positive_label == "positive"

    # Update positive_label (should succeed as no observation tables exist)
    namespace.update(
        update_payload={"positive_label": {"value": "negative", "observation_table_id": None}},
        allow_update_local=False,
        url=f"{namespace._route}/{namespace.id}",
        skip_update_schema_check=True,
    )
    assert namespace.cached_model.positive_label == "negative"

    namespace.delete()


def test_target_namespace_update_with_positive_label_invalid_target_type(item_entity):
    """
    Test that updating TargetNamespace with positive_label for non-classification fails
    """
    # Try to update regression target namespace with positive_label (should fail)
    with pytest.raises(RecordUpdateException) as exc:
        target_namespace = TargetNamespace.create(
            "regression_with_label",
            primary_entity=["item"],
            dtype=DBVarType.INT,
            window="7d",
            target_type=TargetType.REGRESSION,
        )
        target_namespace.update_positive_label(1)

    assert (
        "Positive label can only be set for target namespace of type classification, but got regression."
        in str(exc.value)
    )
