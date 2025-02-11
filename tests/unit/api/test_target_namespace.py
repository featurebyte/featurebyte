"""
Test target namespace module
"""

import pytest

from featurebyte import TargetType
from featurebyte.api.target_namespace import TargetNamespace
from featurebyte.enum import DBVarType
from featurebyte.exception import RecordUpdateException


def test_target_namespace_create_and_delete(item_entity):
    """
    Test target namespace create
    """
    target_namespace = TargetNamespace.create(
        "target_namespace_1", primary_entity=["item"], dtype=DBVarType.FLOAT, window="7d"
    )
    assert target_namespace.name == "target_namespace_1"
    assert target_namespace.window == "7d"
    assert target_namespace.entity_ids == [item_entity.id]
    assert target_namespace.target_type is None

    target_namespace.update_target_type(TargetType.CLASSIFICATION)
    assert target_namespace.target_type == TargetType.CLASSIFICATION

    with pytest.raises(RecordUpdateException) as exc:
        target_namespace.update_target_type(TargetType.REGRESSION)
    assert "Updating target type after setting it is not supported." in str(exc.value)

    assert target_namespace.saved
    target_namespace.delete()
    assert not target_namespace.saved
