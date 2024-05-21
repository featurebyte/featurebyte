"""
Test target namespace module
"""

from featurebyte.api.target_namespace import TargetNamespace
from featurebyte.enum import DBVarType


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

    assert target_namespace.saved
    target_namespace.delete()
    assert not target_namespace.saved
