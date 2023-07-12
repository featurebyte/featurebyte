"""
Test target namespace module
"""
from featurebyte.api.target_namespace import TargetNamespace


def test_target_namespace_create(item_entity):
    """
    Test target namespace create
    """
    target_namespace = TargetNamespace.create("target_namespace_1", entities=["item"], window="7d")
    assert target_namespace.name == "target_namespace_1"
    assert target_namespace.window == "7d"
    assert target_namespace.entity_ids == [item_entity.id]
