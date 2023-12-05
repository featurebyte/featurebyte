"""
Test feast registry model
"""
from featurebyte.feast.model.registry import FeastRegistryModel


def test_feast_registry_model(feast_registry_proto):
    """Test feast registry model"""
    feast_registry = FeastRegistryModel(
        name="feast_registry",
        registry=feast_registry_proto.SerializeToString(),
    )
    feast_registry_dict = feast_registry.dict()

    deserialize_feast_registry = FeastRegistryModel(**feast_registry_dict)
    assert deserialize_feast_registry.name == "feast_registry"
    assert deserialize_feast_registry.registry_proto() == feast_registry_proto
