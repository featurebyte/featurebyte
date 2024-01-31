"""
Test feast registry model
"""
from bson import ObjectId

from featurebyte.feast.model.registry import FeastRegistryModel


def test_feast_registry_model(feast_registry_proto):
    """Test feast registry model"""
    feature_store_id = ObjectId()
    feast_registry = FeastRegistryModel(
        name="feast_registry",
        offline_table_name_prefix="cat1",
        registry=feast_registry_proto.SerializeToString(),
        feature_store_id=feature_store_id,
    )
    feast_registry_dict = feast_registry.dict()

    deserialize_feast_registry = FeastRegistryModel(**feast_registry_dict)
    assert deserialize_feast_registry.name == "feast_registry"
    assert deserialize_feast_registry.registry_proto() == feast_registry_proto
    assert deserialize_feast_registry.feature_store_id == feature_store_id
    assert deserialize_feast_registry.offline_table_name_prefix == "cat1"
