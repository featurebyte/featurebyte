"""
Tests for Entity model
"""
from bson import ObjectId

from featurebyte.models.entity import EntityModel


def test_entity_model():
    """Test creation, serialization & deserialization of an Entity object"""
    entity = EntityModel(name="customer", serving_column_names=["cust_id"])
    entity_dict = entity.dict()
    assert set(entity_dict.keys()) == {"id", "name", "serving_column_names"}
    assert isinstance(entity_dict["id"], ObjectId)
    assert entity_dict["name"] == "customer"
    assert entity_dict["serving_column_names"] == ["cust_id"]

    entity_loaded = EntityModel.parse_raw(entity.json())
    assert entity_loaded == entity
