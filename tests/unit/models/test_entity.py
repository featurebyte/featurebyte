"""
Tests for Entity model
"""
from datetime import datetime

from bson import ObjectId

from featurebyte.models.entity import EntityModel


def test_entity_model():
    """Test creation, serialization & deserialization of an Entity object"""

    entity = EntityModel(
        name="customer",
        serving_names=["cust_id"],
        created_at=datetime(2022, 6, 30),
        updated_at=datetime(2022, 6, 30),
        user_id=ObjectId(),
    )
    entity_dict = entity.dict(by_alias=True)
    assert set(entity_dict.keys()) == {
        "_id",
        "user_id",
        "name",
        "serving_names",
        "created_at",
        "updated_at",
        "ancestor_ids",
        "parents",
        "tabular_data_ids",
        "primary_tabular_data_ids",
        "catalog_id",
    }
    assert isinstance(entity_dict["_id"], ObjectId)
    assert isinstance(entity_dict["user_id"], ObjectId)
    assert entity_dict["name"] == "customer"
    assert entity_dict["serving_names"] == ["cust_id"]
    assert entity_dict["created_at"] == datetime(2022, 6, 30)
    assert entity_dict["updated_at"] == datetime(2022, 6, 30)

    entity_loaded = EntityModel.parse_raw(entity.json(by_alias=True))
    assert entity_loaded == entity
