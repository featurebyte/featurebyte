"""
Tests for Entity model
"""
from datetime import datetime

from bson import ObjectId

from featurebyte.models.entity import EntityModel, EntityNameHistoryEntry


def test_entity_model():
    """Test creation, serialization & deserialization of an Entity object"""
    entity = EntityModel(
        name="customer",
        serving_names=["cust_id"],
        name_history=[EntityNameHistoryEntry(name="Customer", created_at=datetime(2022, 7, 1))],
        created_at=datetime(2022, 6, 30),
    )
    entity_dict = entity.dict(by_alias=True)
    assert set(entity_dict.keys()) == {"_id", "name", "serving_names", "name_history", "created_at"}
    assert isinstance(entity_dict["_id"], ObjectId)
    assert entity_dict["name"] == "customer"
    assert entity_dict["serving_names"] == ["cust_id"]
    assert entity_dict["name_history"] == [{"created_at": datetime(2022, 7, 1), "name": "Customer"}]
    assert entity_dict["created_at"] == datetime(2022, 6, 30)

    entity_loaded = EntityModel.parse_raw(entity.json(by_alias=True))
    assert entity_loaded == entity
