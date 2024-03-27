"""
Tests for Context model
"""

from datetime import datetime

from bson import ObjectId

from featurebyte.models.context import ContextModel


def test_context_model__entity_ids_set_primary_entity_ids():
    """Test set primary_entity_ids for Context object"""

    entity_id = ObjectId("651e2429604674470bacc80c")
    context_dict = {
        "_id": ObjectId("651e2429604674470bacc80e"),
        "block_modification_by": [],
        "catalog_id": ObjectId("23eda344d0313fb925f7883a"),
        "created_at": datetime(2022, 6, 30, 0, 0),
        "default_eda_table_id": None,
        "default_preview_table_id": None,
        "description": None,
        "entity_ids": [entity_id],
        "graph": None,
        "name": "context_1",
        "node_name": None,
        "updated_at": datetime(2022, 6, 30, 0, 0),
        "user_id": ObjectId("651e2429604674470bacc80d"),
    }

    context = ContextModel(**context_dict)
    assert context.primary_entity_ids == [entity_id]
