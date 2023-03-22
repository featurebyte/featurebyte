"""
Unit tests for RelationshipAnalysisService
"""
from bson import ObjectId

from featurebyte.models import EntityModel
from featurebyte.service.relationship_analysis import derive_primary_entity


def test_derive_primary_entity__not_related():
    """
    Test deriving primary entity when entities are not related
    """
    entity_a = EntityModel(id=ObjectId(), name="A", serving_names=["A"])
    entity_b = EntityModel(id=ObjectId(), name="B", serving_names=["B"])
    assert derive_primary_entity([entity_a, entity_b]) == [
        entity_a,
        entity_b,
    ]


def test_derive_primary_entity__related():
    """
    Test deriving primary entity when entities are related
    """
    entity_a = EntityModel(id=ObjectId(), name="A", serving_names=["A"])
    entity_b = EntityModel(
        id=ObjectId(),
        name="B",
        serving_names=["B"],
        parents=[
            {
                "id": entity_a.id,
                "name": entity_a.name,
                "table_type": "scd_table",
                "table_id": ObjectId(),
            }
        ],
        ancestor_ids=[entity_a.id],
    )
    assert derive_primary_entity([entity_a, entity_b]) == [entity_b]


def test_derive_primary_entity__multiple_levels():
    """
    Test deriving primary entity when entities are related on multiple levels
    """
    entity_a = EntityModel(id=ObjectId(), name="A", serving_names=["A"])
    entity_b = EntityModel(
        id=ObjectId(),
        name="B",
        serving_names=["B"],
        parents=[
            {
                "id": entity_a.id,
                "name": entity_a.name,
                "table_type": "scd_table",
                "table_id": ObjectId(),
            }
        ],
        ancestor_ids=[entity_a.id],
    )
    entity_c = EntityModel(
        id=ObjectId(),
        name="C",
        serving_names=["C"],
        parents=[
            {
                "id": entity_b.id,
                "name": entity_b.name,
                "table_type": "scd_table",
                "table_id": ObjectId(),
            }
        ],
        ancestor_ids=[entity_b.id],
    )
    assert derive_primary_entity([entity_a, entity_b, entity_c]) == [
        entity_c,
    ]
