"""
Fixtures for tests/unit/query_graph/model
"""

import pytest
from bson import ObjectId

from featurebyte.query_graph.model.entity_relationship_info import EntityRelationshipInfo


@pytest.fixture
def entity_a():
    """
    Fixture for entity a
    """
    return ObjectId("65b9ce1b40b5573fc4f0000a")


@pytest.fixture
def entity_b():
    """
    Fixture for entity b
    """
    return ObjectId("65b9ce1b40b5573fc4f0000b")


@pytest.fixture
def entity_c():
    """
    Fixture for entity c
    """
    return ObjectId("65b9ce1b40b5573fc4f0000c")


@pytest.fixture
def entity_d():
    """
    Fixture for entity d
    """
    return ObjectId("65b9ce1b40b5573fc4f0000d")


def create_relationship_info(child, parent):
    """
    Create EntityRelationshipInfo for tests
    """
    return EntityRelationshipInfo(
        id=ObjectId(),
        relationship_type="child_parent",
        entity_id=child,
        related_entity_id=parent,
        relation_table_id=ObjectId(),
    )


@pytest.fixture
def b_is_parent_of_a(entity_b, entity_a):
    """
    Fixture to establish a -> b relationship
    """
    return create_relationship_info(child=entity_a, parent=entity_b)


@pytest.fixture
def c_is_parent_of_b(entity_c, entity_b):
    """
    Fixture to establish b -> c relationship
    """
    return create_relationship_info(child=entity_b, parent=entity_c)


@pytest.fixture
def d_is_parent_of_b(entity_d, entity_b):
    """
    Fixture to establish b -> d relationship
    """
    return create_relationship_info(child=entity_b, parent=entity_d)
