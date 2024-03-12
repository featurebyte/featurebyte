"""
Tests for featurebyte/query_graph/model/entity_lookup_plan.py
"""
import pytest
from bson import ObjectId

from featurebyte.query_graph.model.entity_lookup_plan import EntityColumn, EntityLookupPlanner
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


def test_entity_lookup_plan_one_relationship(entity_a, entity_b, entity_c, b_is_parent_of_a):
    """
    Test entity lookup plan generation (a -> b)
    """
    relationships_info = [b_is_parent_of_a]
    plan = EntityLookupPlanner.generate_plan([entity_b], relationships_info)
    assert plan.get_entity_lookup_steps([entity_a]) == [b_is_parent_of_a]
    assert plan.get_entity_lookup_steps([entity_b]) is None
    assert plan.get_entity_lookup_steps([entity_c]) is None


def test_entity_lookup_plan_two_relationships(
    entity_a,
    entity_c,
    b_is_parent_of_a,
    c_is_parent_of_b,
):
    """
    Test entity lookup plan generation (a -> b -> c)
    """
    relationships_info = [
        c_is_parent_of_b,
        b_is_parent_of_a,
    ]
    # Feature / Feature Table's primary entity is C
    plan = EntityLookupPlanner.generate_plan([entity_c], relationships_info)

    # Feature List's primary entity is A. To retrieve features of primary entity C, need to lookup B
    # from A, then lookup C from B.
    assert plan.get_entity_lookup_steps([entity_a]) == [
        b_is_parent_of_a,
        c_is_parent_of_b,
    ]


def test_entity_lookup_plan_composite_feature_list_primary_ids(
    entity_a,
    entity_b,
    entity_c,
    c_is_parent_of_b,
):
    """
    Test entity lookup plan generation. Relationship: (b -> c), starting from (a, b)
    """
    relationships_info = [
        c_is_parent_of_b,
    ]
    # Feature primary entity is C
    plan = EntityLookupPlanner.generate_plan([entity_c], relationships_info)

    # Feature List primary entity is (A, B). A doesn't matter because it's not related to C.
    assert plan.get_entity_lookup_steps([entity_a, entity_b]) == [c_is_parent_of_b]

    # Normal case of looking up C by B
    assert plan.get_entity_lookup_steps([entity_b]) == [c_is_parent_of_b]


def test_entity_lookup_plan_composite_feature_parallel_parents(
    entity_b,
    entity_c,
    entity_d,
    c_is_parent_of_b,
    d_is_parent_of_b,
):
    """
    Relationships:

    (child)     (parent)
    b        -> c
             -> d
    """
    relationships_info = [c_is_parent_of_b, d_is_parent_of_b]

    # Feature List primary entity is (c, d)
    plan = EntityLookupPlanner.generate_plan([entity_c, entity_d], relationships_info)

    # Serving from b is possible with two lookups
    assert set(plan.get_entity_lookup_steps([entity_b])) == {c_is_parent_of_b, d_is_parent_of_b}


def test_generate_lookup_steps_one_step(
    entity_a,
    entity_b,
    b_is_parent_of_a,
    c_is_parent_of_b,
):
    """
    Test generate_lookup_steps
    """
    relationships_info = [b_is_parent_of_a, c_is_parent_of_b]
    lookup_steps = EntityLookupPlanner.generate_lookup_steps(
        available_entity_ids=[entity_a],
        required_entity_ids=[entity_a, entity_b],
        relationships_info=relationships_info,
    )
    assert lookup_steps == [b_is_parent_of_a]


def test_generate_lookup_steps_two_steps(
    entity_a,
    entity_c,
    b_is_parent_of_a,
    c_is_parent_of_b,
):
    """
    Test generate_lookup_steps two steps
    """
    relationships_info = [b_is_parent_of_a, c_is_parent_of_b]
    lookup_steps = EntityLookupPlanner.generate_lookup_steps(
        available_entity_ids=[entity_a],
        required_entity_ids=[entity_c],
        relationships_info=relationships_info,
    )
    assert lookup_steps == [b_is_parent_of_a, c_is_parent_of_b]


def test_generate_lookup_steps_parallel_parents(
    entity_b,
    entity_c,
    entity_d,
    c_is_parent_of_b,
    d_is_parent_of_b,
):
    """
    Relationships:

    (child)     (parent)
    b        -> c
             -> d
    """
    relationships_info = [c_is_parent_of_b, d_is_parent_of_b]
    lookup_steps = EntityLookupPlanner.generate_lookup_steps(
        available_entity_ids=[entity_b],
        required_entity_ids=[entity_c, entity_d],
        relationships_info=relationships_info,
    )
    assert lookup_steps == [c_is_parent_of_b, d_is_parent_of_b]


def test_entity_column__single_branch(
    entity_a,
    entity_b,
    entity_c,
    b_is_parent_of_a,
    c_is_parent_of_b,
):
    """
    Test EntityColumn class (single branch)

    (child)     (parent)
    a        -> b
    """
    entity_column = EntityColumn(
        entity_id=entity_a,
        serving_name="colA",
        child_serving_name=None,
        relationship_info_id=None,
    )
    parent_entity_columns = entity_column.get_parent_entity_columns(
        [b_is_parent_of_a, c_is_parent_of_b],
    )
    assert parent_entity_columns == [
        entity_column,
        EntityColumn(
            entity_id=entity_b,
            serving_name=f"colA_{b_is_parent_of_a.id}",
            child_serving_name="colA",
            relationship_info_id=b_is_parent_of_a.id,
        ),
        EntityColumn(
            entity_id=entity_c,
            serving_name=f"colA_{b_is_parent_of_a.id}_{c_is_parent_of_b.id}",
            child_serving_name=f"colA_{b_is_parent_of_a.id}",
            relationship_info_id=c_is_parent_of_b.id,
        ),
    ]


def test_entity_column__two_branches(
    entity_b,
    entity_c,
    entity_d,
    c_is_parent_of_b,
    d_is_parent_of_b,
):
    """
    Test EntityColumn class (two branches)

    (child)     (parent)
    b        -> c
             -> d
    """
    entity_column = EntityColumn(
        entity_id=entity_b,
        serving_name="colB",
        child_serving_name=None,
        relationship_info_id=None,
    )
    parent_entity_columns = entity_column.get_parent_entity_columns(
        [c_is_parent_of_b, d_is_parent_of_b],
    )
    assert parent_entity_columns == [
        entity_column,
        EntityColumn(
            entity_id=entity_c,
            serving_name=f"colB_{c_is_parent_of_b.id}",
            child_serving_name="colB",
            relationship_info_id=c_is_parent_of_b.id,
        ),
        EntityColumn(
            entity_id=entity_d,
            serving_name=f"colB_{d_is_parent_of_b.id}",
            child_serving_name="colB",
            relationship_info_id=d_is_parent_of_b.id,
        ),
    ]
