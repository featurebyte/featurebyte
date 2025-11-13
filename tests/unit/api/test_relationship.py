"""
Test relationships module
"""

import pandas as pd
import pytest
import pytest_asyncio
from bson import ObjectId

from featurebyte import Entity
from featurebyte.api.catalog import Catalog
from featurebyte.api.relationship import Relationship
from featurebyte.exception import RecordUpdateException
from featurebyte.models.base import PydanticObjectId
from featurebyte.models.relationship import RelationshipInfoModel, RelationshipType
from featurebyte.schema.relationship_info import RelationshipInfoCreate


@pytest.fixture(name="relationship_info_service")
def relationship_info_service_fixture(app_container):
    """
    RelationshipInfoService fixture
    """
    return app_container.relationship_info_service


@pytest.fixture(name="relationship_info_create")
def relationship_info_create_fixture(snowflake_event_table):
    """
    Get a default RelationshipInfoCreate object.
    """
    cust_entity = Entity(name="customer", serving_names=["cust_id"])
    cust_entity.save()
    user_entity = Entity(name="user", serving_names=["user_id"])
    user_entity.save()

    return RelationshipInfoCreate(
        name="test_relationship",
        relationship_type=RelationshipType.CHILD_PARENT,
        entity_id=cust_entity.id,
        related_entity_id=user_entity.id,
        relation_table_id=snowflake_event_table.id,
        enabled=False,
        updated_by=PydanticObjectId(ObjectId()),
    )


@pytest.fixture(name="persistable_relationship_info")
def persistable_relationship_info_fixture(relationship_info_service):
    """
    Get a callback function that will persist a relationship info.
    """

    async def save(relationship_info_create: RelationshipInfoCreate) -> RelationshipInfoModel:
        created_relationship = await relationship_info_service.create_document(
            relationship_info_create
        )
        assert created_relationship.entity_id == relationship_info_create.entity_id
        return created_relationship

    return save


@pytest_asyncio.fixture(name="persisted_relationship_info")
async def persisted_relationship_info_fixture(
    persistable_relationship_info, relationship_info_create
):
    """
    Persisted relationship info fixture
    """
    persisted = await persistable_relationship_info(relationship_info_create)
    yield persisted


@pytest.mark.asyncio
async def test_relationship_get_by_id_without_updated_by(
    persistable_relationship_info, relationship_info_create
):
    """
    Test relationship get by id without updated by field.
    """
    # Create a RelationshipInfoCreate struct with no updated_by field
    default_values = relationship_info_create.model_dump()
    default_values["updated_by"] = None
    updated_relationship_info_create = RelationshipInfoCreate(**default_values)

    # Persist the value
    persisted_relationship_info = await persistable_relationship_info(
        updated_relationship_info_create
    )
    assert persisted_relationship_info.updated_by is None

    # Test that the results are equal
    retrieved_relationship_info = Relationship.get_by_id(persisted_relationship_info.id)
    assert retrieved_relationship_info.id == persisted_relationship_info.id
    assert retrieved_relationship_info.cached_model == persisted_relationship_info


def test_accessing_persisted_relationship_info_attributes(persisted_relationship_info):
    """
    Test accessing relationship info attributes
    """
    # Retrieving one copy from the database
    version_1 = Relationship.get_by_id(id=persisted_relationship_info.id)
    assert not version_1.enabled

    # Retrieving another copy from the database
    version_2 = Relationship.get_by_id(id=persisted_relationship_info.id)
    assert not version_2.enabled

    # Update the enabled value on the first in-memory version
    version_1.enable()
    assert version_1.enabled is True

    # Check that both versions are updated
    assert version_1.enabled
    assert version_2.enabled


def assert_relationship_info(relationship_info_df):
    """
    Helper function to assert state of relationship_info
    """
    assert relationship_info_df.shape[0] == 1
    assert relationship_info_df["id"][0] is not None
    assert relationship_info_df["relationship_type"][0] == "child_parent"
    assert relationship_info_df["entity"][0] == "customer"
    assert relationship_info_df["related_entity"][0] == "user"
    assert relationship_info_df["relation_table"][0] == "sf_event_table"
    assert relationship_info_df["relation_table_type"][0] == "event_table"
    assert not relationship_info_df["enabled"][0]


@pytest.mark.asyncio
async def test_relationships_list(persistable_relationship_info, relationship_info_create):
    """
    Test relationships list
    """
    relationships = Relationship.list()
    assert relationships.shape[0] == 0

    persisted_relationship_info = await persistable_relationship_info(relationship_info_create)
    relationship_type = persisted_relationship_info.relationship_type

    # verify that there's one relationship that was created
    relationships = Relationship.list()
    assert_relationship_info(relationships)

    # apply relationship_type filter for existing filter using enum
    relationships = Relationship.list(relationship_type=relationship_type)
    assert_relationship_info(relationships)

    # apply relationship_type filter for existing filter using string
    relationships = Relationship.list(relationship_type="child_parent")
    assert_relationship_info(relationships)

    # apply relationship_type filter for non-existing filter
    with pytest.raises(ValueError):
        Relationship.list(relationship_type="random_filter")


@pytest.mark.asyncio
async def test_enable(persisted_relationship_info):
    """
    Test enable
    """
    # verify that relationship is not enabled
    assert not persisted_relationship_info.enabled

    # retrieve relationship via get_by_id
    relationship = Relationship.get_by_id(persisted_relationship_info.id)
    assert not relationship.enabled

    # enable relationship
    relationship.enable()

    # verify that relationship is now enabled
    relationship = Relationship.get_by_id(persisted_relationship_info.id)
    assert relationship.enabled

    # disable relationship
    relationship.disable()
    assert relationship.enabled is False

    # verify that relationship is now enabled
    relationship = Relationship.get_by_id(persisted_relationship_info.id)
    assert not relationship.enabled


def test_catalog_id(persisted_relationship_info):
    """
    Test catalog_id
    """
    relationship = Relationship.get_by_id(persisted_relationship_info.id)
    catalog = Catalog.get_active()
    assert relationship.catalog_id == catalog.id


def test_entity_relationship(
    saved_event_table, saved_time_series_table, transaction_entity, cust_id_entity
):
    """Test entity relationship for event and time series table"""
    # tag the event table & time series table primary key columns as entities
    assert saved_event_table.event_id_column == "col_int"
    assert saved_time_series_table.series_id_column == "col_int"
    saved_event_table.col_int.as_entity(transaction_entity.name)
    saved_time_series_table.col_int.as_entity(transaction_entity.name)

    # tag entity to other columns in event table
    saved_event_table.cust_id.as_entity(cust_id_entity.name)

    relationships = Relationship.list()
    relationship = relationships.iloc[0]
    assert relationships.shape[0] == 1
    assert relationship.relationship_type == "child_parent"
    assert relationship.entity == "transaction"
    assert relationship.related_entity == "customer"

    # tag entity to other columns in time series table
    saved_time_series_table.store_id.as_entity(cust_id_entity.name)

    # check no change in relationship
    pd.testing.assert_frame_equal(relationships, Relationship.list())


@pytest.fixture(name="check_relationship", scope="function")
def check_relationship_fixture(transaction_entity, cust_id_entity):
    """
    Helper fixture to check relationship
    """

    def check_func(expected_relationship_type, expected_ancestor_ids):
        # Check the relationship can be listed under the correct type
        relationships = Relationship.list(relationship_type=expected_relationship_type)
        relationship = relationships.iloc[0]
        assert relationships.shape[0] == 1
        assert relationship.relationship_type == expected_relationship_type
        assert relationship.entity == "transaction"
        assert relationship.related_entity == "customer"

        # Check listing using the other relationship type returns no results
        other_relationship_type = (
            RelationshipType.ONE_TO_ONE
            if expected_relationship_type == RelationshipType.CHILD_PARENT
            else RelationshipType.CHILD_PARENT
        )
        relationships = Relationship.list(relationship_type=other_relationship_type)
        assert relationships.shape[0] == 0

        # Check ancestor_ids are set correctly
        Entity._cache.clear()
        _transaction_entity = Entity.get_by_id(transaction_entity.id)
        _cust_id_entity = Entity.get_by_id(cust_id_entity.id)
        assert _transaction_entity.ancestor_ids == expected_ancestor_ids
        assert _cust_id_entity.ancestor_ids == []

        relationship_obj = Relationship.get_by_id(relationship.id)
        return relationship_obj

    return check_func


def check_no_relationships():
    """
    Check no relationships exist
    """
    for relationship_type in [
        RelationshipType.CHILD_PARENT,
        RelationshipType.ONE_TO_ONE,
    ]:
        relationships = Relationship.list(relationship_type=relationship_type)
        assert relationships.shape[0] == 0


def test_update_relationship_type(
    saved_event_table, transaction_entity, cust_id_entity, check_relationship
):
    """
    Test update relationship type
    """
    # Establish a child parent relationship in event table
    assert saved_event_table.event_id_column == "col_int"
    saved_event_table.col_int.as_entity(transaction_entity.name)
    saved_event_table.cust_id.as_entity(cust_id_entity.name)

    # Check the initial relationship type (child-parent)
    relationship = check_relationship(RelationshipType.CHILD_PARENT, [cust_id_entity.id])

    # Update the relationship type to one-to-one
    relationship.update_relationship_type(RelationshipType.ONE_TO_ONE)
    check_relationship(RelationshipType.ONE_TO_ONE, [])

    # Update the relationship type back to child-parent
    relationship.update_relationship_type(RelationshipType.CHILD_PARENT)
    check_relationship(RelationshipType.CHILD_PARENT, [cust_id_entity.id])


def test_update_relationship_type_and_retag_primary_entity(
    saved_event_table, transaction_entity, cust_id_entity, check_relationship
):
    """
    Test update relationship type and re-tag primary entity
    """
    # Establish a child parent relationship in event table
    assert saved_event_table.event_id_column == "col_int"
    saved_event_table.col_int.as_entity(transaction_entity.name)
    saved_event_table.cust_id.as_entity(cust_id_entity.name)

    # Update the relationship type to one-to-one
    relationships = Relationship.list()
    relationship = Relationship.get_by_id(relationships.iloc[0].id)
    relationship.update_relationship_type(RelationshipType.ONE_TO_ONE)

    # Tag primary entity without untagging yet
    saved_event_table.col_int.as_entity(transaction_entity.name)

    # Untag primary entity
    saved_event_table.col_int.as_entity(None)
    check_no_relationships()

    # Retag primary entity
    saved_event_table.col_int.as_entity(transaction_entity.name)
    check_relationship(RelationshipType.CHILD_PARENT, [cust_id_entity.id])


def test_update_relationship_type_and_retag_parent_entity(
    saved_event_table, transaction_entity, cust_id_entity, check_relationship
):
    """
    Test update relationship type and re-tag parent entity
    """
    # Establish a child parent relationship in event table
    assert saved_event_table.event_id_column == "col_int"
    saved_event_table.col_int.as_entity(transaction_entity.name)
    saved_event_table.cust_id.as_entity(cust_id_entity.name)

    # Update the relationship type to one-to-one
    relationships = Relationship.list()
    relationship = Relationship.get_by_id(relationships.iloc[0].id)
    relationship.update_relationship_type(RelationshipType.ONE_TO_ONE)

    # Tag primary entity without untagging yet
    saved_event_table.cust_id.as_entity(cust_id_entity.name)

    # Untag parent entity
    saved_event_table.cust_id.as_entity(None)
    check_no_relationships()

    # Retag parent entity
    saved_event_table.cust_id.as_entity(cust_id_entity.name)
    check_relationship(RelationshipType.CHILD_PARENT, [cust_id_entity.id])


def assert_relationships_equal(expected):
    """
    Helper function to assert relationships are equal
    """
    relationships = Relationship.list()
    actual = relationships[
        [
            "entity",
            "related_entity",
            "relationship_type",
            "relationship_status",
            "relation_table",
        ]
    ].to_dict(orient="records")
    actual = sorted(actual, key=lambda x: (x["entity"], x["related_entity"]))
    assert actual == expected


def assert_ancestor_ids_equal(expected: dict[str, list[ObjectId]]):
    """
    Helper function to assert ancestor ids are equal
    """
    Entity._cache.clear()
    actual = {}
    for _, entity in Entity.list().iterrows():
        entity_name = entity["name"]
        entity = Entity.get(entity_name)
        actual[entity_name] = sorted(entity.ancestor_ids)
    for k in expected:
        expected[k] = sorted(expected[k])
    assert actual == expected


def get_relationship_object_by_entities(
    entity_name: str,
    related_entity_name: str,
):
    """
    Helper function to get relationship object by entity names
    """
    relationships = Relationship.list()
    for relationship_record in relationships.to_dict(orient="records"):
        if (
            relationship_record["entity"] == entity_name
            and relationship_record["related_entity"] == related_entity_name
        ):
            relationship_obj = Relationship.get_by_id(relationship_record["id"])
            return relationship_obj
    raise AssertionError("Expected relationship not found")


def test_two_entities_with_shortcut(
    saved_event_table,
    saved_scd_table,
    transaction_entity,
    cust_id_entity,
    another_entity,
):
    """
    Test multiple paths between entities

    A: transaction_entity
    B: cust_id_entity
    C: another_entity

    A -> B -> C
    A -> C (shortcut)
    """
    # Establish A -> B, A -> C
    saved_event_table.col_int.as_entity(transaction_entity.name)
    saved_event_table.col_text.as_entity(cust_id_entity.name)
    saved_event_table.cust_id.as_entity(another_entity.name)
    assert_relationships_equal([
        {
            "entity": "transaction",
            "related_entity": "another",
            "relationship_type": "child_parent",
            "relationship_status": "inferred",
            "relation_table": "sf_event_table",
        },
        {
            "entity": "transaction",
            "related_entity": "customer",
            "relationship_type": "child_parent",
            "relationship_status": "inferred",
            "relation_table": "sf_event_table",
        },
    ])
    assert_ancestor_ids_equal({
        "transaction": [cust_id_entity.id, another_entity.id],
        "customer": [],
        "another": [],
    })

    # Establish B -> C
    saved_scd_table.col_text.as_entity(cust_id_entity.name)
    saved_scd_table.cust_id.as_entity(another_entity.name)
    assert_relationships_equal([
        {
            "entity": "customer",
            "related_entity": "another",
            "relationship_type": "child_parent",
            "relationship_status": "inferred",
            "relation_table": "sf_scd_table",
        },
        {
            "entity": "transaction",
            "related_entity": "another",
            "relationship_type": "child_parent_shortcut",
            "relationship_status": "inferred",
            "relation_table": "sf_event_table",
        },
        {
            "entity": "transaction",
            "related_entity": "customer",
            "relationship_type": "child_parent",
            "relationship_status": "inferred",
            "relation_table": "sf_event_table",
        },
    ])
    assert_ancestor_ids_equal({
        "transaction": [cust_id_entity.id, another_entity.id],
        "customer": [another_entity.id],
        "another": [],
    })


def test_two_entities_with_conflict(
    snowflake_scd_table,
    snowflake_scd_table_v2,
    cust_id_entity,
    another_entity,
):
    """
    Test cycles in entity relationships. This causes the affected relationships to be marked as
    'conflict', but entity tagging is still allowed

    A: cust_id_entity
    B: another_entity

    T1: A -> B
    T2: B -> A
    """
    # Establish T1: A -> B
    snowflake_scd_table.col_text.as_entity(cust_id_entity.name)
    snowflake_scd_table.cust_id.as_entity(another_entity.name)

    # Establish T2: B -> A
    snowflake_scd_table_v2.col_text.as_entity(another_entity.name)
    snowflake_scd_table_v2.cust_id.as_entity(cust_id_entity.name)

    # No errors during entity tagging. But relationships become:
    assert_relationships_equal([
        {
            "entity": "another",
            "related_entity": "customer",
            "relationship_type": "disabled",
            "relationship_status": "conflict",
            "relation_table": "sf_scd_table_v2",
        },
        {
            "entity": "customer",
            "related_entity": "another",
            "relationship_type": "child_parent",
            "relationship_status": "inferred",
            "relation_table": "sf_scd_table",
        },
    ])
    assert_ancestor_ids_equal({
        "customer": [another_entity.id],
        "another": [],
    })


@pytest.mark.parametrize("tag_primary_key_first", [True, False])
def test_non_shortcut_multiple_relationships_not_allowed(
    snowflake_item_table,
    saved_event_table,
    saved_scd_table,
    item_entity,
    transaction_entity,
    cust_id_entity,
    another_entity,
    tag_primary_key_first,
):
    """
    Test that non-shortcut multiple paths between entities are not allowed

    A: item_entity
    B: transaction_entity
    C: cust_id
    D: another_entity

    A -> B -> C
    A -> D -> C
    """
    # Establish A -> B, A -> D
    snowflake_item_table.item_id_col.as_entity(item_entity.name)
    snowflake_item_table.event_id_col.as_entity(transaction_entity.name)
    snowflake_item_table.item_type.as_entity(another_entity.name)

    # Establish B -> C
    saved_event_table.cust_id.as_entity(None)  # Clear any existing tagging
    saved_event_table.col_int.as_entity(transaction_entity.name)
    saved_event_table.col_text.as_entity(cust_id_entity.name)

    expected_relationships = [
        {
            "entity": "item",
            "related_entity": "another",
            "relationship_type": "child_parent",
            "relationship_status": "inferred",
            "relation_table": "sf_item_table",
        },
        {
            "entity": "item",
            "related_entity": "transaction",
            "relationship_type": "child_parent",
            "relationship_status": "inferred",
            "relation_table": "sf_item_table",
        },
        {
            "entity": "transaction",
            "related_entity": "customer",
            "relationship_type": "child_parent",
            "relationship_status": "inferred",
            "relation_table": "sf_event_table",
        },
    ]
    expected_ancestor_ids = {
        "another": [],
        "transaction": [cust_id_entity.id],
        "item": [
            transaction_entity.id,
            another_entity.id,
            cust_id_entity.id,
        ],
        "customer": [],
    }
    assert_relationships_equal(expected_relationships)
    assert_ancestor_ids_equal(expected_ancestor_ids)

    # Establish D -> C to trigger error
    with pytest.raises(RecordUpdateException) as exc:
        if tag_primary_key_first:
            saved_scd_table.col_text.as_entity(another_entity.name)
            saved_scd_table.cust_id.as_entity(cust_id_entity.name)
        else:
            saved_scd_table.cust_id.as_entity(cust_id_entity.name)
            saved_scd_table.col_text.as_entity(another_entity.name)

    assert "Invalid entity tagging detected" in str(exc.value)

    assert_relationships_equal(expected_relationships)
    assert_ancestor_ids_equal(expected_ancestor_ids)


@pytest.mark.parametrize("untag_child_or_parent_entity", ["child", "parent"])
def test_untag_entities_update_shortcut_to_parent_child(
    saved_event_table,
    saved_scd_table,
    transaction_entity,
    cust_id_entity,
    another_entity,
    untag_child_or_parent_entity,
):
    """
    Test untagging entities with multiple paths between entities

    A: transaction_entity
    B: cust_id_entity
    C: another_entity

    Initial:
    A -> B -> C
    A -> C (shortcut)

    After untagging B -> C:
    A -> B
    A -> C (non-shortcut)
    """
    saved_event_table.col_int.as_entity(transaction_entity.name)
    saved_event_table.col_text.as_entity(cust_id_entity.name)
    saved_event_table.cust_id.as_entity(another_entity.name)
    saved_scd_table.col_text.as_entity(cust_id_entity.name)
    saved_scd_table.cust_id.as_entity(another_entity.name)

    # Untag B -> C, now A -> C becomes a parent-child relationship again
    if untag_child_or_parent_entity == "parent":
        saved_scd_table.cust_id.as_entity(None)
    else:
        saved_scd_table.col_text.as_entity(None)

    assert_relationships_equal([
        {
            "entity": "transaction",
            "related_entity": "another",
            "relationship_type": "child_parent",
            "relationship_status": "inferred",
            "relation_table": "sf_event_table",
        },
        {
            "entity": "transaction",
            "related_entity": "customer",
            "relationship_type": "child_parent",
            "relationship_status": "inferred",
            "relation_table": "sf_event_table",
        },
    ])
    assert_ancestor_ids_equal({
        "transaction": [cust_id_entity.id, another_entity.id],
        "customer": [],
        "another": [],
    })


def test_untag_entities_update_relation_table(
    snowflake_scd_table,
    snowflake_scd_table_v2,
    cust_id_entity,
    another_entity,
):
    """
    Test untagging entities with multiple paths between entities

    A: cust_id_entity
    B: another_entity

    Initial:
    T1: A -> B (active relation table)
    T2: A -> B

    After untagging A -> B in T1:
    T2: A -> B (active relation table)
    """
    # Establish relationships in both SCD tables
    snowflake_scd_table.col_text.as_entity(cust_id_entity.name)
    snowflake_scd_table.cust_id.as_entity(another_entity.name)
    snowflake_scd_table_v2.col_text.as_entity(cust_id_entity.name)
    snowflake_scd_table_v2.cust_id.as_entity(another_entity.name)
    assert_relationships_equal([
        {
            "entity": "customer",
            "related_entity": "another",
            "relationship_type": "child_parent",
            "relationship_status": "to_review",
            "relation_table": "sf_scd_table",
        }
    ])

    # Untag A -> B in the first SCD table
    snowflake_scd_table.cust_id.as_entity(None)

    assert_relationships_equal([
        {
            "entity": "customer",
            "related_entity": "another",
            "relationship_type": "child_parent",
            "relationship_status": "to_review",
            "relation_table": "sf_scd_table_v2",
        }
    ])
    assert_ancestor_ids_equal({
        "customer": [another_entity.id],
        "another": [],
    })


@pytest.mark.parametrize(
    "relationship_type", [RelationshipType.DISABLED, RelationshipType.ONE_TO_ONE]
)
def test_untag_entities_with_non_parent_child_relationships__untag_related_key(
    saved_event_table,
    saved_scd_table,
    transaction_entity,
    cust_id_entity,
    another_entity,
    relationship_type,
):
    """
    Test untagging entities with non parent-child relationships (related key untagged)

    A: transaction_entity
    B: cust_id_entity
    C: another_entity

    Initial:
    A -> B (child_parent)
    A -> C (one_to_one / disabled)

    Untag C.
    """
    # Establish A -> B and A -> C
    saved_event_table.col_int.as_entity(transaction_entity.name)
    saved_event_table.col_text.as_entity(cust_id_entity.name)
    saved_event_table.cust_id.as_entity(another_entity.name)

    # Update A -> C to non parent-child relationship
    relationship = get_relationship_object_by_entities(
        entity_name=transaction_entity.name,
        related_entity_name=another_entity.name,
    )
    relationship.update_relationship_type(relationship_type)
    assert_relationships_equal([
        {
            "entity": "transaction",
            "related_entity": "another",
            "relationship_type": relationship_type,
            "relationship_status": "inferred",
            "relation_table": "sf_event_table",
        },
        {
            "entity": "transaction",
            "related_entity": "customer",
            "relationship_type": "child_parent",
            "relationship_status": "inferred",
            "relation_table": "sf_event_table",
        },
    ])
    assert_ancestor_ids_equal(
        {
            "transaction": [cust_id_entity.id],
            "customer": [],
            "another": [],
        },
    )

    # Untag A -> C
    saved_event_table.cust_id.as_entity(None)

    assert_relationships_equal([
        {
            "entity": "transaction",
            "related_entity": "customer",
            "relationship_type": "child_parent",
            "relationship_status": "inferred",
            "relation_table": "sf_event_table",
        }
    ])
    assert_ancestor_ids_equal(
        {
            "transaction": [cust_id_entity.id],
            "customer": [],
            "another": [],
        },
    )


@pytest.mark.parametrize(
    "relationship_type", [RelationshipType.DISABLED, RelationshipType.ONE_TO_ONE]
)
def test_untag_entities_with_non_parent_child_relationships__untag_primary_key(
    saved_event_table,
    saved_scd_table,
    transaction_entity,
    cust_id_entity,
    another_entity,
    relationship_type,
):
    """
    Test untagging entities with non parent-child relationships (primary key untagged)

    A: transaction_entity
    B: cust_id_entity
    C: another_entity

    Initial:
    T1: A -> B
    T2: B -> C (one_to_one / disabled)

    Untag B in T2.
    """
    # Establish T1: A -> B
    saved_event_table.col_int.as_entity(transaction_entity.name)
    saved_event_table.col_text.as_entity(cust_id_entity.name)

    # Establish T2: B -> C
    saved_scd_table.col_text.as_entity(cust_id_entity.name)
    saved_scd_table.cust_id.as_entity(another_entity.name)

    # Update T2: B -> C to non parent-child relationship
    relationship = get_relationship_object_by_entities(
        entity_name=cust_id_entity.name,
        related_entity_name=another_entity.name,
    )
    relationship.update_relationship_type(relationship_type)
    assert_relationships_equal([
        {
            "entity": "customer",
            "related_entity": "another",
            "relationship_type": relationship_type,
            "relationship_status": "inferred",
            "relation_table": "sf_scd_table",
        },
        {
            "entity": "transaction",
            "related_entity": "customer",
            "relationship_type": "child_parent",
            "relationship_status": "inferred",
            "relation_table": "sf_event_table",
        },
    ])
    assert_ancestor_ids_equal(
        {
            "transaction": [cust_id_entity.id],
            "customer": [],
            "another": [],
        },
    )

    # Untag B in T2
    saved_scd_table.col_text.as_entity(None)

    assert_relationships_equal([
        {
            "entity": "transaction",
            "related_entity": "customer",
            "relationship_type": "child_parent",
            "relationship_status": "inferred",
            "relation_table": "sf_event_table",
        }
    ])
    assert_ancestor_ids_equal(
        {
            "transaction": [cust_id_entity.id],
            "customer": [],
            "another": [],
        },
    )


def test_update_relationship_type_to_disable_with_shortcut(
    saved_event_table,
    saved_scd_table,
    transaction_entity,
    cust_id_entity,
    another_entity,
):
    """
    Test updating relationship type with multiple paths between entities

    A: transaction_entity
    B: cust_id_entity
    C: another_entity

    Initial:
    A -> B -> C
    A -> C (shortcut)

    After changing B -> C to disabled:
    A -> B
    A -> C (non-shortcut)
    """
    # Establish A -> B -> C and A -> C shortcut
    saved_event_table.col_int.as_entity(transaction_entity.name)
    saved_event_table.col_text.as_entity(cust_id_entity.name)
    saved_event_table.cust_id.as_entity(another_entity.name)
    saved_scd_table.col_text.as_entity(cust_id_entity.name)
    saved_scd_table.cust_id.as_entity(another_entity.name)

    # Update B -> C to disabled, now A -> C becomes a parent-child relationship again
    relationship = get_relationship_object_by_entities(
        entity_name=cust_id_entity.name,
        related_entity_name=another_entity.name,
    )
    relationship.update_relationship_type(RelationshipType.DISABLED)

    assert_relationships_equal([
        {
            "entity": "customer",
            "related_entity": "another",
            "relationship_type": "disabled",
            "relationship_status": "inferred",
            "relation_table": "sf_scd_table",
        },
        {
            "entity": "transaction",
            "related_entity": "another",
            "relationship_type": "child_parent",
            "relationship_status": "inferred",
            "relation_table": "sf_event_table",
        },
        {
            "entity": "transaction",
            "related_entity": "customer",
            "relationship_type": "child_parent",
            "relationship_status": "inferred",
            "relation_table": "sf_event_table",
        },
    ])
    assert_ancestor_ids_equal(
        {
            "customer": [],
            "another": [],
            "transaction": [cust_id_entity.id, another_entity.id],
        },
    )
