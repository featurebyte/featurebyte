"""
Fixtures for entity relationship related tests
"""

import pytest
import pytest_asyncio
from bson import ObjectId

from featurebyte.enum import TableDataType
from featurebyte.models.entity import ParentEntity
from featurebyte.models.relationship import RelationshipType
from featurebyte.schema.entity import EntityCreate
from featurebyte.schema.relationship_info import RelationshipInfoCreate


@pytest.fixture(name="relationship_info_service")
def relationship_info_service_fixture(app_container):
    """
    RelationshipInfoService fixture
    """
    return app_container.relationship_info_service


@pytest.fixture(name="entity_relationship_service")
def entity_relationship_service_fixture(app_container):
    """
    EntityRelationshipService fixture
    """
    return app_container.entity_relationship_service


@pytest_asyncio.fixture(name="son_entity_id")
async def son_entity_id_fixture(entity_service):
    """
    Get son entity ID
    """
    entity_id = ObjectId("653878db7b71f90acc09311d")
    await entity_service.create_document(
        data=EntityCreate(
            _id=entity_id,
            name="son",
            serving_name="son",
        )
    )
    return entity_id


@pytest_asyncio.fixture(name="father_entity_id")
async def father_entity_id_fixture(entity_service):
    """
    Get father entity ID
    """
    entity_id = ObjectId("653878db7b71f90acc09311a")
    await entity_service.create_document(
        data=EntityCreate(
            _id=entity_id,
            name="father",
            serving_name="father",
        )
    )
    return entity_id


@pytest_asyncio.fixture(name="mother_entity_id")
async def mother_entity_id_fixture(entity_service):
    """
    Get mother entity ID
    """
    entity_id = ObjectId("653878db7b71f90acc09312a")
    await entity_service.create_document(
        data=EntityCreate(
            _id=entity_id,
            name="mother",
            serving_name="mother",
        )
    )
    return entity_id


@pytest_asyncio.fixture(name="grandpa_entity_id")
async def grandpa_entity_id_fixture(entity_service):
    """
    Get grandpa entity ID
    """
    entity_id = ObjectId("653878db7b71f90acc09311b")
    await entity_service.create_document(
        data=EntityCreate(
            _id=entity_id,
            name="grandpa",
            serving_name="grandpa",
        )
    )
    return entity_id


@pytest_asyncio.fixture(name="granny_entity_id")
async def granny_entity_id_fixture(entity_service):
    """
    Get granny entity ID
    """
    entity_id = ObjectId("653878db7b71f90acc09311c")
    await entity_service.create_document(
        data=EntityCreate(
            _id=entity_id,
            name="granny",
            serving_name="granny",
        )
    )
    return entity_id


@pytest_asyncio.fixture(name="relationship_grandpa_father")
async def relationship_grandpa_father_fixture(
    relationship_info_service,
    entity_relationship_service,
    father_entity_id,
    grandpa_entity_id,
    user_id,
):
    """
    Get a relationship between father and grandpa
    """
    table_id = ObjectId()
    await entity_relationship_service.add_relationship(
        parent=ParentEntity(id=grandpa_entity_id, table_id=table_id, table_type=TableDataType.DIMENSION_TABLE),
        child_id=father_entity_id,
    )
    create_payload = RelationshipInfoCreate(
        name="grandpa_father_relationship",
        relationship_type=RelationshipType.CHILD_PARENT,
        entity_id=father_entity_id,
        related_entity_id=grandpa_entity_id,
        relation_table_id=table_id,
        enabled=True,
        updated_by=user_id,
    )
    relationship = await relationship_info_service.create_document(data=create_payload)
    return relationship


@pytest_asyncio.fixture(name="relationship_granny_father")
async def relationship_granny_father_fixture(
    relationship_info_service,
    entity_relationship_service,
    father_entity_id,
    granny_entity_id,
    user_id,
):
    """
    Get a relationship between father and granny
    """
    table_id = ObjectId()
    await entity_relationship_service.add_relationship(
        parent=ParentEntity(id=granny_entity_id, table_id=table_id, table_type=TableDataType.DIMENSION_TABLE),
        child_id=father_entity_id,
    )
    create_payload = RelationshipInfoCreate(
        name="granny_father_relationship",
        relationship_type=RelationshipType.CHILD_PARENT,
        entity_id=father_entity_id,
        related_entity_id=granny_entity_id,
        relation_table_id=table_id,
        enabled=True,
        updated_by=user_id,
    )
    relationship = await relationship_info_service.create_document(data=create_payload)
    return relationship


@pytest_asyncio.fixture(name="relationship_father_son")
async def relationship_father_son_fixture(
    relationship_info_service, entity_relationship_service, son_entity_id, father_entity_id, user_id
):
    """
    Get a relationship between son and father
    """
    table_id = ObjectId()
    await entity_relationship_service.add_relationship(
        parent=ParentEntity(id=father_entity_id, table_id=table_id, table_type=TableDataType.DIMENSION_TABLE),
        child_id=son_entity_id,
    )
    create_payload = RelationshipInfoCreate(
        name="father_son_relationship",
        relationship_type=RelationshipType.CHILD_PARENT,
        entity_id=son_entity_id,
        related_entity_id=father_entity_id,
        relation_table_id=table_id,
        enabled=True,
        updated_by=user_id,
    )
    relationship = await relationship_info_service.create_document(data=create_payload)
    return relationship


@pytest_asyncio.fixture(name="relationship_mother_son")
async def relationship_mother_son_fixture(
    relationship_info_service, entity_relationship_service, son_entity_id, mother_entity_id, user_id
):
    """
    Get a relationship between son and mother
    """
    table_id = ObjectId()
    await entity_relationship_service.add_relationship(
        parent=ParentEntity(id=mother_entity_id, table_id=table_id, table_type=TableDataType.DIMENSION_TABLE),
        child_id=son_entity_id,
    )
    create_payload = RelationshipInfoCreate(
        name="mother_son_relationship",
        relationship_type=RelationshipType.CHILD_PARENT,
        entity_id=son_entity_id,
        related_entity_id=mother_entity_id,
        relation_table_id=ObjectId(),
        enabled=True,
        updated_by=user_id,
    )
    relationship = await relationship_info_service.create_document(data=create_payload)
    return relationship
