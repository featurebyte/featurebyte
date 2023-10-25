"""
Test entity relationship validator
"""
import pytest
import pytest_asyncio
from bson import ObjectId

from featurebyte.exception import EntityRelationshipConflictError
from featurebyte.models.feature import EntityRelationshipInfo
from featurebyte.models.relationship import RelationshipType
from featurebyte.schema.entity import EntityCreate
from featurebyte.service.validator.entity_relationship_validator import (
    FeatureListEntityRelationshipValidator,
)


@pytest.fixture(name="entity_relationship_validator")
def entity_relationship_validator_fixture(entity_service):
    """
    Get entity relationship validator
    """
    return FeatureListEntityRelationshipValidator(entity_service)


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


@pytest.fixture(name="relationship_grandpa_father")
def relationship_grandpa_father_fixture(father_entity_id, grandpa_entity_id):
    """
    Get a relationship between father and grandpa
    """
    return EntityRelationshipInfo(
        relationship_type=RelationshipType.CHILD_PARENT,
        entity_id=father_entity_id,
        related_entity_id=grandpa_entity_id,
        relation_table_id=ObjectId("653878db7b71f90acc09311e"),
    )


@pytest.fixture(name="relationship_granny_father")
def relationship_granny_father_fixture(father_entity_id, granny_entity_id):
    """
    Get a relationship between father and granny
    """
    return EntityRelationshipInfo(
        relationship_type=RelationshipType.CHILD_PARENT,
        entity_id=father_entity_id,
        related_entity_id=granny_entity_id,
        relation_table_id=ObjectId("653878db7b71f90acc09311f"),
    )


@pytest.fixture(name="relationship_father_son")
def relationship_father_son_fixture(son_entity_id, father_entity_id):
    """
    Get a relationship between son and father
    """
    return EntityRelationshipInfo(
        relationship_type=RelationshipType.CHILD_PARENT,
        entity_id=son_entity_id,
        related_entity_id=father_entity_id,
        relation_table_id=ObjectId("653878db7b71f90acc093120"),
    )


@pytest.fixture(name="relationship_mother_son")
def relationship_mother_son_fixture(son_entity_id, mother_entity_id):
    """
    Get a relationship between son and mother
    """
    return EntityRelationshipInfo(
        relationship_type=RelationshipType.CHILD_PARENT,
        entity_id=son_entity_id,
        related_entity_id=mother_entity_id,
        relation_table_id=ObjectId("653878db7b71f90acc093121"),
    )


@pytest.mark.asyncio
async def test_validator__case_1(
    entity_relationship_validator,
    relationship_grandpa_father,
    relationship_granny_father,
    relationship_father_son,
):
    """Test validator case 1"""
    first_relationships = [relationship_grandpa_father, relationship_father_son]
    second_relationships = [relationship_granny_father, relationship_father_son]
    validator = entity_relationship_validator
    await validator.validate(first_relationships, feature_name="feat1")
    await validator.validate(second_relationships, feature_name="feat2")


@pytest.mark.asyncio
async def test_validator__case_2(
    entity_relationship_validator, relationship_grandpa_father, relationship_mother_son
):
    """Test validator case 2"""
    validator = entity_relationship_validator
    await validator.validate([relationship_grandpa_father], feature_name="feat1")
    await validator.validate([relationship_mother_son], feature_name="feat2")


@pytest.mark.asyncio
async def test_validator__conflict_case_1(
    entity_relationship_validator,
    relationship_grandpa_father,
):
    """Test validator conflict case 1"""
    validator = entity_relationship_validator
    await validator.validate([relationship_grandpa_father], feature_name="feat1")

    conflict_relationship = EntityRelationshipInfo(
        relationship_type=RelationshipType.CHILD_PARENT,
        entity_id=relationship_grandpa_father.related_entity_id,
        related_entity_id=relationship_grandpa_father.entity_id,
        relation_table_id=ObjectId(),
    )
    with pytest.raises(EntityRelationshipConflictError) as exc:
        await validator.validate([conflict_relationship], feature_name="feat2")

    expected_msg = (
        "Entity 'grandpa' is an ancestor of 'father' "
        "(based on features: ['feat1']) but feature 'feat2' has a child-parent relationship between them."
    )
    assert expected_msg in str(exc.value)


@pytest.mark.asyncio
async def test_validator__conflict_case_2(
    entity_relationship_validator,
    relationship_grandpa_father,
    relationship_father_son,
    father_entity_id,
    son_entity_id,
):
    """Test validator conflict case 2"""
    validator = entity_relationship_validator
    await validator.validate([relationship_grandpa_father], feature_name="feat1")
    await validator.validate([relationship_father_son], feature_name="feat2")

    conflict_relationship = EntityRelationshipInfo(
        relationship_type=RelationshipType.CHILD_PARENT,
        entity_id=relationship_grandpa_father.related_entity_id,
        related_entity_id=relationship_father_son.entity_id,
        relation_table_id=ObjectId(),
    )
    with pytest.raises(EntityRelationshipConflictError) as exc:
        await validator.validate([conflict_relationship], feature_name="feat3")

    expected_msg = (
        "Entity 'grandpa' is an ancestor of 'son' (based on features: ['feat1', 'feat2']) "
        "but feature 'feat3' has a child-parent relationship between them."
    )
    assert expected_msg in str(exc.value)

    conflict_relationship = EntityRelationshipInfo(
        relationship_type=RelationshipType.CHILD_PARENT,
        entity_id=father_entity_id,
        related_entity_id=son_entity_id,
        relation_table_id=ObjectId(),
    )
    with pytest.raises(EntityRelationshipConflictError) as exc:
        await validator.validate([conflict_relationship], feature_name="feat3")

    expected_msg = (
        "Entity 'father' is an ancestor of 'son' (based on features: ['feat2']) "
        "but feature 'feat3' has a child-parent relationship between them."
    )
    assert expected_msg in str(exc.value)
