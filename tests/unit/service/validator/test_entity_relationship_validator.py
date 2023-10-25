"""
Test entity relationship validator
"""
import pytest
from bson import ObjectId

from featurebyte.exception import EntityRelationshipConflictError
from featurebyte.models.feature import EntityRelationshipInfo
from featurebyte.models.relationship import RelationshipType
from featurebyte.service.validator.entity_relationship_validator import (
    FeatureListEntityRelationshipValidator,
)


@pytest.fixture(name="son_entity_id")
def son_entity_id_fixture():
    """
    Get son entity ID
    """
    return ObjectId("653878db7b71f90acc09311d")


@pytest.fixture(name="father_entity_id")
def father_entity_id_fixture():
    """
    Get father entity ID
    """
    return ObjectId("653878db7b71f90acc09311a")


@pytest.fixture(name="mother_entity_id")
def mother_entity_id_fixture():
    """
    Get mother entity ID
    """
    return ObjectId("653878db7b71f90acc09312a")


@pytest.fixture(name="grandpa_entity_id")
def grandpa_entity_id_fixture():
    """
    Get grandpa entity ID
    """
    return ObjectId("653878db7b71f90acc09311b")


@pytest.fixture(name="granny_entity_id")
def granny_entity_id_fixture():
    """
    Get granny entity ID
    """
    return ObjectId("653878db7b71f90acc09311c")


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


def test_validator__case_1(
    relationship_grandpa_father, relationship_granny_father, relationship_father_son
):
    """Test validator case 1"""
    first_relationships = [relationship_grandpa_father, relationship_father_son]
    second_relationships = [relationship_granny_father, relationship_father_son]
    validator = FeatureListEntityRelationshipValidator()
    validator.validate(first_relationships, feature_name="feat1")
    validator.validate(second_relationships, feature_name="feat2")


def test_validator__case_2(relationship_grandpa_father, relationship_mother_son):
    """Test validator case 2"""
    validator = FeatureListEntityRelationshipValidator()
    validator.validate([relationship_grandpa_father], feature_name="feat1")
    validator.validate([relationship_mother_son], feature_name="feat2")


def test_validator__conflict_case_1(
    relationship_grandpa_father, grandpa_entity_id, father_entity_id
):
    """Test validator conflict case 1"""
    validator = FeatureListEntityRelationshipValidator()
    validator.validate([relationship_grandpa_father], feature_name="feat1")

    conflict_relationship = EntityRelationshipInfo(
        relationship_type=RelationshipType.CHILD_PARENT,
        entity_id=relationship_grandpa_father.related_entity_id,
        related_entity_id=relationship_grandpa_father.entity_id,
        relation_table_id=ObjectId(),
    )
    with pytest.raises(EntityRelationshipConflictError) as exc:
        validator.validate([conflict_relationship], feature_name="feat2")

    expected_msg = (
        f"Entity {grandpa_entity_id} is an ancestor of {father_entity_id} "
        "(based on features: ['feat1']) but feature 'feat2' has a child-parent relationship between them."
    )
    assert expected_msg in str(exc.value)


def test_validator__conflict_case_2(
    relationship_grandpa_father,
    relationship_father_son,
    grandpa_entity_id,
    father_entity_id,
    son_entity_id,
):
    """Test validator conflict case 2"""
    validator = FeatureListEntityRelationshipValidator()
    validator.validate([relationship_grandpa_father], feature_name="feat1")
    validator.validate([relationship_father_son], feature_name="feat2")

    conflict_relationship = EntityRelationshipInfo(
        relationship_type=RelationshipType.CHILD_PARENT,
        entity_id=relationship_grandpa_father.related_entity_id,
        related_entity_id=relationship_father_son.entity_id,
        relation_table_id=ObjectId(),
    )
    with pytest.raises(EntityRelationshipConflictError) as exc:
        validator.validate([conflict_relationship], feature_name="feat3")

    expected_msg = (
        f"Entity {grandpa_entity_id} is an ancestor of {son_entity_id} (based on features: ['feat1', 'feat2']) "
        f"but feature 'feat3' has a child-parent relationship between them."
    )
    assert expected_msg in str(exc.value)

    conflict_relationship = EntityRelationshipInfo(
        relationship_type=RelationshipType.CHILD_PARENT,
        entity_id=father_entity_id,
        related_entity_id=son_entity_id,
        relation_table_id=ObjectId(),
    )
    with pytest.raises(EntityRelationshipConflictError) as exc:
        validator.validate([conflict_relationship], feature_name="feat3")

    expected_msg = (
        f"Entity {father_entity_id} is an ancestor of {son_entity_id} (based on features: ['feat2']) "
        f"but feature 'feat3' has a child-parent relationship between them."
    )
    assert expected_msg in str(exc.value)
