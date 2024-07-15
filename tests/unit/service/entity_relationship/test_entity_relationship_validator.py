"""
Test entity relationship validator
"""

import pytest
from bson import ObjectId

from featurebyte.exception import EntityRelationshipConflictError
from featurebyte.models.relationship import RelationshipType
from featurebyte.query_graph.model.entity_relationship_info import EntityRelationshipInfo
from featurebyte.service.validator.entity_relationship_validator import (
    FeatureListEntityRelationshipValidator,
)


@pytest.fixture(name="entity_relationship_validator")
def entity_relationship_validator_fixture(entity_service):
    """
    Get entity relationship validator
    """
    return FeatureListEntityRelationshipValidator(entity_service)


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
    await validator._validate(first_relationships, feature_name="feat1")
    await validator._validate(second_relationships, feature_name="feat2")


@pytest.mark.asyncio
async def test_validator__case_2(entity_relationship_validator, relationship_grandpa_father, relationship_mother_son):
    """Test validator case 2"""
    validator = entity_relationship_validator
    await validator._validate([relationship_grandpa_father], feature_name="feat1")
    await validator._validate([relationship_mother_son], feature_name="feat2")


@pytest.mark.asyncio
async def test_validator__conflict_case_1(
    entity_relationship_validator,
    relationship_grandpa_father,
):
    """Test validator conflict case 1"""
    validator = entity_relationship_validator
    await validator._validate([relationship_grandpa_father], feature_name="feat1")

    conflict_relationship = EntityRelationshipInfo(
        relationship_type=RelationshipType.CHILD_PARENT,
        entity_id=relationship_grandpa_father.related_entity_id,
        related_entity_id=relationship_grandpa_father.entity_id,
        relation_table_id=ObjectId(),
    )
    with pytest.raises(EntityRelationshipConflictError) as exc:
        await validator._validate([conflict_relationship], feature_name="feat2")

    expected_msg = (
        "Entity 'grandpa' is an ancestor of 'father' (based on features: ['feat1']) "
        "but 'grandpa' is a child of 'father' based on 'feat2'. "
        "Consider excluding 'feat2' from the Feature List to fix the error."
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
    await validator._validate([relationship_grandpa_father], feature_name="feat1")
    await validator._validate([relationship_father_son], feature_name="feat2")

    conflict_relationship = EntityRelationshipInfo(
        relationship_type=RelationshipType.CHILD_PARENT,
        entity_id=relationship_grandpa_father.related_entity_id,
        related_entity_id=relationship_father_son.entity_id,
        relation_table_id=ObjectId(),
    )
    with pytest.raises(EntityRelationshipConflictError) as exc:
        await validator._validate([conflict_relationship], feature_name="feat3")

    expected_msg = (
        "Entity 'grandpa' is an ancestor of 'son' (based on features: ['feat1', 'feat2']) "
        "but 'grandpa' is a child of 'son' based on 'feat3'. "
        "Consider excluding 'feat3' from the Feature List to fix the error."
    )
    assert expected_msg in str(exc.value)

    conflict_relationship = EntityRelationshipInfo(
        relationship_type=RelationshipType.CHILD_PARENT,
        entity_id=father_entity_id,
        related_entity_id=son_entity_id,
        relation_table_id=ObjectId(),
    )
    with pytest.raises(EntityRelationshipConflictError) as exc:
        await validator._validate([conflict_relationship], feature_name="feat3")

    expected_msg = (
        "Entity 'father' is an ancestor of 'son' (based on features: ['feat2']) "
        "but 'father' is a child of 'son' based on 'feat3'. "
        "Consider excluding 'feat3' from the Feature List to fix the error."
    )
    assert expected_msg in str(exc.value)
