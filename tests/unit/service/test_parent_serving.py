"""
Unit tests for ParentEntityLookupService
"""
import pytest

from featurebyte.exception import EntityJoinPathNotFoundError
from featurebyte.models.entity_validation import EntityInfo
from featurebyte.models.parent_serving import JoinStep


@pytest.mark.asyncio
async def test_get_join_steps__no_op(entity_a, entity_b, parent_entity_lookup_service):
    """
    Test no joins required as all required entities are provided
    """
    entity_info = EntityInfo(
        required_entities=[entity_a, entity_b], provided_entities=[entity_a, entity_b]
    )
    join_steps = await parent_entity_lookup_service.get_required_join_steps(entity_info)
    assert join_steps == []


@pytest.mark.asyncio
async def test_get_join_steps__not_found(entity_a, entity_b, parent_entity_lookup_service):
    """
    Test no path can be found because no valid relationships are registered
    """
    entity_info = EntityInfo(required_entities=[entity_a, entity_b], provided_entities=[entity_a])
    with pytest.raises(EntityJoinPathNotFoundError):
        _ = await parent_entity_lookup_service.get_required_join_steps(entity_info)


@pytest.mark.asyncio
async def test_get_join_steps__one_step(
    entity_a, entity_b, b_is_parent_of_a, parent_entity_lookup_service
):
    """
    Test looking up parent entity in one join
    """
    data = b_is_parent_of_a
    entity_info = EntityInfo(required_entities=[entity_a, entity_b], provided_entities=[entity_a])
    join_steps = await parent_entity_lookup_service.get_required_join_steps(entity_info)
    assert join_steps == [
        JoinStep(
            data=data.dict(by_alias=True),
            parent_key="b",
            child_key="a",
        )
    ]


@pytest.mark.asyncio
async def test_get_join_steps__two_steps(
    entity_a,
    entity_c,
    b_is_parent_of_a,
    c_is_parent_of_b,
    parent_entity_lookup_service,
):
    """
    Test looking up parent entity in two joins
    """
    data_a_to_b = b_is_parent_of_a
    data_b_to_c = c_is_parent_of_b
    entity_info = EntityInfo(required_entities=[entity_a, entity_c], provided_entities=[entity_a])
    join_steps = await parent_entity_lookup_service.get_required_join_steps(entity_info)
    assert join_steps == [
        JoinStep(
            data=data_a_to_b.dict(by_alias=True),
            parent_key="b",
            child_key="a",
        ),
        JoinStep(
            data=data_b_to_c.dict(by_alias=True),
            parent_key="c",
            child_key="b",
        ),
    ]
