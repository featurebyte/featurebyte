"""
Unit tests for ParentEntityLookupService
"""
import pytest

from featurebyte.exception import AmbiguousEntityRelationshipError, EntityJoinPathNotFoundError
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
async def test_get_join_steps__one_step(
    entity_a, entity_b, b_is_parent_of_a, parent_entity_lookup_service
):
    """
    Test looking up parent entity in one join

    a (provided) --> b (required)
    """
    data = b_is_parent_of_a
    entity_info = EntityInfo(required_entities=[entity_a, entity_b], provided_entities=[entity_a])
    join_steps = await parent_entity_lookup_service.get_required_join_steps(entity_info)
    assert join_steps == [
        JoinStep(
            data=data.dict(by_alias=True),
            parent_key="b",
            parent_serving_name="B",
            child_key="a",
            child_serving_name="A",
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

    a (provided) --> b --> c (required)
    """
    data_a_to_b = b_is_parent_of_a
    data_b_to_c = c_is_parent_of_b
    entity_info = EntityInfo(required_entities=[entity_a, entity_c], provided_entities=[entity_a])
    join_steps = await parent_entity_lookup_service.get_required_join_steps(entity_info)
    assert join_steps == [
        JoinStep(
            data=data_a_to_b.dict(by_alias=True),
            parent_key="b",
            parent_serving_name="B",
            child_key="a",
            child_serving_name="A",
        ),
        JoinStep(
            data=data_b_to_c.dict(by_alias=True),
            parent_key="c",
            parent_serving_name="C",
            child_key="b",
            child_serving_name="B",
        ),
    ]


@pytest.mark.asyncio
async def test_get_join_steps__two_branches(
    entity_a,
    entity_c,
    entity_d,
    b_is_parent_of_a,
    c_is_parent_of_b,
    d_is_parent_of_b,
    parent_entity_lookup_service,
):
    """
    Test looking up parent entity in two joins

    a (provided) --> b --> c (required)
                      `--> d (required)
    """
    data_a_to_b = b_is_parent_of_a
    data_b_to_c = c_is_parent_of_b
    data_b_to_d = d_is_parent_of_b
    entity_info = EntityInfo(
        required_entities=[entity_a, entity_c, entity_d], provided_entities=[entity_a]
    )
    join_steps = await parent_entity_lookup_service.get_required_join_steps(entity_info)
    assert join_steps == [
        JoinStep(
            data=data_a_to_b.dict(by_alias=True),
            parent_key="b",
            parent_serving_name="B",
            child_key="a",
            child_serving_name="A",
        ),
        JoinStep(
            data=data_b_to_c.dict(by_alias=True),
            parent_key="c",
            parent_serving_name="C",
            child_key="b",
            child_serving_name="B",
        ),
        JoinStep(
            data=data_b_to_d.dict(by_alias=True),
            parent_key="d",
            parent_serving_name="D",
            child_key="b",
            child_serving_name="B",
        ),
    ]


@pytest.mark.asyncio
async def test_get_join_steps__serving_names_mapping(
    entity_a,
    entity_c,
    entity_d,
    b_is_parent_of_a,
    c_is_parent_of_b,
    d_is_parent_of_b,
    parent_entity_lookup_service,
):
    """
    Test looking up parent entity in two joins

    a (provided) --> b --> c (required)
                      `--> d (required)
    """
    data_a_to_b = b_is_parent_of_a
    data_b_to_c = c_is_parent_of_b
    data_b_to_d = d_is_parent_of_b
    entity_info = EntityInfo(
        required_entities=[entity_a, entity_c, entity_d],
        provided_entities=[entity_a],
        serving_names_mapping={"A": "new_A"},
    )
    join_steps = await parent_entity_lookup_service.get_required_join_steps(entity_info)
    assert join_steps == [
        JoinStep(
            data=data_a_to_b.dict(by_alias=True),
            parent_key="b",
            parent_serving_name="B",
            child_key="a",
            child_serving_name="new_A",
        ),
        JoinStep(
            data=data_b_to_c.dict(by_alias=True),
            parent_key="c",
            parent_serving_name="C",
            child_key="b",
            child_serving_name="B",
        ),
        JoinStep(
            data=data_b_to_d.dict(by_alias=True),
            parent_key="d",
            parent_serving_name="D",
            child_key="b",
            child_serving_name="B",
        ),
    ]


@pytest.mark.asyncio
async def test_get_join_steps__not_found(entity_a, entity_b, parent_entity_lookup_service):
    """
    Test no path can be found because no valid relationships are registered
    """
    entity_info = EntityInfo(required_entities=[entity_a, entity_b], provided_entities=[entity_a])
    with pytest.raises(EntityJoinPathNotFoundError):
        _ = await parent_entity_lookup_service.get_required_join_steps(entity_info)


@pytest.mark.asyncio
async def test_get_join_steps__not_found_with_relationships(
    entity_a,
    entity_c,
    c_is_parent_of_b,
    parent_entity_lookup_service,
):
    """
    Test no path can be found because no valid relationships are registered
    """
    _ = c_is_parent_of_b
    entity_info = EntityInfo(required_entities=[entity_c], provided_entities=[entity_a])
    with pytest.raises(EntityJoinPathNotFoundError):
        _ = await parent_entity_lookup_service.get_required_join_steps(entity_info)


@pytest.mark.asyncio
async def test_get_join_steps__ambiguous_relationships(
    entity_info_with_ambiguous_relationships,
    parent_entity_lookup_service,
):
    """
    Test looking up parent entity when there are ambiguous relationships

    a (provided) --> b --> c ---> e (required)
                      `--> d --´
    """
    with pytest.raises(AmbiguousEntityRelationshipError) as exc_info:
        await parent_entity_lookup_service.get_required_join_steps(
            entity_info_with_ambiguous_relationships
        )
    assert str(exc_info.value) == "Cannot find an unambiguous join path for entity entity_e"


@pytest.mark.asyncio
async def test_get_join_steps__multiple_provided(
    entity_a,
    entity_b,
    entity_d,
    b_is_parent_of_a,
    c_is_parent_of_b,
    d_is_parent_of_c,
    parent_entity_lookup_service,
):
    """
    Test looking up parent entity in two joins

    a (provided) --> b (provided) --> c --> d (required)
    """
    _ = b_is_parent_of_a
    data_b_to_c = c_is_parent_of_b
    data_c_to_d = d_is_parent_of_c
    entity_info = EntityInfo(required_entities=[entity_d], provided_entities=[entity_a, entity_b])
    join_steps = await parent_entity_lookup_service.get_required_join_steps(entity_info)
    assert join_steps == [
        JoinStep(
            data=data_b_to_c.dict(by_alias=True),
            parent_key="c",
            parent_serving_name="C",
            child_key="b",
            child_serving_name="B",
        ),
        JoinStep(
            data=data_c_to_d.dict(by_alias=True),
            parent_key="d",
            parent_serving_name="D",
            child_key="c",
            child_serving_name="C",
        ),
    ]
