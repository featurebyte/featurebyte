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
            table=data.dict(by_alias=True),
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
            table=data_a_to_b.dict(by_alias=True),
            parent_key="b",
            parent_serving_name="B",
            child_key="a",
            child_serving_name="A",
        ),
        JoinStep(
            table=data_b_to_c.dict(by_alias=True),
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
            table=data_a_to_b.dict(by_alias=True),
            parent_key="b",
            parent_serving_name="B",
            child_key="a",
            child_serving_name="A",
        ),
        JoinStep(
            table=data_b_to_c.dict(by_alias=True),
            parent_key="c",
            parent_serving_name="C",
            child_key="b",
            child_serving_name="B",
        ),
        JoinStep(
            table=data_b_to_d.dict(by_alias=True),
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
            table=data_a_to_b.dict(by_alias=True),
            parent_key="b",
            parent_serving_name="B",
            child_key="a",
            child_serving_name="new_A",
        ),
        JoinStep(
            table=data_b_to_c.dict(by_alias=True),
            parent_key="c",
            parent_serving_name="C",
            child_key="b",
            child_serving_name="B",
        ),
        JoinStep(
            table=data_b_to_d.dict(by_alias=True),
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
    Test looking up parent entity when more than one entity in the join path are provided. In this
    case, to obtain entity D we can perform two joins from entity B. The join path must not involve
    entity A (otherwise B would be duplicated and that would cause the generated sql to be invalid).

    a (provided) --> b (provided) --> c --> d (required)
    """
    _ = b_is_parent_of_a
    data_b_to_c = c_is_parent_of_b
    data_c_to_d = d_is_parent_of_c
    entity_info = EntityInfo(required_entities=[entity_d], provided_entities=[entity_a, entity_b])
    join_steps = await parent_entity_lookup_service.get_required_join_steps(entity_info)
    assert join_steps == [
        JoinStep(
            table=data_b_to_c.dict(by_alias=True),
            parent_key="c",
            parent_serving_name="C",
            child_key="b",
            child_serving_name="B",
        ),
        JoinStep(
            table=data_c_to_d.dict(by_alias=True),
            parent_key="d",
            parent_serving_name="D",
            child_key="c",
            child_serving_name="C",
        ),
    ]


@pytest.mark.asyncio
async def test_required_entity__complex_and_should_not_error(
    entity_a,
    entity_b,
    b_is_parent_of_a,
    d_is_parent_of_c,
    a_is_parent_of_c_and_d,
    parent_entity_lookup_service,
):
    """
    Test a case where lookup parent entity should pass without error

    c --> d --> a (provided) --> b (required)
     `------>´
    """
    data_a_to_b = b_is_parent_of_a
    _ = d_is_parent_of_c
    _ = a_is_parent_of_c_and_d
    entity_info = EntityInfo(required_entities=[entity_b], provided_entities=[entity_a])
    join_steps = await parent_entity_lookup_service.get_required_join_steps(entity_info)
    assert join_steps == [
        JoinStep(
            table=data_a_to_b.dict(by_alias=True),
            parent_key="b",
            parent_serving_name="B",
            child_key="a",
            child_serving_name="A",
        ),
    ]
