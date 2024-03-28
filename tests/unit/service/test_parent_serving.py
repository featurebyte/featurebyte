"""
Unit tests for ParentEntityLookupService
"""

import pytest

from featurebyte.exception import RequiredEntityNotProvidedError
from featurebyte.models.entity_validation import EntityInfo
from featurebyte.models.parent_serving import EntityLookupInfo, EntityLookupStep


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
    data, relationship_info = b_is_parent_of_a
    entity_info = EntityInfo(required_entities=[entity_a, entity_b], provided_entities=[entity_a])
    join_steps = await parent_entity_lookup_service.get_required_join_steps(entity_info)
    assert join_steps == [
        EntityLookupStep(
            id=relationship_info.id,
            table=data.dict(by_alias=True),
            parent=EntityLookupInfo(
                key="b",
                serving_name="B",
                entity_id=entity_b.id,
            ),
            child=EntityLookupInfo(
                key="a",
                serving_name="A",
                entity_id=entity_a.id,
            ),
        )
    ]


@pytest.mark.asyncio
async def test_get_join_steps__two_steps(
    entity_a,
    entity_b,
    entity_c,
    b_is_parent_of_a,
    c_is_parent_of_b,
    parent_entity_lookup_service,
):
    """
    Test looking up parent entity in two joins

    a (provided) --> b --> c (required)
    """
    data_a_to_b, rel_a_to_b = b_is_parent_of_a
    data_b_to_c, rel_b_to_c = c_is_parent_of_b
    entity_info = EntityInfo(required_entities=[entity_a, entity_c], provided_entities=[entity_a])
    join_steps = await parent_entity_lookup_service.get_required_join_steps(entity_info)
    assert join_steps == [
        EntityLookupStep(
            id=rel_a_to_b.id,
            table=data_a_to_b.dict(by_alias=True),
            parent=EntityLookupInfo(
                entity_id=entity_b.id,
                key="b",
                serving_name="B",
            ),
            child=EntityLookupInfo(
                entity_id=entity_a.id,
                key="a",
                serving_name="A",
            ),
        ),
        EntityLookupStep(
            id=rel_b_to_c.id,
            table=data_b_to_c.dict(by_alias=True),
            parent=EntityLookupInfo(
                entity_id=entity_c.id,
                key="c",
                serving_name="C",
            ),
            child=EntityLookupInfo(
                entity_id=entity_b.id,
                key="b",
                serving_name="B",
            ),
        ),
    ]


@pytest.mark.asyncio
async def test_get_join_steps__two_branches(
    entity_a,
    entity_b,
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
    data_a_to_b, rel_a_to_b = b_is_parent_of_a
    data_b_to_c, rel_b_to_c = c_is_parent_of_b
    data_b_to_d, rel_b_to_d = d_is_parent_of_b
    entity_info = EntityInfo(
        required_entities=[entity_a, entity_c, entity_d], provided_entities=[entity_a]
    )
    join_steps = await parent_entity_lookup_service.get_required_join_steps(entity_info)
    assert join_steps == [
        EntityLookupStep(
            id=rel_a_to_b.id,
            table=data_a_to_b.dict(by_alias=True),
            parent=EntityLookupInfo(
                key="b",
                serving_name="B",
                entity_id=entity_b.id,
            ),
            child=EntityLookupInfo(
                key="a",
                serving_name="A",
                entity_id=entity_a.id,
            ),
        ),
        EntityLookupStep(
            id=rel_b_to_d.id,
            table=data_b_to_d.dict(by_alias=True),
            parent=EntityLookupInfo(
                key="d",
                serving_name="D",
                entity_id=entity_d.id,
            ),
            child=EntityLookupInfo(
                key="b",
                serving_name="B",
                entity_id=entity_b.id,
            ),
        ),
        EntityLookupStep(
            id=rel_b_to_c.id,
            table=data_b_to_c.dict(by_alias=True),
            parent=EntityLookupInfo(
                key="c",
                serving_name="C",
                entity_id=entity_c.id,
            ),
            child=EntityLookupInfo(
                key="b",
                serving_name="B",
                entity_id=entity_b.id,
            ),
        ),
    ]


@pytest.mark.asyncio
async def test_get_join_steps__serving_names_mapping(
    entity_a,
    entity_b,
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
    data_a_to_b, rel_a_to_b = b_is_parent_of_a
    data_b_to_c, rel_b_to_c = c_is_parent_of_b
    data_b_to_d, rel_b_to_d = d_is_parent_of_b
    entity_info = EntityInfo(
        required_entities=[entity_a, entity_c, entity_d],
        provided_entities=[entity_a],
        serving_names_mapping={"A": "new_A"},
    )
    join_steps = await parent_entity_lookup_service.get_required_join_steps(entity_info)
    assert join_steps == [
        EntityLookupStep(
            id=rel_a_to_b.id,
            table=data_a_to_b.dict(by_alias=True),
            parent=EntityLookupInfo(
                key="b",
                serving_name="B",
                entity_id=entity_b.id,
            ),
            child=EntityLookupInfo(
                key="a",
                serving_name="new_A",
                entity_id=entity_a.id,
            ),
        ),
        EntityLookupStep(
            id=rel_b_to_d.id,
            table=data_b_to_d.dict(by_alias=True),
            parent=EntityLookupInfo(
                key="d",
                serving_name="D",
                entity_id=entity_d.id,
            ),
            child=EntityLookupInfo(
                key="b",
                serving_name="B",
                entity_id=entity_b.id,
            ),
        ),
        EntityLookupStep(
            id=rel_b_to_c.id,
            table=data_b_to_c.dict(by_alias=True),
            parent=EntityLookupInfo(
                key="c",
                serving_name="C",
                entity_id=entity_c.id,
            ),
            child=EntityLookupInfo(
                key="b",
                serving_name="B",
                entity_id=entity_b.id,
            ),
        ),
    ]


@pytest.mark.asyncio
async def test_get_join_steps__not_found(entity_a, entity_b, parent_entity_lookup_service):
    """
    Test no path can be found because no valid relationships are registered
    """
    entity_info = EntityInfo(required_entities=[entity_a, entity_b], provided_entities=[entity_a])
    with pytest.raises(RequiredEntityNotProvidedError):
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
    with pytest.raises(RequiredEntityNotProvidedError):
        _ = await parent_entity_lookup_service.get_required_join_steps(entity_info)


@pytest.mark.asyncio
async def test_get_join_steps__multiple_provided(
    entity_a,
    entity_b,
    entity_c,
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
    data_b_to_c, rel_b_to_c = c_is_parent_of_b
    data_c_to_d, rel_c_to_d = d_is_parent_of_c
    entity_info = EntityInfo(required_entities=[entity_d], provided_entities=[entity_a, entity_b])
    join_steps = await parent_entity_lookup_service.get_required_join_steps(entity_info)
    assert join_steps == [
        EntityLookupStep(
            id=rel_b_to_c.id,
            table=data_b_to_c.dict(by_alias=True),
            parent=EntityLookupInfo(
                key="c",
                serving_name="C",
                entity_id=entity_c.id,
            ),
            child=EntityLookupInfo(
                key="b",
                serving_name="B",
                entity_id=entity_b.id,
            ),
        ),
        EntityLookupStep(
            id=rel_c_to_d.id,
            table=data_c_to_d.dict(by_alias=True),
            parent=EntityLookupInfo(
                key="d",
                serving_name="D",
                entity_id=entity_d.id,
            ),
            child=EntityLookupInfo(
                key="c",
                serving_name="C",
                entity_id=entity_c.id,
            ),
        ),
    ]


@pytest.mark.asyncio
async def test_get_join_steps__use_provided_relationships(
    entity_a,
    entity_b,
    b_is_parent_of_a,
    c_is_parent_of_b,
    parent_entity_lookup_service,
):
    """
    Test looking up parent entity using provided relationships
    """
    data, relationship_info_1 = b_is_parent_of_a
    _, relationship_info_2 = c_is_parent_of_b
    entity_info = EntityInfo(required_entities=[entity_a, entity_b], provided_entities=[entity_a])

    # Use [b_is_parent_of_a] as the relationships
    join_steps = await parent_entity_lookup_service.get_required_join_steps(
        entity_info, relationships_info=[relationship_info_1]
    )
    assert join_steps == [
        EntityLookupStep(
            id=relationship_info_1.id,
            table=data.dict(by_alias=True),
            parent=EntityLookupInfo(
                key="b",
                serving_name="B",
                entity_id=entity_b.id,
            ),
            child=EntityLookupInfo(
                key="a",
                serving_name="A",
                entity_id=entity_a.id,
            ),
        )
    ]

    # Use [c_is_parent_of_b] as the relationships
    with pytest.raises(RequiredEntityNotProvidedError):
        await parent_entity_lookup_service.get_required_join_steps(
            entity_info, relationships_info=[relationship_info_2]
        )


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
    data_a_to_b, rel_a_to_b = b_is_parent_of_a
    _ = d_is_parent_of_c
    _ = a_is_parent_of_c_and_d
    entity_info = EntityInfo(required_entities=[entity_b], provided_entities=[entity_a])
    join_steps = await parent_entity_lookup_service.get_required_join_steps(entity_info)
    assert join_steps == [
        EntityLookupStep(
            id=rel_a_to_b.id,
            table=data_a_to_b.dict(by_alias=True),
            parent=EntityLookupInfo(
                key="b",
                serving_name="B",
                entity_id=entity_b.id,
            ),
            child=EntityLookupInfo(key="a", serving_name="A", entity_id=entity_a.id),
        ),
    ]
