"""
Tests for featurebyte/query_graph/model/entity_relationship_info.py
"""

from featurebyte.query_graph.model.entity_relationship_info import EntityAncestorDescendantMapper


def test_keep_related_entity_ids(
    entity_a,
    entity_b,
    entity_c,
    entity_d,
    b_is_parent_of_a,
):
    """
    Test keep_related_entity_ids
    """
    mapper = EntityAncestorDescendantMapper.create([b_is_parent_of_a])
    assert mapper.keep_related_entity_ids([entity_a, entity_b], [entity_a]) == [entity_a, entity_b]
    assert mapper.keep_related_entity_ids([entity_a, entity_b, entity_c], [entity_a]) == [
        entity_a,
        entity_b,
    ]
    assert mapper.keep_related_entity_ids([entity_a, entity_b, entity_c], [entity_b]) == [
        entity_a,
        entity_b,
    ]
    assert mapper.keep_related_entity_ids([entity_a, entity_b, entity_c], [entity_c]) == [entity_c]
