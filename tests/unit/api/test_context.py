"""
Unit test for Context class
"""
import pytest

from featurebyte import Context


@pytest.fixture(name="context")
def context_fixture(catalog, cust_id_entity):
    """
    UseCase fixture
    """
    _ = catalog

    entity_ids = [cust_id_entity.id]

    context = Context(
        name="test_context",
        entity_ids=entity_ids,
    )
    previous_id = context.id
    assert context.saved is False
    context.save()
    assert context.saved is True
    assert context.id == previous_id
    yield context


def test_create_context(catalog, cust_id_entity):
    """
    Test Context.create method
    """
    _ = catalog

    entity_ids = [cust_id_entity.id]
    entity_names = [cust_id_entity.name]
    context = Context.create(name="test_context", entity_names=entity_names)

    # Test get context by id and verify attributes
    retrieved_context = Context.get_by_id(context.id)
    assert retrieved_context.name == "test_context"
    assert retrieved_context.entity_ids == entity_ids
    assert retrieved_context.entities == [cust_id_entity]

    # Test get context by name and verify attributes
    retrieved_context2 = Context.get(context.name)
    assert retrieved_context2.name == "test_context"
    assert retrieved_context2.entity_ids == entity_ids
    assert retrieved_context2.entities == [cust_id_entity]


def test_list_contexts(catalog, context, cust_id_entity):
    """
    Test Context.list method
    """
    _ = catalog

    context_df = Context.list()
    assert len(context_df) == 1
    assert context_df.iloc[0]["name"] == "test_context"
    assert context_df.iloc[0]["entity_ids"] == [str(cust_id_entity.id)]
