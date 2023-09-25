"""
Unit test for Context class
"""
import pytest

from featurebyte import Context, UseCase


@pytest.fixture(name="context_1")
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
    context = Context.create(name="test_context", primary_entity=entity_names)

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


def test_list_contexts(catalog, context_1, cust_id_entity):
    """
    Test Context.list method
    """
    _ = catalog
    _ = context_1

    context_df = Context.list()
    assert len(context_df) == 1
    assert context_df.iloc[0]["name"] == "test_context"
    assert context_df.iloc[0]["entity_ids"] == [str(cust_id_entity.id)]


def test_update_context(catalog, cust_id_entity, target_table):
    """
    Test Context update methods
    """
    _ = catalog

    entity_names = [cust_id_entity.name]
    context = Context.create(name="test_context", primary_entity=entity_names)

    # Test get context by id and verify attributes
    context.update_default_eda_table(target_table.name)
    assert context.default_eda_table.name == target_table.name

    context.update_default_preview_table(target_table.name)
    assert context.default_preview_table.name == target_table.name

    obs_tables = context.list_observation_tables()
    assert len(obs_tables) == 1
    assert obs_tables.iloc[0]["name"] == target_table.name


def test_info(context_1, float_target, target_table, cust_id_entity):
    """
    Test Context.info method
    """

    use_case = UseCase(
        name="test_use_case",
        target_id=float_target.id,
        context_id=context_1.id,
        description="test_use_case description",
    )
    use_case.save()

    context_1.update_default_eda_table(target_table.name)
    context_1.update_default_preview_table(target_table.name)

    context_info = context_1.info()
    assert context_info["name"] == context_1.name
    assert context_info["description"] == context_1.description
    assert context_info["entities"] == [
        {
            "name": cust_id_entity.name,
            "serving_names": cust_id_entity.serving_names,
            "catalog_name": "catalog",
        }
    ]
    assert context_info["default_eda_table"] == target_table.name
    assert context_info["default_preview_table"] == target_table.name
    assert context_info["associated_use_cases"] == [use_case.name]
