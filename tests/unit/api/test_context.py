"""
Unit test for Context class
"""

import pytest

from featurebyte import Context, Propensity, TargetNamespace, TargetType, Treatment, UseCase


@pytest.fixture(name="context_1")
def context_fixture(catalog, cust_id_entity):
    """
    UseCase fixture
    """
    _ = catalog

    entity_ids = [cust_id_entity.id]

    context = Context(
        name="test_context",
        primary_entity_ids=entity_ids,
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
    context = Context.create(
        name="test_context", primary_entity=entity_names, description="test_description"
    )

    # Test get context by id and verify attributes
    retrieved_context = Context.get_by_id(context.id)
    assert retrieved_context.name == "test_context"
    assert retrieved_context.primary_entity_ids == entity_ids
    assert retrieved_context.primary_entities == [cust_id_entity]
    assert retrieved_context.description == "test_description"

    # Test get context by name and verify attributes
    retrieved_context2 = Context.get(context.name)
    assert retrieved_context2.name == "test_context"
    assert retrieved_context2.primary_entity_ids == entity_ids
    assert retrieved_context2.primary_entities == [cust_id_entity]
    assert retrieved_context2.description == "test_description"


def test_create_context_with_treatment(catalog, cust_id_entity):
    """
    Test Context.create method with treatment
    """
    _ = catalog

    entity_ids = [cust_id_entity.id]
    entity_names = [cust_id_entity.name]

    observational_treatment = Treatment(
        scale="binary",
        source="observational",
        design="business-rule",
        time="static",
        time_structure="none",
        interference="none",
        treatment_values=[0, 1],
        control_value=0,
        propensity=Propensity(
            granularity="unit",
            knowledge="estimated",
        ),
    )

    context = Context.create(
        name="test_context_with_treatment",
        primary_entity=entity_names,
        description="test_description",
        treatment=observational_treatment,
    )

    # Test get context by id and verify attributes
    retrieved_context = Context.get_by_id(context.id)
    assert retrieved_context.name == "test_context_with_treatment"
    assert retrieved_context.primary_entity_ids == entity_ids
    assert retrieved_context.primary_entities == [cust_id_entity]
    assert retrieved_context.description == "test_description"
    assert retrieved_context.treatment == observational_treatment

    # Test get context by name and verify attributes
    retrieved_context2 = Context.get(context.name)
    assert retrieved_context2.name == "test_context_with_treatment"
    assert retrieved_context2.primary_entity_ids == entity_ids
    assert retrieved_context2.primary_entities == [cust_id_entity]
    assert retrieved_context2.description == "test_description"
    assert retrieved_context2.treatment == observational_treatment


def test_list_contexts(catalog, context_1, cust_id_entity):
    """
    Test Context.list method
    """
    _ = catalog
    _ = context_1

    context_df = Context.list()
    assert len(context_df) == 1
    assert context_df.iloc[0]["name"] == "test_context"
    assert context_df.iloc[0]["primary_entity_ids"] == [str(cust_id_entity.id)]


def test_add_remove_obs_table(catalog, cust_id_entity, target_table):
    """
    Test Context add/remove observation table methods
    """
    _ = catalog

    entity_names = [cust_id_entity.name]
    context = Context.create(name="test_context", primary_entity=entity_names)

    context.add_observation_table(target_table.name)

    obs_tables = context.list_observation_tables()
    assert len(obs_tables) == 1
    assert obs_tables.iloc[0]["name"] == target_table.name

    context.remove_observation_table(target_table.name)

    obs_tables = context.list_observation_tables()
    assert len(obs_tables) == 0


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

    # test remove default table
    context.remove_default_eda_table()
    retrieved_context = Context.get_by_id(context.id)
    assert retrieved_context.default_eda_table is None

    context.remove_default_preview_table()
    retrieved_context = Context.get_by_id(context.id)
    assert retrieved_context.default_preview_table is None


def test_info(context_1, float_target, target_table, cust_id_entity):
    """
    Test Context.info method
    """

    float_target.update_target_type(TargetType.REGRESSION)
    target_namespace = TargetNamespace.get(float_target.name)
    use_case = UseCase(
        name="test_use_case",
        target_id=float_target.id,
        target_namespace_id=target_namespace.id,
        context_id=context_1.id,
        description="test_use_case description",
    )
    use_case.save()

    context_1.update_default_eda_table(target_table.name)
    context_1.update_default_preview_table(target_table.name)

    context_info = context_1.info()
    assert context_info["name"] == context_1.name
    assert context_info["description"] == context_1.description
    assert context_info["primary_entities"] == [
        {
            "name": cust_id_entity.name,
            "serving_names": cust_id_entity.serving_names,
            "catalog_name": "catalog",
        }
    ]
    assert context_info["default_eda_table"] == target_table.name
    assert context_info["default_preview_table"] == target_table.name
    assert context_info["associated_use_cases"] == [use_case.name]
    assert not context_info["treatment"]
