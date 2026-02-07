"""
Unit test for Context class
"""

import pytest
from bson import ObjectId

from featurebyte import (
    AssignmentDesign,
    AssignmentSource,
    Context,
    Propensity,
    TargetNamespace,
    TargetType,
    Treatment,
    TreatmentInterference,
    TreatmentTime,
    TreatmentTimeStructure,
    TreatmentType,
    UseCase,
)
from featurebyte.enum import DBVarType
from featurebyte.models.context import UserProvidedColumn
from featurebyte.models.feature_list import FeatureListModel
from featurebyte.query_graph.node.schema import DummyTableDetails


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

    observational_treatment = Treatment.create(
        name="test_treatment",
        dtype=DBVarType.INT,
        treatment_type=TreatmentType.BINARY,
        source=AssignmentSource.RANDOMIZED,
        design=AssignmentDesign.SIMPLE_RANDOMIZATION,
        time=TreatmentTime.STATIC,
        time_structure=TreatmentTimeStructure.INSTANTANEOUS,
        interference=TreatmentInterference.NONE,
        treatment_labels=[0, 1],
        control_label=0,
        propensity=Propensity(
            granularity="global",
            knowledge="design-known",
            p_global=0.5,
        ),
    )

    context = Context.create(
        name="test_context_with_treatment",
        primary_entity=entity_names,
        description="test_description",
        treatment_name=observational_treatment.name,
    )

    # Test get context by id and verify attributes
    retrieved_context = Context.get_by_id(context.id)
    assert retrieved_context.name == "test_context_with_treatment"
    assert retrieved_context.primary_entity_ids == entity_ids
    assert retrieved_context.primary_entities == [cust_id_entity]
    assert retrieved_context.description == "test_description"
    assert retrieved_context.treatment_id == observational_treatment.id

    # Test get context by name and verify attributes
    retrieved_context2 = Context.get(context.name)
    assert retrieved_context2.name == "test_context_with_treatment"
    assert retrieved_context2.primary_entity_ids == entity_ids
    assert retrieved_context2.primary_entities == [cust_id_entity]
    assert retrieved_context2.description == "test_description"
    assert retrieved_context.treatment_id == observational_treatment.id


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


def test_user_provided_columns(catalog, cust_id_entity, target_table):
    """
    Test Context with user defined columns
    """
    _ = catalog

    entity_names = [cust_id_entity.name]
    context = Context.create(
        name="test_context",
        primary_entity=entity_names,
        user_provided_columns=[
            {"name": "annual_income", "dtype": DBVarType.FLOAT},
        ],
    )

    assert context.user_provided_columns == [
        UserProvidedColumn(name="annual_income", dtype=DBVarType.FLOAT, description=None),
    ]

    # test add user-provided columns
    context.add_user_provided_column(
        name="credit_score",
        dtype=DBVarType.INT,
    )

    assert context.user_provided_columns == [
        UserProvidedColumn(name="annual_income", dtype=DBVarType.FLOAT, description=None),
        UserProvidedColumn(name="credit_score", dtype=DBVarType.INT, description=None),
    ]

    # test add user-provided columns with name conflict
    with pytest.raises(ValueError) as exc:
        context.add_user_provided_column(
            name="credit_score",
            dtype=DBVarType.FLOAT,
        )
    assert str(exc.value) == "User-provided column with name 'credit_score' already exists"


def test_get_user_provided_feature(catalog, cust_id_entity):
    """
    Test Context.get_user_provided_feature() method
    """
    _ = catalog

    entity_names = [cust_id_entity.name]
    context = Context.create(
        name="test_context_features",
        primary_entity=entity_names,
        user_provided_columns=[
            {"name": "annual_income", "dtype": DBVarType.FLOAT, "description": "Customer income"},
            {"name": "credit_score", "dtype": DBVarType.INT},
        ],
    )

    # Test getting a user-provided feature
    income_feature = context.get_user_provided_feature("annual_income")
    assert income_feature.name == "annual_income"
    assert income_feature.dtype == DBVarType.FLOAT
    assert income_feature.context_id == context.id
    assert income_feature.tabular_source.table_details == DummyTableDetails()
    assert income_feature.feature_store is not None

    # Test getting a feature with a custom name
    score_feature = context.get_user_provided_feature(
        "credit_score", feature_name="my_credit_score"
    )
    assert score_feature.name == "my_credit_score"
    assert score_feature.dtype == DBVarType.INT
    assert score_feature.context_id == context.id

    # Test error for undefined column
    with pytest.raises(ValueError, match="Column 'nonexistent' not defined in context"):
        context.get_user_provided_feature("nonexistent")


def test_get_user_provided_feature_derived(catalog, cust_id_entity):
    """
    Test that user-provided features can be used in derived expressions
    """
    _ = catalog

    entity_names = [cust_id_entity.name]
    context = Context.create(
        name="test_context_derived",
        primary_entity=entity_names,
        user_provided_columns=[
            {"name": "income", "dtype": DBVarType.FLOAT},
            {"name": "expenses", "dtype": DBVarType.FLOAT},
        ],
    )

    income = context.get_user_provided_feature("income")
    expenses = context.get_user_provided_feature("expenses")

    # Derived feature from two user-provided features
    savings = income - expenses
    savings.name = "savings"
    assert savings.context_id is None  # derived features don't auto-inherit context_id
    assert savings.dtype == DBVarType.FLOAT


def test_user_provided_feature_model_context_id():
    """
    Test that FeatureModel properly handles context_id field
    """
    context_id = ObjectId()

    # Verify FeatureModel accepts context_id
    # (just testing the field exists and defaults to None)
    from featurebyte.models.feature import BaseFeatureModel

    assert BaseFeatureModel.model_fields["context_id"].default is None


def test_feature_list_model_context_id_derivation(catalog, cust_id_entity):
    """
    Test that FeatureListModel derives context_id from features
    """

    context1 = Context(
        name="test_context_1",
        primary_entity_ids=[cust_id_entity.id],
        user_provided_columns=[
            UserProvidedColumn(name="income1", dtype=DBVarType.FLOAT),
        ],
    )
    context1.save()
    context2 = Context(
        name="test_context_2",
        primary_entity_ids=[cust_id_entity.id],
        user_provided_columns=[
            UserProvidedColumn(name="income2", dtype=DBVarType.FLOAT),
        ],
    )
    context2.save()
    feature1 = context1.get_user_provided_feature("income1")
    feature1.save()
    feature2 = context2.get_user_provided_feature("income2")
    feature2.save()

    # Test that mixed context_ids raise an error
    with pytest.raises(ValueError, match="All features in a FeatureList must use the same context"):
        FeatureListModel(
            name="test",
            version={"name": "V220710", "suffix": None},
            features=[
                feature1.cached_model,
                feature2.cached_model,
            ],
        )
