"""
Unit test for Context class
"""

import pytest

from featurebyte import (
    AssignmentDesign,
    AssignmentSource,
    Context,
    ForecastPointSchema,
    Propensity,
    TargetNamespace,
    TargetType,
    TimeIntervalUnit,
    Treatment,
    TreatmentInterference,
    TreatmentTime,
    TreatmentTimeStructure,
    TreatmentType,
    UseCase,
)
from featurebyte.api.request_column import RequestColumn
from featurebyte.enum import DBVarType
from featurebyte.models.context import UserProvidedColumn


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


def test_context_forecast_point_with_timezone(catalog, cust_id_entity):
    """
    Test Context.forecast_point property returns RequestColumn with timezone from schema
    """
    _ = catalog

    entity_names = [cust_id_entity.name]

    # Create context with forecast_point_schema including timezone
    forecast_schema = ForecastPointSchema(
        granularity=TimeIntervalUnit.DAY,
        dtype=DBVarType.DATE,
        is_utc_time=False,
        timezone="America/New_York",
    )
    context = Context.create(
        name="forecast_context_with_tz",
        primary_entity=entity_names,
        forecast_point_schema=forecast_schema,
    )

    # Get forecast_point from context
    forecast_point = context.forecast_point

    # Verify it's a RequestColumn
    assert isinstance(forecast_point, RequestColumn)

    # Verify the node has correct parameters
    node_dict = forecast_point.node.model_dump()
    assert node_dict["parameters"]["column_name"] == "FORECAST_POINT"
    assert node_dict["parameters"]["dtype"] == "DATE"

    # Verify timezone info is in dtype_info metadata
    assert node_dict["parameters"]["dtype_info"]["metadata"] is not None
    ts_schema = node_dict["parameters"]["dtype_info"]["metadata"]["timestamp_schema"]
    assert ts_schema["timezone"] == "America/New_York"
    assert ts_schema["is_utc_time"] is False


def test_context_forecast_point_without_schema(catalog, cust_id_entity):
    """
    Test Context.forecast_point raises error when no forecast_point_schema is defined
    """
    _ = catalog

    entity_names = [cust_id_entity.name]

    # Create context without forecast_point_schema
    context = Context.create(
        name="context_without_forecast",
        primary_entity=entity_names,
    )

    # Accessing forecast_point should raise ValueError
    with pytest.raises(ValueError) as exc:
        _ = context.forecast_point

    assert "does not have a forecast_point_schema defined" in str(exc.value)


def test_context_forecast_point_date_part_extraction(catalog, cust_id_entity):
    """
    Test that date parts can be extracted from Context.forecast_point
    """
    _ = catalog

    entity_names = [cust_id_entity.name]

    # Create context with forecast_point_schema
    forecast_schema = ForecastPointSchema(
        granularity=TimeIntervalUnit.DAY,
        dtype=DBVarType.DATE,
        is_utc_time=False,
        timezone="Asia/Singapore",
    )
    context = Context.create(
        name="forecast_context_for_extract",
        primary_entity=entity_names,
        forecast_point_schema=forecast_schema,
    )

    # Get forecast_point and extract month
    forecast_point = context.forecast_point
    month = forecast_point.dt.month

    # Verify month extraction works
    assert month.dtype == DBVarType.INT

    # Verify the DT_EXTRACT node has timezone metadata
    from featurebyte.query_graph.graph import GlobalQueryGraph

    graph = GlobalQueryGraph()
    dt_extract_node = graph.get_node_by_name(month.node.name)
    assert dt_extract_node.parameters.timestamp_metadata is not None
    assert dt_extract_node.parameters.timestamp_metadata.timestamp_schema is not None
    assert (
        dt_extract_node.parameters.timestamp_metadata.timestamp_schema.timezone == "Asia/Singapore"
    )
