"""
Test forecast_aggregate_asat
"""

import pytest

from featurebyte import Context, ForecastPointSchema, TimeIntervalUnit
from featurebyte.api.target import Target
from featurebyte.enum import DBVarType
from tests.util.helper import get_node


@pytest.fixture(name="forecast_context")
def forecast_context_fixture(catalog, gender_entity):
    """
    Context with forecast_point_schema for testing
    """
    _ = catalog

    forecast_schema = ForecastPointSchema(
        granularity=TimeIntervalUnit.DAY,
        dtype=DBVarType.DATE,
        is_utc_time=False,
        timezone="America/New_York",
    )
    context = Context.create(
        name="test_forecast_context",
        primary_entity=[gender_entity.name],
        forecast_point_schema=forecast_schema,
    )
    yield context


@pytest.fixture(name="forecast_context_utc")
def forecast_context_utc_fixture(catalog, gender_entity):
    """
    Context with UTC forecast_point_schema for testing
    """
    _ = catalog

    forecast_schema = ForecastPointSchema(
        granularity=TimeIntervalUnit.DAY,
        dtype=DBVarType.TIMESTAMP,
        is_utc_time=True,
    )
    context = Context.create(
        name="test_forecast_context_utc",
        primary_entity=[gender_entity.name],
        forecast_point_schema=forecast_schema,
    )
    yield context


def test_forecast_aggregate_asat__valid(
    snowflake_scd_view_with_entity,
    forecast_context,
    gender_entity_id,
):
    """
    Test valid usage of forecast_aggregate_asat
    """
    target = snowflake_scd_view_with_entity.groupby("col_boolean").forecast_aggregate_asat(
        value_column="col_float",
        method="sum",
        target_name="forecast_asat_target",
        context=forecast_context,
        fill_value=0.0,
    )
    assert isinstance(target, Target)
    target_dict = target.model_dump()
    graph_dict = target_dict["graph"]

    target_aggregate_node_name = "project_1"
    node_dict = get_node(graph_dict, target_aggregate_node_name)
    assert node_dict == {
        "name": target_aggregate_node_name,
        "type": "project",
        "output_type": "series",
        "parameters": {"columns": ["forecast_asat_target"]},
    }

    asat_node_dict = get_node(graph_dict, "forecast_aggregate_as_at_1")
    assert asat_node_dict["name"] == "forecast_aggregate_as_at_1"
    assert asat_node_dict["type"] == "forecast_aggregate_as_at"
    assert asat_node_dict["output_type"] == "frame"

    params = asat_node_dict["parameters"]
    assert params["agg_func"] == "sum"
    assert params["parent"] == "col_float"
    assert params["name"] == "forecast_asat_target"
    assert params["keys"] == ["col_boolean"]
    assert params["entity_ids"] == [gender_entity_id]
    assert params["serving_names"] == ["gender"]
    assert params["use_forecast_point"] is True
    assert params["forecast_point_schema"] is not None
    assert params["forecast_point_schema"]["granularity"] == "DAY"
    assert params["forecast_point_schema"]["is_utc_time"] is False
    assert params["forecast_point_schema"]["timezone"] == "America/New_York"


def test_forecast_aggregate_asat__offset(
    snowflake_scd_view_with_entity,
    forecast_context,
    gender_entity_id,
):
    """
    Test offset parameter
    """
    target = snowflake_scd_view_with_entity.groupby("col_boolean").forecast_aggregate_asat(
        value_column="col_float",
        method="sum",
        target_name="forecast_asat_target",
        context=forecast_context,
        offset="7d",
        fill_value=0.0,
    )

    assert isinstance(target, Target)
    target_dict = target.model_dump()
    graph_dict = target_dict["graph"]

    asat_node_dict = get_node(graph_dict, "forecast_aggregate_as_at_1")
    assert asat_node_dict["name"] == "forecast_aggregate_as_at_1"
    assert asat_node_dict["type"] == "forecast_aggregate_as_at"

    params = asat_node_dict["parameters"]
    assert params["offset"] == "7d"
    assert params["use_forecast_point"] is True


def test_forecast_aggregate_asat__utc_context(
    snowflake_scd_view_with_entity,
    forecast_context_utc,
    gender_entity_id,
):
    """
    Test forecast_aggregate_asat with UTC context
    """
    target = snowflake_scd_view_with_entity.groupby("col_boolean").forecast_aggregate_asat(
        value_column="col_float",
        method="avg",
        target_name="forecast_asat_target_utc",
        context=forecast_context_utc,
        fill_value=0.0,
    )

    assert isinstance(target, Target)
    target_dict = target.model_dump()
    graph_dict = target_dict["graph"]

    asat_node_dict = get_node(graph_dict, "forecast_aggregate_as_at_1")
    params = asat_node_dict["parameters"]
    assert params["use_forecast_point"] is True
    assert params["forecast_point_schema"]["is_utc_time"] is True
    assert params["forecast_point_schema"]["timezone"] is None


def test_forecast_aggregate_asat__no_forecast_schema(
    snowflake_scd_view_with_entity,
    catalog,
    gender_entity,
):
    """
    Test forecast_aggregate_asat raises error when context has no forecast_point_schema
    """
    _ = catalog

    # Create context without forecast_point_schema
    context_without_forecast = Context.create(
        name="context_without_forecast_schema",
        primary_entity=[gender_entity.name],
    )

    with pytest.raises(ValueError) as exc:
        snowflake_scd_view_with_entity.groupby("col_boolean").forecast_aggregate_asat(
            value_column="col_float",
            method="sum",
            target_name="forecast_asat_target",
            context=context_without_forecast,
            fill_value=0.0,
        )

    assert "does not have a forecast_point_schema" in str(exc.value)


def test_forecast_aggregate_asat__fill_value_required(
    snowflake_scd_view_with_entity,
    forecast_context,
):
    """
    Test forecast_aggregate_asat requires fill_value for certain methods
    """
    with pytest.raises(ValueError) as exc:
        snowflake_scd_view_with_entity.groupby("col_boolean").forecast_aggregate_asat(
            value_column="col_float",
            method="max",
            target_name="forecast_asat_target",
            context=forecast_context,
        )

    assert "fill_value is required for method max" in str(exc.value)


def test_forecast_aggregate_asat__snapshots_view(
    snowflake_snapshots_table_with_entity,
    another_entity,
    catalog,
):
    """
    Test forecast_aggregate_asat with SnapshotsView
    """
    _ = catalog

    # Create context with forecast_point_schema for the snapshots entity
    forecast_schema = ForecastPointSchema(
        granularity=TimeIntervalUnit.DAY,
        dtype=DBVarType.DATE,
        is_utc_time=False,
        timezone="America/New_York",
    )
    context = Context.create(
        name="test_forecast_context_snapshots",
        primary_entity=[another_entity.name],
        forecast_point_schema=forecast_schema,
    )

    snapshots_view = snowflake_snapshots_table_with_entity.get_view()
    target = snapshots_view.groupby("col_binary").forecast_aggregate_asat(
        value_column="col_float",
        method="sum",
        target_name="forecast_asat_snapshots_target",
        context=context,
        fill_value=0.0,
    )
    assert isinstance(target, Target)
    target_dict = target.model_dump()
    graph_dict = target_dict["graph"]

    asat_node_dict = get_node(graph_dict, "forecast_aggregate_as_at_1")
    assert asat_node_dict["type"] == "forecast_aggregate_as_at"

    params = asat_node_dict["parameters"]
    assert params["use_forecast_point"] is True
    assert params["forecast_point_schema"] is not None
    assert params["snapshots_parameters"] is not None
    assert params["snapshots_parameters"]["snapshot_datetime_column"] == "date"


def test_forecast_aggregate_asat__snapshots_view_with_offset(
    snowflake_snapshots_table_with_entity,
    another_entity,
    catalog,
):
    """
    Test forecast_aggregate_asat with SnapshotsView and integer offset
    """
    _ = catalog

    # Create context with forecast_point_schema for the snapshots entity
    forecast_schema = ForecastPointSchema(
        granularity=TimeIntervalUnit.DAY,
        dtype=DBVarType.DATE,
        is_utc_time=True,
    )
    context = Context.create(
        name="test_forecast_context_snapshots_offset",
        primary_entity=[another_entity.name],
        forecast_point_schema=forecast_schema,
    )

    snapshots_view = snowflake_snapshots_table_with_entity.get_view()
    target = snapshots_view.groupby("col_binary").forecast_aggregate_asat(
        value_column="col_float",
        method="sum",
        target_name="forecast_asat_snapshots_target",
        context=context,
        offset=2,  # Integer offset for snapshots
        fill_value=0.0,
    )
    assert isinstance(target, Target)
    target_dict = target.model_dump()
    graph_dict = target_dict["graph"]

    asat_node_dict = get_node(graph_dict, "forecast_aggregate_as_at_1")
    params = asat_node_dict["parameters"]
    assert params["snapshots_parameters"]["offset_size"] == 2
