"""
Tests for TargetExecutor forecast_point_schema handling.

Verifies that when forecast_point_schema is provided via ExecutorParams,
it is used instead of resolving from the observation table's context.
"""

from unittest.mock import AsyncMock, Mock, patch

import pandas as pd
import pytest
from bson import ObjectId

from featurebyte.enum import DBVarType, SpecialColumnName, TimeIntervalUnit
from featurebyte.models.observation_table import ObservationTableModel
from featurebyte.query_graph.model.forecast_point_schema import ForecastPointSchema
from featurebyte.service.target_helper.base_feature_or_target_computer import ExecutorParams
from featurebyte.service.target_helper.compute_target import TargetExecutor


@pytest.fixture(name="forecast_point_schema")
def forecast_point_schema_fixture():
    """ForecastPointSchema fixture"""
    return ForecastPointSchema(
        granularity=TimeIntervalUnit.DAY,
        dtype=DBVarType.TIMESTAMP,
        is_utc_time=True,
    )


@pytest.fixture(name="mock_executor_params_dataframe")
def mock_executor_params_dataframe_fixture(forecast_point_schema):
    """ExecutorParams with a DataFrame observation set and forecast_point_schema"""
    observation_set = pd.DataFrame({
        "POINT_IN_TIME": pd.to_datetime(["2023-01-01"]),
        "FORECAST_POINT": pd.to_datetime(["2023-01-08"]),
        "entity_id": [1],
    })
    return Mock(
        spec=ExecutorParams,
        observation_set=observation_set,
        forecast_point_schema=forecast_point_schema,
        graph=Mock(),
        nodes=[Mock()],
        feature_store=Mock(),
        output_table_details=Mock(),
        serving_names_mapping=None,
        parent_serving_preparation=None,
        progress_callback=None,
        session=Mock(),
    )


def _make_column_info(col_name):
    """Create a mock column info with the given name (Mock's name attr is special)."""
    col = Mock()
    col.name = col_name
    return col


@pytest.fixture(name="mock_executor_params_obs_table")
def mock_executor_params_obs_table_fixture(forecast_point_schema):
    """ExecutorParams with an ObservationTableModel and forecast_point_schema"""
    obs_table = Mock(spec=ObservationTableModel)
    obs_table.id = ObjectId()
    obs_table.context_id = ObjectId()
    obs_table.has_row_index = True
    obs_table.columns_info = [
        _make_column_info(SpecialColumnName.POINT_IN_TIME),
        _make_column_info(SpecialColumnName.FORECAST_POINT),
    ]
    return Mock(
        spec=ExecutorParams,
        observation_set=obs_table,
        forecast_point_schema=forecast_point_schema,
        graph=Mock(),
        nodes=[Mock()],
        feature_store=Mock(),
        output_table_details=Mock(),
        serving_names_mapping=None,
        parent_serving_preparation=None,
        progress_callback=None,
        session=Mock(),
    )


@pytest.fixture(name="target_executor")
def target_executor_fixture():
    """TargetExecutor with mocked dependencies"""
    return TargetExecutor(
        feature_table_cache_service=Mock(),
        cron_helper=Mock(),
        system_metrics_service=Mock(),
        observation_table_service=AsyncMock(),
        context_service=AsyncMock(),
    )


@pytest.mark.asyncio
async def test_executor_uses_forecast_point_schema_from_params_dataframe(
    target_executor, mock_executor_params_dataframe, forecast_point_schema
):
    """
    When forecast_point_schema is provided in ExecutorParams and observation_set is a DataFrame,
    the executor should use it directly (passing to get_target) instead of None.
    """
    with patch("featurebyte.service.target_helper.compute_target.get_target") as mock_get_target:
        mock_get_target.return_value = Mock(historical_features_metrics=Mock())
        await target_executor.execute(mock_executor_params_dataframe)

        mock_get_target.assert_called_once()
        call_kwargs = mock_get_target.call_args[1]
        assert call_kwargs["forecast_point_schema"] is forecast_point_schema


@pytest.mark.asyncio
async def test_executor_uses_forecast_point_schema_from_params_obs_table(
    target_executor, mock_executor_params_obs_table, forecast_point_schema
):
    """
    When forecast_point_schema is provided in ExecutorParams and observation_set is an
    ObservationTableModel, the executor should use it and NOT look up the context.
    """
    # Make observation_table_service.get_document succeed (not temp table)
    target_executor.observation_table_service.get_document = AsyncMock()

    with patch("featurebyte.service.target_helper.compute_target.FeatureTableCacheService"):
        target_executor.feature_table_cache_service.create_view_or_table_from_cache = AsyncMock(
            return_value=(False, Mock())
        )
        await target_executor.execute(mock_executor_params_obs_table)

        # Verify executor_params.forecast_point_schema takes precedence over the one
        # resolved from the observation table's context
        call_kwargs = (
            target_executor.feature_table_cache_service.create_view_or_table_from_cache.call_args[1]
        )
        assert call_kwargs["forecast_point_schema"] is forecast_point_schema


@pytest.mark.asyncio
async def test_executor_falls_back_to_context_when_no_schema_provided(target_executor):
    """
    When forecast_point_schema is None in ExecutorParams but observation_set is an
    ObservationTableModel with context_id, the executor should resolve from context.
    """
    context_id = ObjectId()
    mock_context = Mock()
    mock_context.forecast_point_schema = ForecastPointSchema(
        granularity=TimeIntervalUnit.HOUR,
        dtype=DBVarType.TIMESTAMP,
    )

    obs_table = Mock(spec=ObservationTableModel)
    obs_table.id = ObjectId()
    obs_table.context_id = context_id
    obs_table.has_row_index = True
    obs_table.columns_info = [
        _make_column_info(SpecialColumnName.POINT_IN_TIME),
        _make_column_info(SpecialColumnName.FORECAST_POINT),
    ]

    executor_params = Mock(
        spec=ExecutorParams,
        observation_set=obs_table,
        forecast_point_schema=None,
        graph=Mock(),
        nodes=[Mock()],
        feature_store=Mock(),
        output_table_details=Mock(),
        serving_names_mapping=None,
        parent_serving_preparation=None,
        progress_callback=None,
        session=Mock(),
    )

    target_executor.observation_table_service.get_document = AsyncMock()
    target_executor.context_service.get_document = AsyncMock(return_value=mock_context)
    target_executor.feature_table_cache_service.create_view_or_table_from_cache = AsyncMock(
        return_value=(False, Mock())
    )

    await target_executor.execute(executor_params)

    # Context service SHOULD have been called to resolve forecast_point_schema
    target_executor.context_service.get_document.assert_called_once_with(context_id)

    call_kwargs = (
        target_executor.feature_table_cache_service.create_view_or_table_from_cache.call_args[1]
    )
    assert call_kwargs["forecast_point_schema"] is mock_context.forecast_point_schema
