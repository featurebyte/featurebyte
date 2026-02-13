"""
Integration tests for the forecast point related features
"""

import pandas as pd
import pytest
from bson import ObjectId

from featurebyte import Context, FeatureJobSetting, FeatureList, UseCase
from featurebyte.enum import DBVarType, SpecialColumnName, TargetType, TimeIntervalUnit
from featurebyte.exception import RecordCreationException
from featurebyte.query_graph.model.forecast_point_schema import ForecastPointSchema
from featurebyte.query_graph.model.timestamp_schema import TimeZoneColumn
from tests.util.helper import check_preview_and_compute_historical_features


def _create_forecast_context(user_entity, format_string, suffix):
    """Create a forecast context for integration tests."""
    forecast_schema = ForecastPointSchema(
        granularity=TimeIntervalUnit.DAY,
        dtype=DBVarType.VARCHAR,
        is_utc_time=False,
        timezone="America/New_York",
        format_string=format_string,
    )
    return Context.create(
        name=f"forecast_context_{suffix}_{ObjectId()}",
        primary_entity=[user_entity.name],
        forecast_point_schema=forecast_schema,
    )


def test_forecast_point_hour_feature(user_entity, timestamp_format_string):
    """Test Context.get_forecast_point_feature().dt.hour via preview & historical feature table."""
    forecast_context = _create_forecast_context(
        user_entity=user_entity, format_string=timestamp_format_string, suffix="hour"
    )

    feature = forecast_context.get_forecast_point_feature().dt.hour
    feature.name = "forecast_context_hour"

    feature_list = FeatureList([feature], "test_forecast_context_hour_feature_list")
    preview_params = pd.DataFrame([
        {
            "POINT_IN_TIME": pd.Timestamp("2001-01-02 10:00:00"),
            "üser id": 1,
            "FORECAST_POINT": "2001|01|10",
        }
    ])
    expected = preview_params.copy()
    expected["forecast_context_hour"] = [0]
    check_preview_and_compute_historical_features(
        feature_list, preview_params, expected, context_name=forecast_context.name
    )


def test_days_until_forecast_from_latest_event(event_table, timestamp_format_string, user_entity):
    """Test forecast point minus latest event timestamp via preview & historical feature table."""
    forecast_context = _create_forecast_context(
        user_entity=user_entity, format_string=timestamp_format_string, suffix="days_until"
    )

    view = event_table.get_view()
    latest_event_timestamp_feature = view.groupby("ÜSER ID").aggregate_over(
        value_column="ËVENT_TIMESTAMP",
        method="latest",
        windows=["90d"],
        feature_names=["latest_event_timestamp_90d"],
        feature_job_setting=FeatureJobSetting(blind_spot="30m", period="1h", offset="30m"),
    )["latest_event_timestamp_90d"]

    feature = (
        forecast_context.get_forecast_point_feature() - latest_event_timestamp_feature
    ).dt.day
    feature.name = "Days Until Forecast From Latest Event"

    feature_list = FeatureList([feature], "test_days_until_forecast_feature_list")
    preview_params = pd.DataFrame([
        {
            "POINT_IN_TIME": pd.Timestamp("2001-01-02 10:00:00"),
            "üser id": 1,
            "FORECAST_POINT": "2001|01|10",
        }
    ])
    expected = preview_params.copy()
    expected["Days Until Forecast From Latest Event"] = [7.845613]
    check_preview_and_compute_historical_features(
        feature_list, preview_params, expected, context_name=forecast_context.name
    )


@pytest.mark.asyncio
async def test_observation_table_invalid_forecast_point_format(
    session, data_source, user_entity, timestamp_format_string
):
    """Test that observation table creation fails when FORECAST_POINT values don't match format."""
    # Create a context with VARCHAR forecast_point_schema
    forecast_schema = ForecastPointSchema(
        granularity=TimeIntervalUnit.DAY,
        dtype=DBVarType.VARCHAR,
        is_utc_time=False,
        timezone="America/New_York",
        format_string=timestamp_format_string,
    )
    context = Context.create(
        name=f"test_forecast_validation_context_{ObjectId()}",
        primary_entity=[user_entity.name],
        forecast_point_schema=forecast_schema,
    )

    # Create a target for the use case
    from featurebyte import TargetNamespace

    target_namespace = TargetNamespace.create(
        name=f"test_target_{ObjectId()}",
        primary_entity=[user_entity.name],
        dtype=DBVarType.FLOAT,
        target_type=TargetType.REGRESSION,
    )

    use_case = UseCase.create(
        name=f"test_use_case_{ObjectId()}",
        target_name=target_namespace.name,
        context_name=context.name,
    )

    # Create a DataFrame with invalid FORECAST_POINT values
    table_name = f"TEST_INVALID_FORECAST_POINT_{ObjectId().binary.hex()[:8].upper()}"
    df = pd.DataFrame({
        SpecialColumnName.POINT_IN_TIME: pd.to_datetime([
            "2001-01-02 10:00:00",
            "2001-01-03 10:00:00",
            "2001-01-04 10:00:00",
        ]),
        "üser id": [1, 2, 3],
        SpecialColumnName.FORECAST_POINT: [
            "2001|01|10",  # Valid format
            "invalid-date-format",  # Invalid format
            "2001/01/15",  # Wrong delimiter
        ],
    })
    await session.register_table(table_name, df)

    try:
        # Get the source table
        source_table = data_source.get_source_table(
            database_name=session.database_name,
            schema_name=session.schema_name,
            table_name=table_name,
        )

        # Attempt to create observation table - should fail
        with pytest.raises(RecordCreationException) as exc:
            source_table.create_observation_table(
                name=f"test_obs_table_invalid_forecast_{ObjectId()}",
                columns_rename_mapping={"üser id": "üser id"},
                use_case_name=use_case.name,
            )
        assert "FORECAST_POINT" in str(exc.value)
        assert "invalid-date-format" in str(exc.value) or "2001/01/15" in str(exc.value)
    finally:
        # Cleanup
        await session.drop_table(
            table_name,
            schema_name=session.schema_name,
            database_name=session.database_name,
        )
        use_case.delete()
        target_namespace.delete()
        context.delete()


@pytest.mark.asyncio
async def test_observation_table_valid_forecast_point_format(
    session, data_source, user_entity, timestamp_format_string
):
    """Test that observation table creation succeeds when FORECAST_POINT values match format."""
    # Create a context with VARCHAR forecast_point_schema
    forecast_schema = ForecastPointSchema(
        granularity=TimeIntervalUnit.DAY,
        dtype=DBVarType.VARCHAR,
        is_utc_time=False,
        timezone="America/New_York",
        format_string=timestamp_format_string,
    )
    context = Context.create(
        name=f"test_forecast_valid_context_{ObjectId()}",
        primary_entity=[user_entity.name],
        forecast_point_schema=forecast_schema,
    )

    # Create a target for the use case
    from featurebyte import TargetNamespace

    target_namespace = TargetNamespace.create(
        name=f"test_target_valid_{ObjectId()}",
        primary_entity=[user_entity.name],
        dtype=DBVarType.FLOAT,
        target_type=TargetType.REGRESSION,
    )

    use_case = UseCase.create(
        name=f"test_use_case_valid_{ObjectId()}",
        target_name=target_namespace.name,
        context_name=context.name,
    )

    # Create a DataFrame with valid FORECAST_POINT values
    table_name = f"TEST_VALID_FORECAST_POINT_{ObjectId().binary.hex()[:8].upper()}"
    df = pd.DataFrame({
        SpecialColumnName.POINT_IN_TIME: pd.to_datetime([
            "2001-01-02 10:00:00",
            "2001-01-03 10:00:00",
            "2001-01-04 10:00:00",
        ]),
        "üser id": [1, 2, 3],
        SpecialColumnName.FORECAST_POINT: [
            "2001|01|10",
            "2001|01|15",
            "2001|01|20",
        ],
    })
    await session.register_table(table_name, df)

    obs_table = None
    try:
        # Get the source table
        source_table = data_source.get_source_table(
            database_name=session.database_name,
            schema_name=session.schema_name,
            table_name=table_name,
        )

        # Create observation table - should succeed
        obs_table = source_table.create_observation_table(
            name=f"test_obs_table_valid_forecast_{ObjectId()}",
            columns_rename_mapping={"üser id": "üser id"},
            use_case_name=use_case.name,
        )
        assert obs_table is not None
        assert obs_table.shape()[0] == 3
    finally:
        # Cleanup - remove use_case association before deleting observation table
        if obs_table is not None:
            obs_table.update(
                update_payload={"use_case_id_to_remove": str(use_case.id)},
                allow_update_local=False,
                skip_update_schema_check=True,
            )
            obs_table.delete()
        await session.drop_table(
            table_name,
            schema_name=session.schema_name,
            database_name=session.database_name,
        )
        use_case.delete()
        target_namespace.delete()
        context.delete()


@pytest.mark.asyncio
async def test_observation_table_invalid_timezone_values(
    session, data_source, user_entity, timestamp_format_string
):
    """Test that observation table creation fails when timezone column has invalid values."""
    # Create a context with forecast_point_schema that has a timezone column
    forecast_schema = ForecastPointSchema(
        granularity=TimeIntervalUnit.DAY,
        dtype=DBVarType.VARCHAR,
        is_utc_time=False,
        timezone=TimeZoneColumn(column_name="FORECAST_TIMEZONE", type="timezone"),
        format_string=timestamp_format_string,
    )
    context = Context.create(
        name=f"test_tz_validation_context_{ObjectId()}",
        primary_entity=[user_entity.name],
        forecast_point_schema=forecast_schema,
    )

    # Create a target for the use case
    from featurebyte import TargetNamespace

    target_namespace = TargetNamespace.create(
        name=f"test_target_tz_{ObjectId()}",
        primary_entity=[user_entity.name],
        dtype=DBVarType.FLOAT,
        target_type=TargetType.REGRESSION,
    )

    use_case = UseCase.create(
        name=f"test_use_case_tz_{ObjectId()}",
        target_name=target_namespace.name,
        context_name=context.name,
    )

    # Create a DataFrame with invalid timezone values
    table_name = f"TEST_INVALID_TIMEZONE_{ObjectId().binary.hex()[:8].upper()}"
    df = pd.DataFrame({
        SpecialColumnName.POINT_IN_TIME: pd.to_datetime([
            "2001-01-02 10:00:00",
            "2001-01-03 10:00:00",
            "2001-01-04 10:00:00",
        ]),
        "üser id": [1, 2, 3],
        SpecialColumnName.FORECAST_POINT: [
            "2001|01|10",
            "2001|01|15",
            "2001|01|20",
        ],
        "FORECAST_TIMEZONE": [
            "America/New_York",  # Valid
            "Invalid/Timezone",  # Invalid
            "Europe/London",  # Valid
        ],
    })
    await session.register_table(table_name, df)

    try:
        # Get the source table
        source_table = data_source.get_source_table(
            database_name=session.database_name,
            schema_name=session.schema_name,
            table_name=table_name,
        )

        # Attempt to create observation table - should fail
        with pytest.raises(RecordCreationException) as exc:
            source_table.create_observation_table(
                name=f"test_obs_table_invalid_tz_{ObjectId()}",
                columns_rename_mapping={"üser id": "üser id"},
                use_case_name=use_case.name,
            )
        assert "FORECAST_TIMEZONE" in str(exc.value)
        assert "Invalid/Timezone" in str(exc.value)
    finally:
        # Cleanup
        await session.drop_table(
            table_name,
            schema_name=session.schema_name,
            database_name=session.database_name,
        )
        use_case.delete()
        target_namespace.delete()
        context.delete()


@pytest.mark.asyncio
async def test_observation_table_invalid_utc_offset_values(
    session, data_source, user_entity, timestamp_format_string
):
    """Test that observation table creation fails when timezone offset column has invalid values."""
    # Create a context with forecast_point_schema that has a timezone offset column
    forecast_schema = ForecastPointSchema(
        granularity=TimeIntervalUnit.DAY,
        dtype=DBVarType.VARCHAR,
        is_utc_time=False,
        timezone=TimeZoneColumn(column_name="FORECAST_TIMEZONE", type="offset"),
        format_string=timestamp_format_string,
    )
    context = Context.create(
        name=f"test_offset_validation_context_{ObjectId()}",
        primary_entity=[user_entity.name],
        forecast_point_schema=forecast_schema,
    )

    # Create a target for the use case
    from featurebyte import TargetNamespace

    target_namespace = TargetNamespace.create(
        name=f"test_target_offset_{ObjectId()}",
        primary_entity=[user_entity.name],
        dtype=DBVarType.FLOAT,
        target_type=TargetType.REGRESSION,
    )

    use_case = UseCase.create(
        name=f"test_use_case_offset_{ObjectId()}",
        target_name=target_namespace.name,
        context_name=context.name,
    )

    # Create a DataFrame with invalid UTC offset values
    table_name = f"TEST_INVALID_OFFSET_{ObjectId().binary.hex()[:8].upper()}"
    df = pd.DataFrame({
        SpecialColumnName.POINT_IN_TIME: pd.to_datetime([
            "2001-01-02 10:00:00",
            "2001-01-03 10:00:00",
            "2001-01-04 10:00:00",
        ]),
        "üser id": [1, 2, 3],
        SpecialColumnName.FORECAST_POINT: [
            "2001|01|10",
            "2001|01|15",
            "2001|01|20",
        ],
        "FORECAST_TIMEZONE": [
            "+05:30",  # Valid
            "invalid_offset",  # Invalid
            "-03:00",  # Valid
        ],
    })
    await session.register_table(table_name, df)

    try:
        # Get the source table
        source_table = data_source.get_source_table(
            database_name=session.database_name,
            schema_name=session.schema_name,
            table_name=table_name,
        )

        # Attempt to create observation table - should fail
        with pytest.raises(RecordCreationException) as exc:
            source_table.create_observation_table(
                name=f"test_obs_table_invalid_offset_{ObjectId()}",
                columns_rename_mapping={"üser id": "üser id"},
                use_case_name=use_case.name,
            )
        assert "FORECAST_TIMEZONE" in str(exc.value)
        assert "invalid_offset" in str(exc.value)
    finally:
        # Cleanup
        await session.drop_table(
            table_name,
            schema_name=session.schema_name,
            database_name=session.database_name,
        )
        use_case.delete()
        target_namespace.delete()
        context.delete()
