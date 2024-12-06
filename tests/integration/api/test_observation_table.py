"""
Integration tests for ObservationTable
"""

import os

import pandas as pd
import pytest

from featurebyte.api.entity import Entity
from featurebyte.api.observation_table import ObservationTable
from featurebyte.enum import SpecialColumnName
from featurebyte.exception import RecordCreationException
from featurebyte.models.observation_table import UploadedFileInput
from featurebyte.models.request_input import RequestInputType
from tests.integration.api.materialized_table.utils import (
    check_location_valid,
    check_materialized_table_accessible,
    check_materialized_table_preview_methods,
)


@pytest.fixture(name="normal_user_id_entity", scope="session")
def new_user_id_entity_fixture():
    """
    Fixture for a new user id entity
    """
    entity = Entity(name="normal user id", serving_names=["User ID"])
    entity.save()
    return entity


@pytest.mark.parametrize("source_type", ["snowflake"], indirect=True)
@pytest.mark.asyncio
async def test_observation_table_from_source_table(
    data_source, feature_store, session, source_type, catalog, normal_user_id_entity
):
    """
    Test creating an observation table from a source table
    """
    _ = normal_user_id_entity
    source_table = data_source.get_source_table(
        database_name=session.database_name,
        schema_name=session.schema_name,
        table_name="ORIGINAL_OBSERVATION_TABLE",
    )
    sample_rows = 123
    observation_table = source_table.create_observation_table(
        f"MY_OBSERVATION_TABLE_{source_type}", sample_rows=sample_rows
    )
    assert observation_table.name == f"MY_OBSERVATION_TABLE_{source_type}"
    table_details = observation_table.location.table_details
    check_location_valid(table_details, session)
    await check_materialized_table_accessible(table_details, session, source_type, sample_rows)

    user_id_entity_col_name = "User ID"
    check_materialized_table_preview_methods(
        observation_table, expected_columns=["POINT_IN_TIME", user_id_entity_col_name]
    )
    assert observation_table.least_recent_point_in_time is not None
    assert "User ID" in observation_table.entity_column_name_to_count
    df = observation_table.to_pandas()
    expected_min = df["POINT_IN_TIME"].min()
    expected_max = df["POINT_IN_TIME"].max()
    expected_cust_id_unique_count = df[user_id_entity_col_name].nunique()
    assert observation_table.entity_column_name_to_count["User ID"] == expected_cust_id_unique_count
    assert df.columns.tolist() == ["POINT_IN_TIME", user_id_entity_col_name]

    def _convert_timestamp_for_timezones(timestamp_str):
        current_timestamp = pd.Timestamp(timestamp_str)
        if current_timestamp.tzinfo is not None:
            current_timestamp = current_timestamp.tz_convert("UTC").tz_localize(None)
        current_timestamp = current_timestamp.isoformat()
        return str(current_timestamp)

    assert _convert_timestamp_for_timezones(
        observation_table.least_recent_point_in_time
    ) == _convert_timestamp_for_timezones(str(expected_min))
    assert _convert_timestamp_for_timezones(
        observation_table.most_recent_point_in_time
    ) == _convert_timestamp_for_timezones(str(expected_max))


@pytest.mark.asyncio
async def test_observation_table_min_interval_between_entities(
    catalog, session, data_source, normal_user_id_entity
):
    _ = catalog, normal_user_id_entity
    df = pd.read_csv(
        os.path.join(os.path.dirname(__file__), "fixtures", "observation_table_time_interval.csv")
    )
    df[SpecialColumnName.POINT_IN_TIME] = pd.to_datetime(
        df[SpecialColumnName.POINT_IN_TIME].astype(str)
    )
    table_name = "observation_table_time_interval"
    await session.register_table(table_name, df)

    database_table = data_source.get_source_table(
        database_name=session.database_name,
        schema_name=session.schema_name,
        table_name=table_name,
    )
    sample_rows = 123
    observation_table = database_table.create_observation_table(
        "MY_OBSERVATION_TABLE_FOR_INTERVALS", sample_rows=sample_rows
    )
    assert observation_table.min_interval_secs_between_entities == 3600


@pytest.mark.asyncio
async def test_observation_table_from_view(
    event_table, scd_table, session, source_type, user_entity
):
    """
    Test creating an observation table from a view
    """
    _ = user_entity
    view = event_table.get_view()
    scd_view = scd_table.get_view()
    view = view.join(scd_view, on="ÜSER ID")
    sample_rows = 123
    observation_table = view.create_observation_table(
        f"MY_OBSERVATION_TABLE_FROM_VIEW_{source_type}",
        sample_rows=sample_rows,
        columns=[view.timestamp_column, "ÜSER ID"],
        columns_rename_mapping={view.timestamp_column: "POINT_IN_TIME", "ÜSER ID": "üser id"},
    )
    assert observation_table.name == f"MY_OBSERVATION_TABLE_FROM_VIEW_{source_type}"
    table_details = observation_table.location.table_details
    check_location_valid(table_details, session)
    await check_materialized_table_accessible(table_details, session, source_type, sample_rows)

    check_materialized_table_preview_methods(
        observation_table,
        expected_columns=["POINT_IN_TIME", "üser id"],
    )


@pytest.mark.asyncio
async def test_observation_table_cleanup(scd_table, session, source_type):
    """
    Test that invalid observation tables are cleaned up
    """

    async def _get_num_observation_tables():
        tables = await session.list_tables(
            database_name=session.database_name, schema_name=session.schema_name
        )
        observation_table_names = [
            table.name for table in tables if table.name.startswith("OBSERVATION_TABLE")
        ]
        return len(observation_table_names)

    view = scd_table.get_view()
    view["POINT_IN_TIME"] = 123

    num_observation_tables_before = await _get_num_observation_tables()

    with pytest.raises(RecordCreationException) as exc:
        view.create_observation_table(
            f"BAD_OBSERVATION_TABLE_FROM_VIEW_{source_type}",
            primary_entities=["User"],
            columns_rename_mapping={"User ID": "üser id"},
        )

    expected_msg = "Point in time column should have timestamp type; got INT"
    assert expected_msg in str(exc.value)

    num_observation_tables_after = await _get_num_observation_tables()
    assert num_observation_tables_before == num_observation_tables_after


@pytest.mark.parametrize("file_type", ["csv", "parquet"])
@pytest.mark.parametrize("source_type", ["snowflake"], indirect=True)
@pytest.mark.asyncio
async def test_observation_table_upload(
    feature_store, catalog, customer_entity, session, source_type, file_type
):
    _ = catalog, customer_entity

    # Upload observation table
    file_name = f"observation_table.{file_type}"
    file_path = os.path.join(os.path.dirname(__file__), "fixtures", file_name)
    if file_type == "csv":
        df = pd.read_csv(file_path)
    elif file_type == "parquet":
        df = pd.read_parquet(file_path)
    else:
        raise ValueError(f"Unsupported file type: {file_type}")
    number_of_rows = df.shape[0]
    observation_table = ObservationTable.upload(
        file_path=file_path,
        name=f"uploaded_observation_table_{file_type}",
    )

    # Assert response
    assert observation_table.name == f"uploaded_observation_table_{file_type}"
    assert observation_table.request_input == UploadedFileInput(
        type=RequestInputType.UPLOADED_FILE,
        file_name=file_name,
    )
    expected_columns = {SpecialColumnName.POINT_IN_TIME, "cust_id"}
    actual_columns = {column.name for column in observation_table.columns_info}
    assert expected_columns == actual_columns

    # Assert materialized table
    await check_materialized_table_accessible(
        observation_table.location.table_details, session, source_type, number_of_rows
    )
    check_materialized_table_preview_methods(
        observation_table, [SpecialColumnName.POINT_IN_TIME, "cust_id"], number_of_rows
    )


@pytest.mark.asyncio
async def test_observation_table_sample_time_range(
    event_table, scd_table, session, source_type, user_entity
):
    """
    Test creating an observation table from a view sampled with time range
    """
    _ = user_entity
    view = event_table.get_view()
    scd_view = scd_table.get_view()
    view = view.join(scd_view, on="ÜSER ID")
    sample_rows = 123
    observation_table = view.create_observation_table(
        f"MY_OBSERVATION_TABLE_FROM_VIEW_{source_type}_TIME_RANGE_SAMPLED",
        sample_rows=sample_rows,
        sample_from_timestamp="2001-02-01T00:00:00Z",
        sample_to_timestamp="2001-06-30T00:00:00Z",
        columns=[view.timestamp_column, "ÜSER ID"],
        columns_rename_mapping={view.timestamp_column: "POINT_IN_TIME", "ÜSER ID": "üser id"},
    )
    assert (
        observation_table.name == f"MY_OBSERVATION_TABLE_FROM_VIEW_{source_type}_TIME_RANGE_SAMPLED"
    )
    table_details = observation_table.location.table_details
    check_location_valid(table_details, session)
    await check_materialized_table_accessible(table_details, session, source_type, sample_rows)

    df_preview = observation_table.preview(limit=10)
    assert df_preview.columns.tolist() == ["POINT_IN_TIME", "üser id"]

    df_describe = observation_table.describe()
    assert pd.to_datetime(df_describe.loc["min", "POINT_IN_TIME"]) >= pd.Timestamp("2001-02-01")
    assert pd.to_datetime(df_describe.loc["max", "POINT_IN_TIME"]) <= pd.Timestamp("2001-06-30")
