"""
Integration tests for ObservationTable
"""
import os
from http import HTTPStatus

import pandas as pd
import pytest

from featurebyte.api.entity import Entity
from featurebyte.api.observation_table import ObservationTable
from featurebyte.common.utils import dataframe_to_arrow_bytes
from featurebyte.config import Configurations
from featurebyte.enum import SpecialColumnName
from featurebyte.exception import RecordCreationException
from featurebyte.models.observation_table import UploadedFileInput
from featurebyte.models.request_input import RequestInputType
from featurebyte.schema.observation_table import ObservationTableUpload
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
            table for table in tables if table.startswith("OBSERVATION_TABLE")
        ]
        return len(observation_table_names)

    view = scd_table.get_view()
    view["POINT_IN_TIME"] = 123

    num_observation_tables_before = await _get_num_observation_tables()

    with pytest.raises(RecordCreationException) as exc:
        view.create_observation_table(f"BAD_OBSERVATION_TABLE_FROM_VIEW_{source_type}")

    expected_msg = "Point in time column should have timestamp type; got INT"
    assert expected_msg in str(exc.value)

    num_observation_tables_after = await _get_num_observation_tables()
    assert num_observation_tables_before == num_observation_tables_after


@pytest.mark.parametrize("source_type", ["snowflake"], indirect=True)
@pytest.mark.asyncio
async def test_observation_table_upload(
    feature_store, catalog, customer_entity, session, source_type
):
    _ = catalog, customer_entity
    # Create upload request
    upload_request = ObservationTableUpload(
        name="uploaded_observation_table",
        feature_store_id=feature_store.id,
        purpose="other",
        primary_entity_ids=[customer_entity.id],
    )
    # Read CSV file
    df = pd.read_csv(os.path.join(os.path.dirname(__file__), "fixtures", "observation_table.csv"))
    number_of_rows = df.shape[0]
    df[SpecialColumnName.POINT_IN_TIME] = pd.to_datetime(df[SpecialColumnName.POINT_IN_TIME])
    files = {"observation_set": dataframe_to_arrow_bytes(df)}
    data = {"payload": upload_request.json()}

    # Call upload route
    observation_table_route = "/observation_table"
    test_api_client = Configurations().get_client()
    response = test_api_client.put(observation_table_route, data=data, files=files)
    assert response.status_code == HTTPStatus.CREATED, response.json()
    response_dict = response.json()
    observation_table_id = response_dict["payload"]["output_document_id"]

    # Get observation table
    observation_table = ObservationTable.get_by_id(observation_table_id)

    # Assert response
    assert observation_table.name == "uploaded_observation_table"
    assert observation_table.primary_entity_ids == [customer_entity.id]
    assert observation_table.purpose == "other"
    assert observation_table.request_input == UploadedFileInput(type=RequestInputType.UPLOADED_FILE)
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
