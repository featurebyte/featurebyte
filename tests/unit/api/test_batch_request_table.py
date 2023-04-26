"""
Unit tests for BatchRequestTable class
"""
import pandas as pd
import pytest

from featurebyte.api.batch_request_table import BatchRequestTable


@pytest.fixture(name="batch_request_table_from_source")
def batch_request_table_from_source_fixture(
    snowflake_database_table,
    snowflake_execute_query_batch_request_table_patcher,
    snowflake_query_map,
):
    with snowflake_execute_query_batch_request_table_patcher(snowflake_query_map, True):
        return snowflake_database_table.create_batch_request_table(
            "batch_request_table_from_source_table"
        )


@pytest.fixture(name="batch_request_table_from_view")
def batch_request_table_from_view_fixture(
    snowflake_event_view, snowflake_execute_query_batch_request_table_patcher, snowflake_query_map
):
    with snowflake_execute_query_batch_request_table_patcher(snowflake_query_map, True):
        return snowflake_event_view.create_batch_request_table(
            "batch_request_table_from_event_view"
        )


def test_get(batch_request_table_from_source):
    """
    Test retrieving an BatchRequestTable object by name
    """
    batch_request_table = BatchRequestTable.get(batch_request_table_from_source.name)
    assert batch_request_table.name == batch_request_table_from_source.name


def test_list(batch_request_table_from_source, batch_request_table_from_view):
    """
    Test listing BatchRequestTable objects
    """
    df = BatchRequestTable.list()
    df = df.sort_values("name").reset_index(drop=True)
    expected = pd.DataFrame(
        [
            {
                "id": batch_request_table_from_view.id,
                "name": "batch_request_table_from_event_view",
                "type": "view",
                "feature_store_name": "sf_featurestore",
                "created_at": batch_request_table_from_view.created_at,
            },
            {
                "id": batch_request_table_from_source.id,
                "name": "batch_request_table_from_source_table",
                "type": "source_table",
                "feature_store_name": "sf_featurestore",
                "created_at": batch_request_table_from_source.created_at,
            },
        ]
    )
    pd.testing.assert_frame_equal(df, expected)


def test_info(batch_request_table_from_view):
    """
    Test get request table info
    """
    info_dict = batch_request_table_from_view.info()
    assert info_dict["table_details"]["table_name"].startswith("BATCH_REQUEST_TABLE_")
    assert info_dict == {
        "name": "batch_request_table_from_event_view",
        "type": "view",
        "feature_store_name": "sf_featurestore",
        "table_details": {
            "database_name": "sf_database",
            "schema_name": "sf_schema",
            "table_name": info_dict["table_details"]["table_name"],
        },
        "columns_info": [{"name": "cust_id", "dtype": "INT"}],
        "created_at": info_dict["created_at"],
        "updated_at": None,
    }
