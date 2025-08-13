"""
Test cases for feature_store_warehouse.py
"""

import textwrap

import pandas as pd
import pytest

from featurebyte.common.utils import dataframe_to_json
from featurebyte.query_graph.model.common_table import TabularSource
from featurebyte.query_graph.model.table import TableSpec
from featurebyte.query_graph.node.schema import TableDetails
from featurebyte.schema.feature_store import FeatureStoreShape


@pytest.mark.parametrize("has_row_index", [False, True])
@pytest.mark.asyncio
async def test_download_table(
    feature_store_warehouse_service,
    feature_store,
    mock_snowflake_session,
    has_row_index,
):
    """
    Test download_table
    """

    # mock row count query
    mock_snowflake_session.execute_query.return_value = pd.DataFrame({
        "row_count": [100],
    })

    # mock list_table_schema query
    df_list_table_schema = pd.DataFrame({
        "col_a": {"name": "col_a"},
        "col_b": {"name": "col_b"},
    })
    if has_row_index:
        df_list_table_schema["__FB_TABLE_ROW_INDEX"] = {"name": "__FB_TABLE_ROW_INDEX"}
    mock_snowflake_session.list_table_schema.return_value = df_list_table_schema

    # check download_table triggers expected queries
    _ = await feature_store_warehouse_service.download_table(
        location=TabularSource(
            feature_store_id=feature_store.id,
            table_details=TableDetails(
                database_name="my_db",
                schema_name="my_schema",
                table_name="my_table",
            ),
        ),
    )
    if has_row_index:
        expected_query = textwrap.dedent(
            """
            SELECT
              "col_a",
              "col_b"
            FROM "my_db"."my_schema"."my_table"
            ORDER BY
              "__FB_TABLE_ROW_INDEX"
            """
        ).strip()
    else:
        expected_query = textwrap.dedent(
            """
            SELECT
              "col_a",
              "col_b"
            FROM "my_db"."my_schema"."my_table"
            """
        ).strip()
    args, _ = mock_snowflake_session.get_async_query_stream.call_args
    assert args[0] == expected_query


@pytest.mark.asyncio
async def test_table_shape(
    feature_store_warehouse_service,
    feature_store,
    mock_snowflake_session,
):
    """
    Test value counts
    """
    mock_snowflake_session.list_table_schema.return_value = {
        "col_a": {"name": "col_a"},
        "col_b": {"name": "col_b"},
    }
    mock_snowflake_session.execute_query.return_value = pd.DataFrame({
        "row_count": [100],
    })
    result = await feature_store_warehouse_service.table_shape(
        location=TabularSource(
            feature_store_id=feature_store.id,
            table_details=TableDetails(
                database_name="my_db",
                schema_name="my_schema",
                table_name="my_table",
            ),
        ),
    )
    assert result == FeatureStoreShape(num_rows=100, num_cols=2)


@pytest.mark.asyncio
async def test_table_preview(
    feature_store_warehouse_service,
    feature_store,
    mock_snowflake_session,
):
    """
    Test value counts
    """
    expected_data = pd.DataFrame({
        "col_a": [1, 2, 3],
        "col_b": ["a", "b", "c"],
    })
    mock_snowflake_session.execute_query.return_value = expected_data
    result = await feature_store_warehouse_service.table_preview(
        location=TabularSource(
            feature_store_id=feature_store.id,
            table_details=TableDetails(
                database_name="my_db",
                schema_name="my_schema",
                table_name="my_table",
            ),
        ),
        limit=3,
    )
    assert result == dataframe_to_json(expected_data)

    expected_query = textwrap.dedent(
        """
        SELECT
          *
        FROM "my_db"."my_schema"."my_table"
        LIMIT 3
        """
    ).strip()
    args, _ = mock_snowflake_session.execute_query.call_args
    assert args[0] == expected_query


@pytest.mark.parametrize("is_featurebyte_schema", [True, False])
@pytest.mark.asyncio
async def test_list_tables(
    feature_store_warehouse_service,
    feature_store,
    mock_get_feature_store_session,
    mock_is_featurebyte_schema,
    is_featurebyte_schema,
):
    """
    Test list_tables
    """
    mock_session = mock_get_feature_store_session.return_value
    mock_session.source_type = "snowflake"
    mock_session.no_schema_error = Exception
    table_specs = [
        TableSpec(name="__table1", description="table1"),
        TableSpec(name="FEATURE_TABLE_CACHE", description="feature table cache"),
        TableSpec(name="OBSERVATION_TABLE", description="observation table"),
        TableSpec(name="HISTORICAL_FEATURE_TABLE", description="historical feature table"),
        TableSpec(name="BATCH_REQUEST_TABLE", description="batch request table"),
        TableSpec(name="BATCH_FEATURE_TABLE", description="batch feature table"),
        TableSpec(name="TARGET_TABLE", description="target table"),
        TableSpec(name="managed_view", description="managed view"),
        TableSpec(name="table1", description="table1"),
        TableSpec(name="table2", description="table2"),
    ]

    mock_is_featurebyte_schema.return_value = is_featurebyte_schema

    mock_session.list_tables.return_value = table_specs
    tables = await feature_store_warehouse_service.list_tables(
        feature_store=feature_store,
        database_name="db_name",
        schema_name="schema_name",
    )
    if is_featurebyte_schema:
        assert tables == table_specs[2:8]
    else:
        assert tables == table_specs[1:]
