"""
Unit tests for StaticSourceTable class
"""

from unittest.mock import call, patch

import pandas as pd
import pytest

from featurebyte.api.static_source_table import StaticSourceTable
from featurebyte.enum import DBVarType
from featurebyte.exception import RecordRetrievalException
from featurebyte.query_graph.model.column_info import ColumnSpecWithDescription


def test_get(static_source_table_from_source):
    """
    Test retrieving an StaticSourceTable object by name
    """
    static_source_table = StaticSourceTable.get(static_source_table_from_source.name)
    assert static_source_table == static_source_table_from_source


@pytest.mark.usefixtures("static_source_table_from_source", "static_source_table_from_view")
def test_list(catalog):
    """
    Test listing StaticSourceTable objects
    """
    _ = catalog
    df = StaticSourceTable.list()
    assert df.columns.tolist() == [
        "id",
        "name",
        "type",
        "shape",
        "feature_store_name",
        "created_at",
    ]
    assert df["name"].tolist() == [
        "static_source_table_from_event_view",
        "static_source_table_from_source_table",
    ]
    assert (df["feature_store_name"] == "sf_featurestore").all()
    assert df["type"].tolist() == ["view", "source_table"]
    assert df["shape"].tolist() == [[100, 2]] * 2


def test_delete(static_source_table_from_view):
    """
    Test delete method
    """
    # check table can be retrieved before deletion
    _ = StaticSourceTable.get(static_source_table_from_view.name)

    static_source_table_from_view.delete()

    # check the deleted batch feature table is not found anymore
    with pytest.raises(RecordRetrievalException) as exc:
        StaticSourceTable.get(static_source_table_from_view.name)

    expected_msg = (
        f'StaticSourceTable (name: "{static_source_table_from_view.name}") not found. '
        f"Please save the StaticSourceTable object first."
    )
    assert expected_msg in str(exc.value)


def test_info(static_source_table_from_source):
    """Test get static source table info"""
    info_dict = static_source_table_from_source.info()
    assert info_dict["table_details"]["table_name"].startswith("STATIC_SOURCE_TABLE_")
    assert info_dict == {
        "name": "static_source_table_from_source_table",
        "type": "source_table",
        "feature_store_name": "sf_featurestore",
        "table_details": {
            "database_name": "sf_database",
            "schema_name": "sf_schema",
            "table_name": info_dict["table_details"]["table_name"],
        },
        "created_at": info_dict["created_at"],
        "updated_at": None,
        "description": None,
    }


def test_shape(static_source_table_from_source):
    """
    Test shape method
    """
    assert static_source_table_from_source.shape() == (100, 2)


def test_data_source(static_source_table_from_source):
    """
    Test the underlying SourceTable is constructed properly
    """
    static_source_table = static_source_table_from_source
    source_table = static_source_table._source_table
    assert source_table.feature_store.id == static_source_table.location.feature_store_id
    assert source_table.tabular_source.table_details == static_source_table.location.table_details


@patch("featurebyte.session.snowflake.SnowflakeSession.list_table_schema")
@patch("featurebyte.session.snowflake.SnowflakeSession.execute_query")
def test_preview(mock_execute_query, mock_list_table_schema, static_source_table_from_source):
    """
    Test preview() calls the underlying SourceTable's preview() method
    """

    def side_effect(query, **kwargs):
        _ = query, kwargs
        return pd.DataFrame()

    mock_execute_query.side_effect = side_effect
    mock_list_table_schema.return_value = {
        "col_index": ColumnSpecWithDescription(name="col_index", dtype=DBVarType.INT)
    }
    static_source_table_from_source.preview(limit=123)
    assert len(mock_execute_query.call_args_list) == 1

    sql_query = mock_execute_query.call_args_list[0][0][0]
    assert mock_execute_query.call_args_list[0]

    # check generated SQL query
    assert 'SELECT\n  "col_index" AS "col_index"' in sql_query
    assert 'FROM "sf_database"."sf_schema"."STATIC_SOURCE_TABLE_' in sql_query
    assert not sql_query.endswith("ORDER BY\n  __FB_TABLE_ROW_INDEX NULLS FIRST\nLIMIT 123")


def test_sample(static_source_table_from_source, mock_source_table):
    """
    Test sample() calls the underlying SourceTable's sample() method
    """
    result = static_source_table_from_source.sample(size=123, seed=456)
    assert mock_source_table.sample.call_args == call(size=123, seed=456)
    assert result is mock_source_table.sample.return_value


def test_describe(static_source_table_from_source, mock_source_table):
    """
    Test describe() calls the underlying SourceTable's describe() method
    """
    result = static_source_table_from_source.describe(size=123, seed=456)
    assert mock_source_table.describe.call_args == call(size=123, seed=456)
    assert result is mock_source_table.describe.return_value


def test_update_description(static_source_table_from_source):
    """Test update description"""
    assert static_source_table_from_source.description is None
    static_source_table_from_source.update_description("new description")
    assert static_source_table_from_source.description == "new description"
    assert static_source_table_from_source.info()["description"] == "new description"
    static_source_table_from_source.update_description(None)
    assert static_source_table_from_source.description is None
    assert static_source_table_from_source.info()["description"] is None
