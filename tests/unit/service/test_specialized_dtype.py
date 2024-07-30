"""Unit tests for data type info detectors (embedding and flat dict)"""

import json
from unittest.mock import patch

import numpy as np
import pandas as pd
import pytest
from bson import ObjectId

from featurebyte.enum import DBVarType
from featurebyte.models.dimension_table import DimensionTableModel
from featurebyte.models.event_table import EventTableModel
from featurebyte.models.feature_store import TableStatus
from featurebyte.models.item_table import ItemTableModel
from featurebyte.models.scd_table import SCDTableModel
from featurebyte.query_graph.model.column_info import ColumnInfo
from featurebyte.query_graph.model.common_table import TabularSource
from featurebyte.query_graph.node.schema import TableDetails
from featurebyte.service.specialized_dtype import SpecializedDtypeDetectionService


@pytest.fixture(name="patch_get_row_count", autouse=True)
def patch_get_row_count_fixture():
    """Patch _get_row_count method for PreviewService"""
    with patch("featurebyte.service.preview.PreviewService._get_row_count") as method:
        method.return_value = 100
        yield


@pytest.fixture(name="event_table")
def event_table_fixture(feature_store):
    """Event table fixture"""
    return EventTableModel(
        id=ObjectId(),
        name="even_table",
        tabular_source=TabularSource(
            feature_store_id=feature_store.id,
            table_details=TableDetails(
                database_name="db",
                schema_name="schema",
                table_name="table",
            ),
        ),
        columns_info=[
            ColumnInfo(name="event_id_col", dtype=DBVarType.VARCHAR),
            ColumnInfo(name="event_timestamp_col", dtype=DBVarType.TIMESTAMP),
            ColumnInfo(name="embedding_col", dtype=DBVarType.ARRAY),
            ColumnInfo(name="dict_col", dtype=DBVarType.OBJECT),
        ],
        event_id_column="event_id_col",
        event_timestamp_column="event_timestamp_col",
        default_feature_job_setting=None,
        status=TableStatus.PUBLIC_DRAFT,
        created_at=None,
        updated_at=None,
    )


@pytest.fixture(name="item_table")
def item_table_fixture(feature_store):
    """Item table fixture"""
    return ItemTableModel(
        id=ObjectId(),
        name="item_table",
        tabular_source=TabularSource(
            feature_store_id=feature_store.id,
            table_details=TableDetails(
                database_name="db",
                schema_name="schema",
                table_name="table",
            ),
        ),
        columns_info=[
            ColumnInfo(name="event_id_col", dtype=DBVarType.VARCHAR),
            ColumnInfo(name="item_id_col", dtype=DBVarType.VARCHAR),
            ColumnInfo(name="embedding_col", dtype=DBVarType.ARRAY),
            ColumnInfo(name="dict_col", dtype=DBVarType.OBJECT),
        ],
        event_id_column="event_id_col",
        item_id_column="item_id_col",
        event_table_id=ObjectId(),
        status=TableStatus.PUBLIC_DRAFT,
        created_at=None,
        updated_at=None,
    )


@pytest.fixture(name="dimension_table")
def dimension_table_fixture(feature_store):
    """Dimension table fixture"""
    return DimensionTableModel(
        id=ObjectId(),
        name="dimension_table",
        tabular_source=TabularSource(
            feature_store_id=feature_store.id,
            table_details=TableDetails(
                database_name="db",
                schema_name="schema",
                table_name="table",
            ),
        ),
        columns_info=[
            ColumnInfo(name="dimension_id_col", dtype=DBVarType.VARCHAR),
            ColumnInfo(name="embedding_col", dtype=DBVarType.ARRAY),
            ColumnInfo(name="dict_col", dtype=DBVarType.STRUCT),
        ],
        dimension_id_column="dimension_id_col",
        status=TableStatus.PUBLIC_DRAFT,
        created_at=None,
        updated_at=None,
    )


@pytest.fixture(name="scd_table")
def scd_table_fixture(feature_store):
    """SCD table fixture"""
    return SCDTableModel(
        id=ObjectId(),
        name="dimension_table",
        tabular_source=TabularSource(
            feature_store_id=feature_store.id,
            table_details=TableDetails(
                database_name="db",
                schema_name="schema",
                table_name="table",
            ),
        ),
        columns_info=[
            ColumnInfo(name="natural_key_col", dtype=DBVarType.VARCHAR),
            ColumnInfo(name="surrogate_key_col", dtype=DBVarType.VARCHAR),
            ColumnInfo(name="effective_timestamp_col", dtype=DBVarType.TIMESTAMP),
            ColumnInfo(name="end_timestamp_col", dtype=DBVarType.TIMESTAMP),
            ColumnInfo(name="current_flag_col", dtype=DBVarType.BOOL),
            ColumnInfo(name="embedding_col", dtype=DBVarType.ARRAY),
            ColumnInfo(name="dict_col", dtype=DBVarType.STRUCT),
        ],
        natural_key_column="natural_key_col",
        surrogate_key_column="surrogate_key_col",
        effective_timestamp_column="effective_timestamp_col",
        end_timestamp_column="end_timestamp_col",
        current_flag="current_flag_col",
    )


@pytest.fixture(params=["event_table", "item_table", "dimension_table", "scd_table"], name="table")
def table_fixture(request):
    """All tables fixture"""
    return request.getfixturevalue(request.param)


@pytest.mark.asyncio
async def test_add_columns_attributes(
    preview_service,
    feature_store_service,
    feature_store,
    mock_snowflake_session,
    table,
):
    """Test adding columns attributes"""
    _ = feature_store

    mock_snowflake_session.execute_query_long_running.return_value = pd.DataFrame({
        "event_id_col": ["a", "b", "c"],
        "event_timestamp_col": [
            pd.to_datetime("2023-01-01"),
            pd.to_datetime("2023-01-02"),
            pd.to_datetime("2023-01-03"),
        ],
        "embedding_col": [np.array([0, 1, 2]), np.array([3, 4, 5]), np.array([6, 7, 8])],
        "dict_col": [
            json.dumps({"a": 1, "b": 2}),
            json.dumps({"c": 3, "d": 4}),
            json.dumps({"e": 5, "f": 6}),
        ],
    })

    svc = SpecializedDtypeDetectionService(preview_service, feature_store_service)
    await svc.detect_and_update_column_dtypes(table)

    embedding_col = [col for col in table.columns_info if col.name == "embedding_col"][0]
    assert embedding_col.dtype == DBVarType.EMBEDDING

    flat_dict_col = [col for col in table.columns_info if col.name == "dict_col"][0]
    assert flat_dict_col.dtype == DBVarType.FLAT_DICT


@pytest.mark.parametrize(
    "sample",
    [
        pd.DataFrame({
            "event_id_col": ["a", "b", "c"],
            "event_timestamp_col": [
                pd.to_datetime("2023-01-01"),
                pd.to_datetime("2023-01-02"),
                pd.to_datetime("2023-01-03"),
            ],
            "embedding_col": [np.array([0, 1]), np.array([3, 4, 5]), np.array([6, 7, 8])],
            "dict_col": [
                json.dumps({"a": 1, "b": 2}),
                json.dumps({"c": 3, "d": 4}),
                json.dumps({"e": 5, "f": 6}),
            ],
        }),
        pd.DataFrame({
            "event_id_col": ["a", "b", "c"],
            "event_timestamp_col": [
                pd.to_datetime("2023-01-01"),
                pd.to_datetime("2023-01-02"),
                pd.to_datetime("2023-01-03"),
            ],
            "embedding_col": [
                np.array([0, 1, 2]),
                np.array([3, 4, 5]),
                np.array([[6, 7, 8], [9, 10, 11]]),
            ],
            "dict_col": [
                json.dumps({"a": 1, "b": 2}),
                json.dumps({"c": 3, "d": 4}),
                json.dumps({"e": 5, "f": 6}),
            ],
        }),
        pd.DataFrame({
            "event_id_col": ["a", "b", "c"],
            "event_timestamp_col": [
                pd.to_datetime("2023-01-01"),
                pd.to_datetime("2023-01-02"),
                pd.to_datetime("2023-01-03"),
            ],
            "embedding_col": [np.array([0, 1, 2]), np.array([3, "a", 5]), np.array([6, 7, 8])],
            "dict_col": [
                json.dumps({"a": 1, "b": 2}),
                json.dumps({"c": 3, "d": 4}),
                json.dumps({"e": 5, "f": 6}),
            ],
        }),
    ],
)
@pytest.mark.asyncio
async def test_not_embeddding_column(
    preview_service,
    feature_store_service,
    feature_store,
    mock_snowflake_session,
    table,
    sample,
):
    """Test array is not embedding"""
    _ = feature_store

    mock_snowflake_session.execute_query_long_running.return_value = sample

    svc = SpecializedDtypeDetectionService(preview_service, feature_store_service)
    await svc.detect_and_update_column_dtypes(table)

    embedding_col = [col for col in table.columns_info if col.name == "embedding_col"][0]
    assert embedding_col.dtype == DBVarType.ARRAY

    flat_dict_col = [col for col in table.columns_info if col.name == "dict_col"][0]
    assert flat_dict_col.dtype == DBVarType.FLAT_DICT


@pytest.mark.asyncio
async def test_nested_dict_column(
    preview_service,
    feature_store_service,
    feature_store,
    mock_snowflake_session,
    table,
):
    """Test dict is not flat"""
    _ = feature_store

    mock_snowflake_session.execute_query_long_running.return_value = pd.DataFrame({
        "event_id_col": ["a", "b", "c"],
        "event_timestamp_col": [
            pd.to_datetime("2023-01-01"),
            pd.to_datetime("2023-01-02"),
            pd.to_datetime("2023-01-03"),
        ],
        "embedding_col": [np.array([0, 1, 2]), np.array([3, 4, 5]), np.array([6, 7, 8])],
        "dict_col": [
            json.dumps({"a": 1, "b": 2}),
            json.dumps({"c": 3, "d": {"dd": 4}}),
            json.dumps({"e": 5, "f": 6}),
        ],
    })

    svc = SpecializedDtypeDetectionService(preview_service, feature_store_service)
    await svc.detect_and_update_column_dtypes(table)

    embedding_col = [col for col in table.columns_info if col.name == "embedding_col"][0]
    assert embedding_col.dtype == DBVarType.EMBEDDING

    flat_dict_col = [col for col in table.columns_info if col.name == "dict_col"][0]
    assert flat_dict_col.dtype in DBVarType.dictionary_types()
