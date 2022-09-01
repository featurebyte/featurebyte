"""
Common test fixtures used across test files in core directory
"""
import pytest

from featurebyte.core.frame import Frame
from featurebyte.core.series import Series
from featurebyte.enum import DBVarType
from featurebyte.query_graph.graph import (
    GlobalQueryGraph,
    GlobalQueryGraphState,
    NodeOutputType,
    NodeType,
)


@pytest.fixture(name="global_graph")
def global_query_graph():
    """
    Empty query graph fixture
    """
    GlobalQueryGraphState.reset()
    yield GlobalQueryGraph()


@pytest.fixture(name="dataframe")
def dataframe_fixture(global_graph, snowflake_feature_store):
    """
    Frame test fixture
    """
    columns_info = [
        {"name": "CUST_ID", "dtype": DBVarType.INT},
        {"name": "PRODUCT_ACTION", "dtype": DBVarType.VARCHAR},
        {"name": "VALUE", "dtype": DBVarType.FLOAT},
        {"name": "MASK", "dtype": DBVarType.BOOL},
        {"name": "TIMESTAMP", "dtype": DBVarType.TIMESTAMP},
        {"name": "PROMOTION_START_DATE", "dtype": DBVarType.DATE},
    ]
    node = global_graph.add_operation(
        node_type=NodeType.INPUT,
        node_params={
            "columns": [col["name"] for col in columns_info],
            "timestamp": "VALUE",
            "dbtable": {
                "database_name": "db",
                "schema_name": "public",
                "table_name": "transaction",
            },
            "feature_store": {
                "type": "snowflake",
                "details": {
                    "database": "db",
                    "sf_schema": "public",
                },
            },
        },
        node_output_type=NodeOutputType.FRAME,
        input_nodes=[],
    )
    yield Frame(
        feature_store=snowflake_feature_store,
        tabular_source={
            "feature_store_id": snowflake_feature_store.id,
            "table_details": {
                "database_name": "db",
                "schema_name": "public",
                "table_name": "some_table_name",
            },
        },
        columns_info=columns_info,
        node=node,
        column_lineage_map={col["name"]: (node.name,) for col in columns_info},
        row_index_lineage=(node.name,),
    )


@pytest.fixture()
def bool_series(dataframe):
    """
    Series with boolean var type
    """
    series = dataframe["MASK"]
    assert isinstance(series, Series)
    assert series.name == "MASK"
    assert series.dtype == DBVarType.BOOL
    yield series


@pytest.fixture()
def int_series(dataframe):
    """
    Series with integer var type
    """
    series = dataframe["CUST_ID"]
    assert isinstance(series, Series)
    assert series.name == "CUST_ID"
    assert series.dtype == DBVarType.INT
    yield series


@pytest.fixture()
def float_series(dataframe):
    """
    Series with float var type
    """
    series = dataframe["VALUE"]
    assert isinstance(series, Series)
    assert series.name == "VALUE"
    assert series.dtype == DBVarType.FLOAT
    yield series


@pytest.fixture()
def varchar_series(dataframe):
    """
    Series with string var type
    """
    series = dataframe["PRODUCT_ACTION"]
    assert isinstance(series, Series)
    assert series.name == "PRODUCT_ACTION"
    assert series.dtype == DBVarType.VARCHAR
    yield series


@pytest.fixture()
def timestamp_series(dataframe):
    """
    Series with timestamp var type
    """
    series = dataframe["TIMESTAMP"]
    assert isinstance(series, Series)
    assert series.name == "TIMESTAMP"
    assert series.dtype == DBVarType.TIMESTAMP
    yield series


@pytest.fixture()
def timestamp_series_2(dataframe):
    """
    Another series with timestamp var type
    """
    series = dataframe["PROMOTION_START_DATE"]
    assert isinstance(series, Series)
    assert series.name == "PROMOTION_START_DATE"
    assert series.dtype == DBVarType.DATE
    yield series


@pytest.fixture()
def timedelta_series(timestamp_series, timestamp_series_2):
    """
    Series with timedelta var type
    """
    series = timestamp_series - timestamp_series_2
    assert isinstance(series, Series)
    assert series.dtype == DBVarType.TIMEDELTA
    yield series
