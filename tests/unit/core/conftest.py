"""
Common test fixtures used across test files in core directory
"""
import textwrap

import pytest

from featurebyte.core.frame import Frame
from featurebyte.core.generic import QueryObject
from featurebyte.core.series import Series
from featurebyte.core.timedelta import to_timedelta
from featurebyte.enum import DBVarType
from featurebyte.query_graph.graph import (
    GlobalGraphState,
    GlobalQueryGraph,
    NodeOutputType,
    NodeType,
)


@pytest.fixture(name="global_graph")
def global_query_graph():
    """
    Empty query graph fixture
    """
    GlobalGraphState.reset()
    # Resetting GlobalGraph invalidates the QueryObject's operation structure cache
    QueryObject._operation_structure_cache.clear()
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
            "type": "source_table",
            "columns": [col["name"] for col in columns_info],
            "timestamp": "VALUE",
            "table_details": {
                "database_name": "db",
                "schema_name": "public",
                "table_name": "transaction",
            },
            "feature_store_details": {
                "type": "snowflake",
                "details": {
                    "account": "sf_account",
                    "warehouse": "sf_warehouse",
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
        node_name=node.name,
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
    assert series.is_numeric
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
    assert series.is_numeric
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


@pytest.fixture()
def timedelta_series_from_int(int_series):
    """
    Series with timedelta var type
    """
    series = to_timedelta(int_series, unit="second")
    assert isinstance(series, Series)
    assert series.dtype == DBVarType.TIMEDELTA
    yield series


@pytest.fixture(name="expression_sql_template")
def expression_sql_template_fixture():
    """SQL template used to construct the expected sql code"""

    template_sql = textwrap.dedent(
        """
        SELECT
        {expression}
        FROM "db"."public"."transaction"
        LIMIT 10
        """
    ).strip()

    class Formatter:
        def format(self, expression):
            expression = textwrap.dedent(expression).strip()
            formatted = template_sql.format(expression=textwrap.indent(expression, " " * 2))
            return formatted

    return Formatter()
