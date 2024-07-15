"""
Tests for cosine similarity UDF
"""

import pandas as pd
import pytest
import pytest_asyncio
from sqlglot import expressions

from featurebyte.query_graph.sql.adapter import get_sql_adapter
from featurebyte.query_graph.sql.common import quoted_identifier, sql_to_string

TEST_TABLE_NAME = "OBJECT_AGG_TEST_TABLE"


@pytest_asyncio.fixture(name="setup_test_data", scope="module")
async def setup_test_data_fixture(session):
    """
    Setup test data
    """
    # Prepare test data
    table = pd.DataFrame(
        {
            "id_col": ["1", "2"],
            "val_col": [None, -1.5],
        }
    )
    await session.register_table(table_name=TEST_TABLE_NAME, dataframe=table)
    yield
    await session.drop_table(
        TEST_TABLE_NAME, schema_name=session.schema_name, database_name=session.database_name
    )


@pytest.mark.asyncio
async def test_object_agg_udf(source_type, session, setup_test_data):
    """
    Test object aggregate UDF
    """
    _ = setup_test_data
    adapter = get_sql_adapter(source_type)
    object_agg_expr = adapter.object_agg(quoted_identifier("id_col"), quoted_identifier("val_col"))
    select_expr = expressions.select(
        expressions.alias_(
            expression=object_agg_expr,
            alias="OUT",
            quoted=True,
        )
    ).from_(quoted_identifier(TEST_TABLE_NAME))
    df = await session.execute_query(sql_to_string(select_expr, source_type))
    actual = df.iloc[0]["OUT"]
    assert actual == {"2": -1.5}
