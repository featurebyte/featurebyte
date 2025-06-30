"""
Test adding comments to tables or columns
"""

import pandas as pd
import pytest
import pytest_asyncio
from bson import ObjectId

from featurebyte.query_graph.model.column_info import ColumnSpecDetailed
from featurebyte.session.base import BaseSession


@pytest_asyncio.fixture(scope="module")
async def registered_table(session):
    """
    Fixture for a registered table
    """
    table_name = f"test_table_for_comment_{str(ObjectId())}"
    df = pd.DataFrame({
        "a": [1, 2, 3],
        "b": [4, 5, 6],
    })
    await session.register_table(table_name, df)
    yield table_name
    await session.drop_table(
        table_name=table_name,
        schema_name=session.schema_name,
        database_name=session.database_name,
    )


@pytest.mark.asyncio
async def test_comment_table(session: BaseSession, registered_table):
    """
    Test adding comment to table
    """
    await session.comment_table(registered_table, "some comment")
    table_details = await session.get_table_details(
        registered_table,
        database_name=session.database_name,
        schema_name=session.schema_name,
    )
    assert table_details.description == "some comment"


@pytest.mark.asyncio
async def test_comment_column(session: BaseSession, registered_table):
    """
    Test adding comment to table
    """
    await session.comment_column(registered_table, "a", "this is column a")
    column_specs = await session.list_table_schema(
        table_name=registered_table,
        database_name=session.database_name,
        schema_name=session.schema_name,
    )
    assert column_specs == {
        "a": ColumnSpecDetailed(name="a", dtype="INT", description="this is column a"),
        "b": ColumnSpecDetailed(name="b", dtype="INT", description=None),
    }
