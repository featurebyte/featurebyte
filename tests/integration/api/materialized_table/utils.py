"""
Test utils for materialized tables
"""

from typing import List

from sqlglot import parse_one

from featurebyte import SourceType
from featurebyte.query_graph.node.schema import TableDetails
from featurebyte.query_graph.sql.common import sql_to_string
from featurebyte.session.base import BaseSession


async def check_materialized_table_accessible(
    table_details: TableDetails,
    session: BaseSession,
    source_type: SourceType,
    expected_num_rows: int,
):
    """
    Check materialized table is available
    """
    query = sql_to_string(
        parse_one(
            f"""
            SELECT COUNT(*) FROM "{table_details.database_name}"."{table_details.schema_name}"."{table_details.table_name}"
            """
        ),
        source_type=source_type,
    )
    df = await session.execute_query(query)
    num_rows = df.iloc[0, 0]
    assert num_rows == expected_num_rows


def check_location_valid(table_details: TableDetails, session: BaseSession):
    """
    Check that the location attribute of the materialized table is valid
    """
    table_details_dict = table_details.model_dump()
    table_details_dict.pop("table_name")
    assert table_details_dict == {
        "database_name": session.database_name,
        "schema_name": session.schema_name,
    }


def check_materialized_table_preview_methods(table, expected_columns: List[str], number_of_rows=15):
    """
    Check that preview, sample and describe methods work on materialized tables
    """
    df_preview = table.preview(limit=number_of_rows)
    assert df_preview.shape[0] == number_of_rows
    assert df_preview.columns.tolist() == expected_columns

    df_sample = table.sample(size=number_of_rows)
    assert df_sample.shape[0] == number_of_rows
    # Table weight column is not included in sample preview
    if "__FB_TABLE_ROW_WEIGHT" in expected_columns:
        expected_columns = [col for col in expected_columns if col != "__FB_TABLE_ROW_WEIGHT"]
    assert df_sample.columns.tolist() == expected_columns

    df_describe = table.describe()
    assert df_describe.shape[1] > 0
    assert set(df_describe.index).issuperset(["top", "min", "max"])
