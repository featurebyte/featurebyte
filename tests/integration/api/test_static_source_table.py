"""
Integration tests for StaticSourceTable
"""
import pytest
from sqlglot import parse_one

from featurebyte.exception import RecordCreationException
from featurebyte.query_graph.sql.common import sql_to_string


def check_location_valid(static_source_table, session):
    """
    Check that the location attribute of the static source table is valid
    """
    table_details = static_source_table.location.table_details
    table_details_dict = table_details.dict()
    table_details_dict.pop("table_name")
    assert table_details_dict == {
        "database_name": session.database_name,
        "schema_name": session.schema_name,
    }


async def check_materialized_table_accessible(
    static_source_table, session, source_type, expected_num_rows
):
    """
    Check materialized table is available
    """
    table_details = static_source_table.location.table_details
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


def check_materialized_table_preview_methods(table, expected_columns):
    """
    Check that preview, sample and describe methods work on materialized tables
    """
    df_preview = table.preview(limit=15)
    assert df_preview.shape[0] == 15
    assert df_preview.columns.tolist() == expected_columns

    df_sample = table.sample(size=20)
    assert df_sample.shape[0] == 20
    assert df_sample.columns.tolist() == expected_columns

    df_describe = table.describe()
    assert df_describe.shape[1] > 0
    assert set(df_describe.index).issuperset(["top", "min", "max"])


@pytest.mark.asyncio
@pytest.mark.parametrize("source_type", ["snowflake"], indirect=True)
async def test_static_source_table_from_source_table(
    data_source, feature_store, session, source_type, transaction_data_upper_case
):
    """
    Test creating an static source table from a source table
    """
    source_table = data_source.get_source_table(
        database_name=session.database_name,
        schema_name=session.schema_name,
        table_name="TEST_TABLE",
    )
    sample_rows = 123
    static_source_table = source_table.create_static_source_table(
        f"MY_STATIC_SOURCE_TABLE_{source_type}", sample_rows=sample_rows
    )
    assert static_source_table.name == f"MY_STATIC_SOURCE_TABLE_{source_type}"
    check_location_valid(static_source_table, session)
    await check_materialized_table_accessible(
        static_source_table, session, source_type, sample_rows
    )

    check_materialized_table_preview_methods(
        static_source_table, expected_columns=transaction_data_upper_case.columns.tolist()
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("source_type", ["snowflake"], indirect=True)
async def test_static_source_table_from_view(event_table, scd_table, session, source_type):
    """
    Test creating an static source table from a view
    """
    view = event_table.get_view()
    scd_view = scd_table.get_view()
    view = view.join(scd_view, on="ÜSER ID")
    sample_rows = 123
    static_source_table = view.create_static_source_table(
        f"MY_STATIC_SOURCE_TABLE_FROM_VIEW_{source_type}",
        sample_rows=sample_rows,
        columns=[view.timestamp_column, "ÜSER ID"],
        columns_rename_mapping={view.timestamp_column: "TS"},
    )
    assert static_source_table.name == f"MY_STATIC_SOURCE_TABLE_FROM_VIEW_{source_type}"
    check_location_valid(static_source_table, session)
    await check_materialized_table_accessible(
        static_source_table, session, source_type, sample_rows
    )

    check_materialized_table_preview_methods(
        static_source_table,
        expected_columns=["TS", "ÜSER ID"],
    )
