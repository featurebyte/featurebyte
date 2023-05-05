"""
Integration tests for ObservationTable
"""
import pytest
from sqlglot import parse_one

from featurebyte.exception import RecordCreationException
from featurebyte.query_graph.sql.common import sql_to_string


def check_location_valid(observation_table, session):
    """
    Check that the location attribute of the observation table is valid
    """
    table_details = observation_table.location.table_details
    table_details_dict = table_details.dict()
    table_details_dict.pop("table_name")
    assert table_details_dict == {
        "database_name": session.database_name,
        "schema_name": session.schema_name,
    }


async def check_materialized_table_accessible(
    observation_table, session, source_type, expected_num_rows
):
    """
    Check materialized table is available
    """
    table_details = observation_table.location.table_details
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
async def test_observation_table_from_source_table(
    data_source, feature_store, session, source_type
):
    """
    Test creating an observation table from a source table
    """
    source_table = data_source.get_source_table(
        database_name=session.database_name,
        schema_name=session.schema_name,
        table_name="ORIGINAL_OBSERVATION_TABLE",
    )
    sample_rows = 123
    observation_table = source_table.create_observation_table(
        f"MY_OBSERVATION_TABLE_{source_type}", sample_rows=sample_rows
    )
    assert observation_table.name == f"MY_OBSERVATION_TABLE_{source_type}"
    check_location_valid(observation_table, session)
    await check_materialized_table_accessible(observation_table, session, source_type, sample_rows)

    check_materialized_table_preview_methods(
        observation_table, expected_columns=["POINT_IN_TIME", "User ID"]
    )


@pytest.mark.asyncio
async def test_observation_table_from_view(event_table, scd_table, session, source_type):
    """
    Test creating an observation table from a view
    """
    view = event_table.get_view()
    scd_view = scd_table.get_view()
    view = view.join(scd_view, on="ÜSER ID")
    sample_rows = 123
    observation_table = view.create_observation_table(
        f"MY_OBSERVATION_TABLE_FROM_VIEW_{source_type}",
        sample_rows=sample_rows,
        columns=[view.timestamp_column, "ÜSER ID"],
        columns_rename_mapping={view.timestamp_column: "POINT_IN_TIME"},
    )
    assert observation_table.name == f"MY_OBSERVATION_TABLE_FROM_VIEW_{source_type}"
    check_location_valid(observation_table, session)
    await check_materialized_table_accessible(observation_table, session, source_type, sample_rows)

    check_materialized_table_preview_methods(
        observation_table,
        expected_columns=["POINT_IN_TIME", "ÜSER ID"],
    )


@pytest.mark.asyncio
async def test_observation_table_cleanup(scd_table, session, source_type):
    """
    Test that invalid observation tables are cleaned up
    """

    async def _get_num_observation_tables():
        tables = await session.list_tables(
            database_name=session.database_name, schema_name=session.schema_name
        )
        observation_table_names = [
            table for table in tables if table.startswith("OBSERVATION_TABLE")
        ]
        return len(observation_table_names)

    view = scd_table.get_view()
    view["POINT_IN_TIME"] = 123

    num_observation_tables_before = await _get_num_observation_tables()

    with pytest.raises(RecordCreationException) as exc:
        view.create_observation_table(f"BAD_OBSERVATION_TABLE_FROM_VIEW_{source_type}")

    expected_msg = "Point in time column should have timestamp type; got INT"
    assert expected_msg in str(exc.value)

    num_observation_tables_after = await _get_num_observation_tables()
    assert num_observation_tables_before == num_observation_tables_after
