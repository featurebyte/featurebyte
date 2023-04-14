"""
Integration tests for ObservationTable
"""
import pytest
from sqlglot import parse_one

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


@pytest.mark.asyncio
async def test_observation_table_from_view(scd_table, session, source_type):
    """
    Test creating an observation table from a view
    """
    view = scd_table.get_view()
    view["POINT_IN_TIME"] = view["Effective Timestamp"]
    sample_rows = 123
    observation_table = view.create_observation_table(
        f"MY_OBSERVATION_TABLE_FROM_VIEW_{source_type}", sample_rows=sample_rows
    )
    assert observation_table.name == f"MY_OBSERVATION_TABLE_FROM_VIEW_{source_type}"
    check_location_valid(observation_table, session)
    await check_materialized_table_accessible(observation_table, session, source_type, sample_rows)
