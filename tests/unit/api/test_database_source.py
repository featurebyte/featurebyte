"""
Unit test for DatabaseSource
"""
from featurebyte.api.database_table import DatabaseTable


def test_get_session(
    snowflake_connector, snowflake_execute_query, snowflake_database_source, config
):
    """
    Test DatabaseSource.get_session return expected session
    """
    _ = snowflake_connector, snowflake_execute_query
    session = snowflake_database_source.get_session(credentials=config.credentials)
    assert session.dict() == {
        "source_type": "snowflake",
        "account": "sf_account",
        "warehouse": "sf_warehouse",
        "sf_schema": "sf_schema",
        "database": "sf_database",
        "username": "sf_user",
        "password": "sf_password",
    }


def test_list_tables(
    snowflake_connector, snowflake_execute_query, snowflake_database_source, config
):
    """
    Test list_tables return expected results
    """
    _ = snowflake_connector, snowflake_execute_query
    output = snowflake_database_source.list_tables(credentials=config.credentials)
    assert output == ["sf_table", "sf_view"]


def test__getitem__retrieve_database_table(
    snowflake_connector, snowflake_execute_query, snowflake_database_source, config
):
    """
    Test retrieval database table by indexing
    """
    _ = snowflake_connector, snowflake_execute_query
    database_table = snowflake_database_source["sf_table", config.credentials]
    assert isinstance(database_table, DatabaseTable)
