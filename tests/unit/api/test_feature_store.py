"""
Unit test for DatabaseSource
"""
from featurebyte.api.database_table import DatabaseTable


def test_get_session(snowflake_connector, snowflake_execute_query, snowflake_feature_store, config):
    """
    Test DatabaseSource.get_session return expected session
    """
    _ = snowflake_connector, snowflake_execute_query
    session = snowflake_feature_store.get_session(credentials=config.credentials)
    assert session.dict() == {
        "source_type": "snowflake",
        "account": "sf_account",
        "warehouse": "sf_warehouse",
        "sf_schema": "sf_schema",
        "database": "sf_database",
        "username": "sf_user",
        "password": "sf_password",
    }


def test_list_databases(
    snowflake_connector, snowflake_execute_query, snowflake_feature_store, config
):
    """
    Test list_databases return expected results
    """
    _ = snowflake_connector, snowflake_execute_query
    output = snowflake_feature_store.list_databases(credentials=config.credentials)
    assert output == ["sf_database"]


def test_list_schema(snowflake_connector, snowflake_execute_query, snowflake_feature_store, config):
    """
    Test test_list_schema return expected results
    """
    _ = snowflake_connector, snowflake_execute_query
    output = snowflake_feature_store.list_schemas(credentials=config.credentials)
    assert output == ["sf_schema"]


def test_list_tables(snowflake_connector, snowflake_execute_query, snowflake_feature_store, config):
    """
    Test list_tables return expected results
    """
    _ = snowflake_connector, snowflake_execute_query
    output = snowflake_feature_store.list_tables(credentials=config.credentials)
    assert output == ["sf_table", "sf_view"]


def test__getitem__retrieve_database_table(
    snowflake_connector, snowflake_execute_query, snowflake_feature_store, config
):
    """
    Test retrieval database table by indexing
    """
    _ = snowflake_connector, snowflake_execute_query
    database_table = snowflake_feature_store.get_table(
        database_name="sf_database",
        schema_name="sf_schema",
        table_name="sf_table",
        credentials=config.credentials,
    )
    assert isinstance(database_table, DatabaseTable)
