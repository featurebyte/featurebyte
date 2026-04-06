import pytest_asyncio
from sqlglot import parse_one

from featurebyte import CronFeatureJobSetting, TimeInterval
from featurebyte.enum import TimeIntervalUnit
from featurebyte.query_graph.model.timestamp_schema import TimestampSchema
from featurebyte.query_graph.sql.common import sql_to_string


@pytest_asyncio.fixture(name="source_table_with_invalid_dates", scope="session")
async def source_table_with_invalid_dates_fixture(session, feature_store, catalog):
    """
    Fixture for a source table with invalid dates
    """
    _ = catalog
    query = sql_to_string(
        parse_one(
            """
            CREATE TABLE TABLE_INVALID_DATES AS
            SELECT 1 AS "id", CAST('2021-01-01 10:00:00' AS TIMESTAMP) AS "date_col"
            UNION ALL
            SELECT 2 AS "id", CAST('0019-01-01 10:00:00' AS TIMESTAMP) AS "date_col"
            UNION ALL
            SELECT 3 AS "id", CAST('0019-01-01 10:00:00' AS TIMESTAMP) AS "date_col"
            UNION ALL
            SELECT 4 AS "id", CAST('0019-01-01 10:00:00' AS TIMESTAMP) AS "date_col"
            UNION ALL
            SELECT 5 AS "id", CAST('9019-01-01 10:00:00' AS TIMESTAMP) AS "date_col"
            UNION ALL
            SELECT 6 AS "id", CAST('2023-01-01 10:00:00' AS TIMESTAMP) AS "date_col"
            """,
            read="snowflake",
        ),
        source_type=session.source_type,
    )
    await session.execute_query(query)
    ds = feature_store.get_data_source()
    return ds.get_source_table(
        table_name="TABLE_INVALID_DATES",
        database_name=session.database_name,
        schema_name=session.schema_name,
    )


@pytest_asyncio.fixture(name="source_table_with_numeric_strings", scope="session")
async def source_table_with_numeric_strings_fixture(session, feature_store, catalog):
    """
    Fixture for a source table with VARCHAR column containing numeric values
    """
    _ = catalog
    query = sql_to_string(
        parse_one(
            """
            CREATE TABLE TEST_CAST_NUMERIC AS
            SELECT 1 AS "event_id", '100' AS "numeric_string", CAST('2001-01-01 12:00:00' AS TIMESTAMP) AS "event_timestamp", 1 AS "cust_id"
            UNION ALL
            SELECT 2 AS "event_id", '200' AS "numeric_string", CAST('2001-01-01 13:00:00' AS TIMESTAMP) AS "event_timestamp", 1 AS "cust_id"
            UNION ALL
            SELECT 3 AS "event_id", '300' AS "numeric_string", CAST('2001-01-01 14:00:00' AS TIMESTAMP) AS "event_timestamp", 2 AS "cust_id"
            UNION ALL
            SELECT 4 AS "event_id", 'invalid' AS "numeric_string", CAST('2001-01-01 15:00:00' AS TIMESTAMP) AS "event_timestamp", 2 AS "cust_id"
            """,
            read="snowflake",
        ),
        source_type=session.source_type,
    )
    await session.execute_query(query)
    ds = feature_store.get_data_source()
    return ds.get_source_table(
        table_name="TEST_CAST_NUMERIC",
        database_name=session.database_name,
        schema_name=session.schema_name,
    )


@pytest_asyncio.fixture(name="time_series_table_with_date_col", scope="session")
async def time_series_table_with_date_col_fixture(
    session, feature_store, catalog, user_entity, timestamp_format_string
):
    """
    TimeSeriesTable where the reference datetime column is named 'date' — same name as the
    calendar datetime column in calendar_table_with_date_col. Both columns use the same
    formatted-string schema (e.g. "YYYY|MM|DD"), so apply_snapshots_datetime_transform applies
    no real transformation and exits early for both join keys.
    """
    _ = catalog
    _ = user_entity
    query = sql_to_string(
        parse_one(
            """
            CREATE OR REPLACE TABLE TIME_SERIES_DATE_COL AS
            SELECT '2001|01|01' AS "date", 1 AS "user_id", 1.0 AS "value"
            UNION ALL
            SELECT '2001|06|15' AS "date", 1 AS "user_id", 2.0 AS "value"
            UNION ALL
            SELECT '2001|12|25' AS "date", 1 AS "user_id", 3.0 AS "value"
            """,
            read="snowflake",
        ),
        source_type=session.source_type,
    )
    await session.execute_query(query)
    ds = feature_store.get_data_source()
    source_table = ds.get_source_table(
        table_name="TIME_SERIES_DATE_COL",
        database_name=session.database_name,
        schema_name=session.schema_name,
    )
    time_series_table = source_table.create_time_series_table(
        name=f"{session.source_type}_time_series_table_date_col",
        reference_datetime_column="date",
        reference_datetime_schema=TimestampSchema(format_string=timestamp_format_string),
        time_interval=TimeInterval(unit=TimeIntervalUnit.DAY, value=1),
        series_id_column="user_id",
    )
    time_series_table["user_id"].as_entity(user_entity.name)
    return time_series_table


@pytest_asyncio.fixture(name="snapshots_table_with_date_col_1", scope="session")
async def snapshots_table_with_date_col_1_fixture(
    session, feature_store, catalog, user_entity, timestamp_format_string
):
    """
    First SnapshotsTable with snapshot_datetime_column='date'. Mirrors the SELL_PRICES table in
    the production query. Joining a TimeSeriesView with this view produces SQL where both L and R
    expose a 'date' column. A subsequent snapshot join on that result triggers the SQL ambiguity.
    """
    _ = catalog
    _ = user_entity
    query = sql_to_string(
        parse_one(
            """
            CREATE OR REPLACE TABLE SNAPSHOTS_DATE_COL_1 AS
            SELECT '2001|01|01' AS "date", 1 AS "user_id", 1.5 AS "price"
            UNION ALL
            SELECT '2001|06|15' AS "date", 1 AS "user_id", 2.5 AS "price"
            UNION ALL
            SELECT '2001|12|25' AS "date", 1 AS "user_id", 3.5 AS "price"
            """,
            read="snowflake",
        ),
        source_type=session.source_type,
    )
    await session.execute_query(query)
    ds = feature_store.get_data_source()
    source_table = ds.get_source_table(
        table_name="SNAPSHOTS_DATE_COL_1",
        database_name=session.database_name,
        schema_name=session.schema_name,
    )
    snapshots_table = source_table.create_snapshots_table(
        name=f"{session.source_type}_snapshots_table_date_col_1",
        snapshot_datetime_column="date",
        snapshot_datetime_schema=TimestampSchema(format_string=timestamp_format_string),
        time_interval=TimeInterval(unit=TimeIntervalUnit.DAY, value=1),
        series_id_column="user_id",
    )
    snapshots_table.update_default_feature_job_setting(
        CronFeatureJobSetting(crontab="0 8 * * *", timezone="Asia/Singapore")
    )
    snapshots_table["user_id"].as_entity(user_entity.name)
    return snapshots_table


@pytest_asyncio.fixture(name="snapshots_table_with_date_col_2", scope="session")
async def snapshots_table_with_date_col_2_fixture(
    session, feature_store, catalog, user_entity, timestamp_format_string
):
    """
    Second SnapshotsTable with snapshot_datetime_column='date'. Mirrors the FOOD_STAMPS table in
    the production query. Joining the result of (TimeSeriesView + snapshots_table_with_date_col_1)
    with this view calls apply_snapshots_datetime_transform on the first join's SQL, where 'date'
    is ambiguous between L and R.
    """
    _ = catalog
    _ = user_entity
    query = sql_to_string(
        parse_one(
            """
            CREATE OR REPLACE TABLE SNAPSHOTS_DATE_COL_2 AS
            SELECT '2001|01|01' AS "date", 1 AS "user_id", TRUE AS "has_discount"
            UNION ALL
            SELECT '2001|06|15' AS "date", 1 AS "user_id", FALSE AS "has_discount"
            UNION ALL
            SELECT '2001|12|25' AS "date", 1 AS "user_id", TRUE AS "has_discount"
            """,
            read="snowflake",
        ),
        source_type=session.source_type,
    )
    await session.execute_query(query)
    ds = feature_store.get_data_source()
    source_table = ds.get_source_table(
        table_name="SNAPSHOTS_DATE_COL_2",
        database_name=session.database_name,
        schema_name=session.schema_name,
    )
    snapshots_table = source_table.create_snapshots_table(
        name=f"{session.source_type}_snapshots_table_date_col_2",
        snapshot_datetime_column="date",
        snapshot_datetime_schema=TimestampSchema(format_string=timestamp_format_string),
        time_interval=TimeInterval(unit=TimeIntervalUnit.DAY, value=1),
        series_id_column="user_id",
    )
    snapshots_table.update_default_feature_job_setting(
        CronFeatureJobSetting(crontab="0 8 * * *", timezone="Asia/Singapore")
    )
    snapshots_table["user_id"].as_entity(user_entity.name)
    return snapshots_table


@pytest_asyncio.fixture(name="dimension_table_with_date_col", scope="session")
async def dimension_table_with_date_col_fixture(session, feature_store, catalog, user_entity):
    """
    DimensionTable that has a 'date' attribute column — same name as the reference datetime column
    in time_series_table_with_date_col. When a TimeSeriesView is first joined with this view
    (regular join on user_id), the resulting SQL exposes both L."date" and R."date" in its FROM
    clause. A subsequent CalendarView join then calls apply_snapshots_datetime_transform on that
    SQL, where DATE_TRUNC('day', "date") is ambiguous between L and R.
    """
    _ = catalog
    _ = user_entity
    query = sql_to_string(
        parse_one(
            """
            CREATE OR REPLACE TABLE DIMENSION_DATE_COL AS
            SELECT 1 AS "user_id", '2001|01|01' AS "date", 'attr_a' AS "attr_value"
            UNION ALL
            SELECT 2 AS "user_id", '2001|06|15' AS "date", 'attr_b' AS "attr_value"
            UNION ALL
            SELECT 3 AS "user_id", '2001|12|25' AS "date", 'attr_c' AS "attr_value"
            """,
            read="snowflake",
        ),
        source_type=session.source_type,
    )
    await session.execute_query(query)
    ds = feature_store.get_data_source()
    source_table = ds.get_source_table(
        table_name="DIMENSION_DATE_COL",
        database_name=session.database_name,
        schema_name=session.schema_name,
    )
    dimension_table = source_table.create_dimension_table(
        name=f"{session.source_type}_dimension_table_date_col",
        dimension_id_column="user_id",
    )
    dimension_table["user_id"].as_entity(user_entity.name)
    return dimension_table


@pytest_asyncio.fixture(name="calendar_table_with_date_col", scope="session")
async def calendar_table_with_date_col_fixture(
    session, feature_store, catalog, user_entity, timestamp_format_string
):
    """
    CalendarTable where the calendar datetime column is named 'date' — same name as the
    reference datetime column in time_series_table_with_date_col. Used to reproduce the SQL
    ambiguous column name error when joining a time series view with a calendar view.
    """
    _ = catalog
    _ = user_entity
    query = sql_to_string(
        parse_one(
            """
            CREATE OR REPLACE TABLE CALENDAR_DATE_COL AS
            SELECT '2001|01|01' AS "date", 1 AS "user_id", 'New Year''s Day' AS "holiday_name"
            UNION ALL
            SELECT '2001|06|15' AS "date", 1 AS "user_id", NULL AS "holiday_name"
            UNION ALL
            SELECT '2001|12|25' AS "date", 1 AS "user_id", 'Christmas Day' AS "holiday_name"
            """,
            read="snowflake",
        ),
        source_type=session.source_type,
    )
    await session.execute_query(query)
    ds = feature_store.get_data_source()
    source_table = ds.get_source_table(
        table_name="CALENDAR_DATE_COL",
        database_name=session.database_name,
        schema_name=session.schema_name,
    )
    calendar_table = source_table.create_calendar_table(
        name=f"{session.source_type}_calendar_table_date_col",
        calendar_datetime_column="date",
        calendar_datetime_schema=TimestampSchema(format_string=timestamp_format_string),
        series_id_column="user_id",
    )
    calendar_table["user_id"].as_entity(user_entity.name)
    return calendar_table


@pytest_asyncio.fixture(name="calendar_table_with_date_col_2", scope="session")
async def calendar_table_with_date_col_2_fixture(
    session, feature_store, catalog, user_entity, timestamp_format_string
):
    """
    Second CalendarTable where the calendar datetime column is named 'date'. Used alongside
    calendar_table_with_date_col to reproduce the SQL ambiguous column name error when two
    calendar joins are chained on a TimeSeriesView that also has a 'date' column.
    """
    _ = catalog
    _ = user_entity
    query = sql_to_string(
        parse_one(
            """
            CREATE OR REPLACE TABLE CALENDAR_DATE_COL_2 AS
            SELECT '2001|01|01' AS "date", 1 AS "user_id", TRUE AS "is_weekend"
            UNION ALL
            SELECT '2001|06|15' AS "date", 1 AS "user_id", FALSE AS "is_weekend"
            UNION ALL
            SELECT '2001|12|25' AS "date", 1 AS "user_id", FALSE AS "is_weekend"
            """,
            read="snowflake",
        ),
        source_type=session.source_type,
    )
    await session.execute_query(query)
    ds = feature_store.get_data_source()
    source_table = ds.get_source_table(
        table_name="CALENDAR_DATE_COL_2",
        database_name=session.database_name,
        schema_name=session.schema_name,
    )
    calendar_table = source_table.create_calendar_table(
        name=f"{session.source_type}_calendar_table_date_col_2",
        calendar_datetime_column="date",
        calendar_datetime_schema=TimestampSchema(format_string=timestamp_format_string),
        series_id_column="user_id",
    )
    calendar_table["user_id"].as_entity(user_entity.name)
    return calendar_table
