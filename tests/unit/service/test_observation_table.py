"""
Test for ObservationTableService
"""

import textwrap
from unittest import mock
from unittest.mock import AsyncMock, Mock, patch

import pandas as pd
import pytest
from bson import ObjectId

from featurebyte.enum import DBVarType, SourceType, SpecialColumnName
from featurebyte.exception import (
    MissingPointInTimeColumnError,
    ObservationTableInvalidSamplingError,
    UnsupportedPointInTimeColumnTypeError,
)
from featurebyte.models.materialized_table import ColumnSpecWithEntityId
from featurebyte.models.observation_table import ObservationTableModel
from featurebyte.models.request_input import (
    DownSamplingInfo,
    SourceTableRequestInput,
    SplitInfo,
    TargetValueSamplingRate,
)
from featurebyte.query_graph.model.column_info import ColumnSpecWithDescription
from featurebyte.query_graph.model.common_table import TabularSource
from featurebyte.query_graph.node.schema import TableDetails
from featurebyte.service.observation_table import validate_columns_info
from featurebyte.session.base import BaseSession


@pytest.fixture(autouse=True)
def patch_unique_identifier():
    """
    Patch unique identifier generator
    """
    with patch("featurebyte.service.preview.ObjectId", return_value=ObjectId("0" * 24)):
        yield


@pytest.fixture(name="observation_table_from_source_table")
def observation_table_from_source_table_fixture(event_table, user):
    """
    Fixture for an ObservationTable from a source table
    """
    request_input = SourceTableRequestInput(source=event_table.tabular_source)
    location = TabularSource(**{
        "feature_store_id": event_table.tabular_source.feature_store_id,
        "table_details": {
            "database_name": "fb_database",
            "schema_name": "fb_schema",
            "table_name": "fb_materialized_table",
        },
    })
    return ObservationTableModel(
        name="observation_table_from_source_table",
        location=location,
        request_input=request_input.model_dump(by_alias=True),
        columns_info=[
            {"name": "a", "dtype": "INT"},
            {"name": "b", "dtype": "INT"},
            {"name": "c", "dtype": "INT"},
        ],
        num_rows=1000,
        most_recent_point_in_time="2023-01-15T10:00:00",
        user_id=user.id,
    )


@pytest.fixture(name="table_details")
def table_details_fixture():
    """
    Fixture for a TableDetails
    """
    return TableDetails(
        database_name="fb_database",
        schema_name="fb_schema",
        table_name="fb_table",
    )


@pytest.fixture(name="point_in_time_has_missing_values")
def point_in_time_has_missing_values_fixture():
    """
    Fixture to determine whether point in time has missing values
    """
    return False


@pytest.fixture(name="db_session")
def db_session_fixture(point_in_time_has_missing_values, adapter):
    """
    Fixture for a db session
    """

    async def mock_list_table_schema(*args, **kwargs):
        _ = args
        _ = kwargs
        return {
            "POINT_IN_TIME": ColumnSpecWithDescription(name="POINT_IN_TIME", dtype="TIMESTAMP"),
            "cust_id": ColumnSpecWithDescription(name="cust_id", dtype="VARCHAR"),
        }

    async def execute_query_long_running(*args, **kwargs):
        query = args[0]
        _ = kwargs
        if "stats" in query:
            return pd.DataFrame(
                {
                    "dtype": ["timestamp", "int"],
                    "unique": [5, 2],
                    "%missing": [1 if point_in_time_has_missing_values else 0, 0],
                    "min": ["2023-01-01T10:00:00+08:00", 1],
                    "max": ["2023-01-15T10:00:00+08:00", 10],
                },
            )
        if "COUNT(*)" in query:
            return pd.DataFrame({"row_count": [1000]})
        if "INTERVAL" in query:
            return pd.DataFrame({"MIN_INTERVAL": [3600]})
        raise NotImplementedError(f"Unexpected query: {query}")

    mock_db_session = Mock(
        name="mock_session",
        spec=BaseSession,
        list_table_schema=Mock(side_effect=mock_list_table_schema),
        execute_query_long_running=Mock(side_effect=execute_query_long_running),
        source_type=SourceType.SNOWFLAKE,
        schema_name="my_schema",
        database_name="my_db",
        adapter=adapter,
    )
    return mock_db_session


@pytest.mark.asyncio
async def test_create_observation_table_from_source_table(
    observation_table_from_source_table, observation_table_service, catalog
):
    """
    Test creating an ObservationTable from a source table
    """
    await observation_table_service.create_document(observation_table_from_source_table)
    loaded_table = await observation_table_service.get_document(
        observation_table_from_source_table.id
    )
    loaded_table_dict = loaded_table.model_dump(exclude={"created_at", "updated_at"})
    expected_dict = observation_table_from_source_table.model_dump(
        exclude={"created_at", "updated_at"}
    )
    expected_dict["catalog_id"] = catalog.id
    assert expected_dict == loaded_table_dict


@pytest.mark.asyncio
async def test_validate__missing_point_in_time(
    observation_table_service,
    snowflake_feature_store,
    table_details,
    adapter,
):
    """
    Test validation of missing point in time
    """

    async def mock_list_table_schema(*args, **kwargs):
        _ = args
        _ = kwargs
        return {
            "a": ColumnSpecWithDescription(name="a", dtype="INT"),
            "b": ColumnSpecWithDescription(name="b", dtype="FLOAT"),
            "not_point_in_time": ColumnSpecWithDescription(
                name="not_point_in_time", dtype="VARCHAR"
            ),
        }

    # TODO: we should consolidate the definition of mock_snowflake_session in a single fixture and
    #  override whatever is necessary in tests if needed.
    mock_db_session = Mock(
        name="mock_session",
        spec=BaseSession,
        list_table_schema=Mock(side_effect=mock_list_table_schema),
        source_type=SourceType.SNOWFLAKE,
        adapter=adapter,
    )
    with pytest.raises(MissingPointInTimeColumnError):
        await observation_table_service.validate_materialized_table_and_get_metadata(
            mock_db_session, table_details, snowflake_feature_store
        )


@pytest.mark.asyncio
async def test_validate__most_recent_point_in_time(
    observation_table_service,
    db_session,
    table_details,
    cust_id_entity,
    snowflake_feature_store,
    insert_credential,
):
    """
    Test validate_materialized_table_and_get_metadata triggers expected query
    """
    _ = cust_id_entity, insert_credential
    with mock.patch(
        "featurebyte.service.preview.PreviewService._get_feature_store_session"
    ) as mock_get_feature_store_session:
        mock_get_feature_store_session.return_value = (snowflake_feature_store, db_session)
        metadata = await observation_table_service.validate_materialized_table_and_get_metadata(
            db_session, table_details, snowflake_feature_store
        )

        expected_query = textwrap.dedent(
            """
            WITH stats AS (
              SELECT
                COUNT(DISTINCT "POINT_IN_TIME") AS "unique__0",
                (
                  1.0 - COUNT("POINT_IN_TIME") / NULLIF(COUNT(*), 0)
                ) * 100 AS "%missing__0",
                MIN(
                  IFF(
                    CAST("POINT_IN_TIME" AS TIMESTAMP) < CAST('1900-01-01' AS TIMESTAMP)
                    OR CAST("POINT_IN_TIME" AS TIMESTAMP) > CAST('2200-01-01' AS TIMESTAMP),
                    NULL,
                    "POINT_IN_TIME"
                  )
                ) AS "min__0",
                MAX(
                  IFF(
                    CAST("POINT_IN_TIME" AS TIMESTAMP) < CAST('1900-01-01' AS TIMESTAMP)
                    OR CAST("POINT_IN_TIME" AS TIMESTAMP) > CAST('2200-01-01' AS TIMESTAMP),
                    NULL,
                    "POINT_IN_TIME"
                  )
                ) AS "max__0",
                COUNT(DISTINCT "cust_id") AS "unique__1",
                (
                  1.0 - COUNT("cust_id") / NULLIF(COUNT(*), 0)
                ) * 100 AS "%missing__1",
                NULL AS "min__1",
                NULL AS "max__1"
              FROM "__FB_TEMPORARY_TABLE_000000000000000000000000"
            ), joined_tables_0 AS (
              SELECT
                *
              FROM stats
            )
            SELECT
              'TIMESTAMP' AS "dtype__0",
              "unique__0",
              "%missing__0",
              "min__0",
              "max__0",
              'VARCHAR' AS "dtype__1",
              "unique__1",
              "%missing__1",
              "min__1",
              "max__1"
            FROM joined_tables_0
            """
        ).strip()
        query = db_session.execute_query_long_running.call_args[0][0]
        assert query == expected_query

        assert metadata == {
            "columns_info": [
                ColumnSpecWithEntityId(name="POINT_IN_TIME", dtype="TIMESTAMP", entity_id=None),
                ColumnSpecWithEntityId(
                    name="cust_id", dtype="VARCHAR", entity_id=ObjectId("63f94ed6ea1f050131379214")
                ),
            ],
            "least_recent_point_in_time": "2023-01-01T02:00:00",
            "most_recent_point_in_time": "2023-01-15T02:00:00",
            "num_rows": 1000,
            "entity_column_name_to_count": {"cust_id": 2},
            "min_interval_secs_between_entities": 3600,
        }


@pytest.mark.asyncio
async def test_validate__supported_type_point_in_time(
    observation_table_service, table_details, snowflake_feature_store
):
    """
    Test validate_materialized_table_and_get_metadata validates the type of point in time column
    """

    async def mock_list_table_schema(*args, **kwargs):
        _ = args
        _ = kwargs
        return {
            "POINT_IN_TIME": ColumnSpecWithDescription(name="POINT_IN_TIME", dtype="VARCHAR"),
            "cust_id": ColumnSpecWithDescription(name="cust_id", dtype="VARCHAR"),
        }

    mock_db_session = Mock(
        name="mock_session",
        spec=BaseSession,
        list_table_schema=Mock(side_effect=mock_list_table_schema),
        source_type=SourceType.SNOWFLAKE,
    )
    with pytest.raises(UnsupportedPointInTimeColumnTypeError) as exc:
        await observation_table_service.validate_materialized_table_and_get_metadata(
            mock_db_session, table_details, snowflake_feature_store
        )

    assert str(exc.value) == "Point in time column should have timestamp type; got VARCHAR"


@pytest.mark.asyncio
@pytest.mark.parametrize("point_in_time_has_missing_values", [True])
async def test_validate__point_in_time_no_missing_values(
    observation_table_service,
    db_session,
    table_details,
    cust_id_entity,
    snowflake_feature_store,
    insert_credential,
):
    """
    Test validate point in time has no missing values
    """
    _ = insert_credential, cust_id_entity

    with mock.patch(
        "featurebyte.service.preview.PreviewService._get_feature_store_session"
    ) as mock_get_feature_store_session:
        mock_get_feature_store_session.return_value = (snowflake_feature_store, db_session)
        with pytest.raises(ValueError) as exc_info:
            await observation_table_service.validate_materialized_table_and_get_metadata(
                db_session, table_details, snowflake_feature_store
            )
    assert str(exc_info.value) == (
        "These columns in the observation table must not contain any missing values: POINT_IN_TIME"
    )


@pytest.mark.asyncio
async def test_request_input_get_row_count(
    observation_table_from_source_table, db_session, snowflake_feature_store
):
    """
    Test get_row_count triggers expected query
    """
    db_session.execute_query = AsyncMock(return_value=pd.DataFrame({"row_count": [1000]}))
    db_session.get_source_info.return_value.source_type = SourceType.SNOWFLAKE

    row_count = await observation_table_from_source_table.request_input.get_row_count(
        db_session,
        await observation_table_from_source_table.request_input.get_query_expr(
            db_session, snowflake_feature_store
        ),
    )
    assert row_count == 1000

    expected_query = textwrap.dedent(
        """
        SELECT
          COUNT(*) AS "row_count"
        FROM (
          SELECT
            "POINT_IN_TIME" AS "POINT_IN_TIME",
            "cust_id" AS "cust_id"
          FROM "sf_database"."sf_schema"."sf_event_table"
        )
        """
    ).strip()
    query = db_session.execute_query_long_running.call_args[0][0]
    assert query == expected_query


def test_get_minimum_iet_sql_expr(observation_table_service, table_details, adapter):
    """
    Test get_minimum_iet_sql_expr
    """
    expr = observation_table_service.get_minimum_iet_sql_expr(["entity"], table_details, adapter)
    expr_sql = expr.sql(pretty=True, dialect="snowflake")
    expected_query = textwrap.dedent(
        """
        SELECT
          MIN("INTERVAL") AS "MIN_INTERVAL"
        FROM (
          SELECT
            DATEDIFF(MICROSECOND, "PREVIOUS_POINT_IN_TIME", "POINT_IN_TIME") / 1000000 AS "INTERVAL"
          FROM (
            SELECT
              LAG("POINT_IN_TIME") OVER (PARTITION BY "entity" ORDER BY "POINT_IN_TIME") AS "PREVIOUS_POINT_IN_TIME",
              "POINT_IN_TIME"
            FROM "fb_database"."fb_schema"."fb_table"
          )
        )
        WHERE
          NOT "INTERVAL" IS NULL
        """
    ).strip()
    assert expr_sql == expected_query


ENTITY_ID_1 = ObjectId("646f6c1b0ed28a5271123456")
ENTITY_ID_2 = ObjectId("646f6c1b0ed28a5271234567")
POINT_IN_TIME_COL = ColumnSpecWithEntityId(
    name=str(SpecialColumnName.POINT_IN_TIME), dtype=DBVarType.TIMESTAMP
)
INT_POINT_IN_TIME_COL = ColumnSpecWithEntityId(
    name=str(SpecialColumnName.POINT_IN_TIME), dtype=DBVarType.INT
)
INT_COL = ColumnSpecWithEntityId(name="col2", dtype=DBVarType.INT, entity_id=None)
INT_COL_WITH_ENTITY_1 = ColumnSpecWithEntityId(
    name="col3", dtype=DBVarType.INT, entity_id=ENTITY_ID_1
)
INT_COL_WITH_ENTITY_2 = ColumnSpecWithEntityId(
    name="col4", dtype=DBVarType.INT, entity_id=ENTITY_ID_2
)


class MockTargetNamespace(Mock):
    """
    Mock class for target namespace
    """

    def __init__(self, name, dtype):
        super().__init__()
        self.name = name
        self.dtype = dtype


@pytest.mark.parametrize(
    "columns_info, primary_entity_ids, skip_entity_checks, target_namespace, expected_error, expected_msg",
    [
        (
            [POINT_IN_TIME_COL, INT_COL],
            [],
            False,
            None,
            ValueError,
            "At least one entity column should be provided.",
        ),
        ([POINT_IN_TIME_COL, INT_COL], [], True, None, None, None),
        ([POINT_IN_TIME_COL, INT_COL_WITH_ENTITY_1], [], False, None, None, None),
        (
            [INT_COL_WITH_ENTITY_1],
            [],
            False,
            None,
            MissingPointInTimeColumnError,
            "Point in time column not provided: POINT_IN_TIME",
        ),
        (
            [INT_POINT_IN_TIME_COL],
            [],
            False,
            None,
            UnsupportedPointInTimeColumnTypeError,
            "Point in time column should have timestamp type; got INT",
        ),
        (
            [POINT_IN_TIME_COL, INT_COL_WITH_ENTITY_1],
            [ENTITY_ID_2],
            False,
            None,
            ValueError,
            "Primary entity IDs passed in are not present in the column info:",
        ),
        ([POINT_IN_TIME_COL, INT_COL_WITH_ENTITY_1], [ENTITY_ID_2], True, None, None, None),
        (
            [POINT_IN_TIME_COL, INT_COL_WITH_ENTITY_1],
            [],
            False,
            MockTargetNamespace(name=str(SpecialColumnName.POINT_IN_TIME), dtype=DBVarType.INT),
            ValueError,
            'Target column "POINT_IN_TIME" should have dtype "INT"',
        ),
        (
            [POINT_IN_TIME_COL, INT_COL_WITH_ENTITY_1],
            [],
            False,
            MockTargetNamespace(name="target", dtype=DBVarType.INT),
            ValueError,
            'Target column "target" not found.',
        ),
        (
            [POINT_IN_TIME_COL, INT_COL_WITH_ENTITY_1],
            [],
            False,
            MockTargetNamespace(name=str(SpecialColumnName.POINT_IN_TIME), dtype=DBVarType.VARCHAR),
            ValueError,
            'Target column "POINT_IN_TIME" should have dtype "VARCHAR"',
        ),
        (
            [POINT_IN_TIME_COL, INT_COL_WITH_ENTITY_1],
            [],
            False,
            MockTargetNamespace(name=str(SpecialColumnName.POINT_IN_TIME), dtype=None),
            None,
            None,
        ),
    ],
)
def test_validate_columns_info(
    columns_info,
    primary_entity_ids,
    skip_entity_checks,
    target_namespace,
    expected_error,
    expected_msg,
):
    """
    Test validate_columns_info
    """
    if expected_error is not None:
        with pytest.raises(expected_error) as exc:
            validate_columns_info(
                columns_info, primary_entity_ids, skip_entity_checks, target_namespace
            )
        assert expected_msg in str(exc.value)
    else:
        validate_columns_info(
            columns_info, primary_entity_ids, skip_entity_checks, target_namespace
        )


# Tests for FORECAST_POINT column validation


@pytest.mark.asyncio
async def test_validate_columns__forecast_context_missing_forecast_point(observation_table_service):
    """
    Test validation fails when forecast context is provided but FORECAST_POINT column is missing
    """
    from featurebyte.enum import TimeIntervalUnit
    from featurebyte.exception import MissingForecastPointColumnError
    from featurebyte.models.context import ContextModel
    from featurebyte.query_graph.model.forecast_point_schema import ForecastPointSchema

    # Create a mock context with forecast_point_schema
    forecast_schema = ForecastPointSchema(
        granularity=TimeIntervalUnit.DAY,
        dtype=DBVarType.DATE,
        timezone="America/New_York",
    )
    mock_context = Mock(spec=ContextModel)
    mock_context.name = "test_forecast_context"
    mock_context.forecast_point_schema = forecast_schema

    # Available columns do not include FORECAST_POINT
    available_columns = ["POINT_IN_TIME", "entity_col"]

    with pytest.raises(MissingForecastPointColumnError) as exc:
        await observation_table_service._validate_columns(
            available_columns=available_columns,
            primary_entity_ids=None,
            target_column=None,
            treatment_column=None,
            context=mock_context,
        )
    assert "FORECAST_POINT" in str(exc.value)
    assert "test_forecast_context" in str(exc.value)


@pytest.mark.asyncio
async def test_validate_columns__forecast_context_missing_timezone_column(
    observation_table_service,
):
    """
    Test validation fails when forecast context requires timezone column but it's missing
    """
    from featurebyte.enum import TimeIntervalUnit
    from featurebyte.exception import MissingForecastTimezoneColumnError
    from featurebyte.models.context import ContextModel
    from featurebyte.query_graph.model.forecast_point_schema import ForecastPointSchema
    from featurebyte.query_graph.model.timestamp_schema import TimeZoneColumn

    # Create a mock context with forecast_point_schema that requires timezone column
    forecast_schema = ForecastPointSchema(
        granularity=TimeIntervalUnit.DAY,
        dtype=DBVarType.DATE,
        is_utc_time=False,
        timezone=TimeZoneColumn(column_name="FORECAST_TIMEZONE", type="timezone"),
    )
    mock_context = Mock(spec=ContextModel)
    mock_context.name = "test_forecast_context"
    mock_context.forecast_point_schema = forecast_schema

    # Available columns include FORECAST_POINT but not FORECAST_TIMEZONE
    available_columns = ["POINT_IN_TIME", "FORECAST_POINT", "entity_col"]

    with pytest.raises(MissingForecastTimezoneColumnError) as exc:
        await observation_table_service._validate_columns(
            available_columns=available_columns,
            primary_entity_ids=None,
            target_column=None,
            treatment_column=None,
            context=mock_context,
        )
    assert "FORECAST_TIMEZONE" in str(exc.value)
    assert "test_forecast_context" in str(exc.value)


@pytest.mark.asyncio
async def test_validate_columns__forecast_context_with_all_required_columns(
    observation_table_service,
):
    """
    Test validation passes when all required forecast columns are present
    """
    from featurebyte.enum import TimeIntervalUnit
    from featurebyte.models.context import ContextModel
    from featurebyte.query_graph.model.forecast_point_schema import ForecastPointSchema
    from featurebyte.query_graph.model.timestamp_schema import TimeZoneColumn

    # Create a mock context with forecast_point_schema that requires timezone column
    forecast_schema = ForecastPointSchema(
        granularity=TimeIntervalUnit.DAY,
        dtype=DBVarType.DATE,
        is_utc_time=False,
        timezone=TimeZoneColumn(column_name="FORECAST_TIMEZONE", type="timezone"),
    )
    mock_context = Mock(spec=ContextModel)
    mock_context.name = "test_forecast_context"
    mock_context.forecast_point_schema = forecast_schema

    # Available columns include all required columns
    available_columns = ["POINT_IN_TIME", "FORECAST_POINT", "FORECAST_TIMEZONE", "entity_col"]

    # Should not raise any exception
    target_namespace_id, treatment_id = await observation_table_service._validate_columns(
        available_columns=available_columns,
        primary_entity_ids=None,
        target_column=None,
        treatment_column=None,
        context=mock_context,
    )
    assert target_namespace_id is None
    assert treatment_id is None


@pytest.mark.asyncio
async def test_validate_columns__non_forecast_context_no_forecast_point_required(
    observation_table_service,
):
    """
    Test validation passes for non-forecast context without FORECAST_POINT column
    """
    from featurebyte.models.context import ContextModel

    # Create a mock context without forecast_point_schema
    mock_context = Mock(spec=ContextModel)
    mock_context.name = "test_regular_context"
    mock_context.forecast_point_schema = None

    # Available columns do not include FORECAST_POINT (which is fine for non-forecast context)
    available_columns = ["POINT_IN_TIME", "entity_col"]

    # Should not raise any exception
    target_namespace_id, treatment_id = await observation_table_service._validate_columns(
        available_columns=available_columns,
        primary_entity_ids=None,
        target_column=None,
        treatment_column=None,
        context=mock_context,
    )
    assert target_namespace_id is None
    assert treatment_id is None


@pytest.mark.asyncio
async def test_validate_columns__forecast_point_dtype_mismatch(observation_table_service):
    """
    Test validation fails when forecast point column dtype doesn't match schema dtype
    """
    from featurebyte.enum import TimeIntervalUnit
    from featurebyte.exception import UnsupportedForecastPointColumnTypeError
    from featurebyte.models.context import ContextModel
    from featurebyte.query_graph.model.forecast_point_schema import ForecastPointSchema

    # Create a mock context with forecast_point_schema expecting DATE dtype
    forecast_schema = ForecastPointSchema(
        granularity=TimeIntervalUnit.DAY,
        dtype=DBVarType.DATE,
        timezone="America/New_York",
    )
    mock_context = Mock(spec=ContextModel)
    mock_context.name = "test_forecast_context"
    mock_context.forecast_point_schema = forecast_schema

    # Available columns include FORECAST_POINT but with wrong dtype (TIMESTAMP instead of DATE)
    available_columns = ["POINT_IN_TIME", "FORECAST_POINT", "entity_col"]
    column_dtypes = {
        "POINT_IN_TIME": DBVarType.TIMESTAMP,
        "FORECAST_POINT": DBVarType.TIMESTAMP,  # Should be DATE per schema
        "entity_col": DBVarType.VARCHAR,
    }

    with pytest.raises(UnsupportedForecastPointColumnTypeError) as exc:
        await observation_table_service._validate_columns(
            available_columns=available_columns,
            primary_entity_ids=None,
            target_column=None,
            treatment_column=None,
            context=mock_context,
            column_dtypes=column_dtypes,
        )
    assert "TIMESTAMP" in str(exc.value)
    assert "DATE" in str(exc.value)
    assert "test_forecast_context" in str(exc.value)


@pytest.mark.asyncio
async def test_validate_columns__forecast_point_dtype_match(observation_table_service):
    """
    Test validation passes when forecast point column dtype matches schema dtype
    """
    from featurebyte.enum import TimeIntervalUnit
    from featurebyte.models.context import ContextModel
    from featurebyte.query_graph.model.forecast_point_schema import ForecastPointSchema

    # Create a mock context with forecast_point_schema expecting DATE dtype
    forecast_schema = ForecastPointSchema(
        granularity=TimeIntervalUnit.DAY,
        dtype=DBVarType.DATE,
        timezone="America/New_York",
    )
    mock_context = Mock(spec=ContextModel)
    mock_context.name = "test_forecast_context"
    mock_context.forecast_point_schema = forecast_schema

    # Available columns include FORECAST_POINT with correct dtype
    available_columns = ["POINT_IN_TIME", "FORECAST_POINT", "entity_col"]
    column_dtypes = {
        "POINT_IN_TIME": DBVarType.TIMESTAMP,
        "FORECAST_POINT": DBVarType.DATE,  # Matches schema
        "entity_col": DBVarType.VARCHAR,
    }

    # Should not raise any exception
    target_namespace_id, treatment_id = await observation_table_service._validate_columns(
        available_columns=available_columns,
        primary_entity_ids=None,
        target_column=None,
        treatment_column=None,
        context=mock_context,
        column_dtypes=column_dtypes,
    )
    assert target_namespace_id is None
    assert treatment_id is None


@pytest.mark.asyncio
async def test_validate_columns__timezone_column_dtype_mismatch(observation_table_service):
    """
    Test validation fails when timezone column dtype is not VARCHAR
    """
    from featurebyte.enum import TimeIntervalUnit
    from featurebyte.exception import InvalidForecastTimezoneColumnTypeError
    from featurebyte.models.context import ContextModel
    from featurebyte.query_graph.model.forecast_point_schema import ForecastPointSchema
    from featurebyte.query_graph.model.timestamp_schema import TimeZoneColumn

    # Create a mock context with forecast_point_schema that requires timezone column
    forecast_schema = ForecastPointSchema(
        granularity=TimeIntervalUnit.DAY,
        dtype=DBVarType.DATE,
        is_utc_time=False,
        timezone=TimeZoneColumn(column_name="FORECAST_TIMEZONE", type="timezone"),
    )
    mock_context = Mock(spec=ContextModel)
    mock_context.name = "test_forecast_context"
    mock_context.forecast_point_schema = forecast_schema

    # Available columns include FORECAST_TIMEZONE but with wrong dtype
    available_columns = ["POINT_IN_TIME", "FORECAST_POINT", "FORECAST_TIMEZONE", "entity_col"]
    column_dtypes = {
        "POINT_IN_TIME": DBVarType.TIMESTAMP,
        "FORECAST_POINT": DBVarType.DATE,
        "FORECAST_TIMEZONE": DBVarType.INT,  # Should be VARCHAR
        "entity_col": DBVarType.VARCHAR,
    }

    with pytest.raises(InvalidForecastTimezoneColumnTypeError) as exc:
        await observation_table_service._validate_columns(
            available_columns=available_columns,
            primary_entity_ids=None,
            target_column=None,
            treatment_column=None,
            context=mock_context,
            column_dtypes=column_dtypes,
        )
    assert "FORECAST_TIMEZONE" in str(exc.value)
    assert "INT" in str(exc.value)
    assert "VARCHAR" in str(exc.value)
    assert "test_forecast_context" in str(exc.value)


@pytest.mark.asyncio
async def test_validate_columns__timezone_column_dtype_valid(observation_table_service):
    """
    Test validation passes when timezone column dtype is VARCHAR
    """
    from featurebyte.enum import TimeIntervalUnit
    from featurebyte.models.context import ContextModel
    from featurebyte.query_graph.model.forecast_point_schema import ForecastPointSchema
    from featurebyte.query_graph.model.timestamp_schema import TimeZoneColumn

    # Create a mock context with forecast_point_schema that requires timezone column
    forecast_schema = ForecastPointSchema(
        granularity=TimeIntervalUnit.DAY,
        dtype=DBVarType.DATE,
        is_utc_time=False,
        timezone=TimeZoneColumn(column_name="FORECAST_TIMEZONE", type="timezone"),
    )
    mock_context = Mock(spec=ContextModel)
    mock_context.name = "test_forecast_context"
    mock_context.forecast_point_schema = forecast_schema

    # Available columns include all required columns with correct dtypes
    available_columns = ["POINT_IN_TIME", "FORECAST_POINT", "FORECAST_TIMEZONE", "entity_col"]
    column_dtypes = {
        "POINT_IN_TIME": DBVarType.TIMESTAMP,
        "FORECAST_POINT": DBVarType.DATE,
        "FORECAST_TIMEZONE": DBVarType.VARCHAR,  # Correct type
        "entity_col": DBVarType.VARCHAR,
    }

    # Should not raise any exception
    target_namespace_id, treatment_id = await observation_table_service._validate_columns(
        available_columns=available_columns,
        primary_entity_ids=None,
        target_column=None,
        treatment_column=None,
        context=mock_context,
        column_dtypes=column_dtypes,
    )
    assert target_namespace_id is None
    assert treatment_id is None


@pytest.mark.asyncio
async def test_validate_columns__no_dtype_validation_when_dtypes_not_provided(
    observation_table_service,
):
    """
    Test that dtype validation is skipped when column_dtypes is not provided
    """
    from featurebyte.enum import TimeIntervalUnit
    from featurebyte.models.context import ContextModel
    from featurebyte.query_graph.model.forecast_point_schema import ForecastPointSchema
    from featurebyte.query_graph.model.timestamp_schema import TimeZoneColumn

    # Create a mock context with forecast_point_schema
    forecast_schema = ForecastPointSchema(
        granularity=TimeIntervalUnit.DAY,
        dtype=DBVarType.DATE,
        is_utc_time=False,
        timezone=TimeZoneColumn(column_name="FORECAST_TIMEZONE", type="timezone"),
    )
    mock_context = Mock(spec=ContextModel)
    mock_context.name = "test_forecast_context"
    mock_context.forecast_point_schema = forecast_schema

    # Available columns include all required columns (dtypes not provided)
    available_columns = ["POINT_IN_TIME", "FORECAST_POINT", "FORECAST_TIMEZONE", "entity_col"]

    # Should not raise any exception even without dtype info
    target_namespace_id, treatment_id = await observation_table_service._validate_columns(
        available_columns=available_columns,
        primary_entity_ids=None,
        target_column=None,
        treatment_column=None,
        context=mock_context,
        # column_dtypes not provided (None by default)
    )
    assert target_namespace_id is None
    assert treatment_id is None


# Tests for timezone validation


@pytest.mark.asyncio
async def test_validate_forecast_timezone_values__valid_iana_timezones(observation_table_service):
    """
    Test validation passes when timezone column contains valid IANA timezone names
    """
    from featurebyte.enum import TimeIntervalUnit
    from featurebyte.models.context import ContextModel
    from featurebyte.query_graph.model.forecast_point_schema import ForecastPointSchema
    from featurebyte.query_graph.model.timestamp_schema import TimeZoneColumn
    from featurebyte.query_graph.node.schema import TableDetails

    # Create a mock context with forecast_point_schema that requires timezone column
    forecast_schema = ForecastPointSchema(
        granularity=TimeIntervalUnit.DAY,
        dtype=DBVarType.DATE,
        is_utc_time=False,
        timezone=TimeZoneColumn(column_name="FORECAST_TIMEZONE", type="timezone"),
    )
    mock_context = Mock(spec=ContextModel)
    mock_context.name = "test_forecast_context"
    mock_context.forecast_point_schema = forecast_schema

    # Mock db_session to return valid IANA timezone values
    mock_db_session = AsyncMock()
    mock_db_session.source_type = "snowflake"
    mock_db_session.execute_query.return_value = pd.DataFrame({
        "FORECAST_TIMEZONE": ["America/New_York", "Europe/London", "Asia/Tokyo"]
    })

    table_details = TableDetails(
        database_name="test_db",
        schema_name="test_schema",
        table_name="test_table",
    )

    # Should not raise any exception
    await observation_table_service._validate_forecast_timezone_values(
        mock_db_session, table_details, mock_context
    )


@pytest.mark.asyncio
async def test_validate_forecast_timezone_values__invalid_iana_timezone(observation_table_service):
    """
    Test validation fails when timezone column contains invalid IANA timezone names
    """
    from featurebyte.enum import TimeIntervalUnit
    from featurebyte.exception import InvalidForecastTimezoneValueError
    from featurebyte.models.context import ContextModel
    from featurebyte.query_graph.model.forecast_point_schema import ForecastPointSchema
    from featurebyte.query_graph.model.timestamp_schema import TimeZoneColumn
    from featurebyte.query_graph.node.schema import TableDetails

    # Create a mock context with forecast_point_schema that requires timezone column
    forecast_schema = ForecastPointSchema(
        granularity=TimeIntervalUnit.DAY,
        dtype=DBVarType.DATE,
        is_utc_time=False,
        timezone=TimeZoneColumn(column_name="FORECAST_TIMEZONE", type="timezone"),
    )
    mock_context = Mock(spec=ContextModel)
    mock_context.name = "test_forecast_context"
    mock_context.forecast_point_schema = forecast_schema

    # Mock db_session to return invalid timezone value
    mock_db_session = AsyncMock()
    mock_db_session.source_type = "snowflake"
    mock_db_session.execute_query.return_value = pd.DataFrame({
        "FORECAST_TIMEZONE": ["America/New_York", "Invalid/Timezone", "Europe/London"]
    })

    table_details = TableDetails(
        database_name="test_db",
        schema_name="test_schema",
        table_name="test_table",
    )

    with pytest.raises(InvalidForecastTimezoneValueError) as exc:
        await observation_table_service._validate_forecast_timezone_values(
            mock_db_session, table_details, mock_context
        )
    assert "Invalid/Timezone" in str(exc.value)
    assert "IANA timezone names" in str(exc.value)
    assert "test_forecast_context" in str(exc.value)


@pytest.mark.asyncio
async def test_validate_forecast_timezone_values__valid_utc_offsets(observation_table_service):
    """
    Test validation passes when timezone column contains valid UTC offsets
    """
    from featurebyte.enum import TimeIntervalUnit
    from featurebyte.models.context import ContextModel
    from featurebyte.query_graph.model.forecast_point_schema import ForecastPointSchema
    from featurebyte.query_graph.model.timestamp_schema import TimeZoneColumn
    from featurebyte.query_graph.node.schema import TableDetails

    # Create a mock context with forecast_point_schema that requires offset type timezone column
    forecast_schema = ForecastPointSchema(
        granularity=TimeIntervalUnit.DAY,
        dtype=DBVarType.DATE,
        is_utc_time=False,
        timezone=TimeZoneColumn(column_name="FORECAST_TIMEZONE", type="offset"),
    )
    mock_context = Mock(spec=ContextModel)
    mock_context.name = "test_forecast_context"
    mock_context.forecast_point_schema = forecast_schema

    # Mock db_session to return valid UTC offset values
    mock_db_session = AsyncMock()
    mock_db_session.source_type = "snowflake"
    mock_db_session.execute_query.return_value = pd.DataFrame({
        "FORECAST_TIMEZONE": ["+05:30", "-03:00", "+00:00"]
    })

    table_details = TableDetails(
        database_name="test_db",
        schema_name="test_schema",
        table_name="test_table",
    )

    # Should not raise any exception
    await observation_table_service._validate_forecast_timezone_values(
        mock_db_session, table_details, mock_context
    )


@pytest.mark.asyncio
async def test_validate_forecast_timezone_values__invalid_utc_offset(observation_table_service):
    """
    Test validation fails when timezone column contains invalid UTC offsets
    """
    from featurebyte.enum import TimeIntervalUnit
    from featurebyte.exception import InvalidForecastTimezoneValueError
    from featurebyte.models.context import ContextModel
    from featurebyte.query_graph.model.forecast_point_schema import ForecastPointSchema
    from featurebyte.query_graph.model.timestamp_schema import TimeZoneColumn
    from featurebyte.query_graph.node.schema import TableDetails

    # Create a mock context with forecast_point_schema that requires offset type timezone column
    forecast_schema = ForecastPointSchema(
        granularity=TimeIntervalUnit.DAY,
        dtype=DBVarType.DATE,
        is_utc_time=False,
        timezone=TimeZoneColumn(column_name="FORECAST_TIMEZONE", type="offset"),
    )
    mock_context = Mock(spec=ContextModel)
    mock_context.name = "test_forecast_context"
    mock_context.forecast_point_schema = forecast_schema

    # Mock db_session to return invalid offset value
    mock_db_session = AsyncMock()
    mock_db_session.source_type = "snowflake"
    mock_db_session.execute_query.return_value = pd.DataFrame({
        "FORECAST_TIMEZONE": ["+05:30", "invalid_offset", "-03:00"]
    })

    table_details = TableDetails(
        database_name="test_db",
        schema_name="test_schema",
        table_name="test_table",
    )

    with pytest.raises(InvalidForecastTimezoneValueError) as exc:
        await observation_table_service._validate_forecast_timezone_values(
            mock_db_session, table_details, mock_context
        )
    assert "invalid_offset" in str(exc.value)
    assert "UTC offsets" in str(exc.value)
    assert "test_forecast_context" in str(exc.value)


@pytest.mark.asyncio
async def test_validate_forecast_timezone_values__no_timezone_column(observation_table_service):
    """
    Test validation is skipped when forecast_point_schema doesn't have timezone column
    """
    from featurebyte.enum import TimeIntervalUnit
    from featurebyte.models.context import ContextModel
    from featurebyte.query_graph.model.forecast_point_schema import ForecastPointSchema
    from featurebyte.query_graph.node.schema import TableDetails

    # Create a mock context with forecast_point_schema without timezone column
    forecast_schema = ForecastPointSchema(
        granularity=TimeIntervalUnit.DAY,
        dtype=DBVarType.DATE,
        timezone="America/New_York",  # Global timezone, not a column reference
    )
    mock_context = Mock(spec=ContextModel)
    mock_context.name = "test_forecast_context"
    mock_context.forecast_point_schema = forecast_schema

    mock_db_session = AsyncMock()
    table_details = TableDetails(
        database_name="test_db",
        schema_name="test_schema",
        table_name="test_table",
    )

    # Should not raise any exception and should not call execute_query
    await observation_table_service._validate_forecast_timezone_values(
        mock_db_session, table_details, mock_context
    )
    mock_db_session.execute_query.assert_not_called()


@pytest.mark.asyncio
async def test_validate_forecast_timezone_values__no_forecast_point_schema(
    observation_table_service,
):
    """
    Test validation is skipped when context doesn't have forecast_point_schema
    """
    from featurebyte.models.context import ContextModel
    from featurebyte.query_graph.node.schema import TableDetails

    # Create a mock context without forecast_point_schema
    mock_context = Mock(spec=ContextModel)
    mock_context.name = "test_regular_context"
    mock_context.forecast_point_schema = None

    mock_db_session = AsyncMock()
    table_details = TableDetails(
        database_name="test_db",
        schema_name="test_schema",
        table_name="test_table",
    )

    # Should not raise any exception and should not call execute_query
    await observation_table_service._validate_forecast_timezone_values(
        mock_db_session, table_details, mock_context
    )
    mock_db_session.execute_query.assert_not_called()


@pytest.mark.asyncio
async def test_validate_forecast_point_format_string__valid_values(observation_table_service):
    """
    Test validation passes when all FORECAST_POINT values match the format string
    """
    from featurebyte.enum import TimeIntervalUnit
    from featurebyte.models.context import ContextModel
    from featurebyte.query_graph.model.forecast_point_schema import ForecastPointSchema
    from featurebyte.query_graph.node.schema import TableDetails
    from featurebyte.query_graph.sql.adapter import SnowflakeAdapter
    from featurebyte.query_graph.sql.source_info import SourceInfo

    # Create a mock context with VARCHAR dtype and format_string
    forecast_schema = ForecastPointSchema(
        granularity=TimeIntervalUnit.DAY,
        dtype=DBVarType.VARCHAR,
        format_string="YYYY-MM-DD",
        timezone="America/New_York",
    )
    mock_context = Mock(spec=ContextModel)
    mock_context.name = "test_forecast_context"
    mock_context.forecast_point_schema = forecast_schema

    # Mock db_session - no invalid values found (empty result)
    mock_db_session = AsyncMock()
    mock_db_session.source_type = "snowflake"
    mock_db_session.execute_query.return_value = pd.DataFrame()  # Empty = all valid
    # Create a real adapter instance for proper SQL generation
    source_info = SourceInfo(
        database_name="test_db", schema_name="test_schema", source_type=SourceType.SNOWFLAKE
    )
    mock_db_session.adapter = SnowflakeAdapter(source_info)

    table_details = TableDetails(
        database_name="test_db",
        schema_name="test_schema",
        table_name="test_table",
    )

    # Should not raise any exception
    await observation_table_service._validate_forecast_point_format_string_values(
        mock_db_session, table_details, mock_context
    )
    mock_db_session.execute_query.assert_called_once()


@pytest.mark.asyncio
async def test_validate_forecast_point_format_string__invalid_values(observation_table_service):
    """
    Test validation fails when FORECAST_POINT values don't match the format string
    """
    from featurebyte.enum import SpecialColumnName, TimeIntervalUnit
    from featurebyte.exception import InvalidForecastPointValueError
    from featurebyte.models.context import ContextModel
    from featurebyte.query_graph.model.forecast_point_schema import ForecastPointSchema
    from featurebyte.query_graph.node.schema import TableDetails
    from featurebyte.query_graph.sql.adapter import SnowflakeAdapter
    from featurebyte.query_graph.sql.source_info import SourceInfo

    # Create a mock context with VARCHAR dtype and format_string
    forecast_schema = ForecastPointSchema(
        granularity=TimeIntervalUnit.DAY,
        dtype=DBVarType.VARCHAR,
        format_string="YYYY-MM-DD",
        timezone="America/New_York",
    )
    mock_context = Mock(spec=ContextModel)
    mock_context.name = "test_forecast_context"
    mock_context.forecast_point_schema = forecast_schema

    # Mock db_session - returns invalid values
    mock_db_session = AsyncMock()
    mock_db_session.source_type = "snowflake"
    mock_db_session.execute_query.return_value = pd.DataFrame({
        SpecialColumnName.FORECAST_POINT: ["invalid-date", "2024/01/15"]
    })
    # Create a real adapter instance for proper SQL generation
    source_info = SourceInfo(
        database_name="test_db", schema_name="test_schema", source_type=SourceType.SNOWFLAKE
    )
    mock_db_session.adapter = SnowflakeAdapter(source_info)

    table_details = TableDetails(
        database_name="test_db",
        schema_name="test_schema",
        table_name="test_table",
    )

    with pytest.raises(InvalidForecastPointValueError) as exc:
        await observation_table_service._validate_forecast_point_format_string_values(
            mock_db_session, table_details, mock_context
        )
    assert "invalid-date" in str(exc.value)
    assert "YYYY-MM-DD" in str(exc.value)
    assert "test_forecast_context" in str(exc.value)


@pytest.mark.asyncio
async def test_validate_forecast_point_format_string__skipped_for_non_varchar(
    observation_table_service,
):
    """
    Test validation is skipped when dtype is not VARCHAR
    """
    from featurebyte.enum import TimeIntervalUnit
    from featurebyte.models.context import ContextModel
    from featurebyte.query_graph.model.forecast_point_schema import ForecastPointSchema
    from featurebyte.query_graph.node.schema import TableDetails

    # Create a mock context with DATE dtype (not VARCHAR)
    forecast_schema = ForecastPointSchema(
        granularity=TimeIntervalUnit.DAY,
        dtype=DBVarType.DATE,  # Not VARCHAR
        timezone="America/New_York",
    )
    mock_context = Mock(spec=ContextModel)
    mock_context.name = "test_forecast_context"
    mock_context.forecast_point_schema = forecast_schema

    mock_db_session = AsyncMock()
    mock_db_session.source_type = "snowflake"

    table_details = TableDetails(
        database_name="test_db",
        schema_name="test_schema",
        table_name="test_table",
    )

    # Should not raise any exception and should not call execute_query
    await observation_table_service._validate_forecast_point_format_string_values(
        mock_db_session, table_details, mock_context
    )
    mock_db_session.execute_query.assert_not_called()


@pytest.mark.asyncio
async def test_validate_forecast_point_format_string__skipped_without_format_string(
    observation_table_service,
):
    """
    Test validation is skipped when format_string is not specified
    """
    from featurebyte.models.context import ContextModel
    from featurebyte.query_graph.model.forecast_point_schema import ForecastPointSchema
    from featurebyte.query_graph.node.schema import TableDetails

    # Create a mock context with VARCHAR dtype but no format_string
    # Note: This shouldn't happen in practice as format_string is required for VARCHAR
    forecast_schema = Mock(spec=ForecastPointSchema)
    forecast_schema.dtype = DBVarType.VARCHAR
    forecast_schema.format_string = None  # No format string

    mock_context = Mock(spec=ContextModel)
    mock_context.name = "test_forecast_context"
    mock_context.forecast_point_schema = forecast_schema

    mock_db_session = AsyncMock()
    mock_db_session.source_type = "snowflake"

    table_details = TableDetails(
        database_name="test_db",
        schema_name="test_schema",
        table_name="test_table",
    )

    # Should not raise any exception and should not call execute_query
    await observation_table_service._validate_forecast_point_format_string_values(
        mock_db_session, table_details, mock_context
    )
    mock_db_session.execute_query.assert_not_called()


@pytest.mark.asyncio
async def test_validate_forecast_point_format_string__databricks(observation_table_service):
    """
    Test validation works with Databricks source type
    """
    from featurebyte.enum import TimeIntervalUnit
    from featurebyte.models.context import ContextModel
    from featurebyte.query_graph.model.forecast_point_schema import ForecastPointSchema
    from featurebyte.query_graph.node.schema import TableDetails
    from featurebyte.query_graph.sql.adapter import DatabricksAdapter
    from featurebyte.query_graph.sql.source_info import SourceInfo

    forecast_schema = ForecastPointSchema(
        granularity=TimeIntervalUnit.DAY,
        dtype=DBVarType.VARCHAR,
        format_string="yyyy-MM-dd",
        timezone="America/New_York",
    )
    mock_context = Mock(spec=ContextModel)
    mock_context.name = "test_forecast_context"
    mock_context.forecast_point_schema = forecast_schema

    mock_db_session = AsyncMock()
    mock_db_session.source_type = "databricks"
    mock_db_session.execute_query.return_value = pd.DataFrame()
    # Create a real adapter instance for proper SQL generation
    source_info = SourceInfo(
        database_name="test_db", schema_name="test_schema", source_type=SourceType.DATABRICKS
    )
    mock_db_session.adapter = DatabricksAdapter(source_info)

    table_details = TableDetails(
        database_name="test_db",
        schema_name="test_schema",
        table_name="test_table",
    )

    await observation_table_service._validate_forecast_point_format_string_values(
        mock_db_session, table_details, mock_context
    )
    mock_db_session.execute_query.assert_called_once()
    # Verify the query uses to_timestamp for Databricks
    query_arg = mock_db_session.execute_query.call_args[0][0]
    assert "to_timestamp" in query_arg.lower()


@pytest.mark.asyncio
async def test_validate_forecast_point_format_string__bigquery(observation_table_service):
    """
    Test validation works with BigQuery source type
    """
    from featurebyte.enum import TimeIntervalUnit
    from featurebyte.models.context import ContextModel
    from featurebyte.query_graph.model.forecast_point_schema import ForecastPointSchema
    from featurebyte.query_graph.node.schema import TableDetails
    from featurebyte.query_graph.sql.adapter import BigQueryAdapter
    from featurebyte.query_graph.sql.source_info import SourceInfo

    forecast_schema = ForecastPointSchema(
        granularity=TimeIntervalUnit.DAY,
        dtype=DBVarType.VARCHAR,
        format_string="%Y-%m-%d",
        timezone="America/New_York",
    )
    mock_context = Mock(spec=ContextModel)
    mock_context.name = "test_forecast_context"
    mock_context.forecast_point_schema = forecast_schema

    mock_db_session = AsyncMock()
    mock_db_session.source_type = "bigquery"
    mock_db_session.execute_query.return_value = pd.DataFrame()
    # Create a real adapter instance for proper SQL generation
    source_info = SourceInfo(
        database_name="test_db", schema_name="test_schema", source_type=SourceType.BIGQUERY
    )
    mock_db_session.adapter = BigQueryAdapter(source_info)

    table_details = TableDetails(
        database_name="test_db",
        schema_name="test_schema",
        table_name="test_table",
    )

    await observation_table_service._validate_forecast_point_format_string_values(
        mock_db_session, table_details, mock_context
    )
    mock_db_session.execute_query.assert_called_once()
    # Verify the query uses SAFE.PARSE_TIMESTAMP for BigQuery
    query_arg = mock_db_session.execute_query.call_args[0][0]
    assert "SAFE.PARSE_TIMESTAMP" in query_arg


@pytest.mark.asyncio
async def test_validate_forecast_point_format_string__spark(observation_table_service):
    """
    Test validation works with Spark source type (inherits from Databricks adapter)
    """
    from featurebyte.enum import TimeIntervalUnit
    from featurebyte.models.context import ContextModel
    from featurebyte.query_graph.model.forecast_point_schema import ForecastPointSchema
    from featurebyte.query_graph.node.schema import TableDetails
    from featurebyte.query_graph.sql.adapter import SparkAdapter
    from featurebyte.query_graph.sql.source_info import SourceInfo

    forecast_schema = ForecastPointSchema(
        granularity=TimeIntervalUnit.DAY,
        dtype=DBVarType.VARCHAR,
        format_string="yyyy-MM-dd",
        timezone="America/New_York",
    )
    mock_context = Mock(spec=ContextModel)
    mock_context.name = "test_forecast_context"
    mock_context.forecast_point_schema = forecast_schema

    mock_db_session = AsyncMock()
    mock_db_session.source_type = "spark"
    mock_db_session.execute_query.return_value = pd.DataFrame()
    # Create a real adapter instance for proper SQL generation
    source_info = SourceInfo(
        database_name="test_db", schema_name="test_schema", source_type=SourceType.SPARK
    )
    mock_db_session.adapter = SparkAdapter(source_info)

    table_details = TableDetails(
        database_name="test_db",
        schema_name="test_schema",
        table_name="test_table",
    )

    await observation_table_service._validate_forecast_point_format_string_values(
        mock_db_session, table_details, mock_context
    )
    mock_db_session.execute_query.assert_called_once()
    # Verify the query uses to_timestamp for Spark (inherited from Databricks)
    query_arg = mock_db_session.execute_query.call_args[0][0]
    assert "to_timestamp" in query_arg.lower()


# Tests for forecast horizon computation


def test_get_max_forecast_horizon_sql_expr__basic(
    observation_table_service, table_details, adapter
):
    """
    Test get_max_forecast_horizon_sql_expr generates correct SQL for basic case
    """
    expr = observation_table_service.get_max_forecast_horizon_sql_expr(table_details, adapter)
    expr_sql = expr.sql(pretty=True, dialect="snowflake")
    expected_query = textwrap.dedent(
        """
        SELECT
          MAX("HORIZON_SECS") AS "MAX_HORIZON"
        FROM (
          SELECT
            DATEDIFF(MICROSECOND, "POINT_IN_TIME", "FORECAST_POINT") / 1000000 AS "HORIZON_SECS"
          FROM "fb_database"."fb_schema"."fb_table"
        )
        """
    ).strip()
    assert expr_sql == expected_query


def test_get_max_forecast_horizon_sql_expr__with_varchar_format(
    observation_table_service, table_details, adapter
):
    """
    Test get_max_forecast_horizon_sql_expr handles VARCHAR type with format string
    """
    from featurebyte.enum import TimeIntervalUnit
    from featurebyte.query_graph.model.forecast_point_schema import ForecastPointSchema

    forecast_schema = ForecastPointSchema(
        granularity=TimeIntervalUnit.DAY,
        dtype=DBVarType.VARCHAR,
        format_string="YYYY-MM-DD HH24:MI:SS",
        is_utc_time=True,
        timezone="Etc/UTC",
    )
    expr = observation_table_service.get_max_forecast_horizon_sql_expr(
        table_details, adapter, forecast_schema
    )
    expr_sql = expr.sql(pretty=True, dialect="snowflake")

    expected_query = textwrap.dedent(
        """
        SELECT
          MAX("HORIZON_SECS") AS "MAX_HORIZON"
        FROM (
          SELECT
            DATEDIFF(
              MICROSECOND,
              "POINT_IN_TIME",
              TO_TIMESTAMP("FORECAST_POINT", 'YYYY-MM-DD HH24:MI:SS')
            ) / 1000000 AS "HORIZON_SECS"
          FROM "fb_database"."fb_schema"."fb_table"
        )
        """
    ).strip()
    assert expr_sql == expected_query


def test_get_max_forecast_horizon_sql_expr__with_timezone_conversion(
    observation_table_service, table_details, adapter
):
    """
    Test get_max_forecast_horizon_sql_expr handles timezone conversion for local time
    """
    from featurebyte.enum import TimeIntervalUnit
    from featurebyte.query_graph.model.forecast_point_schema import ForecastPointSchema

    forecast_schema = ForecastPointSchema(
        granularity=TimeIntervalUnit.DAY,
        dtype=DBVarType.DATE,
        is_utc_time=False,
        timezone="America/New_York",
    )
    expr = observation_table_service.get_max_forecast_horizon_sql_expr(
        table_details, adapter, forecast_schema
    )
    expr_sql = expr.sql(pretty=True, dialect="snowflake")

    expected_query = textwrap.dedent(
        """
        SELECT
          MAX("HORIZON_SECS") AS "MAX_HORIZON"
        FROM (
          SELECT
            DATEDIFF(
              MICROSECOND,
              "POINT_IN_TIME",
              CONVERT_TIMEZONE('America/New_York', 'UTC', "FORECAST_POINT")
            ) / 1000000 AS "HORIZON_SECS"
          FROM "fb_database"."fb_schema"."fb_table"
        )
        """
    ).strip()
    assert expr_sql == expected_query


@pytest.mark.parametrize(
    "seconds, granularity, expected",
    [
        (3600, "HOUR", 1),
        (7200, "HOUR", 2),
        (86400, "DAY", 1),
        (172800, "DAY", 2),
        (604800, "WEEK", 1),
        (1209600, "WEEK", 2),
        (2592000, "MONTH", 1),  # 30 days approximation
        (60, "MINUTE", 1),
        (120, "MINUTE", 2),
        (31536000, "YEAR", 1),  # 365 days approximation
        (7776000, "QUARTER", 1),  # 90 days approximation
        # Test rounding (Python uses banker's rounding - rounds to even)
        (5400, "HOUR", 2),  # 1.5 hours rounds to 2 (even)
        (43200, "DAY", 0),  # 0.5 days rounds to 0 (even)
        (129600, "DAY", 2),  # 1.5 days rounds to 2 (even)
    ],
)
def test_convert_seconds_to_granularity(observation_table_service, seconds, granularity, expected):
    """
    Test _convert_seconds_to_granularity converts correctly for different granularities
    """
    from featurebyte.enum import TimeIntervalUnit

    granularity_enum = TimeIntervalUnit(granularity)
    result = observation_table_service._convert_seconds_to_granularity(seconds, granularity_enum)
    assert result == expected


@pytest.mark.asyncio
async def test_validate_metadata__with_forecast_point_stats(
    observation_table_service,
    snowflake_feature_store,
    table_details,
    cust_id_entity,
    insert_credential,
    adapter,
):
    """
    Test validate_materialized_table_and_get_metadata includes forecast point stats when available
    """
    from featurebyte.enum import TimeIntervalUnit
    from featurebyte.models.context import ContextModel
    from featurebyte.query_graph.model.forecast_point_schema import ForecastPointSchema
    from featurebyte.query_graph.model.window import CalendarWindow

    _ = cust_id_entity, insert_credential

    # Create a mock context with forecast_point_schema
    forecast_schema = ForecastPointSchema(
        granularity=TimeIntervalUnit.DAY,
        dtype=DBVarType.DATE,
        timezone="Etc/UTC",
    )
    mock_context = Mock(spec=ContextModel)
    mock_context.id = ObjectId()
    mock_context.forecast_point_schema = forecast_schema

    async def mock_list_table_schema(*args, **kwargs):
        _ = args
        _ = kwargs
        from featurebyte.query_graph.model.column_info import ColumnSpecWithDescription

        return {
            "POINT_IN_TIME": ColumnSpecWithDescription(name="POINT_IN_TIME", dtype="TIMESTAMP"),
            "FORECAST_POINT": ColumnSpecWithDescription(name="FORECAST_POINT", dtype="DATE"),
            "cust_id": ColumnSpecWithDescription(name="cust_id", dtype="VARCHAR"),
        }

    async def execute_query_long_running(*args, **kwargs):
        query = args[0]
        _ = kwargs
        if "stats" in query:
            return pd.DataFrame(
                {
                    "dtype": ["timestamp", "date", "varchar"],
                    "unique": [5, 3, 2],
                    "%missing": [0, 0, 0],
                    "min": ["2023-01-01T10:00:00+00:00", "2023-01-05", 1],
                    "max": ["2023-01-15T10:00:00+00:00", "2023-01-20", 10],
                },
            )
        if "COUNT(*)" in query:
            return pd.DataFrame({"row_count": [1000]})
        if "INTERVAL" in query:
            return pd.DataFrame({"MIN_INTERVAL": [3600]})
        if "MAX_HORIZON" in query:
            # 7 days in seconds (forecast_horizon will be 7 + 1 = 8)
            return pd.DataFrame({"MAX_HORIZON": [604800]})
        if "MIN_FORECAST_POINT" in query:
            return pd.DataFrame({
                "MIN_FORECAST_POINT": ["2023-01-05"],
                "MAX_FORECAST_POINT": ["2023-01-20"],
            })
        raise NotImplementedError(f"Unexpected query: {query}")

    mock_db_session = Mock(
        name="mock_session",
        spec=BaseSession,
        list_table_schema=Mock(side_effect=mock_list_table_schema),
        execute_query_long_running=Mock(side_effect=execute_query_long_running),
        source_type=SourceType.SNOWFLAKE,
        schema_name="my_schema",
        database_name="my_db",
        adapter=adapter,
    )

    with (
        mock.patch(
            "featurebyte.service.preview.PreviewService._get_feature_store_session"
        ) as mock_get_feature_store_session,
        mock.patch.object(
            observation_table_service.context_service,
            "get_document",
            return_value=mock_context,
        ),
    ):
        mock_get_feature_store_session.return_value = (snowflake_feature_store, mock_db_session)
        metadata = await observation_table_service.validate_materialized_table_and_get_metadata(
            mock_db_session,
            table_details,
            snowflake_feature_store,
            context_id=mock_context.id,
        )

        # Verify forecast point stats are included
        assert "most_recent_forecast_point" in metadata
        assert "least_recent_forecast_point" in metadata
        assert "forecast_horizon" in metadata

        # Verify values
        # 7 days duration + 1 (since FORECAST_POINT indicates beginning of period) = 8 days
        expected_horizon = CalendarWindow(unit=TimeIntervalUnit.DAY, size=8)
        assert metadata["forecast_horizon"] == expected_horizon


@pytest.mark.asyncio
async def test_validate_metadata__without_forecast_context(
    observation_table_service,
    db_session,
    table_details,
    cust_id_entity,
    snowflake_feature_store,
    insert_credential,
):
    """
    Test validate_materialized_table_and_get_metadata does not include forecast stats without context
    """
    _ = cust_id_entity, insert_credential

    with mock.patch(
        "featurebyte.service.preview.PreviewService._get_feature_store_session"
    ) as mock_get_feature_store_session:
        mock_get_feature_store_session.return_value = (snowflake_feature_store, db_session)
        metadata = await observation_table_service.validate_materialized_table_and_get_metadata(
            db_session, table_details, snowflake_feature_store
        )

        # Verify forecast point stats are NOT included when no context
        assert "most_recent_forecast_point" not in metadata
        assert "least_recent_forecast_point" not in metadata
        assert "forecast_horizon" not in metadata


class TestSplitInfoValidation:
    """Tests for split_info validation in observation table service"""

    @pytest.fixture(name="source_observation_table")
    def source_observation_table_fixture(self, event_table, user):
        """Fixture for a source observation table to split"""
        request_input = SourceTableRequestInput(source=event_table.tabular_source)
        location = TabularSource(**{
            "feature_store_id": event_table.tabular_source.feature_store_id,
            "table_details": {
                "database_name": "fb_database",
                "schema_name": "fb_schema",
                "table_name": "fb_source_table",
            },
        })
        return ObservationTableModel(
            name="source_observation_table",
            location=location,
            request_input=request_input.model_dump(by_alias=True),
            columns_info=[
                {"name": "POINT_IN_TIME", "dtype": "TIMESTAMP"},
                {"name": "cust_id", "dtype": "VARCHAR"},
            ],
            num_rows=1000,
            most_recent_point_in_time="2023-01-15T10:00:00",
            user_id=user.id,
        )

    @pytest.mark.asyncio
    async def test_split_info_with_sample_rows_raises_error(
        self, observation_table_service, source_observation_table, catalog
    ):
        """Test that split_info cannot be used with sample_rows"""
        from featurebyte.models.observation_table import ObservationTableObservationInput
        from featurebyte.schema.observation_table import ObservationTableCreate

        # Create the source observation table first
        await observation_table_service.create_document(source_observation_table)

        # Create a request with split_info and sample_rows
        split_info = SplitInfo(split_index=0, split_ratios=[0.7, 0.3], seed=42)
        request_input = ObservationTableObservationInput(
            observation_table_id=source_observation_table.id,
            split_info=split_info,
        )
        create_payload = ObservationTableCreate(
            name="split_table",
            feature_store_id=source_observation_table.location.feature_store_id,
            request_input=request_input,
            sample_rows=100,  # This should conflict with split_info
        )

        with pytest.raises(ObservationTableInvalidSamplingError) as exc:
            await observation_table_service.get_observation_table_task_payload(create_payload)

        assert "Split cannot be used together with sample_rows" in str(exc.value)

    @pytest.mark.asyncio
    async def test_split_info_with_downsampling_info_raises_error(
        self, observation_table_service, source_observation_table, catalog
    ):
        """Test that split_info cannot be used with downsampling_info"""
        from featurebyte.models.observation_table import ObservationTableObservationInput
        from featurebyte.schema.observation_table import ObservationTableCreate

        # Create the source observation table first
        await observation_table_service.create_document(source_observation_table)

        # Create a request with both split_info and downsampling_info
        split_info = SplitInfo(split_index=0, split_ratios=[0.7, 0.3], seed=42)
        downsampling_info = DownSamplingInfo(
            sampling_rate_per_target_value=[
                TargetValueSamplingRate(target_value=1, rate=0.5),
            ],
            default_sampling_rate=1.0,
        )
        request_input = ObservationTableObservationInput(
            observation_table_id=source_observation_table.id,
            split_info=split_info,
            downsampling_info=downsampling_info,  # This should conflict with split_info
        )
        create_payload = ObservationTableCreate(
            name="split_table",
            feature_store_id=source_observation_table.location.feature_store_id,
            request_input=request_input,
        )

        with pytest.raises(ObservationTableInvalidSamplingError) as exc:
            await observation_table_service.get_observation_table_task_payload(create_payload)

        assert "Split cannot be used together with downsampling_info" in str(exc.value)

    @pytest.mark.asyncio
    async def test_split_info_valid_split(
        self, observation_table_service, source_observation_table, catalog
    ):
        """Test that split_info works when used alone"""
        from featurebyte.models.observation_table import ObservationTableObservationInput
        from featurebyte.schema.observation_table import ObservationTableCreate

        # Create the source observation table first
        await observation_table_service.create_document(source_observation_table)

        # Create a valid split request (no sample_rows or downsampling_info)
        split_info = SplitInfo(split_index=0, split_ratios=[0.7, 0.3], seed=42)
        request_input = ObservationTableObservationInput(
            observation_table_id=source_observation_table.id,
            split_info=split_info,
        )
        create_payload = ObservationTableCreate(
            name="split_table",
            feature_store_id=source_observation_table.location.feature_store_id,
            request_input=request_input,
        )

        # Should not raise an error
        result = await observation_table_service.get_observation_table_task_payload(create_payload)
        assert result is not None
        assert result.request_input.split_info == split_info
