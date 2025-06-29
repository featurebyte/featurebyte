"""
Unit test for base snowflake session.
"""

from __future__ import annotations

import asyncio
import collections
import time
from asyncio.exceptions import TimeoutError as AsyncIOTimeoutError
from typing import Any, OrderedDict
from unittest.mock import Mock, patch

import pandas as pd
import pytest
from snowflake.connector.errors import ProgrammingError

from featurebyte.enum import SourceType
from featurebyte.query_graph.model.column_info import ColumnSpecDetailed
from featurebyte.query_graph.model.table import TableSpec
from featurebyte.session.base import (
    INTERACTIVE_QUERY_TIMEOUT_SECONDS,
    INTERACTIVE_SESSION_TIMEOUT_SECONDS,
    BaseSchemaInitializer,
    BaseSession,
    MetadataSchemaInitializer,
    to_thread,
)

CURRENT_WORKING_SCHEMA_VERSION_TEST = 1


@pytest.fixture(name="base_session_test")
def base_session_test_fixture():
    """
    Base session test fixture.

    This fixture is a no-op implementation of the BaseSession.
    """

    class BaseSessionTestFixture(BaseSession):
        def initializer(self):
            return Mock(name="MockInitializer")

        source_type: SourceType = SourceType.TEST

        async def _cancel_query(self, cursor: Any, query: str) -> bool:
            return True

        @property
        def schema_name(self) -> str:
            return "test base session - schema"

        @property
        def database_name(self) -> str:
            return "test base session - database name"

        @classmethod
        def is_threadsafe(cls) -> bool:
            return True

        async def _list_databases(self) -> list[str]:
            return []

        async def _list_schemas(self, database_name: str | None = None) -> list[str]:
            return []

        async def _list_tables(
            self,
            database_name: str | None = None,
            schema_name: str | None = None,
            timeout: float = INTERACTIVE_QUERY_TIMEOUT_SECONDS,
        ) -> list[TableSpec]:
            return []

        async def list_table_schema(
            self,
            table_name: str | None,
            database_name: str | None = None,
            schema_name: str | None = None,
            timeout: float = INTERACTIVE_SESSION_TIMEOUT_SECONDS,
        ) -> OrderedDict[str, ColumnSpecDetailed]:
            _ = timeout
            return collections.OrderedDict()

        async def register_table(
            self, table_name: str, dataframe: pd.DataFrame, temporary: bool = True
        ) -> None:
            return None

        async def comment_table(self, table_name: str, comment: str) -> None:
            pass

        async def comment_column(self, table_name: str, column_name: str, comment: str) -> None:
            pass

    return BaseSessionTestFixture


@pytest.fixture(name="base_schema_initializer_test")
def base_schema_initializer_test_fixture():
    """
    Base schema initializer fixture.

    This fixture is a no-op implementation of the BaseSchemaInitializer.
    """

    class BaseSchemaInitializerTestFixture(BaseSchemaInitializer):
        """Snowflake schema initializer class"""

        @property
        def sql_directory_name(self) -> str:
            return "test"

        @property
        def current_working_schema_version(self) -> int:
            return CURRENT_WORKING_SCHEMA_VERSION_TEST

        async def create_schema(self) -> None:
            return

        async def drop_all_objects_in_working_schema(self) -> None:
            return

        async def drop_object(self, object_type: str, name: str) -> None:
            return

    return BaseSchemaInitializerTestFixture


@pytest.fixture(name="base_schema_initializer")
def base_schema_initializer_fixture(base_schema_initializer_test, base_session_test):
    base_session = base_session_test()
    return base_schema_initializer_test(base_session)


def test_get_current_working_schema_version(base_schema_initializer_test, base_session_test):
    base_session = base_session_test()
    base_schema_initializer = base_schema_initializer_test(base_session)
    assert (
        base_schema_initializer.current_working_schema_version
        == CURRENT_WORKING_SCHEMA_VERSION_TEST
    )


@pytest.mark.asyncio
async def test_get_working_schema_version__no_data(base_session_test):
    async def mocked_execute_query(self, query, to_log_error=True) -> pd.DataFrame | None:
        _ = self, query, to_log_error
        return None

    with patch.object(BaseSession, "execute_query", mocked_execute_query):
        metadata = await base_session_test().get_working_schema_metadata()
    assert metadata["version"] == MetadataSchemaInitializer.SCHEMA_NO_RESULTS_FOUND


@pytest.mark.asyncio
async def test_get_working_schema_version__with_version_set(base_session_test):
    schema_version = 2

    async def mocked_execute_query_with_version(
        self, query, to_log_error=True
    ) -> pd.DataFrame | None:
        _ = self, query, to_log_error
        return pd.DataFrame({
            "WORKING_SCHEMA_VERSION": [schema_version],
            "FEATURE_STORE_ID": "test_store_id",
        })

    with patch.object(BaseSession, "execute_query", mocked_execute_query_with_version):
        metadata = await base_session_test().get_working_schema_metadata()
    assert metadata["version"] == schema_version


@pytest.mark.asyncio
async def test_get_working_schema_version__schema_not_registered(base_session_test):
    async def mocked_execute_query_with_exception(self, query: str) -> pd.DataFrame | None:
        raise ProgrammingError

    with patch.object(BaseSession, "execute_query", mocked_execute_query_with_exception):
        metadata = await base_session_test().get_working_schema_metadata()
    assert metadata["version"] == MetadataSchemaInitializer.SCHEMA_NOT_REGISTERED


def get_mocked_working_schema_version(version: int):
    async def mocked_get_working_schema_version(self) -> dict[str, Any]:
        return {"version": version}

    return mocked_get_working_schema_version


@pytest.mark.asyncio
async def test_should_update_schema__users_version_gt_code(base_schema_initializer):
    # working schema version in users is greater than current working version in code
    # -> should not update
    mocked_get_working_schema_version = get_mocked_working_schema_version(
        CURRENT_WORKING_SCHEMA_VERSION_TEST + 1
    )

    with patch.object(
        BaseSession, "get_working_schema_metadata", mocked_get_working_schema_version
    ):
        should_update_schema = await base_schema_initializer.should_update_schema()
    assert not should_update_schema


@pytest.mark.asyncio
async def test_should_update_schema__users_version_equals_code(base_schema_initializer):
    # working schema version in users is equal to current working version in code
    # -> should not update
    mocked_get_working_schema_version = get_mocked_working_schema_version(
        CURRENT_WORKING_SCHEMA_VERSION_TEST
    )

    with patch.object(
        BaseSession, "get_working_schema_metadata", mocked_get_working_schema_version
    ):
        should_update_schema = await base_schema_initializer.should_update_schema()
    assert not should_update_schema


@pytest.mark.asyncio
async def test_should_update_schema__users_version_lt_code(base_schema_initializer):
    # working schema version in users is lesser than current working version in code
    # -> should update
    mocked_get_working_schema_version = get_mocked_working_schema_version(
        CURRENT_WORKING_SCHEMA_VERSION_TEST - 1
    )

    with patch.object(
        BaseSession, "get_working_schema_metadata", mocked_get_working_schema_version
    ):
        should_update_schema = await base_schema_initializer.should_update_schema()
    assert should_update_schema


@pytest.mark.asyncio
async def test_should_update_schema__not_registered(base_schema_initializer):
    # should update if schema is not registered
    mocked_get_working_schema_version = get_mocked_working_schema_version(
        MetadataSchemaInitializer.SCHEMA_NOT_REGISTERED
    )

    with patch.object(
        BaseSession, "get_working_schema_metadata", mocked_get_working_schema_version
    ):
        should_update_schema = await base_schema_initializer.should_update_schema()
    assert should_update_schema


@pytest.mark.asyncio
async def test_should_update_schema__no_results_found(base_schema_initializer):
    # should not update if no results found
    mocked_get_working_schema_version = get_mocked_working_schema_version(
        MetadataSchemaInitializer.SCHEMA_NO_RESULTS_FOUND
    )

    with patch.object(
        BaseSession, "get_working_schema_metadata", mocked_get_working_schema_version
    ):
        should_update_schema = await base_schema_initializer.should_update_schema()
    assert should_update_schema


@pytest.mark.asyncio
async def test_to_thread__blocking_function():
    """Check thread execution is interrupted by timeout"""

    def _blocking_sync_function(values):
        try:
            for i in range(10):
                time.sleep(0.1)
        except asyncio.exceptions.CancelledError:
            values[0] = 2

    values = [0]
    with pytest.raises(AsyncIOTimeoutError):
        await to_thread(_blocking_sync_function, 0.1, None, values)

    # expect exception to be raised inside thread on timeout
    time.sleep(0.5)
    assert values[0] == 2


@pytest.mark.asyncio
async def test_to_thread__exception():
    """Check thread execution is interrupted by exception"""

    def _blocking_sync_function(values):
        _ = values
        raise ValueError("This is an exception from the thread!")

    values = [0]
    with pytest.raises(Exception) as exc:
        await to_thread(_blocking_sync_function, 0.5, None, values)

    # expect exception to be raised inside thread
    assert "This is an exception from the thread!" in str(exc.value)


@pytest.mark.asyncio
@patch("featurebyte.session.base.BaseSession.drop_table")
async def test_drop_tables(mock_drop_table, base_session_test):
    """Test drop_tables method"""
    session = base_session_test()
    await session.drop_tables(
        table_names=["table1", "table2"], schema_name="schema", database_name="database"
    )
    expected_call_args_list = [
        (
            (),
            {
                "database_name": "database",
                "schema_name": "schema",
                "table_name": "table1",
                "if_exists": False,
                "timeout": 86400,
            },
        ),
        (
            (),
            {
                "database_name": "database",
                "schema_name": "schema",
                "table_name": "table2",
                "if_exists": False,
                "timeout": 86400,
            },
        ),
    ]
    assert mock_drop_table.call_args_list == expected_call_args_list

    # simulate error happening on the first table
    mock_drop_table.reset_mock()
    mock_drop_table.side_effect = [Exception("Error dropping table"), None]
    with pytest.raises(Exception) as exc:
        await session.drop_tables(
            table_names=["table1", "table2"], schema_name="schema", database_name="database"
        )

    assert "Errors occurred: Error dropping table" in str(exc.value)
    assert mock_drop_table.call_args_list == expected_call_args_list

    # simulate error happening on both tables
    mock_drop_table.reset_mock()
    mock_drop_table.side_effect = [
        Exception("Error dropping table1"),
        Exception("Error dropping table2"),
    ]
    with pytest.raises(Exception) as exc:
        await session.drop_tables(
            table_names=["table1", "table2"], schema_name="schema", database_name="database"
        )

    assert "Errors occurred: Error dropping table1; Error dropping table2" in str(exc.value)
    assert mock_drop_table.call_args_list == expected_call_args_list
