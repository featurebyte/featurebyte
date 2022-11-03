"""
Unit test for base snowflake session.
"""
from __future__ import annotations

from typing import OrderedDict

import collections
from unittest.mock import patch

import pandas as pd
import pytest
from pydantic import Field
from snowflake.connector.errors import ProgrammingError

from featurebyte.enum import DBVarType, SourceType
from featurebyte.session.base import BaseSchemaInitializer, BaseSession, MetadataSchemaInitializer

CURRENT_WORKING_SCHEMA_VERSION_TEST = 1


@pytest.fixture(name="base_session_test")
def base_session_test_fixture():
    """
    Base session test fixture.

    This fixture is a no-op implementation of the BaseSession.
    """

    class BaseSessionTestFixture(BaseSession):
        source_type: SourceType = Field(SourceType.TEST, const=True)

        @property
        def schema_name(self) -> str:
            return "test base session - schema"

        @property
        def database_name(self) -> str:
            return "test base session - database name"

        async def list_databases(self) -> list[str]:
            return []

        async def list_schemas(self, database_name: str | None = None) -> list[str]:
            return []

        async def list_tables(
            self, database_name: str | None = None, schema_name: str | None = None
        ) -> list[str]:
            return []

        async def list_table_schema(
            self,
            table_name: str | None,
            database_name: str | None = None,
            schema_name: str | None = None,
        ) -> OrderedDict[str, DBVarType]:
            return collections.OrderedDict()

        async def register_temp_table(self, table_name: str, dataframe: pd.DataFrame) -> None:
            return None

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

        async def list_functions(self) -> list[str]:
            return []

        async def list_procedures(self) -> list[str]:
            return []

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
async def test_get_working_schema_version__no_data(base_schema_initializer):
    async def mocked_execute_query(self, query: str) -> pd.DataFrame | None:
        return None

    with patch.object(BaseSession, "execute_query", mocked_execute_query):
        version = await base_schema_initializer.get_working_schema_version()
    assert version == MetadataSchemaInitializer.SCHEMA_NO_RESULTS_FOUND


@pytest.mark.asyncio
async def test_get_working_schema_version__with_version_set(base_schema_initializer):
    schema_version = 2

    async def mocked_execute_query_with_version(self, query: str) -> pd.DataFrame | None:
        return pd.DataFrame(
            {
                "WORKING_SCHEMA_VERSION": [schema_version],
            }
        )

    with patch.object(BaseSession, "execute_query", mocked_execute_query_with_version):
        version = await base_schema_initializer.get_working_schema_version()
    assert version == schema_version


@pytest.mark.asyncio
async def test_get_working_schema_version__schema_not_registered(base_schema_initializer):
    async def mocked_execute_query_with_exception(self, query: str) -> pd.DataFrame | None:
        raise ProgrammingError

    with patch.object(BaseSession, "execute_query", mocked_execute_query_with_exception):
        version = await base_schema_initializer.get_working_schema_version()
    assert version == MetadataSchemaInitializer.SCHEMA_NOT_REGISTERED


def get_mocked_working_schema_version(version: int):
    async def mocked_get_working_schema_version(self) -> int:
        return version

    return mocked_get_working_schema_version


@pytest.mark.asyncio
async def test_should_update_schema__users_version_gt_code(base_schema_initializer):
    # working schema version in users is greater than current working version in code
    # -> should not update
    mocked_get_working_schema_version = get_mocked_working_schema_version(
        CURRENT_WORKING_SCHEMA_VERSION_TEST + 1
    )

    with patch.object(
        BaseSchemaInitializer, "get_working_schema_version", mocked_get_working_schema_version
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
        BaseSchemaInitializer, "get_working_schema_version", mocked_get_working_schema_version
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
        BaseSchemaInitializer, "get_working_schema_version", mocked_get_working_schema_version
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
        BaseSchemaInitializer, "get_working_schema_version", mocked_get_working_schema_version
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
        BaseSchemaInitializer, "get_working_schema_version", mocked_get_working_schema_version
    ):
        should_update_schema = await base_schema_initializer.should_update_schema()
    assert should_update_schema
