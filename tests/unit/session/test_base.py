"""
Unit test for base snowflake session.
"""
from typing import OrderedDict

import collections

import pandas as pd
import pytest
from pydantic import Field

from featurebyte.enum import DBVarType, SourceType
from featurebyte.session.base import BaseSchemaInitializer, BaseSession


@pytest.fixture(name="base_session_test")
def base_session_test_fixture():
    """
    Base session test fixture.
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
    """

    class BaseSchemaInitializerTestFixture(BaseSchemaInitializer):
        """Snowflake schema initializer class"""

        @property
        def sql_directory_name(self) -> str:
            return "test"

        async def create_schema(self) -> None:
            return

        async def list_functions(self) -> list[str]:
            return []

        async def list_procedures(self) -> list[str]:
            return []

    return BaseSchemaInitializerTestFixture


def test_get_current_working_schema_version(base_schema_initializer_test, base_session_test):
    base_session = base_session_test()
    base_schema_initializer = base_schema_initializer_test(base_session)
    current_version = 1
    # We have two separate tests here to assert and show developers that
    # (1) the new value is increasing
    assert base_schema_initializer.get_current_working_schema_version() >= current_version
    # (2) the current_version in the test is updated
    assert base_schema_initializer.get_current_working_schema_version() == current_version
