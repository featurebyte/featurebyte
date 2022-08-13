"""
FeatureStore class
"""
from __future__ import annotations

from typing import TYPE_CHECKING, Any, List, Optional, TypeVar

from typeguard import typechecked

from featurebyte.api.api_object import ApiObject
from featurebyte.config import Credentials
from featurebyte.core.generic import ExtendedFeatureStoreModel
from featurebyte.models.feature_store import TableDetails
from featurebyte.schema.feature_store import FeatureStoreCreate

if TYPE_CHECKING:
    from featurebyte.api.database_table import DatabaseTable
else:
    DatabaseTable = TypeVar("DatabaseTable")


class FeatureStore(ExtendedFeatureStoreModel, ApiObject):
    """
    FeatureStore class
    """

    # class variables
    _route = "/feature_store"

    def _get_create_payload(self) -> dict[str, Any]:
        data = FeatureStoreCreate(**self.json_dict())
        return data.json_dict()

    @typechecked
    def list_databases(self, credentials: Optional[Credentials] = None) -> List[str]:
        """
        List databases accessible by the feature store

        Parameters
        ----------
        credentials: Optional[Credentials]
            configuration contains data source settings & credentials

        Returns
        -------
        list databases
        """
        return self.get_session(credentials=credentials).list_databases()

    @typechecked
    def list_schemas(
        self, database_name: Optional[str] = None, credentials: Optional[Credentials] = None
    ) -> List[str]:
        """
        List schemas in the database

        Parameters
        ----------
        database_name: Optional[str]
            Database name
        credentials: Optional[Credentials]
            configuration contains data source settings & credentials


        Returns
        -------
        list schemas
        """
        return self.get_session(credentials=credentials).list_schemas(database_name=database_name)

    @typechecked
    def list_tables(
        self,
        database_name: Optional[str] = None,
        schema_name: Optional[str] = None,
        credentials: Optional[Credentials] = None,
    ) -> List[str]:
        """
        List tables in the schema

        Parameters
        ----------
        database_name: Optional[str]
            Database name
        schema_name: Optional[str]
            Schema name
        credentials: Optional[Credentials]
            configuration contains data source settings & credentials

        Returns
        -------
        list tables
        """
        return self.get_session(credentials=credentials).list_tables(
            database_name=database_name, schema_name=schema_name
        )

    @typechecked
    def get_table(
        self,
        table_name: str,
        database_name: Optional[str] = None,
        schema_name: Optional[str] = None,
        credentials: Optional[Credentials] = None,
    ) -> DatabaseTable:
        """
        Get table from the feature store

        Parameters
        ----------
        table_name: str
            Table name
        database_name: Optional[str]
            Database name
        schema_name: Optional[str]
            Schema name
        credentials: Optional[Credentials]
            configuration contains data source settings & credentials

        Returns
        -------
        DatabaseTable
        """
        # pylint: disable=import-outside-toplevel
        from featurebyte.api.database_table import DatabaseTable

        return DatabaseTable(
            feature_store=self,
            tabular_source=(
                self.id,
                TableDetails(
                    database_name=database_name,
                    schema_name=schema_name,
                    table_name=table_name,
                ),
            ),
            credentials=credentials,
        )
