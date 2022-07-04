"""
FeatureStore class
"""
from __future__ import annotations

from featurebyte.api.database_table import DatabaseTable
from featurebyte.config import Credentials
from featurebyte.core.generic import ExtendedFeatureStoreModel
from featurebyte.models.feature_store import FeatureStoreModel, TableDetails


class FeatureStore(ExtendedFeatureStoreModel):
    """
    FeatureStore class
    """

    def list_databases(self, credentials: Credentials | None = None) -> list[str]:
        """
        List databases accessible by the feature store

        Parameters
        ----------
        credentials: Credentials
            configuration contains data source settings & credentials

        Returns
        -------
        list databases
        """
        return self.get_session(credentials=credentials).list_databases()

    def list_schemas(
        self, database_name: str | None = None, credentials: Credentials | None = None
    ) -> list[str]:
        """
        List schemas in the database

        Parameters
        ----------
        database_name: str | None
            Database name
        credentials: Credentials
            configuration contains data source settings & credentials


        Returns
        -------
        list schemas
        """
        return self.get_session(credentials=credentials).list_schemas(database_name=database_name)

    def list_tables(
        self,
        database_name: str | None = None,
        schema_name: str | None = None,
        credentials: Credentials | None = None,
    ) -> list[str]:
        """
        List tables in the schema

        Parameters
        ----------
        database_name: str | None
            Database name
        schema_name: str | None
            Schema name
        credentials: Credentials
            configuration contains data source settings & credentials

        Returns
        -------
        list tables
        """
        return self.get_session(credentials=credentials).list_tables(
            database_name=database_name, schema_name=schema_name
        )

    def get_table(
        self,
        table_name: str,
        database_name: str | None = None,
        schema_name: str | None = None,
        credentials: Credentials | None = None,
    ) -> DatabaseTable:
        """
        Get table from the feature store

        Parameters
        ----------
        table_name: str
            Table name
        database_name: str | None
            Database name
        schema_name: str | None
            Schema name
        credentials: Credentials
            configuration contains data source settings & credentials

        Returns
        -------
        DatabaseTable
        """
        return DatabaseTable(
            tabular_source=(
                FeatureStoreModel(**self.dict()),
                TableDetails(
                    database_name=database_name,
                    schema_name=schema_name,
                    table_name=table_name,
                ),
            ),
            credentials=credentials,
        )
